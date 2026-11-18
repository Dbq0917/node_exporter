// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !notextfile

package collector

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/common/promslog/flag"
)

type collectorAdapter struct {
	Collector
}

// Describe implements the prometheus.Collector interface.
func (a collectorAdapter) Describe(ch chan<- *prometheus.Desc) {
	// We have to send *some* metric in Describe, but we don't know which ones
	// we're going to get, so just send a dummy metric.
	ch <- prometheus.NewDesc("dummy_metric", "Dummy metric.", nil, nil)
}

// Collect implements the prometheus.Collector interface.
func (a collectorAdapter) Collect(ch chan<- prometheus.Metric) {
	if err := a.Update(ch); err != nil {
		panic(fmt.Sprintf("failed to update collector: %v", err))
	}
}

func TestTextfileCollector(t *testing.T) {
	tests := []struct {
		paths []string
		out   string
	}{
		{
			paths: []string{"fixtures/textfile/no_metric_files"},
			out:   "fixtures/textfile/no_metric_files.out",
		},
		{
			paths: []string{"fixtures/textfile/two_metric_files"},
			out:   "fixtures/textfile/two_metric_files.out",
		},
		{
			paths: []string{"fixtures/textfile/nonexistent_path"},
			out:   "fixtures/textfile/nonexistent_path.out",
		},
		{
			paths: []string{"fixtures/textfile/client_side_timestamp"},
			out:   "fixtures/textfile/client_side_timestamp.out",
		},
		{
			paths: []string{"fixtures/textfile/different_metric_types"},
			out:   "fixtures/textfile/different_metric_types.out",
		},
		{
			paths: []string{"fixtures/textfile/inconsistent_metrics"},
			out:   "fixtures/textfile/inconsistent_metrics.out",
		},
		{
			paths: []string{"fixtures/textfile/histogram"},
			out:   "fixtures/textfile/histogram.out",
		},
		{
			paths: []string{"fixtures/textfile/histogram_extra_dimension"},
			out:   "fixtures/textfile/histogram_extra_dimension.out",
		},
		{
			paths: []string{"fixtures/textfile/summary"},
			out:   "fixtures/textfile/summary.out",
		},
		{
			paths: []string{"fixtures/textfile/summary_extra_dimension"},
			out:   "fixtures/textfile/summary_extra_dimension.out",
		},
		{
			paths: []string{
				"fixtures/textfile/histogram_extra_dimension",
				"fixtures/textfile/summary_extra_dimension",
			},
			out: "fixtures/textfile/glob_extra_dimension.out",
		},
		{
			paths: []string{"fixtures/textfile/*_extra_dimension"},
			out:   "fixtures/textfile/glob_extra_dimension.out",
		},
		{
			paths: []string{"fixtures/textfile/metrics_merge_empty_help"},
			out:   "fixtures/textfile/metrics_merge_empty_help.out",
		},
		{
			paths: []string{"fixtures/textfile/metrics_merge_no_help"},
			out:   "fixtures/textfile/metrics_merge_no_help.out",
		},
		{
			paths: []string{"fixtures/textfile/metrics_merge_same_help"},
			out:   "fixtures/textfile/metrics_merge_same_help.out",
		},
		{
			paths: []string{"fixtures/textfile/metrics_merge_different_help"},
			out:   "fixtures/textfile/metrics_merge_different_help.out",
		},
	}

	for i, test := range tests {
		mtime := 1.0
		c := &textFileCollector{
			paths:  test.paths,
			mtime:  &mtime,
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}

		// Suppress a log message about `nonexistent_path` not existing, this is
		// expected and clutters the test output.
		promslogConfig := &promslog.Config{}
		flag.AddFlags(kingpin.CommandLine, promslogConfig)
		if _, err := kingpin.CommandLine.Parse([]string{"--log.level", "debug"}); err != nil {
			t.Fatal(err)
		}

		registry := prometheus.NewRegistry()
		registry.MustRegister(collectorAdapter{c})

		rw := httptest.NewRecorder()
		promhttp.HandlerFor(registry, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError}).ServeHTTP(rw, &http.Request{})
		got := string(rw.Body.String())

		want, err := os.ReadFile(test.out)
		if err != nil {
			t.Fatalf("%d. error reading fixture file %s: %s", i, test.out, err)
		}

		if string(want) != got {
			t.Fatalf("%d.%q want:\n\n%s\n\ngot:\n\n%s", i, test.paths, string(want), got)
		}
	}
}

func TestTextfileCollectorSupportsUTF8Names(t *testing.T) {
	c := &textFileCollector{
		paths:  []string{"fixtures/textfile/utf8_names"},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectorAdapter{c})

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather textfile with UTF-8 names: %v", err)
	}

	for _, family := range families {
		if family.GetName() != "my.dotted.metric" {
			continue
		}
		if got := family.Metric[0].Label[0].GetName(); got != "error.message" {
			t.Fatalf("expected dotted label name, got %q", got)
		}
		return
	}
	t.Fatalf("expected dotted metric name, got %v", families)
}

// TestTextfileCollectorStaleness exercises mtime-dependent staleness
// handling. Checked-in fixtures cannot pin a file's mtime relative to the
// current time, so this test copies fixture files into a temporary
// directory and controls their mtimes with os.Chtimes.
func TestTextfileCollectorStaleness(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()

	copyFixture := func(name string, mtime time.Time) {
		t.Helper()
		data, err := os.ReadFile(filepath.Join("fixtures/textfile/two_metric_files", name))
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	copyFixture("metrics1.prom", now.Add(-time.Hour)) // stale
	copyFixture("metrics2.prom", now)                 // fresh

	c := &textFileCollector{
		paths:      []string{dir},
		staleAfter: 5 * time.Minute,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:        func() time.Time { return now },
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectorAdapter{c})

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	got := map[string]*dto.MetricFamily{}
	for _, family := range families {
		got[family.GetName()] = family
	}

	// Metrics from the stale file must not be exported.
	for _, name := range []string{"testmetric1_1", "testmetric1_2"} {
		if _, ok := got[name]; ok {
			t.Errorf("expected metric %q from the stale file to be skipped, got %v", name, got[name])
		}
	}
	// Metrics from the fresh file must still be exported.
	for _, name := range []string{"testmetric2_1", "testmetric2_2"} {
		if _, ok := got[name]; !ok {
			t.Errorf("expected metric %q from the fresh file to be exported", name)
		}
	}

	stale := map[string]float64{}
	for _, m := range got["node_textfile_file_stale"].GetMetric() {
		stale[filepath.Base(m.GetLabel()[0].GetValue())] = m.GetGauge().GetValue()
	}
	if stale["metrics1.prom"] != 1 {
		t.Errorf("expected metrics1.prom to be marked stale, got %v", stale["metrics1.prom"])
	}
	if stale["metrics2.prom"] != 0 {
		t.Errorf("expected metrics2.prom to be marked fresh, got %v", stale["metrics2.prom"])
	}

	// mtimes are exported for both files so operators can tell how old the
	// stale data is.
	if len(got["node_textfile_mtime_seconds"].GetMetric()) != 2 {
		t.Errorf("expected mtimes for both files, got %v", got["node_textfile_mtime_seconds"])
	}
}

// TestTextfileCollectorStalenessDisabled verifies that the default
// configuration does not change the exported metric set.
func TestTextfileCollectorStalenessDisabled(t *testing.T) {
	mtime := 1.0
	c := &textFileCollector{
		paths:  []string{"fixtures/textfile/two_metric_files"},
		mtime:  &mtime,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectorAdapter{c})

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() == "node_textfile_file_stale" {
			t.Fatal("node_textfile_file_stale must not be exported when staleness handling is disabled")
		}
	}
}
