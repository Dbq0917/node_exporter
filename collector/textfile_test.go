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
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func newTestTextFileCollector(t *testing.T, dir string) (*prometheus.Registry, *textFileCollector) {
	t.Helper()
	c := &textFileCollector{
		paths:  []string{dir},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectorAdapter{c})
	return registry, c
}

func gatherFamilies(t *testing.T, registry *prometheus.Registry) map[string]*dto.MetricFamily {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather textfile metrics: %v", err)
	}
	result := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		result[family.GetName()] = family
	}
	return result
}

func scrapeErrorValue(t *testing.T, families map[string]*dto.MetricFamily) float64 {
	t.Helper()
	family, ok := families["node_textfile_scrape_error"]
	if !ok {
		t.Fatal("expected node_textfile_scrape_error metric to be exported")
	}
	return family.Metric[0].GetGauge().GetValue()
}

// TestTextfileCollectorAtomicReadDuringWrite verifies the acceptance
// scenario "trigger a reload while the file is being rewritten". The writer
// leaves the file in a deliberately torn state (truncated, only partially
// re-written) at scrape time; the collector must not fail the scrape or drop
// the metric because of that half-written file. Once the writer finishes,
// the next scrape reads the complete new content automatically.
func TestTextfileCollectorAtomicReadDuringWrite(t *testing.T) {
	dir := t.TempDir()
	metricsFile := filepath.Join(dir, "business.prom")
	if err := os.WriteFile(metricsFile, validTextfileData, 0o644); err != nil {
		t.Fatal(err)
	}

	registry, _ := newTestTextFileCollector(t, dir)

	// Gate the completion of the write on the collector having observed the
	// torn snapshot at least once, so the test does not depend on timing
	// races between the retry backoff and the writer.
	tornObserved := make(chan struct{})
	var closeObservedOnce sync.Once
	originalParser := parseTextfile
	parseTextfile = func(data []byte) (map[string]*dto.MetricFamily, error) {
		families, parseErr := originalParser(data)
		if parseErr != nil {
			closeObservedOnce.Do(func() { close(tornObserved) })
		}
		return families, parseErr
	}
	t.Cleanup(func() { parseTextfile = originalParser })

	// Baseline scrape proves the metric is exported before the rewrite.
	families := gatherFamilies(t, registry)
	if got := scrapeErrorValue(t, families); got != 0 {
		t.Fatalf("baseline scrape reported unexpected error %v", got)
	}
	if _, ok := families["business_metric"]; !ok {
		t.Fatal("baseline scrape must export business_metric")
	}

	// Begin a rewrite and deliberately stop in the middle: truncate the file
	// and write only the header lines plus an unfinished value line. This is
	// the torn state a non-atomic business writer would leave on disk.
	tornContent := "# HELP business_metric A metric written by the business.\n# TYPE business_metric gauge\nbusiness_metric "
	f, err := os.OpenFile(metricsFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(tornContent); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Start a scrape while the file is torn, then finish the write partway
	// through the collector's retry window. The in-flight scrape must succeed
	// by reading the completed file rather than failing on the half-written
	// snapshot.
	type gatherResult struct {
		families map[string]*dto.MetricFamily
		err      error
	}
	resultCh := make(chan gatherResult, 1)
	go func() {
		got, gatherErr := registry.Gather()
		familiesByName := make(map[string]*dto.MetricFamily, len(got))
		for _, family := range got {
			familiesByName[family.GetName()] = family
		}
		resultCh <- gatherResult{families: familiesByName, err: gatherErr}
	}()

	// Wait until the collector has actually tried to parse the torn
	// snapshot, then let the business writer finish the rewrite while the
	// collector is still inside its retry loop.
	select {
	case <-tornObserved:
	case <-time.After(2 * time.Second):
		t.Fatal("collector never observed the torn snapshot")
	}
	finishedContent := "# HELP business_metric A metric written by the business.\n# TYPE business_metric gauge\nbusiness_metric 9\n"
	if err := os.WriteFile(metricsFile, []byte(finishedContent), 0o644); err != nil {
		t.Fatal(err)
	}

	result := <-resultCh
	if result.err != nil {
		t.Fatalf("scrape during an in-progress rewrite must not fail: %v", result.err)
	}
	if got := scrapeErrorValue(t, result.families); got != 0 {
		t.Fatalf("scrape during an in-progress rewrite must not set node_textfile_scrape_error, got %v", got)
	}
	family, ok := result.families["business_metric"]
	if !ok {
		t.Fatal("business_metric must be exported from the completed write")
	}
	if got := family.Metric[0].GetGauge().GetValue(); got != 9 {
		t.Fatalf("expected the re-read value 9, got %v", got)
	}

	// A follow-up scrape keeps reading the stable file without any restart.
	families = gatherFamilies(t, registry)
	if got := scrapeErrorValue(t, families); got != 0 {
		t.Fatalf("follow-up scrape must not fail, got node_textfile_scrape_error=%v", got)
	}

	// A file that never presented stable, corrupt bytes must not be
	// quarantined: the torn state was a transient write, not bad content.
	entries, err := os.ReadDir(filepath.Join(dir, quarantineDirName))
	if err == nil && len(entries) != 0 {
		t.Fatalf("expected no quarantined samples for a transient torn write, got %v", entries)
	}
}

// TestTextfileCollectorBadFileIsolationAndRecovery verifies that one
// unparseable file only drops its own metrics, that the bad content is kept
// as a reviewable sample identifying the source file, and that the file is
// automatically picked up again on later scrapes once it is valid again.
func TestTextfileCollectorBadFileIsolationAndRecovery(t *testing.T) {
	dir := t.TempDir()
	goodFile := filepath.Join(dir, "good.prom")
	badFile := filepath.Join(dir, "bad.prom")
	if err := os.WriteFile(goodFile, []byte("# TYPE good_metric gauge\ngood_metric 2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(badFile, []byte("# TYPE bad_metric gauge\nbad_metric this-is-not-a-number\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	registry, _ := newTestTextFileCollector(t, dir)

	families := gatherFamilies(t, registry)
	if got := scrapeErrorValue(t, families); got != 1 {
		t.Fatalf("expected node_textfile_scrape_error 1 for the bad file, got %v", got)
	}
	if _, ok := families["good_metric"]; !ok {
		t.Fatal("a parse failure in one file must not drop metrics from the other files")
	}
	if _, ok := families["bad_metric"]; ok {
		t.Fatal("metrics from the unparseable file must not be exported")
	}

	// The bad file must be identifiable from the retained sample and record.
	samplePath, recordPath := findQuarantinedSample(t, dir, "bad.prom")
	sample, err := os.ReadFile(samplePath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(sample), "bad_metric") {
		t.Fatalf("quarantined sample does not contain the bad file content: %q", sample)
	}
	record, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatal(err)
	}
	recordText := string(record)
	if !strings.Contains(recordText, "source: "+badFile) {
		t.Fatalf("quarantine record must name the source file, got: %s", recordText)
	}
	if !strings.Contains(recordText, "error:") {
		t.Fatalf("quarantine record must explain the parse failure, got: %s", recordText)
	}

	// Repair the file: recovery must happen automatically without a restart.
	if err := os.WriteFile(badFile, []byte("# TYPE bad_metric gauge\nbad_metric 7\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	families = gatherFamilies(t, registry)
	if got := scrapeErrorValue(t, families); got != 0 {
		t.Fatalf("expected automatic recovery after the file was repaired, got scrape error %v", got)
	}
	if _, ok := families["bad_metric"]; !ok {
		t.Fatal("repaired file must be re-read automatically on the next scrape")
	}
	if _, ok := families["good_metric"]; !ok {
		t.Fatal("good file metrics must still be exported after recovery")
	}
}

// TestTextfileCollectorDeletingOneFileOnlyDropsItsMetrics covers the
// acceptance scenario of removing a single file while others remain.
func TestTextfileCollectorDeletingOneFileOnlyDropsItsMetrics(t *testing.T) {
	dir := t.TempDir()
	firstFile := filepath.Join(dir, "first.prom")
	secondFile := filepath.Join(dir, "second.prom")
	if err := os.WriteFile(firstFile, []byte("# TYPE first_metric gauge\nfirst_metric 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondFile, []byte("# TYPE second_metric gauge\nsecond_metric 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	registry, _ := newTestTextFileCollector(t, dir)
	families := gatherFamilies(t, registry)
	if _, ok := families["first_metric"]; !ok {
		t.Fatal("expected first_metric before deleting its file")
	}
	if _, ok := families["second_metric"]; !ok {
		t.Fatal("expected second_metric before deleting the other file")
	}

	if err := os.Remove(firstFile); err != nil {
		t.Fatal(err)
	}
	families = gatherFamilies(t, registry)
	if _, ok := families["first_metric"]; ok {
		t.Fatal("first_metric must disappear once its file is deleted")
	}
	if _, ok := families["second_metric"]; !ok {
		t.Fatal("deleting one file must not drop metrics from the remaining files")
	}
	if got := scrapeErrorValue(t, families); got != 0 {
		t.Fatalf("deleting a file is not a scrape error, got %v", got)
	}
}

// TestTextfileCollectorRecoversFromParserPanic ensures malformed input that
// panics inside the exposition parser is contained to one file and preserved
// for inspection instead of killing the whole scrape.
func TestTextfileCollectorRecoversFromParserPanic(t *testing.T) {
	dir := t.TempDir()
	goodFile := filepath.Join(dir, "good.prom")
	panicFile := filepath.Join(dir, "panic.prom")
	if err := os.WriteFile(goodFile, []byte("# TYPE good_metric gauge\ngood_metric 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	registry, _ := newTestTextFileCollector(t, dir)

	// The expfmt parser contains defensive panic paths for states that
	// should be unreachable. Simulate such a panic to verify containment.
	originalParser := parseTextfile
	parseTextfile = func(data []byte) (map[string]*dto.MetricFamily, error) {
		if bytes.Contains(data, []byte("panic_metric")) {
			panic("simulated expfmt state machine panic")
		}
		return originalParser(data)
	}
	t.Cleanup(func() { parseTextfile = originalParser })

	if err := os.WriteFile(panicFile, []byte("# TYPE panic_metric gauge\npanic_metric 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	families := gatherFamilies(t, registry)
	if got := scrapeErrorValue(t, families); got != 1 {
		t.Fatalf("expected scrape error 1 for the panicking file, got %v", got)
	}
	if _, ok := families["good_metric"]; !ok {
		t.Fatal("a parser panic must not drop metrics from other files")
	}
	if _, err := os.Stat(panicFile); err != nil {
		t.Fatalf("the source file must remain in place, stat error: %v", err)
	}
	if samplePath, _ := findQuarantinedSample(t, dir, "panic.prom"); samplePath == "" {
		t.Fatal("the panicking file content must be quarantined for inspection")
	}
}

func findQuarantinedSample(t *testing.T, dir, sourceName string) (string, string) {
	t.Helper()
	quarantineDir := filepath.Join(dir, quarantineDirName)
	entries, err := os.ReadDir(quarantineDir)
	if err != nil {
		t.Fatalf("failed to read quarantine directory: %v", err)
	}
	prefix := filepath.Base(sourceName) + "."
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) && strings.HasSuffix(entry.Name(), ".bad") {
			samplePath := filepath.Join(quarantineDir, entry.Name())
			return samplePath, samplePath + ".reason"
		}
	}
	t.Fatalf("no quarantined sample for %q found in %s", sourceName, quarantineDir)
	return "", ""
}

// TestTextfileCollectorQuarantineIsBounded verifies that repeated failures
// retain only the newest quarantineMaxSamplesPerFile samples so the audit
// trail cannot grow without limit.
func TestTextfileCollectorQuarantineIsBounded(t *testing.T) {
	dir := t.TempDir()
	badFile := filepath.Join(dir, "flaky.prom")

	registry, _ := newTestTextFileCollector(t, dir)
	for i := 0; i < quarantineMaxSamplesPerFile+3; i++ {
		content := fmt.Sprintf("# TYPE flaky_metric gauge\nflaky_metric not-a-number-%d\n", i)
		if err := os.WriteFile(badFile, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		families := gatherFamilies(t, registry)
		if got := scrapeErrorValue(t, families); got != 1 {
			t.Fatalf("scrape %d expected scrape error 1 for the bad file", i)
		}
		// Distinct timestamps so sample filenames never collide.
		time.Sleep(time.Millisecond)
	}

	quarantineDir := filepath.Join(dir, quarantineDirName)
	entries, err := os.ReadDir(quarantineDir)
	if err != nil {
		t.Fatal(err)
	}
	var badSamples, reasonRecords int
	for _, entry := range entries {
		switch {
		case strings.HasSuffix(entry.Name(), ".bad"):
			badSamples++
		case strings.HasSuffix(entry.Name(), ".bad.reason"):
			reasonRecords++
		}
	}
	if badSamples != quarantineMaxSamplesPerFile {
		t.Fatalf("expected %d retained samples, got %d", quarantineMaxSamplesPerFile, badSamples)
	}
	if reasonRecords != quarantineMaxSamplesPerFile {
		t.Fatalf("expected %d retained reason records, got %d", quarantineMaxSamplesPerFile, reasonRecords)
	}
}

// TestTextfileCollectorAtomicRenameSwap covers the recommended atomic push
// pattern (write a temp file, then rename). A rename during a scrape must
// be detected via the inode check and the scrape must still return complete
// metrics without an error.
func TestTextfileCollectorAtomicRenameSwap(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "atomic.prom")
	if err := os.WriteFile(target, []byte("# TYPE atomic_metric gauge\natomic_metric 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	registry, _ := newTestTextFileCollector(t, dir)
	families := gatherFamilies(t, registry)
	if got := scrapeErrorValue(t, families); got != 0 {
		t.Fatalf("baseline scrape reported unexpected error %v", got)
	}

	tmp := target + ".tmp"
	if err := os.WriteFile(tmp, []byte("# TYPE atomic_metric gauge\natomic_metric 2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, target); err != nil {
		t.Fatal(err)
	}

	families = gatherFamilies(t, registry)
	if got := scrapeErrorValue(t, families); got != 0 {
		t.Fatalf("scrape after an atomic rename must not fail, got %v", got)
	}
	family, ok := families["atomic_metric"]
	if !ok {
		t.Fatal("atomic_metric must be exported after the rename")
	}
	if got := family.Metric[0].GetGauge().GetValue(); got != 2 {
		t.Fatalf("expected the renamed file's value 2, got %v", got)
	}

	entries, err := os.ReadDir(filepath.Join(dir, quarantineDirName))
	if err == nil && len(entries) != 0 {
		t.Fatalf("an atomic rename swap must not quarantine any file, got %v", entries)
	}
}

// TestReadFileSnapshot exercises the snapshot stability check directly.
func TestReadFileSnapshot(t *testing.T) {
	t.Run("stable file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "stable.prom")
		content := append([]byte(nil), validTextfileData...)
		if err := os.WriteFile(path, content, 0o644); err != nil {
			t.Fatal(err)
		}

		data, _, changed, err := readFileSnapshot(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if changed {
			t.Fatal("a quiescent file must produce an unchanged snapshot")
		}
		if !bytes.Equal(data, content) {
			t.Fatalf("snapshot content mismatch: got %q want %q", data, content)
		}
	})

	t.Run("torn file missing trailing newline", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "torn.prom")
		// An unfinished last line has no terminating newline.
		torn := []byte("# TYPE torn_metric gauge\ntorn_metric ")
		if err := os.WriteFile(path, torn, 0o644); err != nil {
			t.Fatal(err)
		}

		_, _, changed, err := readFileSnapshot(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !changed {
			t.Fatal("a snapshot without a trailing newline must be reported as changed")
		}
	})

	t.Run("modified content is detected", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "modified.prom")
		if err := os.WriteFile(path, []byte("# TYPE modified_metric gauge\nmodified_metric 1\n"), 0o644); err != nil {
			t.Fatal(err)
		}

		// Rewrite the file concurrently. The snapshot must either report the
		// change (the two reads straddled the rewrite) or, once the rewrite
		// settles, return a stable view of the new content.
		rewritten := make(chan struct{})
		go func() {
			time.Sleep(2 * time.Millisecond)
			_ = os.WriteFile(path, []byte("# TYPE modified_metric gauge\nmodified_metric 2\n"), 0o644)
			close(rewritten)
		}()

		var sawChanged bool
		for range 100 {
			_, _, changed, err := readFileSnapshot(path)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if changed {
				sawChanged = true
				break
			}
		}
		<-rewritten

		if !sawChanged {
			// The rewrite may have fully settled between reads; a final
			// stable snapshot must then reflect the new content.
			data, _, changed, err := readFileSnapshot(path)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if changed || !strings.Contains(string(data), "modified_metric 2") {
				t.Fatalf("expected a stable snapshot of the new content, got changed=%v data=%q", changed, data)
			}
		}
	})
}
