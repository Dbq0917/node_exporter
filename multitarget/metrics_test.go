// Copyright 2026 The Prometheus Authors
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

package multitarget

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

// TestCollectorRecordsFailedTargetAndKeepsOthers verifies the acceptance
// criteria end to end through the real prometheus registry:
//  1. a target that does not answer is recorded in the error metrics,
//  2. every healthy target still exposes its node_* metrics,
//  3. the failure ratio reflects exactly one failed target.
func TestCollectorRecordsFailedTargetAndKeepsOthers(t *testing.T) {
	const healthyCount = 9

	targets := make([]Target, 0, healthyCount+1)
	servers := make([]*httptest.Server, 0, healthyCount)
	for i := range healthyCount {
		name := hostName(i)
		srv := httptest.NewServer(healthyHandler(name))
		servers = append(servers, srv)
		targets = append(targets, Target{Name: name, URL: srv.URL + "/metrics"})
	}

	hung := httptest.NewServer(blackholeHandler())
	recovered := httptest.NewServer(healthyHandler("host-hung"))
	targets = append(targets, Target{Name: "host-hung", URL: hung.URL + "/metrics"})
	defer func() {
		for _, srv := range servers {
			srv.Close()
		}
	}()
	defer hung.Close()
	defer recovered.Close()

	scraper := newTestScraper(t, targets, 200*time.Millisecond, 10*time.Second, 8)
	registry := prometheus.NewRegistry()
	registry.MustRegister(NewCollector(scraper))

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	// 1. Healthy target metrics survive, each carrying the target label.
	nodeUpByTarget := map[string]float64{}
	selfFailure := map[string]float64{}
	var upByTarget map[string]float64
	var failureRatio float64
	var failureRatioSeen bool

	for _, family := range families {
		switch family.GetName() {
		case "node_up":
			for _, metric := range family.GetMetric() {
				nodeUpByTarget[targetValue(metric)] = metric.GetGauge().GetValue()
			}
		case "node_multitarget_target_up":
			upByTarget = map[string]float64{}
			for _, metric := range family.GetMetric() {
				upByTarget[targetValue(metric)] = metric.GetGauge().GetValue()
			}
		case "node_multitarget_target_scrape_failures_total":
			for _, metric := range family.GetMetric() {
				if reasonValue(metric) == ReasonTimeout {
					selfFailure[targetValue(metric)] = metric.GetCounter().GetValue()
				}
			}
		case "node_multitarget_target_failure_ratio":
			failureRatio = family.GetMetric()[0].GetGauge().GetValue()
			failureRatioSeen = true
		}
	}

	if len(nodeUpByTarget) != healthyCount {
		t.Fatalf("expected node_up from %d healthy targets, got %d", healthyCount, len(nodeUpByTarget))
	}
	for i := range healthyCount {
		name := hostName(i)
		if nodeUpByTarget[name] != 1 {
			t.Fatalf("healthy target %q missing node_up=1, got %v", name, nodeUpByTarget)
		}
	}
	if _, present := nodeUpByTarget["host-hung"]; present {
		t.Fatal("hung target must not contribute node metrics")
	}

	// 2. The failed target is recorded: up=0 and failure counter incremented.
	if got := upByTarget["host-hung"]; got != 0 {
		t.Fatalf("expected host-hung up=0, got %v", got)
	}
	if got := selfFailure["host-hung"]; got != 1 {
		t.Fatalf("expected host-hung timeout failure counter=1, got %v", got)
	}
	for i := range healthyCount {
		name := hostName(i)
		if got := upByTarget[name]; got != 1 {
			t.Fatalf("expected %s up=1, got %v", name, got)
		}
		if got := selfFailure[name]; got != 0 {
			t.Fatalf("healthy target %s must have no timeout failures, got %v", name, got)
		}
	}

	// 3. Failure ratio is exactly 1 of 10 targets.
	if !failureRatioSeen {
		t.Fatal("node_multitarget_target_failure_ratio not emitted")
	}
	if failureRatio != 1.0/float64(healthyCount+1) {
		t.Fatalf("expected failure ratio %v, got %v", 1.0/float64(healthyCount+1), failureRatio)
	}

	// A second round after the target recovers must keep the historical
	// failure counter (counters are persistent) while up recovers and the
	// failure ratio drops to 0.
	targets[healthyCount].URL = recovered.URL + "/metrics"
	scraper.config.Targets = targets

	families, err = registry.Gather()
	if err != nil {
		t.Fatalf("second gather: %v", err)
	}
	for _, family := range families {
		switch family.GetName() {
		case "node_multitarget_target_scrape_failures_total":
			for _, metric := range family.GetMetric() {
				if targetValue(metric) == "host-hung" && reasonValue(metric) == ReasonTimeout {
					if got := metric.GetCounter().GetValue(); got != 1 {
						t.Fatalf("historical timeout failure must remain 1 after recovery, got %v", got)
					}
				}
			}
		case "node_multitarget_target_up":
			for _, metric := range family.GetMetric() {
				if targetValue(metric) == "host-hung" && metric.GetGauge().GetValue() != 1 {
					t.Fatal("host-hung must report up=1 after recovery")
				}
			}
		case "node_multitarget_target_failure_ratio":
			if got := family.GetMetric()[0].GetGauge().GetValue(); got != 0 {
				t.Fatalf("expected failure ratio 0 in an all-healthy round, got %v", got)
			}
		}
	}
}

// TestCollectorEmitsExpectedMetricTexts locks the metric names with a golden
// text block, guarding against accidental renames.
func TestCollectorEmitsExpectedMetricTexts(t *testing.T) {
	srv := httptest.NewServer(healthyHandler("only-host"))
	defer srv.Close()

	scraper := newTestScraper(t, []Target{{Name: "only-host", URL: srv.URL + "/metrics"}}, time.Second, 5*time.Second, 1)
	registry := prometheus.NewRegistry()
	registry.MustRegister(NewCollector(scraper))

	expected := `
# HELP node_multitarget_target_failure_ratio Fraction of targets that failed in the most recent collection round (0 to 1).
# TYPE node_multitarget_target_failure_ratio gauge
node_multitarget_target_failure_ratio 0
# HELP node_multitarget_target_up 1 if the most recent scrape of the target succeeded, 0 otherwise.
# TYPE node_multitarget_target_up gauge
node_multitarget_target_up{target="only-host"} 1
`
	if err := testutil.GatherAndCompare(
		registry,
		strings.NewReader(expected),
		"node_multitarget_target_failure_ratio",
		"node_multitarget_target_up",
	); err != nil {
		t.Fatalf("unexpected metric output: %v", err)
	}
}

func hostName(i int) string {
	return "host-" + string(rune('a'+i))
}

func targetValue(metric *dto.Metric) string {
	for _, label := range metric.GetLabel() {
		if label.GetName() == TargetLabel {
			return label.GetValue()
		}
	}
	return ""
}

func reasonValue(metric *dto.Metric) string {
	for _, label := range metric.GetLabel() {
		if label.GetName() == reasonLabel {
			return label.GetValue()
		}
	}
	return ""
}
