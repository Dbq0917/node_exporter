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

package collector

import (
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// cpuFreqContract returns the cpu collector's contract for
// node_cpu_frequency_hertz: frequency from /proc/cpuinfo, labeled by
// package, core and cpu.
func cpuFreqContract() MetricContract {
	return MetricContract{
		Collector: "cpu",
		Metric:    "node_cpu_frequency_hertz",
		Help:      "CPU frequency in hertz from /proc/cpuinfo.",
		Labels: []LabelSemantic{
			{Name: "package", Meaning: "CPU package (socket) ID from /proc/cpuinfo"},
			{Name: "core", Meaning: "CPU core ID within the package from /proc/cpuinfo"},
			{Name: "cpu", Meaning: "Logical CPU number from /proc/cpuinfo"},
		},
	}
}

// cpufreqFreqContract returns the cpufreq collector's contract for
// node_cpu_frequency_hertz: same metric name, different source and labels.
func cpufreqFreqContract() MetricContract {
	return MetricContract{
		Collector: "cpufreq",
		Metric:    "node_cpu_frequency_hertz",
		Help:      "Current CPU thread frequency in hertz.",
		Labels: []LabelSemantic{
			{Name: "cpu", Meaning: "Logical CPU number from /sys/devices/system/cpu/cpuN/cpufreq"},
		},
	}
}

func bothCPUCollectors() map[string]bool {
	return map[string]bool{"cpu": true, "cpufreq": true}
}

func TestLabelContractConflictDetected(t *testing.T) {
	registry := newContractRegistry()
	cpu, cpufreq := cpuFreqContract(), cpufreqFreqContract()
	registry.register(cpu, cpufreq)

	err := registry.validate(bothCPUCollectors())
	var conflict *LabelConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("expected a LabelConflictError, got: %v", err)
	}
	if conflict.Metric != "node_cpu_frequency_hertz" {
		t.Fatalf("unexpected conflicting metric: %s", conflict.Metric)
	}
	for _, collector := range []string{"cpu", "cpufreq"} {
		if !strings.Contains(err.Error(), collector) {
			t.Fatalf("conflict error must name collector %q: %v", collector, err)
		}
	}

	// The two declarations must remain distinguishable: registering a
	// conflict must not mix the label semantics of the two sources.
	if contractsEquivalent(cpu, cpufreq) {
		t.Fatal("cpu and cpufreq contracts must not be considered equivalent")
	}
	if got := cpu.Labels[0].Name; got != "package" {
		t.Fatalf("cpu contract labels were modified: %v", cpu.Labels)
	}
	if got := cpufreq.Labels[0].Meaning; !strings.Contains(got, "cpufreq") {
		t.Fatalf("cpufreq contract labels were modified: %v", cpufreq.Labels)
	}
}

func TestLabelContractConflictOnlyAmongEnabledCollectors(t *testing.T) {
	registry := newContractRegistry()
	registry.register(cpuFreqContract(), cpufreqFreqContract())

	if err := registry.validate(map[string]bool{"cpu": true}); err != nil {
		t.Fatalf("conflict with a disabled collector must be ignored, got: %v", err)
	}
}

func TestLabelContractIdenticalContractsDoNotConflict(t *testing.T) {
	registry := newContractRegistry()
	shared := MetricContract{
		Collector: "a",
		Metric:    "node_shared_metric",
		Help:      "A metric intentionally exported by two collectors.",
		Labels:    []LabelSemantic{{Name: "device", Meaning: "Block device name"}},
	}
	same := shared
	same.Collector = "b"
	// Label order must not matter for equivalence.
	shared.Labels = append(shared.Labels, LabelSemantic{Name: "extra", Meaning: "e"})
	same.Labels = append([]LabelSemantic{{Name: "extra", Meaning: "e"}}, same.Labels...)
	registry.register(shared, same)

	if err := registry.validate(map[string]bool{"a": true, "b": true}); err != nil {
		t.Fatalf("identical contracts must merge cleanly, got: %v", err)
	}
}

func TestLabelContractPrefixResolution(t *testing.T) {
	registry := newContractRegistry()
	cpufreq := cpufreqFreqContract()
	cpufreq.Prefix = "cpufreq"
	cpufreq.PrefixActive = func() bool { return true }
	registry.register(cpuFreqContract(), cpufreq)

	if got := cpufreq.resolvedName(); got != "node_cpufreq_frequency_hertz" {
		t.Fatalf("unexpected resolved name: %s", got)
	}
	if err := registry.validate(bothCPUCollectors()); err != nil {
		t.Fatalf("active prefix must resolve the conflict, got: %v", err)
	}

	// An inactive prefix does not resolve anything.
	cpufreq.PrefixActive = func() bool { return false }
	registry = newContractRegistry()
	registry.register(cpuFreqContract(), cpufreq)
	if err := registry.validate(bothCPUCollectors()); err == nil {
		t.Fatal("inactive prefix must not resolve the conflict")
	}
}

func TestLabelContractSuppression(t *testing.T) {
	registry := newContractRegistry()
	cpu := cpuFreqContract()
	cpu.Suppressed = func() bool { return true }
	registry.register(cpu, cpufreqFreqContract())

	if err := registry.validate(bothCPUCollectors()); err != nil {
		t.Fatalf("suppressed metric must not conflict, got: %v", err)
	}
}

// staticMetricCollector exports a fixed set of metrics.
type staticMetricCollector struct {
	metrics []prometheus.Metric
}

func (c staticMetricCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range c.metrics {
		ch <- m.Desc()
	}
}

func (c staticMetricCollector) Collect(ch chan<- prometheus.Metric) {
	for _, m := range c.metrics {
		ch <- m
	}
}

// TestLabelContractExportComparison registers two same-name metrics with
// different label semantics, resolves the collision by prefix, and scrapes
// twice: the exported output must match the expectation both times and the
// two series must not merge or overwrite each other.
func TestLabelContractExportComparison(t *testing.T) {
	registry := newContractRegistry()
	cpu := cpuFreqContract()
	cpufreq := cpufreqFreqContract()
	cpufreq.Prefix = "cpufreq"
	cpufreq.PrefixActive = func() bool { return true }
	registry.register(cpu, cpufreq)

	if err := registry.validate(bothCPUCollectors()); err != nil {
		t.Fatalf("contracts must be distinguishable after prefix resolution: %v", err)
	}

	cpuDesc := prometheus.NewDesc(cpu.resolvedName(), cpu.Help, []string{"package", "core", "cpu"}, nil)
	cpufreqDesc := prometheus.NewDesc(cpufreq.resolvedName(), cpufreq.Help, []string{"cpu"}, nil)

	reg := prometheus.NewRegistry()
	reg.MustRegister(staticMetricCollector{metrics: []prometheus.Metric{
		prometheus.MustNewConstMetric(cpuDesc, prometheus.GaugeValue, 2.4e9, "0", "0", "0"),
		prometheus.MustNewConstMetric(cpufreqDesc, prometheus.GaugeValue, 3.2e9, "0"),
	}})

	expected := `# HELP node_cpu_frequency_hertz CPU frequency in hertz from /proc/cpuinfo.
# TYPE node_cpu_frequency_hertz gauge
node_cpu_frequency_hertz{core="0",cpu="0",package="0"} 2.4e+09
# HELP node_cpufreq_frequency_hertz Current CPU thread frequency in hertz.
# TYPE node_cpufreq_frequency_hertz gauge
node_cpufreq_frequency_hertz{cpu="0"} 3.2e+09
`
	metricNames := []string{"node_cpu_frequency_hertz", "node_cpufreq_frequency_hertz"}
	for scrape := 1; scrape <= 2; scrape++ {
		if err := testutil.GatherAndCompare(reg, strings.NewReader(expected), metricNames...); err != nil {
			t.Fatalf("scrape %d does not match expectation: %v", scrape, err)
		}
	}
}

type labelContractNoopCollector struct{}

func (labelContractNoopCollector) Update(ch chan<- prometheus.Metric) error { return nil }

// TestNewNodeCollectorValidatesLabelContracts ensures the startup path
// (NewNodeCollector) fails fast when two enabled collectors declare the same
// metric name with different label semantics.
func TestNewNodeCollectorValidatesLabelContracts(t *testing.T) {
	old := labelContracts
	labelContracts = newContractRegistry()
	defer func() { labelContracts = old }()

	names := []string{"labelcontracttest_a", "labelcontracttest_b"}
	for _, name := range names {
		registerCollector(name, defaultDisabled, func(logger *slog.Logger) (Collector, error) {
			return labelContractNoopCollector{}, nil
		})
		*collectorState[name] = true
	}
	defer func() {
		for _, name := range names {
			delete(collectorState, name)
			delete(factories, name)
			delete(initiatedCollectors, name)
		}
	}()

	conflicting := func(collector, meaning string) MetricContract {
		return MetricContract{
			Collector: collector,
			Metric:    "node_labelcontracttest_metric",
			Help:      "Test metric.",
			Labels:    []LabelSemantic{{Name: "device", Meaning: meaning}},
		}
	}
	labelContracts.register(
		conflicting("labelcontracttest_a", "Block device name"),
		conflicting("labelcontracttest_b", "Network interface name"),
	)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err := NewNodeCollector(logger, names...)
	if err == nil {
		t.Fatal("NewNodeCollector must reject conflicting label contracts at startup")
	}
	var conflict *LabelConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("expected a LabelConflictError, got: %v", err)
	}

	// With only one of the conflicting collectors enabled, startup succeeds.
	if _, err := NewNodeCollector(logger, names[0]); err != nil {
		t.Fatalf("NewNodeCollector with a single contract owner must succeed, got: %v", err)
	}
}

// TestDeclaredLabelContractsValidate guards the declared contract table in
// label_contracts*.go: with every known collector enabled, no two collectors
// may export the same metric name with different label semantics.
func TestDeclaredLabelContractsValidate(t *testing.T) {
	// Tests do not parse command line flags, so collector defaults are not
	// applied. Enable cpufreq explicitly to mirror the production default
	// under which the cpu collector suppresses its frequency metric.
	if state, ok := collectorState["cpufreq"]; ok {
		old := *state
		*state = true
		defer func() { *state = old }()
	}
	enabled := make([]string, 0, len(factories))
	for name := range factories {
		enabled = append(enabled, name)
	}
	if err := validateLabelContracts(enabled); err != nil {
		t.Fatalf("declared label contracts must not conflict: %v", err)
	}
}
