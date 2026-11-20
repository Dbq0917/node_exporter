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
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

type switchableCollector struct {
	mu              sync.Mutex
	desc            *prometheus.Desc
	value           float64
	err             error
	emitBeforeError bool
	emittedValue    float64
}

func (c *switchableCollector) set(value float64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = value
	c.err = err
}

func (c *switchableCollector) setPartialOutput(value float64, enabled bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.emitBeforeError = enabled
	c.emittedValue = value
}

func (c *switchableCollector) Update(ch chan<- prometheus.Metric) error {
	c.mu.Lock()
	value := c.value
	err := c.err
	emitBeforeError := c.emitBeforeError
	emittedValue := c.emittedValue
	c.mu.Unlock()

	if emitBeforeError {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, emittedValue)
	}
	if err != nil {
		return err
	}
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, value)
	return nil
}

func TestCollectorFailureMarksPreviousSeriesUnavailable(t *testing.T) {
	c := &switchableCollector{
		desc: prometheus.NewDesc("node_test_failure_value", "Test metric.", nil, nil),
	}
	c.set(42, nil)
	nc := newTestNodeCollector(c)

	metricFamilies := gatherTestMetrics(t, nc)
	baselineSamples := countSamples(metricFamilies)
	assertValue(t, metricFamilies, "node_test_failure_value", 42)
	assertCollectorSuccess(t, metricFamilies, 1)

	c.set(0, errors.New("injected collector failure"))
	metricFamilies = gatherTestMetrics(t, nc)
	assertUnavailable(t, metricFamilies, "node_test_failure_value")
	assertCollectorSuccess(t, metricFamilies, 0)
	if got := countSamples(metricFamilies); got != baselineSamples {
		t.Fatalf("failure changed sample count: want %d, got %d", baselineSamples, got)
	}

	c.set(7, nil)
	metricFamilies = gatherTestMetrics(t, nc)
	assertValue(t, metricFamilies, "node_test_failure_value", 7)
	assertCollectorSuccess(t, metricFamilies, 1)
	if got := countSamples(metricFamilies); got != baselineSamples {
		t.Fatalf("recovery changed sample count: want %d, got %d", baselineSamples, got)
	}
}

func TestCollectorNoDataDoesNotReplayPreviousSeries(t *testing.T) {
	c := &switchableCollector{
		desc: prometheus.NewDesc("node_test_no_data_value", "Test metric.", nil, nil),
	}
	c.set(42, nil)
	nc := newTestNodeCollector(c)

	gatherTestMetrics(t, nc)

	c.set(0, ErrNoData)
	metricFamilies := gatherTestMetrics(t, nc)
	assertAbsent(t, metricFamilies, "node_test_no_data_value")
	assertCollectorSuccess(t, metricFamilies, 0)

	// A failure after no data must not resurrect the series from before the
	// collector legitimately found nothing to expose.
	c.set(0, errors.New("injected collector failure"))
	metricFamilies = gatherTestMetrics(t, nc)
	assertAbsent(t, metricFamilies, "node_test_no_data_value")
	assertCollectorSuccess(t, metricFamilies, 0)
}

func TestCollectorFailureDiscardsPartialOutput(t *testing.T) {
	c := &switchableCollector{
		desc: prometheus.NewDesc("node_test_partial_value", "Test metric.", nil, nil),
	}
	c.set(1, nil)
	nc := newTestNodeCollector(c)

	gatherTestMetrics(t, nc)

	c.setPartialOutput(99, true)
	c.set(0, errors.New("injected collector failure"))
	metricFamilies := gatherTestMetrics(t, nc)
	assertUnavailable(t, metricFamilies, "node_test_partial_value")
	assertCollectorSuccess(t, metricFamilies, 0)
}

func TestExecuteMarksPreviousSeriesUnavailable(t *testing.T) {
	c := &switchableCollector{
		desc: prometheus.NewDesc("node_test_execute_value", "Test metric.", nil, nil),
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Cleanup(func() {
		defaultResults.clear("execute_failure_test")
	})

	c.set(5, nil)
	metrics := runExecute(c, logger)
	assertMetricValue(t, metrics, "node_test_execute_value", 5)

	c.set(0, errors.New("injected collector failure"))
	metrics = runExecute(c, logger)
	assertMetricUnavailable(t, metrics, "node_test_execute_value")
}

func TestCollectorFailureIsVisibleInExposition(t *testing.T) {
	c := &switchableCollector{
		desc: prometheus.NewDesc("node_test_exposition_value", "Test metric.", nil, nil),
	}
	c.set(42, nil)
	nc := newTestNodeCollector(c)

	registry := prometheus.NewRegistry()
	registry.MustRegister(nc)
	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	scrape := func() string {
		t.Helper()
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("unexpected status code: %d", recorder.Code)
		}
		return recorder.Body.String()
	}

	body := scrape()
	if !strings.Contains(body, "node_test_exposition_value 42") {
		t.Fatalf("successful scrape does not contain the metric value:\n%s", body)
	}

	c.set(0, errors.New("injected collector failure"))
	body = scrape()
	if !strings.Contains(body, "node_test_exposition_value NaN") {
		t.Fatalf("failed scrape does not mark the metric unavailable:\n%s", body)
	}
	if strings.Contains(body, "node_test_exposition_value 42") {
		t.Fatalf("failed scrape still exposes the previous value:\n%s", body)
	}
}

func runExecute(c Collector, logger *slog.Logger) []prometheus.Metric {
	metricsCh := make(chan prometheus.Metric, 16)
	execute("execute_failure_test", c, metricsCh, logger)
	close(metricsCh)

	var metrics []prometheus.Metric
	for metric := range metricsCh {
		metrics = append(metrics, metric)
	}
	return metrics
}

func newTestNodeCollector(c Collector) *NodeCollector {
	return &NodeCollector{
		Collectors: map[string]Collector{"failure_test": c},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		results:    &collectorResults{},
	}
}

func gatherTestMetrics(t *testing.T, nc *NodeCollector) map[string]*dto.MetricFamily {
	t.Helper()

	registry := prometheus.NewRegistry()
	if err := registry.Register(nc); err != nil {
		t.Fatalf("register collector: %v", err)
	}
	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	byName := make(map[string]*dto.MetricFamily, len(metricFamilies))
	for _, metricFamily := range metricFamilies {
		byName[metricFamily.GetName()] = metricFamily
	}
	return byName
}

func countSamples(metricFamilies map[string]*dto.MetricFamily) int {
	var count int
	for _, metricFamily := range metricFamilies {
		count += len(metricFamily.GetMetric())
	}
	return count
}

func assertValue(t *testing.T, metricFamilies map[string]*dto.MetricFamily, name string, want float64) {
	t.Helper()

	value, ok := sampleValue(metricFamilies, name)
	if !ok {
		t.Fatalf("metric %s is missing", name)
	}
	if value != want {
		t.Fatalf("metric %s: want %v, got %v", name, want, value)
	}
}

func assertUnavailable(t *testing.T, metricFamilies map[string]*dto.MetricFamily, name string) {
	t.Helper()

	value, ok := sampleValue(metricFamilies, name)
	if !ok {
		t.Fatalf("metric %s is missing after collector failure", name)
	}
	if !math.IsNaN(value) {
		t.Fatalf("metric %s: want NaN, got %v", name, value)
	}
}

func assertAbsent(t *testing.T, metricFamilies map[string]*dto.MetricFamily, name string) {
	t.Helper()

	if _, ok := metricFamilies[name]; ok {
		t.Fatalf("metric %s must not be exposed", name)
	}
}

func assertMetricValue(t *testing.T, metrics []prometheus.Metric, name string, want float64) {
	t.Helper()

	value, ok := metricValue(metrics, name)
	if !ok {
		t.Fatalf("metric %s is missing", name)
	}
	if value != want {
		t.Fatalf("metric %s: want %v, got %v", name, want, value)
	}
}

func assertMetricUnavailable(t *testing.T, metrics []prometheus.Metric, name string) {
	t.Helper()

	value, ok := metricValue(metrics, name)
	if !ok {
		t.Fatalf("metric %s is missing after collector failure", name)
	}
	if !math.IsNaN(value) {
		t.Fatalf("metric %s: want NaN, got %v", name, value)
	}
}

func metricValue(metrics []prometheus.Metric, name string) (float64, bool) {
	for _, metric := range metrics {
		if !strings.Contains(metric.Desc().String(), `fqName: "`+name+`"`) {
			continue
		}
		var value dto.Metric
		if err := metric.Write(&value); err != nil {
			return 0, false
		}
		return value.GetGauge().GetValue(), true
	}
	return 0, false
}

func assertCollectorSuccess(t *testing.T, metricFamilies map[string]*dto.MetricFamily, want float64) {
	t.Helper()

	metricFamily, ok := metricFamilies["node_scrape_collector_success"]
	if !ok {
		t.Fatal("node_scrape_collector_success is missing")
	}
	for _, metric := range metricFamily.GetMetric() {
		if labelValue(metric, "collector") != "failure_test" {
			continue
		}
		if got := metric.GetGauge().GetValue(); got != want {
			t.Fatalf("node_scrape_collector_success: want %v, got %v", want, got)
		}
		return
	}
	t.Fatal("node_scrape_collector_success for failure_test is missing")
}

func sampleValue(metricFamilies map[string]*dto.MetricFamily, name string) (float64, bool) {
	metricFamily, ok := metricFamilies[name]
	if !ok || len(metricFamily.GetMetric()) == 0 {
		return 0, false
	}
	return metricFamily.GetMetric()[0].GetGauge().GetValue(), true
}

func labelValue(metric *dto.Metric, name string) string {
	for _, label := range metric.GetLabel() {
		if label.GetName() == name {
			return label.GetValue()
		}
	}
	return ""
}
