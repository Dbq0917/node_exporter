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

// Package collector includes all individual collectors to gather and export system metrics.
package collector

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

// Namespace defines the common namespace to be used by all metrics.
const namespace = "node"

var (
	scrapeDurationDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape", "collector_duration_seconds"),
		"node_exporter: Duration of a collector scrape.",
		[]string{"collector"},
		nil,
	)
	scrapeSuccessDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape", "collector_success"),
		"node_exporter: Whether a collector succeeded.",
		[]string{"collector"},
		nil,
	)
)

const (
	defaultEnabled  = true
	defaultDisabled = false
)

var (
	factories              = make(map[string]func(logger *slog.Logger) (Collector, error))
	initiatedCollectorsMtx = sync.Mutex{}
	initiatedCollectors    = make(map[string]Collector)
	collectorState         = make(map[string]*bool)
	forcedCollectors       = map[string]bool{} // collectors which have been explicitly enabled or disabled
	defaultResults         collectorResults
)

func registerCollector(collector string, isDefaultEnabled bool, factory func(logger *slog.Logger) (Collector, error)) {
	var helpDefaultState string
	if isDefaultEnabled {
		helpDefaultState = "enabled"
	} else {
		helpDefaultState = "disabled"
	}

	flagName := fmt.Sprintf("collector.%s", collector)
	flagHelp := fmt.Sprintf("Enable the %s collector (default: %s).", collector, helpDefaultState)
	defaultValue := fmt.Sprintf("%v", isDefaultEnabled)

	flag := kingpin.Flag(flagName, flagHelp).Default(defaultValue).Action(collectorFlagAction(collector)).Bool()
	collectorState[collector] = flag

	factories[collector] = factory
}

// NodeCollector implements the prometheus.Collector interface.
type NodeCollector struct {
	Collectors map[string]Collector
	logger     *slog.Logger
	results    *collectorResults
}

// collectorResults keeps the last successful series of each collector. When a
// collector fails, those series are exposed again with a NaN sample value so
// that a failure is distinguishable from a collector that never had any data.
type collectorResults struct {
	mu   sync.Mutex
	last map[string][]prometheus.Metric
}

func (r *collectorResults) store(name string, metrics []prometheus.Metric) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.last == nil {
		r.last = make(map[string][]prometheus.Metric)
	}
	r.last[name] = append([]prometheus.Metric(nil), metrics...)
}

func (r *collectorResults) load(name string) []prometheus.Metric {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]prometheus.Metric(nil), r.last[name]...)
}

func (r *collectorResults) clear(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.last, name)
}

// DisableDefaultCollectors sets the collector state to false for all collectors which
// have not been explicitly enabled on the command line.
func DisableDefaultCollectors() {
	for c := range collectorState {
		if _, ok := forcedCollectors[c]; !ok {
			*collectorState[c] = false
		}
	}
}

// collectorFlagAction generates a new action function for the given collector
// to track whether it has been explicitly enabled or disabled from the command line.
// A new action function is needed for each collector flag because the ParseContext
// does not contain information about which flag called the action.
// See: https://github.com/alecthomas/kingpin/issues/294
func collectorFlagAction(collector string) func(ctx *kingpin.ParseContext) error {
	return func(_ *kingpin.ParseContext) error {
		forcedCollectors[collector] = true
		return nil
	}
}

// NewNodeCollector creates a new NodeCollector.
func NewNodeCollector(logger *slog.Logger, filters ...string) (*NodeCollector, error) {
	f := make(map[string]bool)
	for _, filter := range filters {
		enabled, exist := collectorState[filter]
		if !exist {
			return nil, fmt.Errorf("missing collector: %s", filter)
		}
		if !*enabled {
			return nil, fmt.Errorf("disabled collector: %s", filter)
		}
		f[filter] = true
	}
	collectors := make(map[string]Collector)
	initiatedCollectorsMtx.Lock()
	defer initiatedCollectorsMtx.Unlock()
	for key, enabled := range collectorState {
		if !*enabled || (len(f) > 0 && !f[key]) {
			continue
		}
		if collector, ok := initiatedCollectors[key]; ok {
			collectors[key] = collector
		} else {
			collector, err := factories[key](logger.With("collector", key))
			if err != nil {
				return nil, err
			}
			collectors[key] = collector
			initiatedCollectors[key] = collector
		}
	}
	return &NodeCollector{Collectors: collectors, logger: logger, results: &collectorResults{}}, nil
}

// Describe implements the prometheus.Collector interface.
func (n NodeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- scrapeDurationDesc
	ch <- scrapeSuccessDesc
}

// Collect implements the prometheus.Collector interface.
func (n NodeCollector) Collect(ch chan<- prometheus.Metric) {
	results := n.results
	if results == nil {
		results = &defaultResults
	}

	wg := sync.WaitGroup{}
	wg.Add(len(n.Collectors))
	for name, c := range n.Collectors {
		go func(name string, c Collector) {
			executeWithResults(name, c, ch, n.logger, results)
			wg.Done()
		}(name, c)
	}
	wg.Wait()
}

func execute(name string, c Collector, ch chan<- prometheus.Metric, logger *slog.Logger) {
	executeWithResults(name, c, ch, logger, &defaultResults)
}

func executeWithResults(name string, c Collector, ch chan<- prometheus.Metric, logger *slog.Logger, results *collectorResults) {
	begin := time.Now()
	metrics, err := collectToSlice(c)
	duration := time.Since(begin)
	var success float64

	if err != nil {
		if IsNoDataError(err) {
			logger.Debug("collector returned no data", "name", name, "duration_seconds", duration.Seconds(), "err", err)
			if results != nil {
				results.clear(name)
			}
		} else {
			logger.Error("collector failed", "name", name, "duration_seconds", duration.Seconds(), "err", err)
			if results != nil {
				for _, metric := range results.load(name) {
					ch <- unavailableMetric{Metric: metric}
				}
			}
		}
		success = 0
	} else {
		logger.Debug("collector succeeded", "name", name, "duration_seconds", duration.Seconds())
		success = 1
		if results != nil {
			results.store(name, metrics)
		}
		for _, metric := range metrics {
			ch <- metric
		}
	}
	ch <- prometheus.MustNewConstMetric(scrapeDurationDesc, prometheus.GaugeValue, duration.Seconds(), name)
	ch <- prometheus.MustNewConstMetric(scrapeSuccessDesc, prometheus.GaugeValue, success, name)
}

// collectToSlice runs a collector with an isolated channel so that partial
// output is not exposed when the collector subsequently reports an error.
func collectToSlice(c Collector) ([]prometheus.Metric, error) {
	metricsCh := make(chan prometheus.Metric)
	done := make(chan struct{})
	var metrics []prometheus.Metric

	go func() {
		defer close(done)
		for metric := range metricsCh {
			metrics = append(metrics, metric)
		}
	}()

	err := c.Update(metricsCh)
	close(metricsCh)
	<-done
	return metrics, err
}

// unavailableMetric preserves the descriptor and labels of a previously
// collected metric while replacing its sample value with NaN.
type unavailableMetric struct {
	prometheus.Metric
}

func (m unavailableMetric) Write(out *dto.Metric) error {
	if err := m.Metric.Write(out); err != nil {
		return err
	}

	unavailable := proto.Clone(out).(*dto.Metric)
	nan := math.NaN()
	switch {
	case unavailable.Gauge != nil:
		unavailable.Gauge.Value = &nan
	case unavailable.Counter != nil:
		unavailable.Counter.Value = &nan
		unavailable.Counter.Exemplar = nil
	case unavailable.Untyped != nil:
		unavailable.Untyped.Value = &nan
	case unavailable.Summary != nil:
		unavailable.Summary.SampleSum = &nan
		for _, quantile := range unavailable.Summary.Quantile {
			quantile.Value = &nan
		}
	case unavailable.Histogram != nil:
		unavailable.Histogram.SampleSum = &nan
		if unavailable.Histogram.SampleCountFloat != nil {
			unavailable.Histogram.SampleCountFloat = &nan
		}
		for _, bucket := range unavailable.Histogram.Bucket {
			if bucket.CumulativeCountFloat != nil {
				bucket.CumulativeCountFloat = &nan
			}
		}
	}

	out.Label = unavailable.Label
	out.Gauge = unavailable.Gauge
	out.Counter = unavailable.Counter
	out.Summary = unavailable.Summary
	out.Untyped = unavailable.Untyped
	out.Histogram = unavailable.Histogram
	out.TimestampMs = unavailable.TimestampMs
	return nil
}

// Collector is the interface a collector has to implement.
type Collector interface {
	// Get new metrics and expose them via prometheus registry.
	Update(ch chan<- prometheus.Metric) error
}

type typedDesc struct {
	desc      *prometheus.Desc
	valueType prometheus.ValueType
}

func (d *typedDesc) mustNewConstMetric(value float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(d.desc, d.valueType, value, labels...)
}

// ErrNoData indicates the collector found no data to collect, but had no other error.
var ErrNoData = errors.New("collector returned no data")

func IsNoDataError(err error) bool {
	return err == ErrNoData
}

// pushMetric helps construct and convert a variety of value types into Prometheus float64 metrics.
func pushMetric(ch chan<- prometheus.Metric, fieldDesc *prometheus.Desc, value any, valueType prometheus.ValueType, labelValues ...string) {
	var fVal float64
	switch val := value.(type) {
	case uint8:
		fVal = float64(val)
	case uint16:
		fVal = float64(val)
	case uint32:
		fVal = float64(val)
	case uint64:
		fVal = float64(val)
	case int64:
		fVal = float64(val)
	case *uint8:
		if val == nil {
			return
		}
		fVal = float64(*val)
	case *uint16:
		if val == nil {
			return
		}
		fVal = float64(*val)
	case *uint32:
		if val == nil {
			return
		}
		fVal = float64(*val)
	case *uint64:
		if val == nil {
			return
		}
		fVal = float64(*val)
	case *int64:
		if val == nil {
			return
		}
		fVal = float64(*val)
	default:
		return
	}

	ch <- prometheus.MustNewConstMetric(fieldDesc, valueType, fVal, labelValues...)
}
