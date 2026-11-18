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
	"context"
	"maps"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	// metricsNamespace is the prefix for all self-monitoring metrics of the
	// multi-target aggregator. Scraped node metrics keep their original
	// "node_" names untouched.
	metricsNamespace = "node_multitarget"

	// TargetLabel is attached to every metric relabeled from a remote target.
	TargetLabel = "target"

	// reasonLabel carries the failure reason on error metrics.
	reasonLabel = "reason"
)

var failureReasons = []string{
	ReasonTimeout,
	ReasonRoundBudget,
	ReasonHTTP,
	ReasonParse,
	ReasonTransport,
}

// Collector runs a scrape round on demand and serves the combined result as
// Prometheus metrics. It implements prometheus.Collector.
//
// All self-monitoring counters live on the collector itself and therefore
// persist across scrape requests, while the relabeled target metrics are
// produced fresh from the most recent round.
type Collector struct {
	scraper *Scraper

	// scrapeRequests counts collection rounds since process start.
	scrapeRequests *prometheus.Desc
	// scrapeDurationSeconds observes whole-round wall-clock duration.
	scrapeDurationSeconds *prometheus.Desc
	// roundTimeoutTotal counts rounds that exhausted the total round budget.
	roundTimeoutTotal *prometheus.Desc
	// targetsTotal is the configured number of targets, per round.
	targetsTotal *prometheus.Desc
	// targetScrapeDurationSeconds observes per-target scrape duration.
	targetScrapeDurationSeconds *prometheus.Desc
	// targetUp reports whether the most recent scrape of a target succeeded.
	targetUp *prometheus.Desc
	// targetScrapeFailuresTotal counts per-target failures by reason.
	targetScrapeFailuresTotal *prometheus.Desc
	// targetFailureRatio is the fraction of targets that failed in the most
	// recent round, between 0 and 1.
	targetFailureRatio *prometheus.Desc

	// mu guards the counters. Collect can be invoked concurrently by the
	// HTTP layer up to web.max-requests.
	mu                  sync.Mutex
	roundsTotal         uint64
	roundTimeouts       uint64
	targetFailuresTotal map[string]map[string]uint64 // target name -> reason -> count
}

// NewCollector builds a Collector for the given scraper.
func NewCollector(scraper *Scraper) *Collector {
	return &Collector{
		scraper:               scraper,
		targetFailuresTotal:   make(map[string]map[string]uint64),
		scrapeRequests:        prometheus.NewDesc(prometheus.BuildFQName(metricsNamespace, "scrape", "rounds_total"), "Total number of multi-target collection rounds.", nil, nil),
		scrapeDurationSeconds: prometheus.NewDesc(prometheus.BuildFQName(metricsNamespace, "scrape", "round_duration_seconds"), "Duration of a whole multi-target collection round.", nil, nil),
		roundTimeoutTotal:     prometheus.NewDesc(prometheus.BuildFQName(metricsNamespace, "scrape", "round_timeout_total"), "Total number of rounds that exhausted the configured round time budget.", nil, nil),
		targetsTotal:          prometheus.NewDesc(prometheus.BuildFQName(metricsNamespace, "scrape", "targets"), "Number of targets configured for the collection round.", nil, nil),
		targetScrapeDurationSeconds: prometheus.NewDesc(
			prometheus.BuildFQName(metricsNamespace, "target", "scrape_duration_seconds"),
			"Duration of scraping a single target.",
			[]string{TargetLabel}, nil,
		),
		targetUp: prometheus.NewDesc(
			prometheus.BuildFQName(metricsNamespace, "target", "up"),
			"1 if the most recent scrape of the target succeeded, 0 otherwise.",
			[]string{TargetLabel}, nil,
		),
		targetScrapeFailuresTotal: prometheus.NewDesc(
			prometheus.BuildFQName(metricsNamespace, "target", "scrape_failures_total"),
			"Total number of failed target scrapes, partitioned by failure reason.",
			[]string{TargetLabel, reasonLabel}, nil,
		),
		targetFailureRatio: prometheus.NewDesc(
			prometheus.BuildFQName(metricsNamespace, "target", "failure_ratio"),
			"Fraction of targets that failed in the most recent collection round (0 to 1).",
			nil, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.scrapeRequests
	ch <- c.scrapeDurationSeconds
	ch <- c.roundTimeoutTotal
	ch <- c.targetsTotal
	ch <- c.targetScrapeDurationSeconds
	ch <- c.targetUp
	ch <- c.targetScrapeFailuresTotal
	ch <- c.targetFailureRatio
}

// Collect implements prometheus.Collector. It runs one full collection round,
// streams the relabeled target metrics, then emits the self-monitoring
// metrics. A failing target only loses its own metrics: all other targets and
// all self-monitoring metrics are still emitted.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	round := c.scraper.ScrapeAll(context.Background())

	for _, result := range round.Results {
		if result.Success {
			// Stream every metric family of the successful target straight
			// into the channel without an extra copy.
			for _, family := range result.Families {
				emitFamily(ch, family)
			}
		}
	}

	c.recordRound(ch, round)
}

// recordRound updates the persistent counters and emits the self-monitoring
// metrics for one round.
func (c *Collector) recordRound(ch chan<- prometheus.Metric, round *RoundResult) {
	c.mu.Lock()
	c.roundsTotal++
	if round.TimedOut {
		c.roundTimeouts++
	}

	for _, result := range round.Results {
		if !result.Success {
			byReason, ok := c.targetFailuresTotal[result.Target.Name]
			if !ok {
				byReason = make(map[string]uint64)
				c.targetFailuresTotal[result.Target.Name] = byReason
			}
			byReason[result.Reason]++
		}
	}

	roundsTotal := c.roundsTotal
	roundTimeouts := c.roundTimeouts
	failureCounters := make(map[string]map[string]uint64, len(c.targetFailuresTotal))
	for target, byReason := range c.targetFailuresTotal {
		reasons := make(map[string]uint64, len(byReason))
		maps.Copy(reasons, byReason)
		failureCounters[target] = reasons
	}
	c.mu.Unlock()

	ch <- prometheus.MustNewConstMetric(c.scrapeRequests, prometheus.CounterValue, float64(roundsTotal))
	ch <- prometheus.MustNewConstMetric(c.scrapeDurationSeconds, prometheus.GaugeValue, round.Duration.Seconds())
	ch <- prometheus.MustNewConstMetric(c.roundTimeoutTotal, prometheus.CounterValue, float64(roundTimeouts))
	ch <- prometheus.MustNewConstMetric(c.targetsTotal, prometheus.GaugeValue, float64(len(round.Results)))

	failed := round.Failed()
	failureRatio := 0.0
	if len(round.Results) > 0 {
		failureRatio = float64(failed) / float64(len(round.Results))
	}
	ch <- prometheus.MustNewConstMetric(c.targetFailureRatio, prometheus.GaugeValue, failureRatio)

	for _, result := range round.Results {
		up := 0.0
		if result.Success {
			up = 1
		}
		ch <- prometheus.MustNewConstMetric(c.targetUp, prometheus.GaugeValue, up, result.Target.Name)
		ch <- prometheus.MustNewConstMetric(
			c.targetScrapeDurationSeconds,
			prometheus.GaugeValue,
			result.Duration.Seconds(),
			result.Target.Name,
		)
	}

	// Emit a failure counter for every target/reason combination so that a
	// target which recovers still shows its historical failure count as 0 for
	// reasons that never occurred.
	for _, result := range round.Results {
		for _, reason := range failureReasons {
			value := 0.0
			if byReason, ok := failureCounters[result.Target.Name]; ok {
				value = float64(byReason[reason])
			}
			ch <- prometheus.MustNewConstMetric(
				c.targetScrapeFailuresTotal,
				prometheus.CounterValue,
				value,
				result.Target.Name,
				reason,
			)
		}
	}
}

// emitFamily converts a scraped dto.MetricFamily into prometheus.Metric values
// and sends them to ch. Unknown/unsupported types are skipped rather than
// aborting the whole round.
func emitFamily(ch chan<- prometheus.Metric, family *dto.MetricFamily) {
	if family == nil {
		return
	}

	var metricType prometheus.ValueType
	switch family.GetType() {
	case dto.MetricType_COUNTER:
		metricType = prometheus.CounterValue
	case dto.MetricType_GAUGE:
		metricType = prometheus.GaugeValue
	default:
		// Histograms and summaries cannot be expressed as a single const
		// metric and are skipped; node_exporter only exposes counters and
		// gauges under the node_ namespace.
		return
	}

	for _, metric := range family.GetMetric() {
		labelNames := make([]string, 0, len(metric.GetLabel()))
		labelValues := make([]string, 0, len(metric.GetLabel()))
		for _, label := range metric.GetLabel() {
			labelNames = append(labelNames, label.GetName())
			labelValues = append(labelValues, label.GetValue())
		}

		desc := prometheus.NewDesc(family.GetName(), family.GetHelp(), labelNames, nil)

		var value float64
		switch metricType {
		case prometheus.CounterValue:
			value = metric.GetCounter().GetValue()
		case prometheus.GaugeValue:
			value = metric.GetGauge().GetValue()
		}
		ch <- prometheus.MustNewConstMetric(desc, metricType, value, labelValues...)
	}
}
