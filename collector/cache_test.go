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
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// countingCollector is a fake upstream collector. Every Collect call is one
// upstream collection round and reports the round number as a gauge.
type countingCollector struct {
	desc        *prometheus.Desc
	labelValues []string
	rounds      atomic.Int64
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
	delay       atomic.Int64
	fail        atomic.Bool
}

func newCountingCollector() *countingCollector {
	return &countingCollector{
		desc: prometheus.NewDesc("node_test_upstream_round", "Current upstream collection round.", nil, nil),
	}
}

func newLabeledCountingCollector(instance string) *countingCollector {
	return &countingCollector{
		desc:        prometheus.NewDesc("node_test_upstream_round", "Current upstream collection round.", []string{"instance"}, nil),
		labelValues: []string{instance},
	}
}

func (f *countingCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- f.desc
}

func (f *countingCollector) Collect(ch chan<- prometheus.Metric) {
	inFlight := f.inFlight.Add(1)
	for {
		maxSeen := f.maxInFlight.Load()
		if inFlight <= maxSeen || f.maxInFlight.CompareAndSwap(maxSeen, inFlight) {
			break
		}
	}
	defer f.inFlight.Add(-1)

	if d := f.delay.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}

	round := f.rounds.Add(1)
	if f.fail.Load() {
		panic("simulated upstream failure")
	}
	if len(f.labelValues) > 0 {
		ch <- prometheus.MustNewConstMetric(f.desc, prometheus.GaugeValue, float64(round), f.labelValues...)
		return
	}
	ch <- prometheus.MustNewConstMetric(f.desc, prometheus.GaugeValue, float64(round))
}

type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func newManualClock() *manualClock {
	return &manualClock{t: time.Unix(1000, 0)}
}

func (m *manualClock) now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.t
}

func (m *manualClock) advance(d time.Duration) {
	m.mu.Lock()
	m.t = m.t.Add(d)
	m.mu.Unlock()
}

func newTestRegistry(t *testing.T, c prometheus.Collector) *prometheus.Registry {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)
	return reg
}

func gatherMetric(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %s", err)
	}
	return metricFromFamilies(t, families, name)
}

func metricFromFamilies(t *testing.T, families []*dto.MetricFamily, name string) float64 {
	t.Helper()
	for _, family := range families {
		if family.GetName() == name {
			if len(family.Metric) != 1 {
				t.Fatalf("metric %s has %d samples, want 1", name, len(family.Metric))
			}
			metric := family.Metric[0]
			switch family.GetType() {
			case dto.MetricType_COUNTER:
				return metric.GetCounter().GetValue()
			case dto.MetricType_GAUGE:
				return metric.GetGauge().GetValue()
			default:
				t.Fatalf("unsupported metric type for %s", name)
			}
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

// gatherOnce performs a single scrape and returns a lookup over all metrics.
func gatherOnce(t *testing.T, reg *prometheus.Registry) map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %s", err)
	}
	values := make(map[string]float64, len(families))
	for _, family := range families {
		if len(family.Metric) != 1 {
			t.Fatalf("metric %s has %d samples, want 1", family.GetName(), len(family.Metric))
		}
		metric := family.Metric[0]
		switch family.GetType() {
		case dto.MetricType_COUNTER:
			values[family.GetName()] = metric.GetCounter().GetValue()
		case dto.MetricType_GAUGE:
			values[family.GetName()] = metric.GetGauge().GetValue()
		default:
			t.Fatalf("unsupported metric type for %s", family.GetName())
		}
	}
	return values
}

func waitForRounds(t *testing.T, f *countingCollector, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f.rounds.Load() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("upstream rounds = %d, want %d", f.rounds.Load(), want)
}

func waitForFailures(t *testing.T, c *CachedCollector, want uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c.collectionFailures.Load() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("collection failures = %d, want %d", c.collectionFailures.Load(), want)
}

func waitForSnapshotRound(t *testing.T, c *CachedCollector, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.RLock()
		snapshot := c.snapshot
		c.mu.RUnlock()
		if snapshot != nil && len(snapshot.metrics) > 0 {
			pb := &dto.Metric{}
			if err := snapshot.metrics[0].Write(pb); err == nil {
				if pb.GetGauge().GetValue() == float64(want) {
					return
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("snapshot never reached round %d", want)
}

// Within one TTL period ten concurrent scrapes must trigger a single upstream
// collection round and every scrape must be marked as a cache reuse.
func TestCachedCollectorConcurrentScrapesShareOneRound(t *testing.T) {
	fake := newCountingCollector()
	cached := NewCachedCollector(fake, time.Minute, slog.Default())
	reg := newTestRegistry(t, cached)

	// Cold start: one blocking collection round.
	if round := gatherMetric(t, reg, "node_test_upstream_round"); round != 1 {
		t.Fatalf("cold scrape round = %v, want 1", round)
	}

	const scrapes = 10
	var wg sync.WaitGroup
	wg.Add(scrapes)
	for range scrapes {
		go func() {
			defer wg.Done()
			if _, err := reg.Gather(); err != nil {
				t.Errorf("concurrent gather: %s", err)
			}
		}()
	}
	wg.Wait()

	if rounds := fake.rounds.Load(); rounds != 1 {
		t.Fatalf("upstream collection rounds = %d, want 1", rounds)
	}
	if max := fake.maxInFlight.Load(); max != 1 {
		t.Fatalf("max concurrent upstream collections = %d, want 1", max)
	}
	// Read the counters directly so the assertions do not add another scrape.
	if requests := cached.requests.Load(); requests != scrapes+1 {
		t.Fatalf("cache requests = %d, want %d", requests, scrapes+1)
	}
	if reused := cached.reused.Load(); reused != scrapes {
		t.Fatalf("cache reuses = %d, want %d", reused, scrapes)
	}
	if collections := cached.collections.Load(); collections != 1 {
		t.Fatalf("collection rounds = %d, want 1", collections)
	}
	if cached := gatherMetric(t, reg, "node_scrape_cache_snapshot_cached"); cached != 1 {
		t.Fatalf("snapshot_cached = %v, want 1", cached)
	}
}

// After one full TTL period the next scrape schedules a refresh and a
// following scrape serves the freshly collected data.
func TestCachedCollectorExpiredSnapshotGetsRefreshed(t *testing.T) {
	clock := newManualClock()
	fake := newCountingCollector()
	cached := NewCachedCollector(fake, time.Minute, slog.Default()).withClock(clock.now)
	reg := newTestRegistry(t, cached)

	first := gatherOnce(t, reg)
	if first["node_test_upstream_round"] != 1 {
		t.Fatalf("first scrape round = %v, want 1", first["node_test_upstream_round"])
	}
	if first["node_scrape_cache_snapshot_cached"] != 0 {
		t.Fatalf("cold scrape snapshot_cached = %v, want 0", first["node_scrape_cache_snapshot_cached"])
	}

	clock.advance(time.Minute + time.Second)

	// The first scrape after expiry serves the stale snapshot immediately and
	// schedules a background refresh.
	stale := gatherOnce(t, reg)
	if stale["node_test_upstream_round"] != 1 {
		t.Fatalf("stale scrape round = %v, want 1", stale["node_test_upstream_round"])
	}
	if stale["node_scrape_cache_snapshot_cached"] != 1 {
		t.Fatalf("stale scrape snapshot_cached = %v, want 1", stale["node_scrape_cache_snapshot_cached"])
	}
	waitForRounds(t, fake, 2)
	waitForSnapshotRound(t, cached, 2)

	// The next scrape serves the new snapshot: data is updated. It is still
	// marked cached because the scrape reuses a previously collected snapshot;
	// the inline cold path is the only non-cached (snapshot_cached == 0) case.
	refreshed := gatherOnce(t, reg)
	if refreshed["node_test_upstream_round"] != 2 {
		t.Fatalf("refreshed scrape round = %v, want 2", refreshed["node_test_upstream_round"])
	}
	if refreshed["node_scrape_cache_snapshot_cached"] != 1 {
		t.Fatalf("refreshed scrape snapshot_cached = %v, want 1", refreshed["node_scrape_cache_snapshot_cached"])
	}
	if refreshed["node_scrape_cache_collections_total"] != 2 {
		t.Fatalf("collection rounds = %v, want 2", refreshed["node_scrape_cache_collections_total"])
	}
	if refreshed["node_scrape_cache_collection_failures_total"] != 0 {
		t.Fatalf("collection failures = %v, want 0", refreshed["node_scrape_cache_collection_failures_total"])
	}
}

// A slow background refresh must not block scrapes: the stale snapshot is
// served while the refresh is still running.
func TestCachedCollectorSlowRefreshDoesNotBlockScrapes(t *testing.T) {
	clock := newManualClock()
	fake := newCountingCollector()
	fake.delay.Store(int64(200 * time.Millisecond))
	cached := NewCachedCollector(fake, time.Minute, slog.Default()).withClock(clock.now)
	reg := newTestRegistry(t, cached)

	gatherMetric(t, reg, "node_test_upstream_round") // cold start blocks once

	clock.advance(time.Minute + time.Second)
	begin := time.Now()
	if round := gatherMetric(t, reg, "node_test_upstream_round"); round != 1 {
		t.Fatalf("stale scrape round = %v, want 1", round)
	}
	if elapsed := time.Since(begin); elapsed > 100*time.Millisecond {
		t.Fatalf("scrape blocked for %s during slow refresh, want stale snapshot served immediately", elapsed)
	}
	if refreshing := gatherMetric(t, reg, "node_scrape_cache_refreshing"); refreshing != 1 {
		t.Fatalf("refreshing = %v, want 1", refreshing)
	}
	waitForRounds(t, fake, 2)
}

// Multiple collector instances keep independent caches and counters; the
// upstream load is the sum of one round per instance, never per scrape.
func TestCachedCollectorMultipleInstancesCountersAreAdditive(t *testing.T) {
	const instances = 3
	const scrapesPerInstance = 5

	fakes := make([]*countingCollector, instances)
	registries := make([]*prometheus.Registry, instances)
	for i := range instances {
		fakes[i] = newLabeledCountingCollector("instance-" + string(rune('a'+i)))
		cached := NewCachedCollector(fakes[i], time.Minute, slog.Default())
		registries[i] = newTestRegistry(t, cached)
	}

	var wg sync.WaitGroup
	for i := range instances {
		for range scrapesPerInstance {
			wg.Add(1)
			reg := registries[i]
			go func() {
				defer wg.Done()
				if _, err := reg.Gather(); err != nil {
					t.Errorf("gather: %s", err)
				}
			}()
		}
	}
	wg.Wait()

	var totalRounds int64
	var collectionCounterSum float64
	for i, fake := range fakes {
		if rounds := fake.rounds.Load(); rounds != 1 {
			t.Fatalf("instance %d upstream rounds = %d, want 1", i, rounds)
		}
		totalRounds += fake.rounds.Load()
		collectionCounterSum += gatherMetric(t, registries[i], "node_scrape_cache_collections_total")
	}
	if totalRounds != instances {
		t.Fatalf("total upstream rounds = %d, want %d (one per instance)", totalRounds, instances)
	}
	if collectionCounterSum != instances {
		t.Fatalf("sum of collection counters = %v, want %d", collectionCounterSum, instances)
	}
}

// A ttl of zero disables caching: every scrape collects upstream and no cache
// metrics are emitted.
func TestCachedCollectorZeroTTLBypassesCache(t *testing.T) {
	fake := newCountingCollector()
	cached := NewCachedCollector(fake, 0, slog.Default())
	reg := newTestRegistry(t, cached)

	for range 3 {
		if _, err := reg.Gather(); err != nil {
			t.Fatalf("gather: %s", err)
		}
	}
	if rounds := fake.rounds.Load(); rounds != 3 {
		t.Fatalf("upstream rounds = %d, want 3 with disabled cache", rounds)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %s", err)
	}
	for _, family := range families {
		if family.GetName() == "node_scrape_cache_requests_total" {
			t.Fatal("cache metrics must not be emitted when ttl is 0")
		}
	}
}

// A failing refresh keeps serving the previous snapshot and counts the failure
// instead of crashing the exporter.
func TestCachedCollectorFailingRefreshKeepsStaleSnapshot(t *testing.T) {
	clock := newManualClock()
	fake := newCountingCollector()
	cached := NewCachedCollector(fake, time.Minute, slog.Default()).withClock(clock.now)
	reg := newTestRegistry(t, cached)

	if round := gatherMetric(t, reg, "node_test_upstream_round"); round != 1 {
		t.Fatalf("first scrape round = %v, want 1", round)
	}

	fake.fail.Store(true)
	clock.advance(time.Minute + time.Second)
	stale := gatherOnce(t, reg)
	if stale["node_test_upstream_round"] != 1 {
		t.Fatalf("scrape after failure round = %v, want stale 1", stale["node_test_upstream_round"])
	}
	if stale["node_scrape_cache_snapshot_cached"] != 1 {
		t.Fatalf("scrape after failure snapshot_cached = %v, want 1", stale["node_scrape_cache_snapshot_cached"])
	}
	waitForRounds(t, fake, 2)
	waitForFailures(t, cached, 1)
	cached.mu.RLock()
	snapshotMetrics := len(cached.snapshot.metrics)
	cached.mu.RUnlock()
	if snapshotMetrics != 1 {
		t.Fatalf("stale snapshot metrics = %d, want the previous round retained", snapshotMetrics)
	}

	// Assert counters directly so the check does not schedule another scrape.
	if failures := cached.collectionFailures.Load(); failures != 1 {
		t.Fatalf("failures = %d, want 1", failures)
	}
	if collections := cached.collections.Load(); collections != 2 {
		t.Fatalf("collections = %d, want 2", collections)
	}
	if rounds := fake.rounds.Load(); rounds != 2 {
		t.Fatalf("upstream rounds = %d, want 2", rounds)
	}
}
