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
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	cacheRequestsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape_cache", "requests_total"),
		"node_exporter: Total number of scrapes served by the collection cache.",
		nil, nil,
	)
	cacheReusedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape_cache", "reused_total"),
		"node_exporter: Total number of scrapes that reused a cached collection instead of collecting.",
		nil, nil,
	)
	cacheCollectionsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape_cache", "collections_total"),
		"node_exporter: Total number of upstream collection rounds triggered.",
		nil, nil,
	)
	cacheCollectionFailuresDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape_cache", "collection_failures_total"),
		"node_exporter: Total number of upstream collection rounds that failed.",
		nil, nil,
	)
	cacheRefreshingDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape_cache", "refreshing"),
		"node_exporter: Whether a background collection round is currently in progress.",
		nil, nil,
	)
	cacheSnapshotAgeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape_cache", "snapshot_age_seconds"),
		"node_exporter: Age of the collection snapshot served to the current scrape.",
		nil, nil,
	)
	cacheSnapshotCachedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape_cache", "snapshot_cached"),
		"node_exporter: Whether the served snapshot was reused from the cache (1) or freshly collected (0).",
		nil, nil,
	)
	cacheLastSuccessDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape_cache", "last_collection_success_timestamp_seconds"),
		"node_exporter: Unix timestamp of the last successful upstream collection round.",
		nil, nil,
	)
)

// cachedSnapshot holds the metrics gathered in one upstream collection round.
type cachedSnapshot struct {
	// metrics are the metric objects produced by one collection round. They
	// are only ever replayed under replayMu because a metric must not be
	// written by several scrapes concurrently.
	metrics []prometheus.Metric
	// collectedAt is read from the (possibly injected) TTL clock and drives
	// freshness checks; wallCollectedAt is the real wall-clock timestamp used
	// for the snapshot age metric.
	wallCollectedAt time.Time
	collectedAt     time.Time
}

// CachedCollector wraps a prometheus.Collector and serves its metrics from a
// time-bounded snapshot. Within one TTL period concurrent scrapes share a
// single upstream collection round. When the snapshot is expired a stale one
// is served immediately while a refresh happens in the background, so a slow
// collection never blocks scrapes. The very first scrape after startup blocks
// until the first snapshot is available.
type CachedCollector struct {
	inner  prometheus.Collector
	ttl    time.Duration
	logger *slog.Logger

	now func() time.Time

	refreshMu   sync.Mutex
	refreshCond *sync.Cond
	refreshing  bool

	mu       sync.RWMutex
	snapshot *cachedSnapshot
	// replayMu serializes replaying a snapshot onto scrape channels. Metric
	// objects mutate internal state when written, so concurrent replays of the
	// same snapshot would corrupt each other.
	replayMu sync.Mutex

	requests           atomic.Uint64
	reused             atomic.Uint64
	collections        atomic.Uint64
	collectionFailures atomic.Uint64
	lastSuccessUnix    atomic.Int64
}

// NewCachedCollector wraps inner. A ttl of zero disables caching and every
// scrape collects directly from the wrapped collector.
func NewCachedCollector(inner prometheus.Collector, ttl time.Duration, logger *slog.Logger) *CachedCollector {
	c := &CachedCollector{
		inner:  inner,
		ttl:    ttl,
		logger: logger,
		now:    time.Now,
	}
	c.refreshCond = sync.NewCond(&c.refreshMu)
	return c
}

// withClock overrides the time source. It is intended for tests.
func (c *CachedCollector) withClock(now func() time.Time) *CachedCollector {
	c.now = now
	return c
}

// Describe implements the prometheus.Collector interface.
func (c *CachedCollector) Describe(ch chan<- *prometheus.Desc) {
	c.inner.Describe(ch)
	ch <- cacheRequestsDesc
	ch <- cacheReusedDesc
	ch <- cacheCollectionsDesc
	ch <- cacheCollectionFailuresDesc
	ch <- cacheRefreshingDesc
	ch <- cacheSnapshotAgeDesc
	ch <- cacheSnapshotCachedDesc
	ch <- cacheLastSuccessDesc
}

// Collect implements the prometheus.Collector interface.
func (c *CachedCollector) Collect(ch chan<- prometheus.Metric) {
	if c.ttl <= 0 {
		c.inner.Collect(ch)
		return
	}

	snapshot, cached := c.snapshotForScrape()
	c.replayMu.Lock()
	defer c.replayMu.Unlock()
	for _, metric := range snapshot.metrics {
		ch <- metric
	}
	if cached {
		c.reused.Add(1)
	}

	c.emitStats(ch, snapshot, cached)
}

// snapshotForScrape returns the snapshot to serve. Fresh snapshots are reused;
// stale snapshots trigger one non-blocking background refresh; when no
// snapshot exists yet the scrape waits for the first collection.
func (c *CachedCollector) snapshotForScrape() (snapshot *cachedSnapshot, cached bool) {
	c.requests.Add(1)

	c.mu.RLock()
	snapshot = c.snapshot
	c.mu.RUnlock()

	if snapshot != nil && c.now().Sub(snapshot.collectedAt) < c.ttl {
		return snapshot, true
	}

	if c.tryStartRefresh() {
		// Only the first scrape without any snapshot blocks; later refreshes
		// run in the background and the stale snapshot is served immediately.
		if snapshot == nil {
			c.collectInline()
			c.mu.RLock()
			fresh := c.snapshot
			c.mu.RUnlock()
			// A newly produced snapshot is fresh data; an empty placeholder
			// left by a failed round means there was nothing to reuse.
			return fresh, false
		}
		go c.collectBackground()
		return snapshot, true
	}

	// Another goroutine already owns a refresh. A stale snapshot can be
	// served straight away; cold-start scrapes wait for the first round.
	if snapshot != nil {
		return snapshot, true
	}
	c.waitForRefresh()
	c.mu.RLock()
	fresh := c.snapshot
	c.mu.RUnlock()
	return fresh, false
}

func (c *CachedCollector) tryStartRefresh() bool {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	if c.refreshing {
		return false
	}
	c.refreshing = true
	return true
}

func (c *CachedCollector) waitForRefresh() {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	for c.refreshing {
		c.refreshCond.Wait()
	}
}

func (c *CachedCollector) finishRefresh() {
	c.refreshMu.Lock()
	c.refreshing = false
	c.refreshCond.Broadcast()
	c.refreshMu.Unlock()
}

// collectInline runs the upstream collection on the calling goroutine and
// waits for it to complete before returning.
func (c *CachedCollector) collectInline() {
	c.runCollection()
	c.finishRefresh()
}

// collectBackground runs the upstream collection without blocking the scrape
// that scheduled it.
func (c *CachedCollector) collectBackground() {
	defer c.finishRefresh()
	c.runCollection()
}

// runCollection performs exactly one upstream collection round. A failing
// round keeps any previously stored snapshot untouched.
func (c *CachedCollector) runCollection() {
	c.collections.Add(1)

	ttlBegin := c.now()
	wallBegin := time.Now()
	metricCh := make(chan prometheus.Metric)
	done := make(chan struct{})
	var metrics []prometheus.Metric
	go func() {
		for metric := range metricCh {
			metrics = append(metrics, metric)
		}
		close(done)
	}()

	failed := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				c.logger.Error("cached collection panicked", "err", r)
				failed = true
			}
		}()
		c.inner.Collect(metricCh)
	}()
	close(metricCh)
	<-done

	if failed {
		c.collectionFailures.Add(1)
		c.mu.Lock()
		if c.snapshot == nil {
			// Ensure cold-start scrapes always have a snapshot to serve;
			// the next TTL period will retry the failing collection.
			c.snapshot = &cachedSnapshot{collectedAt: ttlBegin, wallCollectedAt: wallBegin}
		}
		c.mu.Unlock()
		return
	}

	c.mu.Lock()
	c.snapshot = &cachedSnapshot{
		metrics:         metrics,
		collectedAt:     ttlBegin,
		wallCollectedAt: wallBegin,
	}
	c.mu.Unlock()
	c.lastSuccessUnix.Store(wallBegin.Unix())
}

func (c *CachedCollector) emitStats(ch chan<- prometheus.Metric, snapshot *cachedSnapshot, cached bool) {
	ch <- prometheus.MustNewConstMetric(cacheRequestsDesc, prometheus.CounterValue, float64(c.requests.Load()))
	ch <- prometheus.MustNewConstMetric(cacheReusedDesc, prometheus.CounterValue, float64(c.reused.Load()))
	ch <- prometheus.MustNewConstMetric(cacheCollectionsDesc, prometheus.CounterValue, float64(c.collections.Load()))
	ch <- prometheus.MustNewConstMetric(cacheCollectionFailuresDesc, prometheus.CounterValue, float64(c.collectionFailures.Load()))

	refreshing := float64(0)
	c.refreshMu.Lock()
	if c.refreshing {
		refreshing = 1
	}
	c.refreshMu.Unlock()
	ch <- prometheus.MustNewConstMetric(cacheRefreshingDesc, prometheus.GaugeValue, refreshing)

	age := time.Since(snapshot.wallCollectedAt).Seconds()
	ch <- prometheus.MustNewConstMetric(cacheSnapshotAgeDesc, prometheus.GaugeValue, age)

	cachedValue := float64(0)
	if cached {
		cachedValue = 1
	}
	ch <- prometheus.MustNewConstMetric(cacheSnapshotCachedDesc, prometheus.GaugeValue, cachedValue)

	ch <- prometheus.MustNewConstMetric(cacheLastSuccessDesc, prometheus.GaugeValue, float64(c.lastSuccessUnix.Load()))
}
