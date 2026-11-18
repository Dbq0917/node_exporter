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
	"fmt"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestHundredTargetsWithOneHung reproduces the production incident at 100
// targets: exactly one machine stops answering. The other 99 must still
// produce metrics, the round must stay inside the total budget, and the
// failure ratio must be 1/100.
func TestHundredTargetsWithOneHung(t *testing.T) {
	const targetCount = 100
	const hungIndex = 37

	targets := make([]Target, targetCount)
	servers := make([]*httptest.Server, targetCount)

	for i := range targetCount {
		name := fmt.Sprintf("host-%03d", i)
		if i == hungIndex {
			servers[i] = httptest.NewServer(blackholeHandler())
		} else {
			servers[i] = httptest.NewServer(healthyHandler(name))
		}
		targets[i] = Target{Name: name, URL: servers[i].URL + "/metrics"}
	}
	defer func() {
		for _, srv := range servers {
			srv.Close()
		}
	}()

	scraper := newTestScraper(t, targets, 250*time.Millisecond, 3*time.Second, 25)

	start := time.Now()
	round := scraper.ScrapeAll(context.Background())
	elapsed := time.Since(start)

	if round.Failed() != 1 {
		t.Fatalf("expected exactly 1 failure at 100 targets, got %d", round.Failed())
	}

	healthyMetrics := 0
	for _, result := range round.Results {
		if result.Target.Name == targets[hungIndex].Name {
			if result.Success || result.Reason != ReasonTimeout {
				t.Fatalf("hung target misreported: success=%v reason=%q", result.Success, result.Reason)
			}
			continue
		}
		if !result.Success {
			t.Fatalf("healthy target %s unexpectedly failed: %s", result.Target.Name, result.Reason)
		}
		if result.Families["node_up"] == nil {
			t.Fatalf("target %s returned no node_up family", result.Target.Name)
		}
		healthyMetrics++
	}
	if healthyMetrics != targetCount-1 {
		t.Fatalf("expected %d healthy targets with metrics, got %d", targetCount-1, healthyMetrics)
	}

	// The round is driven by the per-target timeout, not by serial waits: it
	// must finish well under the round budget even with 100 targets.
	if elapsed >= scraper.config.RoundTimeout {
		t.Fatalf("round took %s, exceeded round budget %s", elapsed, scraper.config.RoundTimeout)
	}

	wantRatio := 1.0 / float64(targetCount)
	if got := float64(round.Failed()) / float64(len(round.Results)); got != wantRatio {
		t.Fatalf("expected failure ratio %v, got %v", wantRatio, got)
	}
}

// TestHundredTargetsRoundBudget verifies the total round budget under a
// large target set where many machines are slow: the round always returns by
// the deadline and every unfinished target is accounted for as a failure.
func TestHundredTargetsRoundBudget(t *testing.T) {
	const targetCount = 100

	targets := make([]Target, targetCount)
	servers := make([]*httptest.Server, targetCount)
	for i := range targetCount {
		servers[i] = httptest.NewServer(blackholeHandler())
		targets[i] = Target{
			Name: fmt.Sprintf("slow-%03d", i),
			URL:  servers[i].URL + "/metrics",
		}
	}
	defer func() {
		for _, srv := range servers {
			srv.Close()
		}
	}()

	scraper := newTestScraper(t, targets, 10*time.Second, 400*time.Millisecond, 10)

	start := time.Now()
	round := scraper.ScrapeAll(context.Background())
	elapsed := time.Since(start)

	if !round.TimedOut {
		t.Fatal("expected the 100-target round to hit its budget")
	}
	if round.Failed() != targetCount {
		t.Fatalf("expected all %d targets to be accounted as failures, got %d", targetCount, round.Failed())
	}
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("round budget not enforced at scale: took %s", elapsed)
	}
}

// TestConcurrentRoundsAreIndependent makes sure repeated rounds do not share
// state that could let one bad round corrupt later ones.
func TestConcurrentRoundsAreIndependent(t *testing.T) {
	srv := httptest.NewServer(healthyHandler("steady"))
	defer srv.Close()

	scraper := newTestScraper(t,
		[]Target{{Name: "steady", URL: srv.URL + "/metrics"}},
		time.Second, 5*time.Second, 1,
	)

	var failures atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 10 {
			round := scraper.ScrapeAll(context.Background())
			if round.Failed() != 0 {
				failures.Add(1)
			}
		}
	}()
	<-done

	if got := failures.Load(); got != 0 {
		t.Fatalf("healthy target failed in %d of 10 repeated rounds", got)
	}
}
