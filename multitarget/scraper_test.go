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
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// healthyHandler answers like a minimal node_exporter endpoint.
func healthyHandler(name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, "# HELP node_up whether the node is up\n# TYPE node_up gauge\nnode_up{instance=\"%s\"} 1\n", name)
		fmt.Fprintf(w, "# HELP node_cpu_seconds_total cpu time\n# TYPE node_cpu_seconds_total counter\nnode_cpu_seconds_total{cpu=\"0\",mode=\"idle\"} 42\n")
	})
}

// blackholeHandler never writes a response and blocks until the client gives
// up, simulating a machine that has stopped answering.
func blackholeHandler() http.Handler {
	return http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
}

func newTestScraper(t *testing.T, targets []Target, targetTimeout, roundTimeout time.Duration, concurrency int) *Scraper {
	t.Helper()
	scraper, err := NewScraper(Config{
		Targets:       targets,
		TargetTimeout: targetTimeout,
		RoundTimeout:  roundTimeout,
		Concurrency:   concurrency,
	}, slog.Default())
	if err != nil {
		t.Fatalf("NewScraper: %v", err)
	}
	return scraper
}

// TestHungTargetOnlyAffectsItself is the core isolation guarantee: one target
// that never answers must not prevent any other target from returning metrics.
func TestHungTargetOnlyAffectsItself(t *testing.T) {
	const healthyCount = 4

	healthy := make([]*httptest.Server, 0, healthyCount)
	targets := make([]Target, 0, healthyCount+1)
	for i := range healthyCount {
		srv := httptest.NewServer(healthyHandler(fmt.Sprintf("host-%d", i)))
		healthy = append(healthy, srv)
		targets = append(targets, Target{Name: fmt.Sprintf("host-%d", i), URL: srv.URL + "/metrics"})
	}

	hung := httptest.NewServer(blackholeHandler())
	targets = append(targets, Target{Name: "host-hung", URL: hung.URL + "/metrics"})

	for _, srv := range healthy {
		defer srv.Close()
	}
	defer hung.Close()

	scraper := newTestScraper(t, targets, 200*time.Millisecond, 5*time.Second, 3)

	start := time.Now()
	round := scraper.ScrapeAll(context.Background())
	elapsed := time.Since(start)

	if round.Failed() != 1 {
		t.Fatalf("expected exactly 1 failed target, got %d", round.Failed())
	}

	var hungResult *TargetResult
	healthySucceeded := 0
	for i := range round.Results {
		result := &round.Results[i]
		if result.Target.Name == "host-hung" {
			hungResult = result
			continue
		}
		if result.Success {
			healthySucceeded++
			family := result.Families["node_up"]
			if family == nil {
				t.Fatalf("target %s returned no node_up family", result.Target.Name)
			}
			if got := family.GetMetric()[0].GetLabel(); !hasTargetLabel(got, result.Target.Name) {
				t.Fatalf("target %s metrics missing target label: %v", result.Target.Name, got)
			}
		}
	}

	if healthySucceeded != healthyCount {
		t.Fatalf("expected all %d healthy targets to succeed, got %d", healthyCount, healthySucceeded)
	}
	if hungResult == nil || hungResult.Success {
		t.Fatal("expected the hung target to be recorded as failed")
	}
	if hungResult.Reason != ReasonTimeout {
		t.Fatalf("expected reason %q, got %q (err=%v)", ReasonTimeout, hungResult.Reason, hungResult.Err)
	}

	// The round must be bounded by the per-target timeout plus scheduling
	// slack rather than the round budget, proving the hung target did not
	// serialize the whole round.
	if elapsed > 2*time.Second {
		t.Fatalf("round took %s, hung target blocked the other targets", elapsed)
	}
}

func hasTargetLabel(labels []*dto.LabelPair, want string) bool {
	for _, label := range labels {
		if label.GetName() == TargetLabel && label.GetValue() == want {
			return true
		}
	}
	return false
}

// TestRoundBudgetCancelsSlowTargets proves the total round budget: when more
// targets are slow than the concurrency allows, the round still returns within
// the round timeout and the not-yet-finished targets count as round-budget
// failures.
func TestRoundBudgetCancelsSlowTargets(t *testing.T) {
	// All targets hang. With concurrency 2 and 5 targets, only 2 can hit the
	// per-target timeout; the rest must be cut short by the round budget.
	targets := make([]Target, 5)
	servers := make([]*httptest.Server, 5)
	for i := range targets {
		srv := httptest.NewServer(blackholeHandler())
		servers[i] = srv
		targets[i] = Target{Name: fmt.Sprintf("slow-%d", i), URL: srv.URL + "/metrics"}
	}
	defer func() {
		for _, srv := range servers {
			srv.Close()
		}
	}()

	scraper := newTestScraper(t, targets, 5*time.Second, 300*time.Millisecond, 2)

	start := time.Now()
	round := scraper.ScrapeAll(context.Background())
	elapsed := time.Since(start)

	if round.Failed() != len(targets) {
		t.Fatalf("expected all %d targets to fail, got %d", len(targets), round.Failed())
	}
	if !round.TimedOut {
		t.Fatal("expected the round to be marked as timed out")
	}
	if elapsed > time.Second {
		t.Fatalf("round took %s, round budget was not enforced", elapsed)
	}

	reasons := map[string]int{}
	for _, result := range round.Results {
		reasons[result.Reason]++
		if result.Reason != ReasonTimeout && result.Reason != ReasonRoundBudget {
			t.Fatalf("unexpected reason %q", result.Reason)
		}
	}
	if reasons[ReasonRoundBudget] == 0 {
		t.Fatalf("expected at least one %q failure, got %v", ReasonRoundBudget, reasons)
	}
}

// TestHTTPAndTransportFailuresAreClassified checks non-timeout failure paths.
func TestHTTPAndTransportFailuresAreClassified(t *testing.T) {
	badStatus := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer badStatus.Close()

	refused := httptest.NewServer(healthyHandler("refused"))
	refusedURL := refused.URL
	refused.Close() // nothing listens on the URL now

	targets := []Target{
		{Name: "bad-status", URL: badStatus.URL + "/metrics"},
		{Name: "conn-refused", URL: refusedURL + "/metrics"},
	}

	scraper := newTestScraper(t, targets, time.Second, 5*time.Second, 2)
	round := scraper.ScrapeAll(context.Background())

	byName := map[string]TargetResult{}
	for _, result := range round.Results {
		byName[result.Target.Name] = result
	}

	if byName["bad-status"].Reason != ReasonHTTP {
		t.Fatalf("expected %q, got %q", ReasonHTTP, byName["bad-status"].Reason)
	}
	if byName["conn-refused"].Reason != ReasonTransport {
		t.Fatalf("expected %q, got %q", ReasonTransport, byName["conn-refused"].Reason)
	}
}
