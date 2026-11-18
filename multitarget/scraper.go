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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// Failure reasons recorded in the "reason" label of the error metrics.
const (
	// ReasonTimeout means the target did not answer within the per-target
	// timeout.
	ReasonTimeout = "timeout"
	// ReasonRoundBudget means the round-wide total budget elapsed before the
	// target finished.
	ReasonRoundBudget = "round_budget"
	// ReasonHTTP means the remote endpoint answered with a non-2xx status.
	ReasonHTTP = "http_status"
	// ReasonParse means the response body could not be parsed.
	ReasonParse = "parse"
	// ReasonTransport means the HTTP request failed at the transport layer
	// (connection refused, DNS failure, reset, ...).
	ReasonTransport = "transport"
)

// TargetResult is the outcome of scraping one target in one round.
type TargetResult struct {
	Target Target
	// Start is when the scrape of this target began.
	Start time.Time
	// Duration is how long the scrape took.
	Duration time.Duration
	// Success is false when the target had to be counted as a failure.
	Success bool
	// Reason is empty on success and one of the Reason* constants otherwise.
	Reason string
	// Err is the underlying error, nil on success.
	Err error
	// Families holds the parsed metrics, relabeled with the "target" label.
	// It is nil on failure.
	Families map[string]*dto.MetricFamily
}

// RoundResult is the outcome of one collection round across all targets.
type RoundResult struct {
	// Start is when the round began.
	Start time.Time
	// Duration is the wall-clock duration of the whole round. It is always
	// bounded by the configured round timeout.
	Duration time.Duration
	// Results holds one entry per configured target, in the configured order.
	Results []TargetResult
	// TimedOut is true when the round-wide budget elapsed before every target
	// had finished.
	TimedOut bool
}

// Failed returns the number of targets that failed in this round.
func (r *RoundResult) Failed() int {
	failed := 0
	for _, result := range r.Results {
		if !result.Success {
			failed++
		}
	}
	return failed
}

// Scraper scrapes a fixed set of remote node_exporter targets concurrently,
// enforcing both a per-target timeout and a round-wide total budget.
type Scraper struct {
	config Config
	client *http.Client
	logger *slog.Logger
}

// NewScraper builds a Scraper. The HTTP client has no overall timeout of its
// own: deadlines are always derived from the per-target context so that the
// configured timeouts are the single source of truth.
func NewScraper(config Config, logger *slog.Logger) (*Scraper, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Scraper{
		config: config,
		client: &http.Client{
			// A bounded transport keeps idle conns from piling up across
			// long-lived aggregator processes.
			Transport: &http.Transport{
				MaxIdleConns:        config.Concurrency,
				MaxIdleConnsPerHost: 2,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		logger: logger,
	}, nil
}

// ScrapeAll runs one collection round.
//
// Every target gets its own context bounded by TargetTimeout, while the whole
// round is bounded by RoundTimeout. The round therefore always returns within
// RoundTimeout (plus negligible scheduling overhead), regardless of how many
// targets hang. A failure of one target never aborts or delays the others.
func (s *Scraper) ScrapeAll(parent context.Context) *RoundResult {
	roundStart := time.Now()

	roundCtx, roundCancel := context.WithTimeout(parent, s.config.RoundTimeout)
	defer roundCancel()

	results := make([]TargetResult, len(s.config.Targets))

	// A buffered channel acts as a concurrency-limiting semaphore.
	sem := make(chan struct{}, s.config.Concurrency)
	var wg sync.WaitGroup

	for i, target := range s.config.Targets {
		wg.Add(1)
		go func(index int, t Target) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-roundCtx.Done():
				// The round budget is already gone; do not even start.
				results[index] = s.budgetExceededResult(t, roundStart)
				return
			}
			defer func() { <-sem }()

			results[index] = s.scrapeOne(roundCtx, t)
		}(i, target)
	}

	wg.Wait()

	return &RoundResult{
		Start:    roundStart,
		Duration: time.Since(roundStart),
		Results:  results,
		TimedOut: errors.Is(roundCtx.Err(), context.DeadlineExceeded),
	}
}

// budgetExceededResult builds the result for targets that never got a chance
// to run because the round budget had already elapsed.
func (s *Scraper) budgetExceededResult(t Target, roundStart time.Time) TargetResult {
	return TargetResult{
		Target:   t,
		Start:    roundStart,
		Duration: time.Since(roundStart),
		Success:  false,
		Reason:   ReasonRoundBudget,
		Err:      context.DeadlineExceeded,
	}
}

// scrapeOne scrapes a single target under its own timeout. It never panics the
// round: every failure path is converted into a TargetResult instead.
func (s *Scraper) scrapeOne(roundCtx context.Context, target Target) TargetResult {
	start := time.Now()

	targetCtx, targetCancel := context.WithTimeout(roundCtx, s.config.TargetTimeout)
	defer targetCancel()

	result := TargetResult{Target: target, Start: start}

	req, err := http.NewRequestWithContext(targetCtx, http.MethodGet, target.URL, nil)
	if err != nil {
		return s.fail(result, ReasonTransport, err, start)
	}
	req.Header.Set("Accept", string(expfmt.FmtText))
	req.Header.Set("User-Agent", UserAgent)

	resp, err := s.client.Do(req)
	if err != nil {
		return s.fail(result, classifyError(targetCtx, roundCtx, err), err, start)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Drain so the connection can be reused.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
		return s.fail(result, ReasonHTTP, fmt.Errorf("target %s returned HTTP %d", target.Name, resp.StatusCode), start)
	}

	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		reason := classifyError(targetCtx, roundCtx, err)
		if reason == ReasonTransport && isParseError(err) {
			reason = ReasonParse
		}
		return s.fail(result, reason, err, start)
	}

	relabelFamilies(families, target.Name)

	result.Success = true
	result.Families = families
	result.Duration = time.Since(start)
	return result
}

// fail finalizes an unsuccessful result and logs it for operators.
func (s *Scraper) fail(result TargetResult, reason string, err error, start time.Time) TargetResult {
	result.Success = false
	result.Reason = reason
	result.Err = err
	result.Duration = time.Since(start)
	s.logger.Warn("target scrape failed",
		"target", result.Target.Name,
		"url", result.Target.URL,
		"reason", reason,
		"err", err.Error(),
		"duration_seconds", result.Duration.Seconds(),
	)
	return result
}

// classifyError maps a request error to a failure reason, distinguishing the
// per-target timeout from exhaustion of the round-wide total budget.
func classifyError(targetCtx, roundCtx context.Context, err error) string {
	if errors.Is(targetCtx.Err(), context.DeadlineExceeded) {
		if errors.Is(roundCtx.Err(), context.DeadlineExceeded) {
			return ReasonRoundBudget
		}
		return ReasonTimeout
	}
	if errors.Is(err, context.DeadlineExceeded) {
		if errors.Is(roundCtx.Err(), context.DeadlineExceeded) {
			return ReasonRoundBudget
		}
		return ReasonTimeout
	}
	// A parse error is reported by the caller with ReasonParse; transport
	// failures are the remaining default.
	return ReasonTransport
}

// isParseError reports whether err originates from the exposition format
// parser rather than the HTTP transport.
func isParseError(err error) bool {
	if _, ok := errors.AsType[expfmt.ParseError](err); ok {
		return true
	}
	return strings.Contains(err.Error(), "text format parsing error")
}

// relabelFamilies adds the "target" label to every scraped metric so samples
// from different machines stay distinguishable after aggregation. Metrics
// that already carry a target label are left untouched.
func relabelFamilies(families map[string]*dto.MetricFamily, targetName string) {
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			hasTargetLabel := false
			for _, label := range metric.GetLabel() {
				if label.GetName() == TargetLabel {
					hasTargetLabel = true
					break
				}
			}
			if !hasTargetLabel {
				name := TargetLabel
				value := targetName
				metric.Label = append(metric.Label, &dto.LabelPair{Name: &name, Value: &value})
			}
		}
	}
}
