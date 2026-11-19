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

package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/node_exporter/collector"
)

func TestParseCollectorConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		want    []string
		wantErr bool
	}{
		{
			name:   "yaml list",
			config: "collectors:\n  - time\n  - uname\n",
			want:   []string{"time", "uname"},
		},
		{
			name:   "json list",
			config: `["uname", "time", "time"]`,
			want:   []string{"time", "uname"},
		},
		{
			name:   "comments and duplicates",
			config: "# enabled collectors\nuname\n\ntime\ntime\n",
			want:   []string{"time", "uname"},
		},
		{
			name:    "empty",
			config:  "\n# nothing here\n",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCollectorConfig(strings.NewReader(tt.config))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("want %v, got %v", tt.want, got)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("want %v, got %v", tt.want, got)
				}
			}
		})
	}
}

func TestHandlerReloadDoesNotDuplicateSeries(t *testing.T) {
	restoreCollectorStates(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := collector.ApplyEnabledCollectors([]string{"time"}); err != nil {
		t.Fatalf("apply initial collector state: %v", err)
	}
	h := newHandler(false, 0, logger)

	assertSingleCollectorGeneration(t, h, "node_time_seconds")

	var (
		wg     sync.WaitGroup
		stop   = make(chan struct{})
		errCh  = make(chan error, 16)
		reload = func(name string) {
			t.Helper()
			if err := collector.ApplyEnabledCollectors([]string{name}); err != nil {
				t.Errorf("apply %s: %v", name, err)
				return
			}
			if err := h.Reload(); err != nil {
				t.Errorf("reload %s: %v", name, err)
			}
		}
	)

	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := scrapeAnyGeneration(h, "node_time_seconds", "node_uname_info"); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}()
	}

	for i := range 10 {
		if i%2 == 0 {
			reload("uname")
		} else {
			reload("time")
		}
	}
	close(stop)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
}

func TestReloadCollectorConfigKeepsPreviousStateOnError(t *testing.T) {
	restoreCollectorStates(t)

	path := filepath.Join(t.TempDir(), "collectors.yaml")
	if err := os.WriteFile(path, []byte("time\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := applyCollectorConfigFile(path); err != nil {
		t.Fatalf("apply initial config: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHandler(false, 0, logger)
	assertSingleCollectorGeneration(t, h, "node_time_seconds")

	if err := os.WriteFile(path, []byte("not-a-collector\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := reloadCollectorConfig(path, h); err == nil {
		t.Fatal("expected reload error")
	}

	assertSingleCollectorGeneration(t, h, "node_time_seconds")
}

func restoreCollectorStates(t *testing.T) {
	t.Helper()
	previous := collector.CollectorStates()
	t.Cleanup(func() {
		enabled := make([]string, 0, len(previous))
		for name, state := range previous {
			if state {
				enabled = append(enabled, name)
			}
		}
		if err := collector.ApplyEnabledCollectors(enabled); err != nil {
			t.Fatalf("restore collector state: %v", err)
		}
	})
}

func assertSingleCollectorGeneration(t *testing.T, h *handler, metric string) {
	t.Helper()
	if err := scrapeSingleGeneration(h, metric); err != nil {
		t.Fatal(err)
	}
}

func scrapeSingleGeneration(h *handler, metrics ...string) error {
	return scrape(h, true, metrics...)
}

func scrapeAnyGeneration(h *handler, metrics ...string) error {
	return scrape(h, false, metrics...)
}

func scrape(h *handler, requireAll bool, metrics ...string) error {
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		return &scrapeError{code: rec.Code, body: rec.Body.String()}
	}
	return checkScrapeBody(rec.Body.String(), requireAll, metrics...)
}

type scrapeError struct {
	code int
	body string
}

func (e *scrapeError) Error() string {
	return "unexpected scrape status " + http.StatusText(e.code) + ": " + e.body
}

func checkScrapeBody(body string, requireAll bool, metrics ...string) error {
	seen := make(map[string]struct{})
	found := make(map[string]bool, len(metrics))
	lines := strings.Split(body, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key := line
		if i := strings.LastIndexByte(line, ' '); i >= 0 {
			key = line[:i]
		}
		if _, ok := seen[key]; ok {
			return &duplicateSeriesError{series: key}
		}
		seen[key] = struct{}{}
		for _, metric := range metrics {
			if strings.HasPrefix(key, metric) {
				found[metric] = true
			}
		}
	}

	matched := 0
	for _, metric := range metrics {
		if found[metric] {
			matched++
		}
	}
	if !requireAll && matched > 0 {
		return nil
	}
	for _, metric := range metrics {
		if !found[metric] {
			return &missingSeriesError{metric: metric}
		}
	}
	return nil
}

type duplicateSeriesError struct {
	series string
}

func (e *duplicateSeriesError) Error() string {
	return "duplicate series: " + e.series
}

type missingSeriesError struct {
	metric string
}

func (e *missingSeriesError) Error() string {
	return "missing series: " + e.metric
}
