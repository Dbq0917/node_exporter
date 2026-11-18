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
	"strings"
	"testing"
)

func TestParseTargets(t *testing.T) {
	input := `
# comment line, ignored
host-a http://127.0.0.1:19100/metrics

host-b https://host-b.example:9100/metrics  # inline comments are not supported
`

	// Inline comments are treated as extra fields and must be rejected so the
	// operator notices a malformed target file instead of silently scraping a
	// broken URL.
	if _, err := parseTargets(strings.NewReader(input)); err == nil {
		t.Fatal("expected error for line with extra field, got nil")
	}

	clean := `
# comments and blanks are fine
host-a http://127.0.0.1:19100/metrics
host-b https://host-b.example:9100/metrics
`
	targets, err := parseTargets(strings.NewReader(clean))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("expected 2 targets, got %d", len(targets))
	}
	if targets[0].Name != "host-a" || targets[0].URL != "http://127.0.0.1:19100/metrics" {
		t.Fatalf("unexpected first target: %+v", targets[0])
	}
}

func TestParseTargetsRejectsInvalid(t *testing.T) {
	cases := map[string]string{
		"empty file":      "",
		"only comments":   "# nothing here\n",
		"single field":    "host-a\n",
		"bad scheme":      "host-a ftp://host/metrics\n",
		"missing host":    "host-a http:///metrics\n",
		"duplicate names": "host-a http://h1/metrics\nhost-a http://h2/metrics\n",
		"three fields":    "host-a http://h/metrics extra\n",
	}

	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseTargets(strings.NewReader(input)); err == nil {
				t.Fatalf("expected error for %q", name)
			}
		})
	}
}

func TestConfigValidateDefaultsAndBounds(t *testing.T) {
	cfg := Config{Targets: []Target{{Name: "a", URL: "http://127.0.0.1:9100/metrics"}}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.TargetTimeout != DefaultTargetTimeout || cfg.RoundTimeout != DefaultRoundTimeout || cfg.Concurrency != DefaultConcurrency {
		t.Fatalf("defaults not applied: %+v", cfg)
	}

	// A round budget smaller than the per-target timeout is valid: it lets
	// operators cap total round latency even when individual targets are
	// allowed to wait longer.
	shortBudget := Config{
		Targets:       []Target{{Name: "a", URL: "http://127.0.0.1:9100/metrics"}},
		TargetTimeout: 30,
		RoundTimeout:  10,
	}
	if err := shortBudget.Validate(); err != nil {
		t.Fatalf("short round budget should be valid: %v", err)
	}

	noTargets := Config{}
	if err := noTargets.Validate(); err == nil {
		t.Fatal("expected error when no targets are configured")
	}
}
