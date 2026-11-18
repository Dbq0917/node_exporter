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
	"errors"
	"time"
)

const (
	// DefaultTargetTimeout bounds a single target scrape. A hung target can
	// never delay a round beyond this per-target deadline.
	DefaultTargetTimeout = 10 * time.Second
	// DefaultRoundTimeout is the total budget for one collection round.
	// When it elapses, targets that have not finished are cancelled and
	// counted as failures without holding up the rest of the round.
	DefaultRoundTimeout = 60 * time.Second
	// DefaultConcurrency caps how many targets are scraped in flight at the
	// same time, so a 100-target deployment does not open 100 sockets at once.
	DefaultConcurrency = 20

	// UserAgent identifies requests sent to the remote node_exporters.
	UserAgent = "node_exporter-multitarget"
)

// Config holds the runtime parameters of the multi-target aggregator.
type Config struct {
	// Targets is the full set of remote node_exporter instances to scrape.
	Targets []Target
	// TargetTimeout is the maximum duration any single target scrape may take.
	TargetTimeout time.Duration
	// RoundTimeout is the total time budget for one collection round across
	// all targets. It may be smaller than TargetTimeout to cap total round
	// latency; in that case in-flight targets are cut off by this budget.
	RoundTimeout time.Duration
	// Concurrency limits the number of targets scraped concurrently.
	Concurrency int
}

// Validate checks the configuration and normalizes zero values to defaults.
func (c *Config) Validate() error {
	if len(c.Targets) == 0 {
		return errors.New("at least one target is required")
	}
	if c.TargetTimeout <= 0 {
		c.TargetTimeout = DefaultTargetTimeout
	}
	if c.RoundTimeout <= 0 {
		c.RoundTimeout = DefaultRoundTimeout
	}
	if c.Concurrency <= 0 {
		c.Concurrency = DefaultConcurrency
	}
	return nil
}
