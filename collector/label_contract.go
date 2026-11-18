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
	"errors"
	"fmt"
	"strings"
	"sync"
)

// LabelSemantic documents the meaning of a single variable label of a metric.
type LabelSemantic struct {
	// Name is the label name as exposed on the metric.
	Name string
	// Meaning describes what the label value identifies and where it comes from.
	Meaning string
}

// MetricContract declares which labels a collector attaches to a metric and
// what they mean. Contracts are the reviewable record of label semantics.
// They are validated at startup (NewNodeCollector) so that two collectors can
// never export the same metric name with different label semantics, which
// would silently merge the series and overwrite values at export time.
// See docs/LABELS.md for the merge rules.
type MetricContract struct {
	// Collector is the name of the owning collector.
	Collector string
	// Metric is the fully qualified metric name, e.g. node_cpu_frequency_hertz.
	Metric string
	// Help is the metric help string.
	Help string
	// Labels lists every variable label of the metric and its meaning.
	Labels []LabelSemantic

	// Prefix resolves a metric name collision: when active, the metric is
	// exported with Prefix as its subsystem instead of its own, i.e. as
	// node_<Prefix>_<metric name without the node_ namespace and original
	// subsystem>. The prefix is active when PrefixActive is nil or returns
	// true.
	Prefix       string
	PrefixActive func() bool
	// Suppressed, when non-nil and returning true, means the collector does
	// not export this metric under the current configuration.
	Suppressed func() bool
}

// resolvedName returns the metric name under which c is exported, applying
// the collision-resolution subsystem prefix when it is active.
func (c MetricContract) resolvedName() string {
	if c.Prefix == "" || (c.PrefixActive != nil && !c.PrefixActive()) {
		return c.Metric
	}
	rest, namespaced := strings.CutPrefix(c.Metric, namespace+"_")
	if !namespaced {
		rest = c.Metric
	}
	if i := strings.Index(rest, "_"); i >= 0 {
		return namespace + "_" + c.Prefix + rest[i:]
	}
	return namespace + "_" + c.Prefix + "_" + rest
}

// isSuppressed reports whether the metric is currently not exported.
func (c MetricContract) isSuppressed() bool {
	return c.Suppressed != nil && c.Suppressed()
}

// LabelConflictError reports that two collectors declare the same metric
// name with different label semantics.
type LabelConflictError struct {
	Metric        string
	First, Second MetricContract
}

func (e *LabelConflictError) Error() string {
	return fmt.Sprintf(
		"metric %q is declared by collectors %q and %q with different label semantics (%s labels: %s; %s labels: %s); rename one metric or declare a resolution in its metric contract, see docs/LABELS.md",
		e.Metric, e.First.Collector, e.Second.Collector,
		e.First.Collector, labelSemanticsString(e.First.Labels),
		e.Second.Collector, labelSemanticsString(e.Second.Labels),
	)
}

func labelSemanticsString(labels []LabelSemantic) string {
	parts := make([]string, 0, len(labels))
	for _, l := range labels {
		parts = append(parts, fmt.Sprintf("%s (%s)", l.Name, l.Meaning))
	}
	return strings.Join(parts, ", ")
}

// contractsEquivalent reports whether two contracts describe the same metric:
// identical help string and identical label semantics, order-insensitive.
func contractsEquivalent(a, b MetricContract) bool {
	if a.Help != b.Help || len(a.Labels) != len(b.Labels) {
		return false
	}
	counts := make(map[LabelSemantic]int, len(a.Labels))
	for _, l := range a.Labels {
		counts[l]++
	}
	for _, l := range b.Labels {
		counts[l]--
	}
	for _, n := range counts {
		if n != 0 {
			return false
		}
	}
	return true
}

// contractRegistry records declared metric contracts and validates them
// against each other.
type contractRegistry struct {
	mu        sync.Mutex
	contracts []MetricContract
}

func newContractRegistry() *contractRegistry {
	return &contractRegistry{}
}

func (r *contractRegistry) register(contracts ...MetricContract) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.contracts = append(r.contracts, contracts...)
}

// validate checks all declared contracts pairwise and returns an error for
// every metric name that two enabled collectors would export with different
// label semantics. Contracts of collectors outside enabled are ignored, as
// are suppressed metrics and collisions resolved by an active prefix.
func (r *contractRegistry) validate(enabled map[string]bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for i := range r.contracts {
		for j := i + 1; j < len(r.contracts); j++ {
			a, b := r.contracts[i], r.contracts[j]
			if a.Metric != b.Metric || a.Collector == b.Collector {
				continue
			}
			if !enabled[a.Collector] || !enabled[b.Collector] {
				continue
			}
			if a.isSuppressed() || b.isSuppressed() {
				continue
			}
			if a.resolvedName() != b.resolvedName() {
				continue
			}
			if contractsEquivalent(a, b) {
				continue
			}
			errs = append(errs, &LabelConflictError{Metric: a.Metric, First: a, Second: b})
		}
	}
	return errors.Join(errs...)
}

// labelContracts is the process-wide registry of declared metric label
// contracts. It is populated by registerMetricContract and validated at
// startup by NewNodeCollector.
var labelContracts = newContractRegistry()

// registerMetricContract declares metric label contracts. It is called from
// init functions so that all contracts are recorded before startup
// validation runs.
func registerMetricContract(contracts ...MetricContract) {
	labelContracts.register(contracts...)
}

// validateLabelContracts checks the declared contracts of the given enabled
// collectors. It runs at startup so that label conflicts fail fast instead
// of silently merging series at export time.
func validateLabelContracts(enabledCollectors []string) error {
	enabled := make(map[string]bool, len(enabledCollectors))
	for _, name := range enabledCollectors {
		enabled[name] = true
	}
	if err := labelContracts.validate(enabled); err != nil {
		return fmt.Errorf("metric label contract validation failed: %w", err)
	}
	return nil
}
