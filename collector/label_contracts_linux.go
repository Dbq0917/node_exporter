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

//go:build !nocpu

package collector

// node_cpu_frequency_hertz is the historical metric name collision between
// the cpu and cpufreq collectors: same metric name, different sources and
// different label sets. The documented merge rule (see docs/LABELS.md):
//
//   - Default: the cpu collector suppresses its /proc/cpuinfo variant while
//     the cpufreq collector is enabled without a metric prefix.
//   - With --collector.cpufreq.enable-cpufreq-prefix: the cpufreq collector
//     exports node_cpufreq_frequency_hertz and both metrics coexist.
func init() {
	registerMetricContract(
		MetricContract{
			Collector: "cpu",
			Metric:    "node_cpu_frequency_hertz",
			Help:      "CPU frequency in hertz from /proc/cpuinfo.",
			Labels: []LabelSemantic{
				{Name: "package", Meaning: "CPU package (socket) ID from /proc/cpuinfo"},
				{Name: "core", Meaning: "CPU core ID within the package from /proc/cpuinfo"},
				{Name: "cpu", Meaning: "Logical CPU number from /proc/cpuinfo"},
			},
			Suppressed: func() bool {
				enabled, ok := collectorState["cpufreq"]
				return ok && enabled != nil && *enabled && !*useCPUFreqPrefix
			},
		},
		MetricContract{
			Collector: "cpufreq",
			Metric:    "node_cpu_frequency_hertz",
			Help:      "Current CPU thread frequency in hertz.",
			Labels: []LabelSemantic{
				{Name: "cpu", Meaning: "Logical CPU number from /sys/devices/system/cpu/cpuN/cpufreq"},
			},
			Prefix:       "cpufreq",
			PrefixActive: func() bool { return *useCPUFreqPrefix },
		},
	)
}
