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

// This file is the declared label contract table: the reviewable record of
// which collector attaches which labels to which metric, and what each label
// means. It covers every label name that is shared between collectors with
// different meanings, plus the historical metric name collision between the
// cpu and cpufreq collectors. docs/LABELS.md mirrors this table.
//
// New metrics that reuse a shared label name, or a metric name owned by
// another collector, must add a contract here.

func init() {
	registerMetricContract(
		// Label "device": a block device, a network interface or a
		// filesystem member device, depending on the collector.
		MetricContract{
			Collector: "arp",
			Metric:    "node_arp_entries",
			Help:      "ARP entries by device",
			Labels: []LabelSemantic{
				{Name: "device", Meaning: "Network interface name from /proc/net/arp"},
			},
		},
		MetricContract{
			Collector: "diskstats",
			Metric:    "node_disk_reads_completed_total",
			Help:      "The total number of reads completed successfully.",
			Labels: []LabelSemantic{
				{Name: "device", Meaning: "Block device name from /proc/diskstats"},
			},
		},
		MetricContract{
			Collector: "netdev",
			Metric:    "node_network_receive_bytes_total",
			Help:      "Network device statistic receive_bytes.",
			Labels: []LabelSemantic{
				{Name: "device", Meaning: "Network interface name from /proc/net/dev or rtnetlink"},
			},
		},
		MetricContract{
			Collector: "ethtool",
			Metric:    "node_ethtool_received_bytes_total",
			Help:      "Network interface bytes received",
			Labels: []LabelSemantic{
				{Name: "device", Meaning: "Network interface name from ethtool"},
			},
		},
		MetricContract{
			Collector: "wifi",
			Metric:    "node_wifi_station_info",
			Help:      "Labeled WiFi interface station information as provided by the operating system.",
			Labels: []LabelSemantic{
				{Name: "device", Meaning: "Wireless network interface name"},
				{Name: "bssid", Meaning: "BSSID of the associated access point"},
				{Name: "ssid", Meaning: "SSID of the associated network"},
				{Name: "mode", Meaning: "WiFi PHY mode of the station (e.g. 802.11n)"},
			},
		},
		MetricContract{
			Collector: "bcachefs",
			Metric:    "node_bcachefs_device_info",
			Help:      "Device information.",
			Labels: []LabelSemantic{
				{Name: "uuid", Meaning: "UUID of the bcachefs filesystem"},
				{Name: "device", Meaning: "Member device path of the bcachefs filesystem"},
				{Name: "label", Meaning: "Label of the bcachefs member device"},
				{Name: "state", Meaning: "State of the bcachefs member device"},
			},
		},
		MetricContract{
			Collector: "btrfs",
			Metric:    "node_btrfs_device_size_bytes",
			Help:      "Size of a device that is part of the filesystem.",
			Labels: []LabelSemantic{
				{Name: "device", Meaning: "Member device name of the btrfs filesystem"},
			},
		},

		// Label "mode": CPU mode, network link mode or btrfs RAID profile,
		// depending on the collector.
		MetricContract{
			Collector: "cpu",
			Metric:    "node_cpu_seconds_total",
			Help:      "Seconds the CPUs spent in each mode.",
			Labels: []LabelSemantic{
				{Name: "cpu", Meaning: "Logical CPU number"},
				{Name: "mode", Meaning: "CPU mode from /proc/stat (user, system, idle, ...)"},
			},
		},
		MetricContract{
			Collector: "ethtool",
			Metric:    "node_network_supported_speed_bytes",
			Help:      "Combination of speeds and features supported by network device",
			Labels: []LabelSemantic{
				{Name: "device", Meaning: "Network interface name from ethtool"},
				{Name: "duplex", Meaning: "Duplex mode of the link (e.g. Full)"},
				{Name: "mode", Meaning: "Supported link mode (e.g. 1000baseT/Full)"},
			},
		},
		MetricContract{
			Collector: "btrfs",
			Metric:    "node_btrfs_used_bytes",
			Help:      "Amount of used space by a layout/data type",
			Labels: []LabelSemantic{
				{Name: "block_group_type", Meaning: "Btrfs block group type (data, metadata, system)"},
				{Name: "mode", Meaning: "Btrfs RAID profile of the block group (single, raid1, ...)"},
			},
		},
	)
}
