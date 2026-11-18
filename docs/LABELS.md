# Metric label contracts

Two collectors must never export the same metric name with different label
semantics. If they did, the series would be merged into one metric at export
time, values would overwrite each other, and nothing would look wrong on
monitoring. This document defines the merge rules that prevent that, and
records what each shared label means per collector.

The rules are enforced at startup: `collector.NewNodeCollector` validates the
declared metric label contracts of all enabled collectors and refuses to
start on a conflict. Conflicts are never discovered at export time.

The declared contracts live in `collector/label_contracts.go` and
`collector/label_contracts_linux.go` and are the reviewable record backing
the tables below.

## Merge rules

1. **One metric name, one semantics.** A metric name has exactly one help
   string and one set of label semantics. Multiple collectors may export the
   same metric name only if their contracts are identical; the series then
   merge intentionally.
2. **Conflicts fail at startup.** Same metric name, different label
   semantics: `node_exporter` exits with a `LabelConflictError` naming both
   collectors and both label sets. Series are never silently merged.
3. **Resolution: prefix.** The preferred fix for a collision is to give one
   metric its own subsystem prefix (declared as `Prefix` in its contract),
   e.g. `node_cpu_frequency_hertz` vs `node_cpufreq_frequency_hertz`.
4. **Resolution: documented suppression.** If one collector does not export
   its variant under a given configuration, the contract must say so
   (`Suppressed`), so the remaining exporter is unambiguous.
5. **Existing metrics do not change.** Resolutions must not rename existing
   metrics or alter their label sets; only new/conflicting metrics get a
   prefix, and suppression only encodes behavior that already exists.
6. **Shared label names need a contract.** A new metric that reuses a label
   name already used by another collector, or a metric name owned by another
   collector, must declare a `MetricContract` via `registerMetricContract`.

## Historical collision: `node_cpu_frequency_hertz`

`node_cpu_frequency_hertz` is exported by two collectors with different
sources and label sets:

| Collector | Source | Labels | Meaning |
|-----------|--------|--------|---------|
| `cpu` | `/proc/cpuinfo` | `package`, `core`, `cpu` | Frequency per physical core |
| `cpufreq` | `/sys/devices/system/cpu/cpuN/cpufreq` | `cpu` | Current frequency per logical CPU |

Merge rule (rules 3 and 4 above):

- Default: while `cpufreq` is enabled, the `cpu` collector suppresses its
  `/proc/cpuinfo` variant; only the `cpufreq` series is exported.
- With `--collector.cpufreq.enable-cpufreq-prefix`: the `cpufreq` collector
  exports `node_cpufreq_frequency_hertz` (and `node_cpufreq_*` generally) and
  both metrics coexist. This will be the default in 2.x.

## Label mapping table

Label names shared between collectors, and what each one means. The same
label name on different metrics may identify completely different things.

### `device`

| Collector | Metric | Meaning |
|-----------|--------|---------|
| `arp` | `node_arp_entries` | Network interface name from `/proc/net/arp` |
| `diskstats` | `node_disk_reads_completed_total` (and `node_disk_*`) | Block device name from `/proc/diskstats` |
| `netdev` | `node_network_receive_bytes_total` (and `node_network_*`) | Network interface name from `/proc/net/dev` or rtnetlink |
| `ethtool` | `node_ethtool_received_bytes_total` (and `node_ethtool_*`, `node_network_*_info`) | Network interface name from ethtool |
| `wifi` | `node_wifi_station_info` (and `node_wifi_*`) | Wireless network interface name |
| `bcachefs` | `node_bcachefs_device_info` (and `node_bcachefs_device_*`) | Member device path of the bcachefs filesystem |
| `btrfs` | `node_btrfs_device_size_bytes` (and `node_btrfs_device_*`) | Member device name of the btrfs filesystem |

### `mode`

| Collector | Metric | Meaning |
|-----------|--------|---------|
| `cpu` | `node_cpu_seconds_total` | CPU mode from `/proc/stat` (`user`, `system`, `idle`, ...) |
| `ethtool` | `node_network_supported_speed_bytes`, `node_network_advertised_speed_bytes` | Supported/advertised link mode (e.g. `1000baseT/Full`) |
| `wifi` | `node_wifi_station_info` | WiFi PHY mode of the station (e.g. 802.11n) |
| `btrfs` | `node_btrfs_used_bytes` (and other `node_btrfs_*` layout metrics) | Btrfs RAID profile of the block group (`single`, `raid1`, ...) |

### `cpu`

| Collector | Metric | Meaning |
|-----------|--------|---------|
| `cpu` | `node_cpu_seconds_total`, `node_cpu_frequency_hertz` | Logical CPU number from `/proc/stat` resp. `/proc/cpuinfo` |
| `cpufreq` | `node_cpu_frequency_hertz` / `node_cpufreq_frequency_hertz` | Logical CPU number from `/sys/devices/system/cpu/cpuN/cpufreq` |
