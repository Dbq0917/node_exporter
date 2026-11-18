// Copyright 2015 The Prometheus Authors
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

//go:build !notextfile

package collector

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

var (
	textFileDirectories = kingpin.Flag("collector.textfile.directory", "Directory to read text files with metrics from, supports glob matching. (repeatable)").Default("").Strings()
	mtimeDesc           = prometheus.NewDesc(
		"node_textfile_mtime_seconds",
		"Unixtime mtime of textfiles successfully read.",
		[]string{"file"},
		nil,
	)
	// quarantineDirName stores snapshots of files that could not be parsed.
	// It is a hidden directory without a .prom suffix, so it is never scraped
	// itself and the configured collector paths stay unchanged.
	quarantineDirName = ".textfile-quarantine"
	// quarantineMaxSamplesPerFile keeps one bounded audit trail per source
	// file so that repeatedly failing business metrics cannot fill the disk.
	quarantineMaxSamplesPerFile = 5
	// readSnapshotAttempts bounds retries when a file changes while it is read.
	readSnapshotAttempts = 10
	readSnapshotBackoff  = 20 * time.Millisecond

	// parseTextfile is a function variable so tests can simulate a parser
	// panic (the expfmt state machine has defensive panic paths) and verify
	// the per-file recovery behavior deterministically.
	parseTextfile = func(data []byte) (map[string]*dto.MetricFamily, error) {
		parser := expfmt.NewTextParser(model.UTF8Validation)
		return parser.TextToMetricFamilies(bytes.NewReader(data))
	}
)

type textFileCollector struct {
	paths []string
	// Only set for testing to get predictable output.
	mtime  *float64
	logger *slog.Logger
}

// validTextfileData is a complete, parseable exposition payload used by tests
// that exercise concurrent writes.
var validTextfileData = []byte(`# HELP business_metric A metric written by the business.
# TYPE business_metric gauge
business_metric 1
`)

func init() {
	registerCollector("textfile", defaultEnabled, NewTextFileCollector)
}

// NewTextFileCollector returns a new Collector exposing metrics read from files
// in the given textfile directory.
func NewTextFileCollector(logger *slog.Logger) (Collector, error) {
	c := &textFileCollector{
		paths:  *textFileDirectories,
		logger: logger,
	}
	return c, nil
}

func convertMetricFamily(metricFamily *dto.MetricFamily, ch chan<- prometheus.Metric, logger *slog.Logger) {
	var valType prometheus.ValueType
	var val float64

	allLabelNames := map[string]struct{}{}
	for _, metric := range metricFamily.Metric {
		labels := metric.GetLabel()
		for _, label := range labels {
			if _, ok := allLabelNames[label.GetName()]; !ok {
				allLabelNames[label.GetName()] = struct{}{}
			}
		}
	}

	for _, metric := range metricFamily.Metric {
		if metric.TimestampMs != nil {
			logger.Warn("Ignoring unsupported custom timestamp on textfile collector metric", "metric", metric)
		}

		labels := metric.GetLabel()
		var names []string
		var values []string
		for _, label := range labels {
			names = append(names, label.GetName())
			values = append(values, label.GetValue())
		}

		for k := range allLabelNames {
			if !slices.Contains(names, k) {
				names = append(names, k)
				values = append(values, "")
			}
		}

		metricType := metricFamily.GetType()
		switch metricType {
		case dto.MetricType_COUNTER:
			valType = prometheus.CounterValue
			val = metric.Counter.GetValue()

		case dto.MetricType_GAUGE:
			valType = prometheus.GaugeValue
			val = metric.Gauge.GetValue()

		case dto.MetricType_UNTYPED:
			valType = prometheus.UntypedValue
			val = metric.Untyped.GetValue()

		case dto.MetricType_SUMMARY:
			quantiles := map[float64]float64{}
			for _, q := range metric.Summary.Quantile {
				quantiles[q.GetQuantile()] = q.GetValue()
			}
			ch <- prometheus.MustNewConstSummary(
				prometheus.NewDesc(
					*metricFamily.Name,
					metricFamily.GetHelp(),
					names, nil,
				),
				metric.Summary.GetSampleCount(),
				metric.Summary.GetSampleSum(),
				quantiles, values...,
			)
		case dto.MetricType_HISTOGRAM:
			buckets := map[float64]uint64{}
			for _, b := range metric.Histogram.Bucket {
				buckets[b.GetUpperBound()] = b.GetCumulativeCount()
			}
			ch <- prometheus.MustNewConstHistogram(
				prometheus.NewDesc(
					*metricFamily.Name,
					metricFamily.GetHelp(),
					names, nil,
				),
				metric.Histogram.GetSampleCount(),
				metric.Histogram.GetSampleSum(),
				buckets, values...,
			)
		default:
			panic("unknown metric type")
		}
		if metricType == dto.MetricType_GAUGE || metricType == dto.MetricType_COUNTER || metricType == dto.MetricType_UNTYPED {
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(
					*metricFamily.Name,
					metricFamily.GetHelp(),
					names, nil,
				),
				valType, val, values...,
			)
		}
	}
}

func (c *textFileCollector) exportMTimes(mtimes map[string]time.Time, ch chan<- prometheus.Metric) {
	if len(mtimes) == 0 {
		return
	}

	// Export the mtimes of the successful files.
	// Sorting is needed for predictable output comparison in tests.
	filepaths := make([]string, 0, len(mtimes))
	for path := range mtimes {
		filepaths = append(filepaths, path)
	}
	sort.Strings(filepaths)

	for _, path := range filepaths {
		mtime := float64(mtimes[path].UnixNano() / 1e9)
		if c.mtime != nil {
			mtime = *c.mtime
		}
		ch <- prometheus.MustNewConstMetric(mtimeDesc, prometheus.GaugeValue, mtime, path)
	}
}

// Update implements the Collector interface.
func (c *textFileCollector) Update(ch chan<- prometheus.Metric) error {
	// Iterate over files and accumulate their metrics, but also track any
	// parsing errors so an error metric can be reported.
	var errored bool
	var parsedFamilies []*dto.MetricFamily
	metricsNamesToFiles := map[string][]string{}
	metricsNamesToHelpTexts := map[string][2]string{}

	paths := []string{}
	for _, glob := range c.paths {
		ps, err := filepath.Glob(glob)
		if err != nil || len(ps) == 0 {
			// not glob or not accessible path either way assume single
			// directory and let os.ReadDir handle it
			ps = []string{glob}
		}
		paths = append(paths, ps...)
	}

	mtimes := make(map[string]time.Time)
	for _, path := range paths {
		files, err := os.ReadDir(path)
		if err != nil && path != "" {
			errored = true
			c.logger.Error("failed to read textfile collector directory", "path", path, "err", err)
		}

		for _, f := range files {
			if f.IsDir() {
				// The quarantine directory lives inside each configured
				// directory but is never scraped; do not log it as an error.
				if f.Name() == quarantineDirName {
					continue
				}
				continue
			}
			metricsFilePath := filepath.Join(path, f.Name())
			if !strings.HasSuffix(f.Name(), ".prom") {
				continue
			}

			mtime, families, rawData, shouldQuarantine, err := c.processFile(path, f.Name())

			for _, mf := range families {
				// Check for metrics with inconsistent help texts and take the first help text occurrence.
				if helpTexts, seen := metricsNamesToHelpTexts[*mf.Name]; seen {
					if mf.Help != nil && helpTexts[0] != *mf.Help || helpTexts[1] != "" {
						metricsNamesToHelpTexts[*mf.Name] = [2]string{helpTexts[0], *mf.Help}
						errored = true
						c.logger.Error("inconsistent metric help text",
							"metric", *mf.Name,
							"original_help_text", helpTexts[0],
							"new_help_text", *mf.Help,
							// Only the first file path will be recorded in case of two or more inconsistent help texts.
							"file", metricsNamesToFiles[*mf.Name][0])
						continue
					}
				}
				if mf.Help != nil {
					metricsNamesToHelpTexts[*mf.Name] = [2]string{*mf.Help}
				}
				metricsNamesToFiles[*mf.Name] = append(metricsNamesToFiles[*mf.Name], metricsFilePath)
				parsedFamilies = append(parsedFamilies, mf)
			}

			if err != nil {
				errored = true
				logArgs := []any{"file", metricsFilePath, "err", err}
				if shouldQuarantine {
					samplePath, quarantineErr := quarantineBadFile(path, f.Name(), rawData, err)
					if quarantineErr != nil {
						logArgs = append(logArgs, "quarantine_err", quarantineErr)
					} else if samplePath != "" {
						logArgs = append(logArgs, "quarantined", samplePath)
					}
				}
				c.logger.Error("failed to collect textfile data", logArgs...)
				continue
			}

			mtimes[metricsFilePath] = *mtime
		}
	}

	mfHelp := make(map[string]*string)
	for _, mf := range parsedFamilies {
		if mf.Help == nil {
			if help, ok := mfHelp[*mf.Name]; ok {
				mf.Help = help
				continue
			}
			help := fmt.Sprintf("Metric read from %s", strings.Join(metricsNamesToFiles[*mf.Name], ", "))
			mf.Help = &help
			mfHelp[*mf.Name] = &help
		}
	}

	for _, mf := range parsedFamilies {
		convertMetricFamily(mf, ch, c.logger)
	}

	c.exportMTimes(mtimes, ch)

	// Export if there were errors.
	var errVal float64
	if errored {
		errVal = 1.0
	}

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(
			"node_textfile_scrape_error",
			"1 if there was an error opening or reading a file, 0 otherwise",
			nil, nil,
		),
		prometheus.GaugeValue, errVal,
	)

	return nil
}

// processFile processes a single file, returning its modification time on
// success. The file is fully snapshotted before parsing so that a concurrent
// rewrite can never expose a half-written file to the parser. On failure the
// raw snapshot is returned as well, so the caller can preserve it for
// inspection when quarantine is true (parse failures and parser panics).
// Policy rejections such as client-side timestamps parse fine and are not
// quarantined. A failing file never affects any other file in the directory.
func (c *textFileCollector) processFile(dir, name string) (mtime *time.Time, families map[string]*dto.MetricFamily, rawData []byte, quarantine bool, err error) {
	path := filepath.Join(dir, name)

	// The expfmt parser can panic on malformed input. Recover here so that one
	// bad file cannot take down the whole scrape; the panic is turned into an
	// ordinary per-file error and the snapshot is quarantined by the caller.
	defer func() {
		if r := recover(); r != nil {
			mtime = nil
			families = nil
			quarantine = true
			err = fmt.Errorf("panic while processing textfile data file %q: %v", path, r)
		}
	}()

	// Retry the whole "snapshot + parse" cycle while the file is changing
	// underneath us. A snapshot that parses successfully is by definition a
	// complete, consistent view (old or new content), so the exporter never
	// emits metrics derived from a half-written file. A snapshot that fails to
	// parse is only treated as corruption when the file was stable while it
	// was read; when the file was still changing, the failure is attributed to
	// the in-progress write and the next attempt reads the file again. Because
	// every scrape re-reads the directory, a repaired file is picked up
	// automatically without a restart.
	var lastErr error
	parsed := false
	for attempt := range readSnapshotAttempts {
		if attempt > 0 {
			time.Sleep(readSnapshotBackoff)
		}

		var readChanged bool
		rawData, _, readChanged, err = readFileSnapshot(path)
		if err != nil {
			return nil, nil, nil, false, err
		}

		families, err = parseTextfile(rawData)
		if err != nil && !readChanged {
			// Stable bytes that still do not parse are genuinely corrupt:
			// stop retrying and let the caller quarantine this file.
			return nil, nil, rawData, true, fmt.Errorf("failed to parse textfile data from %q: %w", path, err)
		}
		if err != nil {
			// The file changed during the read and the partial snapshot did
			// not parse; retry in the hope of catching a complete version.
			lastErr = err
			continue
		}
		if readChanged {
			// The snapshot parses, but the file moved underneath us while we
			// read it, so the bytes cannot be trusted to represent one
			// consistent version. Retry for a stable snapshot.
			lastErr = fmt.Errorf("textfile data file %q changed while being read", path)
			continue
		}

		// Stable snapshot that parses successfully: this is a complete,
		// consistent view and is safe to export.
		lastErr = nil
		parsed = true
		break
	}
	if !parsed {
		return nil, nil, rawData, true, fmt.Errorf("failed to parse textfile data from %q after %d attempts: %w", path, readSnapshotAttempts, lastErr)
	}

	if hasTimestamps(families) {
		return nil, nil, rawData, false, fmt.Errorf("textfile %q contains unsupported client-side timestamps, skipping entire file", path)
	}

	// Only stat the file once it has been parsed and validated, so that
	// a failure does not appear fresh. Stat the path rather than the snapshot
	// so the reported mtime reflects the file currently on disk.
	stat, err := os.Stat(path)
	if err != nil {
		return nil, families, rawData, false, fmt.Errorf("failed to stat %q: %w", path, err)
	}

	t := stat.ModTime()
	return &t, families, rawData, false, nil
}

// readFileSnapshot reads a full, point-in-time copy of the file and reports
// whether the file changed while the read was in flight.
//
// Stat alone cannot detect a truncate-rewrite reliably: after O_TRUNC the
// descriptor's reported size already reflects bytes being written (sparse
// holes keep the size growing), so size and mtime can look stable while the
// content underneath a completed read is still changing. Instead, after
// reading, the file is re-opened and its bytes are compared with the
// snapshot. Any mismatch (partial rewrite, atomic rename swap, append) is
// reported as changed so the caller retries. Once the writer finishes, the
// next attempt gets the complete new content, so recovery is automatic on
// the following scrape without a restart.
func readFileSnapshot(path string) ([]byte, os.FileInfo, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to open textfile data file %q: %w", path, err)
	}

	stat, statErr := f.Stat()
	data, readErr := io.ReadAll(f)
	closeErr := f.Close()

	switch {
	case statErr != nil:
		return nil, nil, false, fmt.Errorf("failed to stat textfile data file %q: %w", path, statErr)
	case readErr != nil:
		return nil, nil, false, fmt.Errorf("failed to read textfile data file %q: %w", path, readErr)
	case closeErr != nil:
		return nil, nil, false, fmt.Errorf("failed to close textfile data file %q: %w", path, closeErr)
	}

	// Re-open via the directory entry and compare bytes. A different inode
	// (rename swap) or different content means the snapshot is not a stable
	// point-in-time view.
	verifyFile, err := os.Open(path)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to re-open textfile data file %q for verification: %w", path, err)
	}
	verifyStat, err := verifyFile.Stat()
	verifyData, readErr := io.ReadAll(verifyFile)
	_ = verifyFile.Close()
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to stat textfile data file %q during verification: %w", path, err)
	}
	if readErr != nil {
		return nil, nil, false, fmt.Errorf("failed to verify textfile data file %q: %w", path, readErr)
	}

	changed := !os.SameFile(stat, verifyStat) || !bytes.Equal(data, verifyData)
	// A complete exposition payload ends with a newline on its last line.
	// A snapshot missing the trailing newline was cut off while the writer
	// was still appending lines; on filesystems like tmpfs the truncate
	// already exposes the final size, so size/mtime checks cannot catch this.
	if !changed && len(data) > 0 && data[len(data)-1] != '\n' {
		changed = true
	}
	return data, stat, changed, nil
}

// quarantineBadFile stores a copy of an unparseable textfile together with a
// record explaining the failure, under <dir>/.textfile-quarantine/. The
// quarantine directory contains no *.prom files, so it is never scraped, and
// no existing path or format configuration is changed. Retained samples are
// capped per source file. The returned path is empty when there is no data
// to preserve (for example when the file could not be opened).
func quarantineBadFile(dir, name string, data []byte, parseErr error) (string, error) {
	if len(data) == 0 {
		return "", nil
	}

	quarantineDir := filepath.Join(dir, quarantineDirName)
	if err := os.MkdirAll(quarantineDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create quarantine directory %q: %w", quarantineDir, err)
	}

	timestamp := time.Now().UTC().Format("20060102T150405.000000000")
	base := filepath.Base(name)
	samplePath := filepath.Join(quarantineDir, base+"."+timestamp+".bad")
	if err := os.WriteFile(samplePath, data, 0o644); err != nil {
		return "", fmt.Errorf("failed to write quarantined sample %q: %w", samplePath, err)
	}

	recordPath := samplePath + ".reason"
	record := fmt.Sprintf("source: %s\nquarantined_at: %s\nerror: %s\n",
		filepath.Join(dir, name), timestamp, parseErr.Error())
	if err := os.WriteFile(recordPath, []byte(record), 0o644); err != nil {
		return samplePath, fmt.Errorf("failed to write quarantine record %q: %w", recordPath, err)
	}

	pruneQuarantine(quarantineDir, base)
	return samplePath, nil
}

// pruneQuarantine keeps only the newest quarantineMaxSamplesPerFile samples
// (and their .reason records) for one source file.
func pruneQuarantine(quarantineDir, sourceBase string) {
	prefix := sourceBase + "."
	entries, err := os.ReadDir(quarantineDir)
	if err != nil {
		return
	}

	var samples []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		entryName := entry.Name()
		if strings.HasPrefix(entryName, prefix) && strings.HasSuffix(entryName, ".bad") {
			samples = append(samples, entryName)
		}
	}

	sort.Strings(samples)
	if len(samples) <= quarantineMaxSamplesPerFile {
		return
	}
	for _, stale := range samples[:len(samples)-quarantineMaxSamplesPerFile] {
		_ = os.Remove(filepath.Join(quarantineDir, stale))
		_ = os.Remove(filepath.Join(quarantineDir, stale+".reason"))
	}
}

// hasTimestamps returns true when metrics contain unsupported timestamps.
func hasTimestamps(parsedFamilies map[string]*dto.MetricFamily) bool {
	for _, mf := range parsedFamilies {
		for _, m := range mf.Metric {
			if m.TimestampMs != nil {
				return true
			}
		}
	}
	return false
}
