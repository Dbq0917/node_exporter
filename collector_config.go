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
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"

	"github.com/prometheus/node_exporter/collector"
)

// loadCollectorConfig reads the collector enablement configuration. The file
// contains the complete set of collectors that should be enabled. It accepts
// either a JSON array of names or a line-oriented/YAML list:
//
//	collectors:
//	  - cpu
//	  - meminfo
//
// Blank lines and comments are ignored.
func loadCollectorConfig(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseCollectorConfig(f)
}

func parseCollectorConfig(r io.Reader) ([]string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil, errors.New("collector configuration is empty")
	}

	var names []string
	if strings.HasPrefix(trimmed, "[") {
		if err := json.Unmarshal([]byte(trimmed), &names); err != nil {
			return nil, fmt.Errorf("parse collector configuration: %w", err)
		}
	} else {
		scanner := bufio.NewScanner(strings.NewReader(trimmed))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") || strings.HasSuffix(line, ":") {
				continue
			}
			line = strings.TrimSpace(strings.TrimPrefix(line, "-"))
			if i := strings.Index(line, " #"); i >= 0 {
				line = strings.TrimSpace(line[:i])
			}
			line = strings.Trim(line, `"'`)
			if line == "" {
				continue
			}
			names = append(names, line)
		}
		if err := scanner.Err(); err != nil {
			return nil, err
		}
	}

	if len(names) == 0 {
		return nil, errors.New("collector configuration does not enable any collectors")
	}
	return slices.Compact(slices.Sorted(slices.Values(names))), nil
}

func applyCollectorConfigFile(path string) error {
	names, err := loadCollectorConfig(path)
	if err != nil {
		return err
	}
	return collector.ApplyEnabledCollectors(names)
}

func reloadCollectorConfig(path string, h *handler) error {
	previous := collector.CollectorStates()
	if err := h.ReloadWith(func() error {
		return applyCollectorConfigFile(path)
	}); err != nil {
		// The new configuration could not be built. Put the previous state
		// back so a later filtered scrape cannot observe a half-applied
		// configuration.
		enabled := make([]string, 0, len(previous))
		for name, state := range previous {
			if state {
				enabled = append(enabled, name)
			}
		}
		_ = collector.ApplyEnabledCollectors(enabled)
		collector.PruneCollectors(enabled)
		return err
	}
	return nil
}

func startCollectorConfigReloader(path string, h *handler, logger *slog.Logger) {
	reloadCh := make(chan os.Signal, 1)
	notifyCollectorReload(reloadCh)
	go func() {
		for sig := range reloadCh {
			logger.Info("Reloading collector configuration", "path", path, "signal", sig.String())
			if err := reloadCollectorConfig(path, h); err != nil {
				logger.Error("Failed to reload collector configuration", "path", path, "err", err)
				continue
			}
			logger.Info("Collector configuration reloaded", "path", path)
		}
	}()
}
