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
	"bufio"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
)

// Target describes a single remote node_exporter instance that the
// aggregator scrapes on every collection round.
type Target struct {
	// Name is the stable identifier of the target. It is exposed as the
	// value of the "target" label on every relabeled metric and on all
	// self-monitoring metrics.
	Name string
	// URL is the full metrics URL of the remote node_exporter, for example
	// "http://host-1:9100/metrics".
	URL string
}

// LoadTargetsFile reads targets from path.
//
// One target per line, using the syntax "<name> <url>". Whitespace-separated
// columns are supported. Blank lines and lines starting with "#" are ignored.
// The file is the single source of truth for the set of scraped machines.
func LoadTargetsFile(path string) ([]Target, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open targets file %q: %w", path, err)
	}
	defer f.Close()

	return parseTargets(f)
}

// parseTargets is split out from LoadTargetsFile so callers and tests can
// supply in-memory readers instead of files.
func parseTargets(r io.Reader) ([]Target, error) {
	var targets []Target

	scanner := bufio.NewScanner(r)
	line := 0
	for scanner.Scan() {
		line++
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" || strings.HasPrefix(raw, "#") {
			continue
		}

		fields := strings.Fields(raw)
		if len(fields) != 2 {
			return nil, fmt.Errorf("line %d: expected exactly 2 fields (\"<name> <url>\"), got %d", line, len(fields))
		}

		name := fields[0]
		targetURL := fields[1]
		parsed, err := url.Parse(targetURL)
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid URL %q: %w", line, targetURL, err)
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return nil, fmt.Errorf("line %d: URL %q must use http or https", line, targetURL)
		}
		if parsed.Host == "" {
			return nil, fmt.Errorf("line %d: URL %q is missing a host", line, targetURL)
		}

		targets = append(targets, Target{Name: name, URL: targetURL})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read targets file: %w", err)
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf("targets file contains no targets")
	}

	if err := checkDuplicates(targets); err != nil {
		return nil, err
	}

	return targets, nil
}

func checkDuplicates(targets []Target) error {
	seen := make(map[string]int, len(targets))
	for i, t := range targets {
		if prev, ok := seen[t.Name]; ok {
			return fmt.Errorf("duplicate target name %q on lines %d and %d", t.Name, prev+1, i+1)
		}
		seen[t.Name] = i
	}
	return nil
}
