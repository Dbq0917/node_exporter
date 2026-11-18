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

// Command node_exporter_multitarget aggregates the metrics of many remote
// node_exporter instances into one exposition endpoint. Each target is
// scraped with its own timeout, the whole collection round has a configurable
// total time budget, and per-target failures are recorded as error metrics.
//
// The existing single-host node_exporter is unchanged: this is a separate
// binary with its own flags and its own node_multitarget_* self-monitoring
// metrics. Scraped node_* metric names are preserved verbatim.
package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/exporter-toolkit/bootstrap"

	"github.com/prometheus/node_exporter/multitarget"
)

func main() {
	targetsFile := kingpin.Flag(
		"targets.file",
		"Path to the file listing remote node_exporter targets, one per line as \"<name> <url>\".",
	).Default("/etc/node_exporter/multitarget/targets.txt").String()
	targetTimeout := kingpin.Flag(
		"scrape.target-timeout",
		"Maximum duration for scraping a single target. A hung target is aborted after this and counted as a failure without affecting other targets.",
	).Default(multitarget.DefaultTargetTimeout.String()).Duration()
	roundTimeout := kingpin.Flag(
		"scrape.round-timeout",
		"Total time budget for one collection round across all targets. Targets still running when the budget elapses are cancelled and counted as failures. May be smaller than --scrape.target-timeout to cap total round latency.",
	).Default(multitarget.DefaultRoundTimeout.String()).Duration()
	concurrency := kingpin.Flag(
		"scrape.concurrency",
		"Maximum number of targets scraped concurrently.",
	).Default(fmt.Sprintf("%d", multitarget.DefaultConcurrency)).Int()

	kingpin.CommandLine.UsageWriter(os.Stdout)

	runner := bootstrap.New(bootstrap.Config{
		App:            kingpin.CommandLine,
		Name:           "node_exporter_multitarget",
		Description:    "Aggregate metrics from many remote node_exporter targets with per-target and round-wide timeouts.",
		DefaultAddress: ":19101",
		MetricsHandlerFactory: func(b *bootstrap.Bootstrap) (http.Handler, error) {
			targets, err := multitarget.LoadTargetsFile(*targetsFile)
			if err != nil {
				return nil, fmt.Errorf("load targets: %w", err)
			}

			scraper, err := multitarget.NewScraper(multitarget.Config{
				Targets:       targets,
				TargetTimeout: *targetTimeout,
				RoundTimeout:  *roundTimeout,
				Concurrency:   *concurrency,
			}, b.Logger)
			if err != nil {
				return nil, fmt.Errorf("build scraper: %w", err)
			}

			b.Logger.Info("multi-target aggregator starting",
				"targets", len(targets),
				"target_timeout", targetTimeout.String(),
				"round_timeout", roundTimeout.String(),
				"concurrency", *concurrency,
			)

			registry := prometheus.NewRegistry()
			registry.MustRegister(version.NewCollector("node_exporter_multitarget"))
			registry.MustRegister(multitarget.NewCollector(scraper))

			handler := promhttp.HandlerFor(
				registry,
				promhttp.HandlerOpts{
					ErrorLog:      slog.NewLogLogger(b.Logger.Handler(), slog.LevelError),
					ErrorHandling: promhttp.ContinueOnError,
				},
			)
			return promhttp.InstrumentMetricHandler(registry, handler), nil
		},
	})

	if err := runner.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
