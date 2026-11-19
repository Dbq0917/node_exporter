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

package main

import (
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/user"
	"runtime"
	"slices"
	"sort"
	"sync"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
	promcollectors "github.com/prometheus/client_golang/prometheus/collectors"
	versioncollector "github.com/prometheus/client_golang/prometheus/collectors/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/exporter-toolkit/bootstrap"

	"github.com/prometheus/node_exporter/collector"
)

// handler wraps an unfiltered http.Handler but uses a filtered handler,
// created on the fly, if filtering is requested. Create instances with
// newHandler.
type handler struct {
	mu       sync.RWMutex
	reloadMu sync.RWMutex

	unfilteredHandler http.Handler
	// enabledCollectors list is used for logging and filtering
	enabledCollectors []string
	// exporterMetricsRegistry is a separate registry for the metrics about
	// the exporter itself.
	exporterMetricsRegistry *prometheus.Registry
	includeExporterMetrics  bool
	maxRequests             int
	logger                  *slog.Logger
}

func newHandler(includeExporterMetrics bool, maxRequests int, logger *slog.Logger) *handler {
	h := &handler{
		exporterMetricsRegistry: prometheus.NewRegistry(),
		includeExporterMetrics:  includeExporterMetrics,
		maxRequests:             maxRequests,
		logger:                  logger,
	}
	if h.includeExporterMetrics {
		h.exporterMetricsRegistry.MustRegister(
			promcollectors.NewProcessCollector(promcollectors.ProcessCollectorOpts{}),
			promcollectors.NewGoCollector(),
		)
	}
	innerHandler, err := h.innerHandler()
	if err != nil {
		panic(fmt.Sprintf("Couldn't create metrics handler: %s", err))
	}
	h.unfilteredHandler = innerHandler
	return h
}

// ServeHTTP implements http.Handler.
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	collects := r.URL.Query()["collect[]"]
	h.logger.Debug("collect query:", "collects", collects)

	excludes := r.URL.Query()["exclude[]"]
	h.logger.Debug("exclude query:", "excludes", excludes)

	h.mu.RLock()
	unfilteredHandler := h.unfilteredHandler
	enabledCollectors := append([]string(nil), h.enabledCollectors...)
	h.mu.RUnlock()

	if len(collects) == 0 && len(excludes) == 0 {
		// No filters, use the prepared unfiltered handler.
		unfilteredHandler.ServeHTTP(w, r)
		return
	}

	if len(collects) > 0 && len(excludes) > 0 {
		h.logger.Debug("rejecting combined collect and exclude queries")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Combined collect and exclude queries are not allowed."))
		return
	}

	filters := &collects
	if len(excludes) > 0 {
		// In exclude mode, filtered collectors = enabled - excludeed.
		f := []string{}
		for _, c := range enabledCollectors {
			if slices.Index(excludes, c) == -1 {
				f = append(f, c)
			}
		}
		filters = &f
	}

	// To serve filtered metrics, we create a filtering handler on the fly.
	filteredHandler, err := h.innerHandler(*filters...)
	if err != nil {
		h.logger.Warn("Couldn't create filtered metrics handler:", "err", err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Couldn't create filtered metrics handler: %s", err)
		return
	}
	filteredHandler.ServeHTTP(w, r)
}

// innerHandler is used to create both the one unfiltered http.Handler to be
// wrapped by the outer handler and also the filtered handlers created on the
// fly. The former is accomplished by calling innerHandler without any arguments
// (in which case it will log all the collectors enabled via command-line
// flags).
func (h *handler) innerHandler(filters ...string) (http.Handler, error) {
	h.reloadMu.RLock()
	defer h.reloadMu.RUnlock()

	handler, enabled, err := h.buildHandler(filters...)
	if err != nil {
		return nil, err
	}

	if len(filters) == 0 {
		h.setUnfilteredHandler(handler, enabled)
	}

	return handler, nil
}

// Reload builds a complete collector registry from the current collector
// configuration and atomically swaps it in. Scrapes already in flight keep
// using the previous registry until they finish, and new scrapes see either
// the complete old configuration or the complete new one.
func (h *handler) Reload() error {
	return h.ReloadWith(nil)
}

// ReloadWith applies configure and builds the replacement registry while
// holding the reload lock. Filtered handler construction waits for the swap,
// so it cannot observe a configuration that is not active yet.
func (h *handler) ReloadWith(configure func() error) error {
	h.reloadMu.Lock()
	defer h.reloadMu.Unlock()

	if configure != nil {
		if err := configure(); err != nil {
			return err
		}
	}
	handler, enabled, err := h.buildHandler()
	if err != nil {
		return err
	}
	h.setUnfilteredHandler(handler, enabled)
	return nil
}

func (h *handler) setUnfilteredHandler(handler http.Handler, enabled []string) {
	h.mu.Lock()
	h.unfilteredHandler = handler
	h.enabledCollectors = enabled
	h.mu.Unlock()

	// Collectors which are no longer enabled are not referenced by the new
	// registry. Drop them from the shared cache so they can be garbage
	// collected once any in-flight scrape using them finishes.
	collector.PruneCollectors(enabled)

	h.logger.Info("Enabled collectors")
	for _, c := range enabled {
		h.logger.Info(c)
	}
}

func (h *handler) buildHandler(filters ...string) (http.Handler, []string, error) {
	nc, err := collector.NewNodeCollector(h.logger, filters...)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't create collector: %s", err)
	}

	enabled := make([]string, 0, len(nc.Collectors))
	for n := range nc.Collectors {
		enabled = append(enabled, n)
	}
	sort.Strings(enabled)

	r := prometheus.NewRegistry()
	r.MustRegister(versioncollector.NewCollector("node_exporter"))
	if err := r.Register(nc); err != nil {
		return nil, nil, fmt.Errorf("couldn't register node collector: %s", err)
	}

	var handler http.Handler
	if h.includeExporterMetrics {
		handler = promhttp.HandlerFor(
			prometheus.Gatherers{h.exporterMetricsRegistry, r},
			promhttp.HandlerOpts{
				ErrorLog:            slog.NewLogLogger(h.logger.Handler(), slog.LevelError),
				ErrorHandling:       promhttp.ContinueOnError,
				MaxRequestsInFlight: h.maxRequests,
				Registry:            h.exporterMetricsRegistry,
			},
		)
		// Note that we have to use h.exporterMetricsRegistry here to
		// use the same promhttp metrics for all expositions.
		handler = promhttp.InstrumentMetricHandler(
			h.exporterMetricsRegistry, handler,
		)
	} else {
		handler = promhttp.HandlerFor(
			r,
			promhttp.HandlerOpts{
				ErrorLog:            slog.NewLogLogger(h.logger.Handler(), slog.LevelError),
				ErrorHandling:       promhttp.ContinueOnError,
				MaxRequestsInFlight: h.maxRequests,
			},
		)
	}

	return handler, enabled, nil
}

func main() {
	var (
		disableDefaultCollectors = kingpin.Flag(
			"collector.disable-defaults",
			"Set all collectors to disabled by default.",
		).Default("false").Bool()
		maxProcs = kingpin.Flag(
			"runtime.gomaxprocs", "The target number of CPUs Go will run on (GOMAXPROCS)",
		).Envar("GOMAXPROCS").Default("1").Int()
		collectorConfigFile = kingpin.Flag(
			"collector.config.file",
			"Path to a file listing the collectors to enable. Reloaded on SIGHUP.",
		).String()
	)

	kingpin.CommandLine.UsageWriter(os.Stdout)
	runner := bootstrap.New(bootstrap.Config{
		App:            kingpin.CommandLine,
		Name:           "node_exporter",
		Description:    "Prometheus Node Exporter",
		DefaultAddress: ":9100",
		MetricsHandlerFactory: func(b *bootstrap.Bootstrap) (http.Handler, error) {
			if *disableDefaultCollectors {
				collector.DisableDefaultCollectors()
			}
			if user, err := user.Current(); err == nil && user.Uid == "0" {
				b.Logger.Warn("Node Exporter is running as root user. This exporter is designed to run as unprivileged user, root is not required.")
			}
			if *collectorConfigFile != "" {
				if err := applyCollectorConfigFile(*collectorConfigFile); err != nil {
					return nil, fmt.Errorf("couldn't load collector configuration: %w", err)
				}
			}
			runtime.GOMAXPROCS(*maxProcs)
			b.Logger.Debug("Go MAXPROCS", "procs", runtime.GOMAXPROCS(0))
			b.Handle("/debug/pprof/", http.DefaultServeMux)
			h := newHandler(!b.DisableExporterMetrics, b.MaxRequests, b.Logger)
			if *collectorConfigFile != "" {
				startCollectorConfigReloader(*collectorConfigFile, h, b.Logger)
			}
			return h, nil
		},
	})

	if err := runner.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
