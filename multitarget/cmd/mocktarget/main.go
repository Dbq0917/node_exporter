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

// Command mocktarget is a tiny mock node_exporter used by the end-to-end
// verification script. It is test-only tooling and not shipped as a product
// binary.
package main

import (
	"flag"
	"fmt"
	"net/http"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "address to listen on")
	name := flag.String("name", "mock", "target name embedded in the node_up metric")
	blackhole := flag.Bool("blackhole", false, "never answer requests to simulate a hung target")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if *blackhole {
			<-r.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintln(w, "# HELP node_up test metric from mock target")
		fmt.Fprintln(w, "# TYPE node_up gauge")
		fmt.Fprintf(w, "node_up{instance=%q} 1\n", *name)
	})

	server := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		panic(err)
	}
}
