package metrics

import (
	"net/http"
)

const prometheusContentType = "text/plain; version=0.0.4; charset=utf-8"

// Handler returns an HTTP handler that exposes registry metrics using the
// Prometheus text exposition format.
func Handler(registry *Registry) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", prometheusContentType)
		_, _ = w.Write([]byte(registry.WritePrometheusText()))
	})
}

// HandlerFunc returns a plain handler function for call sites that prefer
// function registration over http.Handler values.
func HandlerFunc(registry *Registry) http.HandlerFunc {
	return Handler(registry).ServeHTTP
}
