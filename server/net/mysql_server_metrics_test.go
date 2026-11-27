package net

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/observability/metrics"
)

func TestMetricsHTTPServerUsesIsolatedLifecycleHandler(t *testing.T) {
	registry := metrics.NewRegistry()
	metrics.RegisterP0Metrics(registry)
	server := newMetricsHTTPServer("127.0.0.1:0", registry)

	getRequest := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	getResponse := httptest.NewRecorder()
	server.Handler.ServeHTTP(getResponse, getRequest)
	if getResponse.Code != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want %d", getResponse.Code, http.StatusOK)
	}
	if !strings.Contains(getResponse.Body.String(), "xmysql_queries_total") {
		t.Fatalf("GET /metrics did not expose the registered runtime metric: %q", getResponse.Body.String())
	}

	postRequest := httptest.NewRequest(http.MethodPost, "/metrics", nil)
	postResponse := httptest.NewRecorder()
	server.Handler.ServeHTTP(postResponse, postRequest)
	if postResponse.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /metrics status = %d, want %d", postResponse.Code, http.StatusMethodNotAllowed)
	}
}

func TestMetricsHTTPServerHealthz(t *testing.T) {
	server := newMetricsHTTPServer("127.0.0.1:0", metrics.NewRegistry())
	getRequest := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	getResponse := httptest.NewRecorder()
	server.Handler.ServeHTTP(getResponse, getRequest)
	if getResponse.Code != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want %d", getResponse.Code, http.StatusOK)
	}
	if getResponse.Body.String() != "ok\n" {
		t.Fatalf("GET /healthz body = %q, want %q", getResponse.Body.String(), "ok\n")
	}

	postRequest := httptest.NewRequest(http.MethodPost, "/healthz", nil)
	postResponse := httptest.NewRecorder()
	server.Handler.ServeHTTP(postResponse, postRequest)
	if postResponse.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /healthz status = %d, want %d", postResponse.Code, http.StatusMethodNotAllowed)
	}
}

func TestMetricsHTTPServerHealthzHonorsReadiness(t *testing.T) {
	ready := false
	server := newMetricsHTTPServerWithReadiness("127.0.0.1:0", metrics.NewRegistry(), func() bool {
		return ready
	})

	notReadyResponse := httptest.NewRecorder()
	server.Handler.ServeHTTP(notReadyResponse, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if notReadyResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /healthz while not ready status = %d, want %d", notReadyResponse.Code, http.StatusServiceUnavailable)
	}

	ready = true
	readyResponse := httptest.NewRecorder()
	server.Handler.ServeHTTP(readyResponse, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if readyResponse.Code != http.StatusOK {
		t.Fatalf("GET /healthz while ready status = %d, want %d", readyResponse.Code, http.StatusOK)
	}
}
