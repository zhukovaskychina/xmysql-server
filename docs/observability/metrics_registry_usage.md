# Metrics Registry Usage

## Purpose

`server/observability/metrics` provides a small in-process metrics registry for P0-D observability work.

It supports:

- counters,
- gauges,
- histograms,
- Prometheus text exposition output,
- registration of the minimum P0-D metric catalog.

## Example

```go
registry := metrics.NewRegistry()
metrics.RegisterP0Metrics(registry)

_ = registry.IncCounter("xmysql_queries_total", 1, metrics.Labels{
    "database": "test",
    "status": "ok",
})

text := registry.WritePrometheusText()
```

## Current boundary

This package is a metrics export foundation. P0-D is not complete until runtime server paths update the registry and a production-consumable export path is exposed or archived as evidence.

## Focused P0-D live metrics endpoint evidence

`cmd/p0_metrics_endpoint` exposes the P0 metric catalog over HTTP for focused endpoint probing. The wrapper script starts the endpoint, waits for `/healthz`, runs the existing `/metrics` probe, verifies the probe report, and stops the endpoint:

```powershell
./scripts/generate_p0d_metrics_endpoint_evidence.ps1 -ReportDir reports
```

The delivery audit consumes the generated `metrics_endpoint_probe_<timestamp>.json` as `-P0DMetricsEndpointJson`.

The full P0 evidence suite can generate this endpoint probe automatically:

```powershell
./scripts/run_p0_evidence_suite.ps1 -GenerateP0DMetricsEndpointEvidence
```

Scope boundary: this focused endpoint proves `/metrics` reachability and required P0 metric-name presence for a local metrics endpoint. It does not prove the full XMySQL server process updates every metric under production traffic.

