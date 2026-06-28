# Metrics HTTP Handler Usage

## Purpose

`server/observability/metrics` includes an HTTP handler for Prometheus text exposition.

This is the HTTP export foundation for P0-D. It does not automatically expose a live endpoint until the server runtime wires the handler into an HTTP listener or existing admin endpoint.

## Example

```go
registry := metrics.NewRegistry()
metrics.RegisterP0Metrics(registry)

http.Handle("/metrics", metrics.Handler(registry))
```

## Behavior

- `GET /metrics` returns Prometheus text exposition.
- Non-GET methods return `405 Method Not Allowed`.
- Content type is `text/plain; version=0.0.4; charset=utf-8`.

## Current boundary

The handler is a reusable export surface. P0-D live metrics export is not fully accepted until:

- runtime server paths update the registry,
- the handler is mounted in a production-consumable endpoint or equivalent export path,
- endpoint/export evidence is archived under `reports/`.

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

