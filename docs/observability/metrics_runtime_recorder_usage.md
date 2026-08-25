# Metrics Runtime Recorder Usage

## Purpose

`metrics.RuntimeRecorder` provides a small adapter between runtime events and the P0-D metric catalog.

It avoids scattering metric names and label keys across query, transaction, lock, recovery, and checkpoint code paths.

## Example

```go
registry := metrics.NewRegistry()
metrics.RegisterP0Metrics(registry)
recorder := metrics.NewRuntimeRecorder(registry)

recorder.RecordQuery("test", "select", "ok", 12*time.Millisecond)
recorder.RecordQueryError("test", "duplicate_key", "ER_DUP_ENTRY")
recorder.SetActiveConnections("default", 7)
```

## Current boundary

This adapter is ready for runtime integration, but it is not yet called by live server paths.

P0-D remains incomplete until:

- query execution paths call `RecordQuery` and `RecordQueryError`,
- transaction paths call transaction recorder methods,
- lock manager paths call `RecordLockWait`,
- recovery/checkpoint paths update recovery and checkpoint metrics,
- metrics are exposed through a production-consumable endpoint or textfile,
- evidence is archived under `reports/`.
