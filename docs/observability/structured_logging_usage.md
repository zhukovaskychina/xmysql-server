# Structured Logging Usage

## Purpose

`server/observability/logging` provides JSON-lines encoders for the P0-D slow-query and error-log field contracts.

It is a runtime logging foundation. It does not automatically connect to the query executor or engine error paths until those paths call the encoder.

## Slow query event

```go
event := logging.NewSlowQueryEvent(
    "trace-id",
    101,
    9001,
    "test",
    "select_user_by_status",
    1520*time.Millisecond,
    120000,
    50,
    "",
    "slow",
)

_ = encoder.WriteSlowQuery(event)
```

## Error event

```go
event := logging.NewErrorEvent(
    "ERROR",
    "trace-id",
    "engine",
    "insert",
    "ER_DUP_ENTRY",
    "duplicate_key",
    "duplicate key conflict",
    12*time.Millisecond,
    103,
    9003,
)

_ = encoder.WriteError(event)
```

## Evidence helper

Generate structured sample logs with:

```powershell
go run ./cmd/p0_structured_log_export `
  -slow-output reports/structured_slow_query_<timestamp>.log `
  -error-output reports/structured_error_<timestamp>.log
```

## Current boundary

This package proves the logging contract can be encoded by code. P0-D is not complete until runtime query and error paths generate these events under configured thresholds.
