# MySQL 8.4 Command Support Baseline

This table records the command behavior currently implemented by both network
handlers. `partial` means the command path exists and has focused Go tests, but
does not yet claim wire-level parity with every MySQL capability negotiation.

| Command | Status | Notes |
|---|---|---|
| `COM_QUERY` | partial | Query execution, top-level multi-statements, result terminators |
| `COM_INIT_DB` | supported | Session database update |
| `COM_PING` | supported | OK response |
| `COM_STMT_PREPARE` | supported | Parameter metadata baseline |
| `COM_STMT_EXECUTE` | partial | Text SQL binding; cursor flag opens server-side state |
| `COM_STMT_SEND_LONG_DATA` | supported | Per-statement/per-parameter accumulation and consume-on-execute |
| `COM_STMT_FETCH` | supported | Server-side cursor row-count pagination with binary row encoding |
| `COM_STMT_RESET` | supported | Clears parameter types, long data and cursor state |
| `COM_STMT_CLOSE` | supported | Removes statement and cursor state without response |
| `COM_RESET_CONNECTION` | supported | Clears prepared state, transaction flags, database, locks, warnings, user variables and session variables |
| `COM_BINLOG_DUMP` / `COM_BINLOG_DUMP_GTID` | partial | Native event stream and GTID filtering are implemented for the current logical replication source; full MySQL event/plugin parity remains pending |
| Unknown command | supported | Explicit `ER_NOT_SUPPORTED_YET` response |

## Verification

```powershell
go test ./server/net ./server/protocol -count=1
```

The `partial` entries must not be promoted to full compatibility until the JDBC
prepared-statement and capability-negotiation cases are added to the matrix.
