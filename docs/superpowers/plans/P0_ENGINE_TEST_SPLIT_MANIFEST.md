# P0 Engine Test Split Manifest

This project must avoid using the whole `go test ./server/innodb/engine` package as the only P0 signal. Use:

```bash
scripts/verify_p0_engine_suites.sh
scripts/verify_p0_core.sh
```

Suites:

- `dml-operators`: INSERT/UPDATE/DELETE operator storage write contracts.
- `storage-integrated-dml`: page row format, update/delete slot consistency, checkpoint write gate.
- `show`: SQL entry routing for SHOW plus InfoSchema-backed metadata.
- `checkpoint`: write gate block/unblock semantics.

`verify_p0_core.sh` runs the engine split plus buffer pool and manager P0 suites.
