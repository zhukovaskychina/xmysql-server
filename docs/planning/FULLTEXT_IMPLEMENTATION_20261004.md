# FULLTEXT implementation scope

## Status

FULLTEXT is no longer `deferred`; it is now tracked as `partial` in the compatibility scope matrix. The implementation is InnoDB-only and keeps the existing storage format. It adds a durable compatibility sidecar to each table metadata file rather than claiming byte-level compatibility with MySQL InnoDB FTS auxiliary tables.

## Implemented in this slice

- `CREATE TABLE ... FULLTEXT INDEX/KEY`.
- `ALTER TABLE ... ADD FULLTEXT INDEX/KEY`.
- Standalone `CREATE FULLTEXT INDEX`.
- `DROP INDEX` and `RENAME INDEX` auxiliary-state maintenance.
- Unicode letter/digit tokenization, lower-casing, and the current stopword set.
- Natural-language matching and the practical Boolean-mode operators `+`, `-`, and `*`.
- Exact FULLTEXT-index column-list validation for `MATCH(...) AGAINST(...)`.
- Durable inverted segments rebuilt after autocommit DML and after explicit `COMMIT`; `ROLLBACK` does not publish index-sidecar changes.

## Current compatibility boundary

The query executor still uses a row-scan fallback after Fulltext filtering. The persisted segments provide deterministic DDL/restart metadata and a maintenance contract, but are not yet wired into the optimizer as a storage-key candidate scan. Exact MySQL relevance ranking, query expansion, parser plugins (`ngram`/`MeCab`), complete stopword configuration, all `INFORMATION_SCHEMA.INNODB_FT_*` tables, and official MySQL crash/replication interoperability remain open work.

## Verification

```text
go test ./server/innodb/engine -run 'TestFullText' -count=1
go test ./server/innodb/engine ./server/innodb/sqlparser -count=1
```
