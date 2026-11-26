# CONTENT client acceptance

The opt-in Go wire fixture starts a loopback listener with a temporary data directory and a test account, then runs ordinary Python and Java clients against the same SQL contract. It does not use an existing server or user database.

Requirements: Python with `python/content-requirements.txt`, Java 17+, and Connector/J (the project version tested is 8.0.33). Set these environment variables to local dependency paths:

```powershell
$env:XMYSQL_RUN_EXTERNAL_CONTENT_CLIENTS = '1'
$env:XMYSQL_TEST_PYTHON = '<python executable>'
$env:XMYSQL_TEST_JAVA = '<java executable>'
$env:XMYSQL_TEST_JDBC_JAR = '<mysql-connector-j jar>'
go test -mod=readonly -p 1 ./server/net -run '^TestMySQLDriverQueryCompatibility/external_content_clients$' -count=1 -v -timeout=6m
```

The fixture supplies the endpoint and credentials through the child process environment. An enabled test fails if a required executable or driver is unavailable; without opt-in the external test is explicitly skipped. Scripts emit JSON only after all assertions pass.

Coverage:

- PyMySQL text parameters with binary prefix; Connector/J emulated and confirmed server prepared statements.
- CONTENT CRUD, binary LONGBLOB metadata, all 256 byte values, revision-fenced slices, update revision and transaction rollback.
- Connector/J `setBinaryStream` with 2 MiB + 37 bytes, 256 KiB send chunks and a parameter after the stream. The client packet limit is 1 MiB; the result is verified using bounded slices rather than requesting a row larger than that limit.

This is client interoperability evidence. It does not establish full driver coverage, large-file memory ceilings, cross-client concurrency, transfer resumption, crash recovery or backup/restore.
