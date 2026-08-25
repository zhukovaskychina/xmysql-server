# XMySQL Server Architecture Diagrams

Generated architecture diagrams for the major modules of XMySQL Server. Arrows describe data origin, processing direction, returned results, and durable storage boundaries.

1. [Network Access](./01-network-access.png)
2. [MySQL Protocol](./02-mysql-protocol.png)
3. [SQL Dispatch and Session](./03-sql-dispatch-session.png)
4. [SQL Execution Engine](./04-sql-execution-engine.png)
5. [SQL Parser and Optimizer](./05-sql-parser-optimizer.png)
6. [InnoDB Storage](./06-innodb-storage.png)
7. [Transactions, MVCC and Recovery](./07-transactions-mvcc-recovery.png)
8. [Replication, Backup and Observability](./08-replication-backup-observability.png)

The diagrams were generated with the built-in ImageGen capability and checked against the repository module layout. The transaction/recovery diagram was regenerated to use the actual `server/innodb/manager/` paths.
