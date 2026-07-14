#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GO_BIN="${GO_BIN:-/Users/zhukovasky/sdk/go1.24.3/bin/go}"

if [[ ! -x "${GO_BIN}" ]]; then
  GO_BIN="$(command -v go)"
fi

cd "${ROOT_DIR}"

run_engine_suite() {
  local name="$1"
  local pattern="$2"
  echo "==> engine P0 suite: ${name}"
  "${GO_BIN}" test ./server/innodb/engine -run "${pattern}" -count=1 -v
}

run_engine_suite "dml-operators" 'Test(InsertOperator_(ParseInsertRows)|InsertOperator(InsertRowWritesThroughStorageAdapter|DuplicateKeyReturnsError|FindDuplicateRecordUsesStorageAdapter)|UpdateOperator(ApplySetClauseChangesTargetColumn|PersistsUpdatedRecord)|DeleteOperatorDeletesOnlyScannedRecords)'
run_engine_suite "storage-integrated-dml" 'Test(StorageIntegratedDML(WriteReturnsContextErrorWhenCheckpointGateTimesOut|InsertFailsWhenBTreeManagerMissing)|StorageIntegratedDMLExecutor_(FindRowsTo(Update|Delete)RejectsMissingWhere|MarkRowDeletedInPageContentPreservesOtherRows|PageRows(AppendAndReplace|RejectReplaceDeletedSlot)|DataSerialization))'
run_engine_suite "show" 'Test(ShowExecutor_Show|XMySQLExecutor_ShowTablesUsesInfoSchemaInsteadOfDataDir|XMySQLExecutor_Execute(Query_Show|ShowStatementWithQuery_Show))'
run_engine_suite "checkpoint" 'TestCheckpointManager_(WriteGateBlocksAndUnblocksWritePermits|WriteSharpCheckpointUnblocksOnSuccess|WriteSharpCheckpointUnblocksOnError)'
