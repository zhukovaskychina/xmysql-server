package engine

import (
	"fmt"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

func isShowEngineInnoDBStatus(query string) bool {
	q := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	return q == "show engine innodb status"
}

// executeShowEngineInnoDBStatus returns the stable three-column shape used by
// MySQL for SHOW ENGINE INNODB STATUS. The text intentionally exposes the
// engine's live transaction and lock-wait state instead of fabricating a
// static banner, while keeping the report compatible with common clients.
func (e *XMySQLExecutor) executeShowEngineInnoDBStatus(ctx *ExecutionContext) {
	status := e.innoDBStatusText()
	result := newInformationSchemaSelectResult(
		"SHOW ENGINE INNODB STATUS",
		[]string{"Type", "Name", "Status"},
		[][]interface{}{{"InnoDB", "InnoDB", status}},
	)
	result.ResultType = common.RESULT_TYPE_QUERY
	ctx.Results <- &Result{
		ResultType: common.RESULT_TYPE_QUERY,
		Data:       result,
		Message:    "SHOW ENGINE INNODB STATUS executed successfully",
	}
}

func (e *XMySQLExecutor) innoDBStatusText() string {
	var b strings.Builder
	b.WriteString("=====================================\n")
	fmt.Fprintf(&b, "%s 0x0 INNODB MONITOR OUTPUT\n", time.Now().UTC().Format("2006-01-02 15:04:05"))
	b.WriteString("=====================================\n")
	b.WriteString("\n")
	b.WriteString("------------------------\n")
	b.WriteString("BACKGROUND THREAD\n")
	b.WriteString("------------------------\n")
	b.WriteString("srv_master_thread loops: 0 srv_active, 0 srv_shutdown, 0 srv_idle\n")
	b.WriteString("\n")
	b.WriteString("------------\n")
	b.WriteString("TRANSACTIONS\n")
	b.WriteString("------------\n")
	active := 0
	if e != nil && e.txManager != nil {
		active = len(e.txManager.GetLongTransactions(0))
	}
	fmt.Fprintf(&b, "Trx count %d\n", active)
	b.WriteString("Purge done for trx's n:o < 0 undo n:o < 0 state: running\n")

	b.WriteString("\n------------------------\n")
	b.WriteString("LOCK WAIT SUMMARY\n")
	b.WriteString("------------------------\n")
	edges := 0
	if e != nil && e.lockManager != nil {
		waits := e.lockManager.WaitGraphSnapshot()
		edges = len(waits)
		for _, wait := range waits {
			age := time.Since(wait.Since)
			if age < 0 {
				age = 0
			}
			fmt.Fprintf(&b, "TRANSACTION %d WAITING FOR TRANSACTION %d; resource=%s; lock_type=%d; mode=%d; wait_age=%s\n", wait.WaitingTxID, wait.BlockingTxID, wait.ResourceID, wait.LockType, wait.Mode, age.Round(time.Millisecond))
		}
		if deadlock := e.lockManager.LastDeadlockSnapshot(); deadlock != nil {
			fmt.Fprintf(&b, "LATEST DEADLOCK: victim=%d; cycle=%v; wait_age=%s\n", deadlock.VictimTxID, deadlock.Cycle, deadlock.WaitDuration.Round(time.Millisecond))
		}
	}
	fmt.Fprintf(&b, "Lock waits: %d\n", edges)
	metadataEdges := e.performanceSchemaMetadataWaitEdges()
	for _, wait := range metadataEdges {
		age := time.Since(wait.WaitStarted)
		if age < 0 {
			age = 0
		}
		fmt.Fprintf(&b, "METADATA LOCK WAIT: table=%s; waiting=%s; blocking=%s; wait_age=%s\n", wait.Table, wait.WaitingOwner, wait.BlockingOwner, age.Round(time.Millisecond))
	}
	fmt.Fprintf(&b, "Metadata lock waits: %d\n", len(metadataEdges))
	b.WriteString("\n--------\n")
	b.WriteString("FILE I/O\n")
	b.WriteString("--------\n")
	b.WriteString("Pending flushes (fsync) log: 0; buffer pool pages: 0\n")
	b.WriteString("\n============================\n")
	b.WriteString("END OF INNODB MONITOR OUTPUT\n")
	b.WriteString("============================\n")
	return b.String()
}
