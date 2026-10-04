package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

func TestPerformanceSchemaRWLockInstancesExposeDDLCoordinatorRuntime(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	lock := executor.QueryExecutor.getDDLCoordinator().lockFor("app.orders")
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockWrite, "thread/901"))
	t.Cleanup(func() { lock.unlockOwned(tableLockWrite, "thread/901") })

	result := mustSelectResultSQL(t, executor, "", "select name, object_instance_begin, write_locked_by_thread_id, read_locked_by_count from performance_schema.rwlock_instances")
	var values []basic.Value
	for _, record := range result.Records {
		candidate := record.GetValues()
		if candidate[0].String() == "wait/synch/rwlock/sql/xmysql/table_ddl" && candidate[2].Int() == 901 {
			values = candidate
			break
		}
	}
	require.NotNil(t, values)
	require.Equal(t, "wait/synch/rwlock/sql/xmysql/table_ddl", values[0].String())
	require.NotZero(t, values[1].Int())
	require.Equal(t, int64(901), values[2].Int())
	require.Equal(t, int64(0), values[3].Int())
}

func TestPerformanceSchemaRWLockInstancesHonorCapacityAndLostStatus(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_rwlock_instances", "0")
	require.NoError(t, err)
	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })

	lock := executor.QueryExecutor.getDDLCoordinator().lockFor("app.orders")
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockWrite, "thread/902"))
	t.Cleanup(func() { lock.unlockOwned(tableLockWrite, "thread/902") })

	instances := mustSelectResultSQL(t, executor, "", "select name from performance_schema.rwlock_instances")
	require.Empty(t, instances.Records)
	status := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_rwlock_instances_lost'")
	require.Len(t, status.Records, 1)
	require.Greater(t, status.Records[0].GetValues()[0].Int(), int64(0))
}

func TestPerformanceSchemaRWLockInstrumentSettingControlsInstances(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	lock := executor.QueryExecutor.getDDLCoordinator().lockFor("app.readers")
	require.NoError(t, lock.lockWithContextOwned(context.Background(), tableLockRead, "thread/903"))
	t.Cleanup(func() { lock.unlockOwned(tableLockRead, "thread/903") })

	visible := mustSelectResultSQL(t, executor, "", "select name, read_locked_by_count from performance_schema.rwlock_instances where read_locked_by_count > 0")
	require.NotEmpty(t, visible.Records)
	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set enabled='NO' where name='wait/synch/rwlock/sql/xmysql/table_ddl'")
	hidden := mustSelectResultSQL(t, executor, "", "select name from performance_schema.rwlock_instances")
	require.Empty(t, hidden.Records)
}

func TestPerformanceSchemaMutexInstancesExposeExecutorRuntime(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	result := mustSelectResultSQL(t, executor, "", "select name, object_instance_begin, locked_by_thread_id from performance_schema.mutex_instances")
	var found []basic.Value
	for _, record := range result.Records {
		candidate := record.GetValues()
		if candidate[0].String() == "wait/synch/mutex/sql/xmysql/account" {
			found = candidate
			break
		}
	}
	require.NotNil(t, found)
	require.NotZero(t, found[1].Int())
	require.True(t, found[2].IsNull())
}

func TestPerformanceSchemaMutexInstancesExposeActiveQueryOwner(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	executor.QueryExecutor.lockPerformanceSchemaMutex(&executor.QueryExecutor.activeQueryMu, "wait/synch/mutex/sql/xmysql/active_query", 904)
	t.Cleanup(func() {
		executor.QueryExecutor.unlockPerformanceSchemaMutex(&executor.QueryExecutor.activeQueryMu, "wait/synch/mutex/sql/xmysql/active_query")
	})

	var objectInstanceBegin int64
	var lockedByThreadID interface{}
	for _, instance := range executor.QueryExecutor.performanceSchemaMutexInstances() {
		if instance.name == "wait/synch/mutex/sql/xmysql/active_query" {
			objectInstanceBegin = instance.objectInstanceBegin
			lockedByThreadID = instance.lockedByThreadID
			break
		}
	}
	require.NotZero(t, objectInstanceBegin)
	require.Equal(t, int64(904), lockedByThreadID)
}

func TestPerformanceSchemaMutexWaitLifecycleExposesCurrentAndHistory(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	const instrument = performanceSchemaXAMutexInstrument
	const ownerThreadID int64 = 900
	const waiterThreadID int64 = 901
	executor.QueryExecutor.lockPerformanceSchemaMutex(&executor.QueryExecutor.xaMu, instrument, ownerThreadID)
	ownerHeld := true
	defer func() {
		if ownerHeld {
			executor.QueryExecutor.unlockPerformanceSchemaMutex(&executor.QueryExecutor.xaMu, instrument)
		}
	}()

	waiterStarted := make(chan struct{})
	waiterDone := make(chan struct{})
	go func() {
		close(waiterStarted)
		executor.QueryExecutor.lockPerformanceSchemaMutex(&executor.QueryExecutor.xaMu, instrument, waiterThreadID)
		executor.QueryExecutor.unlockPerformanceSchemaMutex(&executor.QueryExecutor.xaMu, instrument)
		close(waiterDone)
	}()
	<-waiterStarted

	var objectInstanceBegin int64
	for _, instance := range executor.QueryExecutor.performanceSchemaMutexInstances() {
		if instance.name == instrument {
			objectInstanceBegin = instance.objectInstanceBegin
			break
		}
	}
	require.NotZero(t, objectInstanceBegin)
	require.Eventually(t, func() bool {
		result := mustSelectResultSQL(t, executor, "", "select thread_id, object_instance_begin, operation from performance_schema.events_waits_current where event_name='wait/synch/mutex/sql/xmysql/xa'")
		for _, record := range result.Records {
			values := record.GetValues()
			if values[0].Int() == waiterThreadID && values[1].Int() == objectInstanceBegin && values[2].String() == "lock" {
				return true
			}
		}
		return false
	}, 2*time.Second, 20*time.Millisecond)

	ownerHeld = false
	executor.QueryExecutor.unlockPerformanceSchemaMutex(&executor.QueryExecutor.xaMu, instrument)
	select {
	case <-waiterDone:
	case <-time.After(2 * time.Second):
		t.Fatal("mutex waiter did not complete after owner release")
	}

	history := mustSelectResultSQL(t, executor, "", "select thread_id, object_instance_begin, operation from performance_schema.events_waits_history_long where event_name='wait/synch/mutex/sql/xmysql/xa'")
	foundHistory := false
	for _, record := range history.Records {
		values := record.GetValues()
		if values[0].Int() == waiterThreadID && values[1].Int() == objectInstanceBegin && values[2].String() == "lock" {
			foundHistory = true
			break
		}
	}
	require.True(t, foundHistory)

	summary := mustSelectResultSQL(t, executor, "", "select event_name, object_instance_begin, count_star from performance_schema.events_waits_summary_by_instance where event_name='wait/synch/mutex/sql/xmysql/xa'")
	foundSummary := false
	for _, record := range summary.Records {
		values := record.GetValues()
		if values[0].String() == instrument && values[1].String() == fmt.Sprint(objectInstanceBegin) && values[2].Int() > 0 {
			foundSummary = true
			break
		}
	}
	require.True(t, foundSummary)

	mustExecSQL(t, executor, "", "truncate table performance_schema.events_waits_summary_by_instance")
	resetSummary := mustSelectResultSQL(t, executor, "", "select event_name, object_instance_begin, count_star from performance_schema.events_waits_summary_by_instance where event_name='wait/synch/mutex/sql/xmysql/xa'")
	for _, record := range resetSummary.Records {
		values := record.GetValues()
		if values[0].String() == instrument && values[1].String() == fmt.Sprint(objectInstanceBegin) {
			require.Equal(t, int64(0), values[2].Int())
		}
	}

	globalBefore := mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_waits_summary_global_by_event_name where event_name='wait/synch/mutex/sql/xmysql/xa'")
	require.Equal(t, [][]interface{}{{instrument, "1"}}, selectResultRows(globalBefore))
	threadBefore := mustSelectResultSQL(t, executor, "", "select thread_id, event_name, count_star from performance_schema.events_waits_summary_by_thread_by_event_name where event_name='wait/synch/mutex/sql/xmysql/xa'")
	require.Equal(t, [][]interface{}{{"901", instrument, "1"}}, selectResultRows(threadBefore))

	mustExecSQL(t, executor, "", "truncate table performance_schema.events_waits_summary_global_by_event_name")
	globalAfter := mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_waits_summary_global_by_event_name where event_name='wait/synch/mutex/sql/xmysql/xa'")
	require.Equal(t, [][]interface{}{{instrument, "0"}}, selectResultRows(globalAfter))
	threadAfterGlobal := mustSelectResultSQL(t, executor, "", "select thread_id, event_name, count_star from performance_schema.events_waits_summary_by_thread_by_event_name where event_name='wait/synch/mutex/sql/xmysql/xa'")
	require.Equal(t, [][]interface{}{{"901", instrument, "0"}}, selectResultRows(threadAfterGlobal))

	mustExecSQL(t, executor, "", "truncate table performance_schema.events_waits_summary_by_thread_by_event_name")
	threadAfter := mustSelectResultSQL(t, executor, "", "select thread_id, event_name, count_star from performance_schema.events_waits_summary_by_thread_by_event_name where event_name='wait/synch/mutex/sql/xmysql/xa'")
	require.Equal(t, [][]interface{}{{"901", instrument, "0"}}, selectResultRows(threadAfter))
}

func TestPerformanceSchemaMutexWaitThreadSummaryTruncateIsIndependent(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	const instrument = performanceSchemaXAMutexInstrument
	const ownerThreadID int64 = 910
	const waiterThreadID int64 = 911
	runPerformanceSchemaMutexWait(t, executor, instrument, ownerThreadID, waiterThreadID)

	before := mustSelectResultSQL(t, executor, "", "select thread_id, event_name, count_star from performance_schema.events_waits_summary_by_thread_by_event_name where event_name='wait/synch/mutex/sql/xmysql/xa'")
	require.Equal(t, [][]interface{}{{"911", instrument, "1"}}, selectResultRows(before))
	mustExecSQL(t, executor, "", "truncate table performance_schema.events_waits_summary_by_thread_by_event_name")
	after := mustSelectResultSQL(t, executor, "", "select thread_id, event_name, count_star from performance_schema.events_waits_summary_by_thread_by_event_name where event_name='wait/synch/mutex/sql/xmysql/xa'")
	require.Equal(t, [][]interface{}{{"911", instrument, "0"}}, selectResultRows(after))
	global := mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_waits_summary_global_by_event_name where event_name='wait/synch/mutex/sql/xmysql/xa'")
	require.Equal(t, [][]interface{}{{instrument, "1"}}, selectResultRows(global))
}

func TestPerformanceSchemaMutexWaitAccountSummaryTruncateIsIndependent(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	const instrument = performanceSchemaXAMutexInstrument
	const ownerThreadID int64 = 920
	const waiterThreadID int64 = 921
	accountSession := newTestMySQLSession()
	accountSession.SetParamByName("connection_id", waiterThreadID)
	accountSession.SetParamByName("user", "mutex_account_user")
	accountSession.SetParamByName("host", "mutex-account.example")
	executor.QueryExecutor.SetProcesslistProvider(func() []server.MySQLServerSession {
		return []server.MySQLServerSession{accountSession}
	})
	runPerformanceSchemaMutexWait(t, executor, instrument, ownerThreadID, waiterThreadID)

	before := mustSelectResultSQL(t, executor, "", "select user, host, event_name, count_star from performance_schema.events_waits_summary_by_account_by_event_name where event_name='wait/synch/mutex/sql/xmysql/xa'")
	require.Equal(t, [][]interface{}{{"mutex_account_user", "mutex-account.example", instrument, "1"}}, selectResultRows(before))
	mustExecSQL(t, executor, "", "truncate table performance_schema.events_waits_summary_by_account_by_event_name")
	after := mustSelectResultSQL(t, executor, "", "select user, host, event_name, count_star from performance_schema.events_waits_summary_by_account_by_event_name where event_name='wait/synch/mutex/sql/xmysql/xa'")
	require.Equal(t, [][]interface{}{{"mutex_account_user", "mutex-account.example", instrument, "0"}}, selectResultRows(after))
	global := mustSelectResultSQL(t, executor, "", "select event_name, count_star from performance_schema.events_waits_summary_global_by_event_name where event_name='wait/synch/mutex/sql/xmysql/xa'")
	require.Equal(t, [][]interface{}{{instrument, "1"}}, selectResultRows(global))
}

func TestPerformanceSchemaMutexWaitHistoryHonorsIndependentCapacity(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	enableAllPerformanceSchemaConsumersForTest(t, executor)
	const instrument = performanceSchemaXAMutexInstrument
	admin := newTestMySQLSession()
	admin.SetParamByName("global_privileges", []common.PrivilegeType{common.AllPriv})
	mustExecSessionSQL(t, executor, admin, "", "set global performance_schema_events_waits_history_size=1")
	mustExecSessionSQL(t, executor, admin, "", "set global performance_schema_events_waits_history_long_size=2")
	runPerformanceSchemaMutexWait(t, executor, instrument, 930, 931)
	runPerformanceSchemaMutexWait(t, executor, instrument, 932, 931)

	shortHistory := mustSelectResultSQL(t, executor, "", "select thread_id, event_name from performance_schema.events_waits_history where event_name='wait/synch/mutex/sql/xmysql/xa'")
	longHistory := mustSelectResultSQL(t, executor, "", "select thread_id, event_name from performance_schema.events_waits_history_long where event_name='wait/synch/mutex/sql/xmysql/xa'")
	require.Len(t, shortHistory.Records, 1)
	require.Len(t, longHistory.Records, 2)

	mustExecSessionSQL(t, executor, admin, "", "set global performance_schema_events_waits_history_long_size=1")
	trimmedLongHistory := mustSelectResultSQL(t, executor, "", "select thread_id, event_name from performance_schema.events_waits_history_long where event_name='wait/synch/mutex/sql/xmysql/xa'")
	require.Len(t, trimmedLongHistory.Records, 1)
}

func runPerformanceSchemaMutexWait(t *testing.T, executor *XMySQLEngine, instrument string, ownerThreadID, waiterThreadID int64) {
	t.Helper()
	executor.QueryExecutor.lockPerformanceSchemaMutex(&executor.QueryExecutor.xaMu, instrument, ownerThreadID)
	ownerHeld := true
	defer func() {
		if ownerHeld {
			executor.QueryExecutor.unlockPerformanceSchemaMutex(&executor.QueryExecutor.xaMu, instrument)
		}
	}()
	waiterStarted := make(chan struct{})
	waiterDone := make(chan struct{})
	go func() {
		close(waiterStarted)
		executor.QueryExecutor.lockPerformanceSchemaMutex(&executor.QueryExecutor.xaMu, instrument, waiterThreadID)
		executor.QueryExecutor.unlockPerformanceSchemaMutex(&executor.QueryExecutor.xaMu, instrument)
		close(waiterDone)
	}()
	<-waiterStarted
	require.Eventually(t, func() bool {
		current, _ := executor.QueryExecutor.performanceSchemaMutexWaitSnapshots(false, false)
		for _, wait := range current {
			if wait.threadID == waiterThreadID && wait.eventName == instrument {
				return true
			}
		}
		return false
	}, 2*time.Second, 20*time.Millisecond)
	ownerHeld = false
	executor.QueryExecutor.unlockPerformanceSchemaMutex(&executor.QueryExecutor.xaMu, instrument)
	select {
	case <-waiterDone:
	case <-time.After(2 * time.Second):
		t.Fatal("mutex waiter did not complete after owner release")
	}
}

func TestPerformanceSchemaConditionInstancesExposeGlobalReadLockRuntime(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	shape := mustSelectResultSQL(t, executor, "", "select * from performance_schema.cond_instances")
	require.Equal(t, []string{"NAME", "OBJECT_INSTANCE_BEGIN"}, shape.Columns)
	result := mustSelectResultSQL(t, executor, "", "select name, object_instance_begin from performance_schema.cond_instances")
	var found []basic.Value
	for _, record := range result.Records {
		candidate := record.GetValues()
		if candidate[0].String() == "wait/synch/cond/sql/xmysql/global_read_lock" {
			found = candidate
			break
		}
	}
	require.NotNil(t, found)
	require.NotZero(t, found[1].Int())
}

func TestPerformanceSchemaConditionInstancesHonorCapacityAndInstrumentSetting(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_cond_instances", "0")
	require.NoError(t, err)
	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })

	instances := mustSelectResultSQL(t, executor, "", "select name from performance_schema.cond_instances")
	require.Empty(t, instances.Records)
	status := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_cond_instances_lost'")
	require.Len(t, status.Records, 1)
	require.Greater(t, status.Records[0].GetValues()[0].Int(), int64(0))

	visibleExecutor := newTestStorageIntegratedExecutor(t, t.TempDir())
	visible := mustSelectResultSQL(t, visibleExecutor, "", "select name from performance_schema.cond_instances")
	require.Len(t, visible.Records, 1)
	mustExecSQL(t, visibleExecutor, "", "update performance_schema.setup_instruments set enabled='NO' where name='wait/synch/cond/sql/xmysql/global_read_lock'")
	hidden := mustSelectResultSQL(t, visibleExecutor, "", "select name from performance_schema.cond_instances")
	require.Empty(t, hidden.Records)
}

func TestPerformanceSchemaMutexInstancesHonorCapacityAndInstrumentSetting(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_mutex_instances", "1")
	require.NoError(t, err)
	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })

	instances := mustSelectResultSQL(t, executor, "", "select name from performance_schema.mutex_instances")
	require.Len(t, instances.Records, 1)
	status := mustSelectResultSQL(t, executor, "", "select variable_value from performance_schema.global_status where variable_name='Performance_schema_mutex_instances_lost'")
	require.Len(t, status.Records, 1)
	require.Greater(t, status.Records[0].GetValues()[0].Int(), int64(0))

	mustExecSQL(t, executor, "", "update performance_schema.setup_instruments set enabled='NO' where name='wait/synch/mutex/sql/xmysql/account'")
	hidden := mustSelectResultSQL(t, executor, "", "select name from performance_schema.mutex_instances where name='wait/synch/mutex/sql/xmysql/account'")
	require.Empty(t, hidden.Records)
}

func TestPerformanceSchemaSyncInstrumentClassesHonorCapacityAndLostStatus(t *testing.T) {
	cfg := conf.NewCfg()
	cfg.DataDir = t.TempDir()
	cfg.InnodbDataDir = cfg.DataDir
	section, err := cfg.Raw.NewSection("performance_schema")
	require.NoError(t, err)
	_, err = section.NewKey("max_mutex_classes", "1")
	require.NoError(t, err)
	_, err = section.NewKey("max_rwlock_classes", "0")
	require.NoError(t, err)
	_, err = section.NewKey("max_cond_classes", "0")
	require.NoError(t, err)
	executor := NewXMySQLEngine(cfg)
	t.Cleanup(func() { require.NoError(t, executor.Close()) })

	mutexes := mustSelectResultSQL(t, executor, "", "select name from performance_schema.setup_instruments where name like 'wait/synch/mutex/sql/xmysql/%'")
	require.Len(t, mutexes.Records, 1)
	require.Equal(t, "wait/synch/mutex/sql/xmysql/account", mutexes.Records[0].GetValues()[0].String())
	rwlocks := mustSelectResultSQL(t, executor, "", "select name from performance_schema.setup_instruments where name='wait/synch/rwlock/sql/xmysql/table_ddl'")
	require.Empty(t, rwlocks.Records)

	status := mustSelectResultSQL(t, executor, "", "select variable_name, variable_value from performance_schema.global_status where variable_name in ('Performance_schema_cond_classes_lost','Performance_schema_mutex_classes_lost','Performance_schema_rwlock_classes_lost') order by variable_name")
	statusValues := make(map[string]string, len(status.Records))
	for _, record := range status.Records {
		values := record.GetValues()
		statusValues[values[0].String()] = values[1].String()
	}
	require.Equal(t, "4", statusValues["Performance_schema_mutex_classes_lost"])
	require.Equal(t, "1", statusValues["Performance_schema_rwlock_classes_lost"])
	require.Equal(t, "1", statusValues["Performance_schema_cond_classes_lost"])
}
