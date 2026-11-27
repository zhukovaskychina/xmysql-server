package replication

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMySQLBinlogSourceReadsOfficialTransaction(t *testing.T) {
	sourceURL := os.Getenv("XMYSQL_OFFICIAL_BINLOG_URL")
	dsn := os.Getenv("XMYSQL_OFFICIAL_BINLOG_DSN")
	if sourceURL == "" || dsn == "" {
		t.Skip("set XMYSQL_OFFICIAL_BINLOG_URL and XMYSQL_OFFICIAL_BINLOG_DSN for the official MySQL fixture")
	}

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Ping())

	databaseName := fmt.Sprintf("p4_binlog_%d", time.Now().UnixNano())
	tableName := "rows"
	_, err = db.Exec("CREATE DATABASE " + databaseName)
	require.NoError(t, err)
	defer db.Exec("DROP DATABASE " + databaseName)
	_, err = db.Exec("CREATE TABLE " + databaseName + "." + tableName + " (id INT PRIMARY KEY, value VARCHAR(64))")
	require.NoError(t, err)

	var file string
	var position uint64
	var ignored interface{}
	row := db.QueryRow("SHOW BINARY LOG STATUS")
	require.NoError(t, row.Scan(&file, &position, &ignored, &ignored, &ignored))

	cfg, err := ParseMySQLBinlogSourceURL(sourceURL, 202)
	require.NoError(t, err)
	cfg.BinlogFile = file
	cfg.BinlogPosition = position
	source, err := NewMySQLBinlogSource(cfg)
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO " + databaseName + "." + tableName + " VALUES (1, 'official')")
	require.NoError(t, err)

	frames, err := source.DumpTransaction(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, frames)
	decoded, err := DecodeNativeBinlogTransactions(frames)
	require.NoError(t, err)
	require.NotEmpty(t, decoded)
	var sawRow bool
	for _, event := range decoded {
		if len(event.Changes) > 0 {
			sawRow = true
			break
		}
	}
	require.True(t, sawRow, "official native stream should contain the inserted row")
}

func TestMySQLBinlogSourceReadsOfficialSourceUUID(t *testing.T) {
	sourceURL := os.Getenv("XMYSQL_OFFICIAL_BINLOG_URL")
	dsn := os.Getenv("XMYSQL_OFFICIAL_BINLOG_DSN")
	if sourceURL == "" || dsn == "" {
		t.Skip("set XMYSQL_OFFICIAL_BINLOG_URL and XMYSQL_OFFICIAL_BINLOG_DSN for the official MySQL fixture")
	}

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Ping())

	var expected string
	require.NoError(t, db.QueryRow("SELECT @@GLOBAL.server_uuid").Scan(&expected))
	cfg, err := ParseMySQLBinlogSourceURL(sourceURL, 205)
	require.NoError(t, err)
	source, err := NewMySQLBinlogSource(cfg)
	require.NoError(t, err)

	actual, err := source.SourceUUID(context.Background())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestMySQLBinlogSourceReadsOfficialGTIDTransaction(t *testing.T) {
	sourceURL := os.Getenv("XMYSQL_OFFICIAL_BINLOG_URL")
	dsn := os.Getenv("XMYSQL_OFFICIAL_BINLOG_DSN")
	if sourceURL == "" || dsn == "" {
		t.Skip("set XMYSQL_OFFICIAL_BINLOG_URL and XMYSQL_OFFICIAL_BINLOG_DSN for the official MySQL fixture")
	}

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Ping())

	databaseName := fmt.Sprintf("p4_gtid_%d", time.Now().UnixNano())
	_, err = db.Exec("CREATE DATABASE " + databaseName)
	require.NoError(t, err)
	defer db.Exec("DROP DATABASE " + databaseName)
	_, err = db.Exec("CREATE TABLE " + databaseName + ".rows (id INT PRIMARY KEY, value VARCHAR(64))")
	require.NoError(t, err)

	var executed string
	require.NoError(t, db.QueryRow("SELECT @@GLOBAL.gtid_executed").Scan(&executed))
	parsedURL, err := url.Parse(sourceURL)
	require.NoError(t, err)
	query := parsedURL.Query()
	query.Set("gtid_auto_position", "true")
	parsedURL.RawQuery = query.Encode()
	cfg, err := ParseMySQLBinlogSourceURL(parsedURL.String(), 204)
	require.NoError(t, err)
	cfg.BinlogFile = ""
	cfg.BinlogPosition = 4
	cfg.GTIDSet = executed
	cfg.GTIDAutoPosition = true
	source, err := NewMySQLBinlogSource(cfg)
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO " + databaseName + ".rows VALUES (1, 'official-gtid')")
	require.NoError(t, err)
	frames, err := source.DumpTransaction(context.Background())
	require.NoError(t, err)
	decoded, err := DecodeNativeBinlogTransactions(frames)
	require.NoError(t, err)
	var sawRow bool
	for _, event := range decoded {
		if appliedContainsValue(event.Changes, "official-gtid") {
			sawRow = true
			break
		}
	}
	require.True(t, sawRow, "official GTID native stream should contain the inserted row")
}

func TestMySQLBinlogSourceReadsOfficialXATransaction(t *testing.T) {
	sourceURL := os.Getenv("XMYSQL_OFFICIAL_BINLOG_URL")
	dsn := os.Getenv("XMYSQL_OFFICIAL_BINLOG_DSN")
	if sourceURL == "" || dsn == "" {
		t.Skip("set XMYSQL_OFFICIAL_BINLOG_URL and XMYSQL_OFFICIAL_BINLOG_DSN for the official MySQL fixture")
	}

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Ping())

	databaseName := fmt.Sprintf("p4_xa_%d", time.Now().UnixNano())
	_, err = db.Exec("CREATE DATABASE " + databaseName)
	require.NoError(t, err)
	defer db.Exec("DROP DATABASE " + databaseName)
	_, err = db.Exec("CREATE TABLE " + databaseName + ".rows (id INT PRIMARY KEY, value VARCHAR(64))")
	require.NoError(t, err)

	var file string
	var position uint64
	var ignored interface{}
	row := db.QueryRow("SHOW BINARY LOG STATUS")
	require.NoError(t, row.Scan(&file, &position, &ignored, &ignored, &ignored))

	gtrid := fmt.Sprintf("gtrid_%d", time.Now().UnixNano())
	bqual := "branch"
	_, err = db.Exec("XA START '" + gtrid + "', '" + bqual + "', 1")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO " + databaseName + ".rows VALUES (1, 'xa')")
	require.NoError(t, err)
	_, err = db.Exec("XA END '" + gtrid + "', '" + bqual + "', 1")
	require.NoError(t, err)
	_, err = db.Exec("XA PREPARE '" + gtrid + "', '" + bqual + "', 1")
	require.NoError(t, err)
	_, err = db.Exec("XA COMMIT '" + gtrid + "', '" + bqual + "', 1")
	require.NoError(t, err)

	cfg, err := ParseMySQLBinlogSourceURL(sourceURL, 203)
	require.NoError(t, err)
	cfg.BinlogFile = file
	cfg.BinlogPosition = position
	source, err := NewMySQLBinlogSource(cfg)
	require.NoError(t, err)
	frames, err := source.DumpTransaction(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, frames)
	decoded, err := NewNativeBinlogDecoder().DecodeTransactions(frames)
	require.NoError(t, err)
	var committed *BinlogEvent
	for index := range decoded {
		if decoded[index].Type == EventXACommit {
			committed = &decoded[index]
			break
		}
	}
	require.NotNil(t, committed)
	require.NotNil(t, committed.XA)
	require.Equal(t, gtrid, committed.XA.GTRID)
	require.Equal(t, bqual, committed.XA.BQUAL)
	require.Len(t, committed.Changes, 1)
}

func TestNativeRuntimePullsOfficialMySQLTransaction(t *testing.T) {
	sourceURL := os.Getenv("XMYSQL_OFFICIAL_BINLOG_URL")
	dsn := os.Getenv("XMYSQL_OFFICIAL_BINLOG_DSN")
	if sourceURL == "" || dsn == "" {
		t.Skip("set XMYSQL_OFFICIAL_BINLOG_URL and XMYSQL_OFFICIAL_BINLOG_DSN for the official MySQL fixture")
	}

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Ping())

	databaseName := fmt.Sprintf("p4_runtime_%d", time.Now().UnixNano())
	_, err = db.Exec("CREATE DATABASE " + databaseName)
	require.NoError(t, err)
	defer db.Exec("DROP DATABASE " + databaseName)
	_, err = db.Exec("CREATE TABLE " + databaseName + ".rows (id INT PRIMARY KEY, value VARCHAR(64))")
	require.NoError(t, err)

	var file string
	var position uint64
	var ignored interface{}
	row := db.QueryRow("SHOW BINARY LOG STATUS")
	require.NoError(t, row.Scan(&file, &position, &ignored, &ignored, &ignored))
	parsedURL, err := url.Parse(sourceURL)
	require.NoError(t, err)
	query := parsedURL.Query()
	query.Set("binlog_file", file)
	query.Set("binlog_pos", fmt.Sprintf("%d", position))
	parsedURL.RawQuery = query.Encode()

	var mu sync.Mutex
	var applied []RowChange
	runtime, err := NewRuntime(RuntimeConfig{
		Role:         RoleReplica,
		DataDir:      t.TempDir(),
		UUID:         "official-runtime-replica",
		ServerID:     240,
		SourceURL:    parsedURL.String(),
		PollInterval: 5 * time.Millisecond,
		ApplyRows: func(changes []RowChange) error {
			mu.Lock()
			defer mu.Unlock()
			applied = append(applied, changes...)
			return nil
		},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, runtime.Start(ctx))
	defer runtime.Close()

	_, err = db.Exec("INSERT INTO " + databaseName + ".rows VALUES (1, 'runtime')")
	require.NoError(t, err)
	gotRows := assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(applied) == 1 && appliedContainsValue(applied, "runtime")
	}, 20*time.Second, 20*time.Millisecond)
	if !gotRows {
		t.Logf("native runtime status after timeout: %+v", runtime.Status())
		mu.Lock()
		t.Logf("native runtime applied rows: %#v", applied)
		mu.Unlock()
	}
	require.True(t, gotRows)
	_, err = db.Exec("INSERT INTO " + databaseName + ".rows VALUES (2, 'runtime-gtid')")
	require.NoError(t, err)
	gotSecond := assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(applied) == 2 && appliedContainsValue(applied, "runtime-gtid")
	}, 20*time.Second, 20*time.Millisecond)
	if !gotSecond {
		t.Logf("native runtime GTID status after second timeout: %+v", runtime.Status())
		mu.Lock()
		t.Logf("native runtime rows after second timeout: %#v", applied)
		mu.Unlock()
	}
	require.True(t, gotSecond)
	require.NotContains(t, runtime.Status().SourceURL, parsedURL.User.String())
	require.Greater(t, runtime.Status().SourcePosition, position)
}

func TestNativeRuntimePullsOfficialMySQLAcrossRotationAndRestart(t *testing.T) {
	sourceURL := os.Getenv("XMYSQL_OFFICIAL_BINLOG_URL")
	dsn := os.Getenv("XMYSQL_OFFICIAL_BINLOG_DSN")
	if sourceURL == "" || dsn == "" {
		t.Skip("set XMYSQL_OFFICIAL_BINLOG_URL and XMYSQL_OFFICIAL_BINLOG_DSN for the official MySQL fixture")
	}

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Ping())

	databaseName := fmt.Sprintf("p4_rotation_%d", time.Now().UnixNano())
	_, err = db.Exec("CREATE DATABASE " + databaseName)
	require.NoError(t, err)
	defer db.Exec("DROP DATABASE " + databaseName)
	_, err = db.Exec("CREATE TABLE " + databaseName + ".rows (id INT PRIMARY KEY, value VARCHAR(64))")
	require.NoError(t, err)

	var file string
	var position uint64
	var ignored interface{}
	row := db.QueryRow("SHOW BINARY LOG STATUS")
	require.NoError(t, row.Scan(&file, &position, &ignored, &ignored, &ignored))
	parsedURL, err := url.Parse(sourceURL)
	require.NoError(t, err)
	query := parsedURL.Query()
	query.Set("binlog_file", file)
	query.Set("binlog_pos", fmt.Sprintf("%d", position))
	parsedURL.RawQuery = query.Encode()

	replicaDir := t.TempDir()
	var mu sync.Mutex
	var applied []RowChange
	apply := func(changes []RowChange) error {
		mu.Lock()
		defer mu.Unlock()
		applied = append(applied, changes...)
		return nil
	}
	runtime, err := NewRuntime(RuntimeConfig{
		Role:         RoleReplica,
		DataDir:      replicaDir,
		UUID:         "official-rotation-replica",
		ServerID:     241,
		SourceURL:    parsedURL.String(),
		PollInterval: 5 * time.Millisecond,
		ApplyRows:    apply,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, runtime.Start(ctx))

	_, err = db.Exec("INSERT INTO " + databaseName + ".rows VALUES (1, 'before-rotate')")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(applied) == 1 && appliedContainsValue(applied, "before-rotate")
	}, 20*time.Second, 20*time.Millisecond)

	_, err = db.Exec("FLUSH BINARY LOGS")
	require.NoError(t, err)
	var rotatedFile string
	var rotatedPosition uint64
	row = db.QueryRow("SHOW BINARY LOG STATUS")
	require.NoError(t, row.Scan(&rotatedFile, &rotatedPosition, &ignored, &ignored, &ignored))
	require.NotEqual(t, file, rotatedFile)
	_, err = db.Exec("INSERT INTO " + databaseName + ".rows VALUES (2, 'after-rotate')")
	require.NoError(t, err)
	gotAfterRotate := assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(applied) == 2 && appliedContainsValue(applied, "after-rotate")
	}, 20*time.Second, 20*time.Millisecond)
	if !gotAfterRotate {
		mu.Lock()
		t.Logf("native rotation status: %+v; applied rows: %#v", runtime.Status(), applied)
		mu.Unlock()
	}
	require.True(t, gotAfterRotate)
	require.Eventually(t, func() bool {
		return runtime.Status().Role == RoleReplica && runtime.replica.LastSourceFile == rotatedFile
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, runtime.Close())

	var restartedMu sync.Mutex
	var restartedApplied []RowChange
	restarted, err := NewRuntime(RuntimeConfig{
		Role:         RoleReplica,
		DataDir:      replicaDir,
		UUID:         "official-rotation-replica",
		ServerID:     241,
		SourceURL:    parsedURL.String(),
		PollInterval: 5 * time.Millisecond,
		ApplyRows: func(changes []RowChange) error {
			restartedMu.Lock()
			defer restartedMu.Unlock()
			restartedApplied = append(restartedApplied, changes...)
			return nil
		},
	})
	require.NoError(t, err)
	restartedCtx, restartedCancel := context.WithCancel(context.Background())
	defer restartedCancel()
	require.NoError(t, restarted.Start(restartedCtx))
	defer restarted.Close()

	_, err = db.Exec("INSERT INTO " + databaseName + ".rows VALUES (3, 'after-restart')")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		restartedMu.Lock()
		defer restartedMu.Unlock()
		return appliedContainsValue(restartedApplied, "after-restart")
	}, 20*time.Second, 20*time.Millisecond)
	require.Equal(t, rotatedFile, restarted.replica.LastSourceFile)
	_ = rotatedPosition
}
