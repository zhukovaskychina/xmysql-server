package net

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/engine"
	"github.com/zhukovaskychina/xmysql-server/server/protocol"
)

type lateGenerationRuntime struct {
	started chan struct{}
	release chan struct{}
}

type wireGenerationRuntime func(context.Context, engine.GenerationRequest) (engine.GenerationResult, error)

func (f wireGenerationRuntime) Generate(ctx context.Context, request engine.GenerationRequest) (engine.GenerationResult, error) {
	return f(ctx, request)
}

type wireEmbeddingRuntime func(context.Context, engine.EmbeddingRequest) (engine.EmbeddingResult, error)

func (f wireEmbeddingRuntime) Embed(ctx context.Context, request engine.EmbeddingRequest) (engine.EmbeddingResult, error) {
	return f(ctx, request)
}

func (r *lateGenerationRuntime) Generate(ctx context.Context, _ engine.GenerationRequest) (engine.GenerationResult, error) {
	close(r.started)
	select {
	case <-ctx.Done():
	case <-r.release:
	}
	return engine.GenerationResult{ModelID: "late-model", ModelVersion: "1", Text: "late answer"}, nil
}

// Exercise the real listener, authentication, dispatcher, storage executor and
// text/prepared result encoders with the ordinary MySQL driver. Every file and
// account belongs to this test's temporary database; no external service is used.
func TestMySQLDriverQueryCompatibility(t *testing.T) {
	dir := t.TempDir()
	cfg := conf.NewCfg()
	cfg.DataDir, cfg.InnodbDataDir = dir, dir
	cfg.InnodbRedoLogDir, cfg.InnodbUndoLogDir = filepath.Join(dir, "redo"), filepath.Join(dir, "undo")
	cfg.InnodbBufferPoolSize = 16 * 1024 * 1024
	cfg.InnodbDoublewrite = false
	cfg.Port = 0
	cfg.SessionTimeoutDuration = time.Minute
	cfg.MySQLSessionParam = conf.MySQLSessionParam{
		TcpNoDelay: true, TcpRBufSize: 64 * 1024, TcpWBufSize: 64 * 1024,
		PkgRQSize: 64, PkgWQSize: 64, MaxMsgLen: 16 * 1024 * 1024,
		TcpReadTimeoutDuration: 5 * time.Second, TcpWriteTimeoutDuration: 5 * time.Second,
		WaitTimeoutDuration: 5 * time.Second, SessionName: "query-compat-test",
	}
	srv := NewMySQLServer(cfg)
	t.Cleanup(srv.uninitServer)
	require.NoError(t, srv.xmysqlEngine.Start(context.Background()))
	for _, query := range []string{
		"create database wire_compat",
		"create user 'query_wire'@'%' identified with mysql_native_password by 'query-test-only'",
		"grant all privileges on wire_compat.* to 'query_wire'@'%'",
	} {
		result := <-srv.xmysqlEngine.ExecuteQuery(nil, query, "")
		require.NoError(t, result.Err, query)
	}
	srv.initServer(cfg)
	listener := srv.serverList[0].(*serverimpl).streamListener
	require.NotNil(t, listener)
	client := mysql.NewConfig()
	client.User, client.Passwd = "query_wire", "query-test-only"
	client.Net, client.Addr, client.DBName = "tcp", listener.Addr().String(), "wire_compat"
	client.Timeout, client.ReadTimeout, client.WriteTimeout = 5*time.Second, 5*time.Second, 5*time.Second
	connector, err := mysql.NewConnector(client)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))
	for _, query := range []string{
		"create table query_rows (id int primary key, k int)",
		"insert into query_rows values (1,10),(2,NULL)",
		"create table empty_rows (id int primary key, k int)",
		"create table binary_rows (id int primary key, body blob)",
		"create table content_client_rows (id int primary key, body content)",
		"create table content_long_rows (id int primary key, body content, marker int)",
	} {
		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
	t.Run("text_result_values_and_metadata", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, "select id,k,k <=> NULL as missing from query_rows order by id")
		require.NoError(t, err)
		defer rows.Close()
		columns, err := rows.ColumnTypes()
		require.NoError(t, err)
		require.Equal(t, []string{"INT", "INT", "BIGINT"}, []string{columns[0].DatabaseTypeName(), columns[1].DatabaseTypeName(), columns[2].DatabaseTypeName()})
		var ids, missing []int64
		var values []sql.NullInt64
		for rows.Next() {
			var id, flag int64
			var k sql.NullInt64
			require.NoError(t, rows.Scan(&id, &k, &flag))
			ids = append(ids, id)
			missing = append(missing, flag)
			values = append(values, k)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []int64{1, 2}, ids)
		require.Equal(t, []int64{0, 1}, missing)
		require.Equal(t, []sql.NullInt64{{Int64: 10, Valid: true}, {}}, values)
	})
	t.Run("prepared_binary_result", func(t *testing.T) {
		statement, err := db.PrepareContext(ctx, "select id,k from query_rows where id = ?")
		require.NoError(t, err)
		defer statement.Close()
		var id int64
		var k sql.NullInt64
		require.NoError(t, statement.QueryRowContext(ctx, 2).Scan(&id, &k))
		require.Equal(t, int64(2), id)
		require.False(t, k.Valid)
	})
	t.Run("scalar_subquery_error_keeps_result_metadata", func(t *testing.T) {
		query := "select (select k from query_rows) as too_many"
		for _, prepared := range []bool{false, true} {
			t.Run(fmt.Sprintf("prepared_%v", prepared), func(t *testing.T) {
				var rows *sql.Rows
				if prepared {
					statement, err := db.PrepareContext(ctx, query)
					require.NoError(t, err)
					defer statement.Close()
					rows, err = statement.QueryContext(ctx)
				} else {
					rows, err = db.QueryContext(ctx, query)
				}
				require.NoError(t, err, "result metadata should be returned before the execution error")
				defer rows.Close()
				columns, err := rows.Columns()
				require.NoError(t, err)
				require.Equal(t, []string{"too_many"}, columns)
				types, err := rows.ColumnTypes()
				require.NoError(t, err)
				require.Equal(t, "INT", types[0].DatabaseTypeName())
				require.False(t, rows.Next())
				var mysqlErr *mysql.MySQLError
				require.ErrorAs(t, rows.Err(), &mysqlErr)
				require.Equal(t, uint16(1242), mysqlErr.Number)
				require.Equal(t, [5]byte{'2', '1', '0', '0', '0'}, mysqlErr.SQLState)
			})
		}
	})
	t.Run("semantic_error_does_not_send_result_metadata", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "create table query_collation_rows (id int primary key, ci_text varchar(20) collate utf8mb4_general_ci, sv_text varchar(20) collate utf8mb4_swedish_ci)")
		require.NoError(t, err)
		defer func() {
			_, dropErr := db.ExecContext(ctx, "drop table query_collation_rows")
			require.NoError(t, dropErr)
		}()
		_, err = db.ExecContext(ctx, "insert into query_collation_rows values (1,'x','x')")
		require.NoError(t, err)
		query := "select id from query_collation_rows where ci_text=sv_text order by id"
		for _, prepared := range []bool{false, true} {
			t.Run(fmt.Sprintf("prepared_%v", prepared), func(t *testing.T) {
				var rows *sql.Rows
				var err error
				if prepared {
					statement, prepareErr := db.PrepareContext(ctx, query)
					if prepareErr != nil {
						err = prepareErr
					} else {
						defer statement.Close()
						rows, err = statement.QueryContext(ctx)
					}
				} else {
					rows, err = db.QueryContext(ctx, query)
				}
				if rows != nil {
					defer rows.Close()
				}
				var columns []string
				if err == nil {
					require.NotNil(t, rows)
					columns, err = rows.Columns()
					require.NoError(t, err)
					for rows.Next() {
					}
					err = rows.Err()
				}
				require.Empty(t, columns, "semantic errors detected before query execution must not expose result metadata")
				var mysqlErr *mysql.MySQLError
				require.ErrorAs(t, err, &mysqlErr)
				require.Equal(t, uint16(1267), mysqlErr.Number)
				require.Equal(t, [5]byte{'H', 'Y', '0', '0', '0'}, mysqlErr.SQLState)
			})
		}
	})
	t.Run("hex_literal_round_trips_as_binary_bytes", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "insert into binary_rows values (1, X'89504E47')")
		require.NoError(t, err)
		var body []byte
		require.NoError(t, db.QueryRowContext(ctx, "select body from binary_rows where id=1").Scan(&body))
		require.Equal(t, []byte{0x89, 0x50, 0x4e, 0x47}, body)
		var encoded string
		var byteLength int64
		require.NoError(t, db.QueryRowContext(ctx, "select hex(body), length(body) from binary_rows where id=1").Scan(&encoded, &byteLength))
		require.Equal(t, "89504E47", encoded)
		require.Equal(t, int64(4), byteLength)
	})
	t.Run("prepared_content_crud_and_transaction_rollback", func(t *testing.T) {
		original := bytes.Repeat([]byte{0x00, 0xff, 0x89, 0x50}, 4096)
		updated := bytes.Repeat([]byte{0x31, 0x00, 0x7f}, 5000)
		rolledBack := bytes.Repeat([]byte{0x55, 0xaa}, 9000)

		insert, err := db.PrepareContext(ctx, "insert into content_client_rows values (?, CONTENT_FROM_BYTES(?, 'application/octet-stream'))")
		require.NoError(t, err)
		_, err = insert.ExecContext(ctx, 1, original)
		require.NoError(t, err)
		require.NoError(t, insert.Close())

		var body []byte
		require.NoError(t, db.QueryRowContext(ctx, "select body from content_client_rows where id=1").Scan(&body))
		require.Equal(t, original, body)

		update, err := db.PrepareContext(ctx, "update content_client_rows set body = CONTENT_FROM_BYTES(?, 'application/octet-stream') where id = ?")
		require.NoError(t, err)
		_, err = update.ExecContext(ctx, updated, 1)
		require.NoError(t, err)
		require.NoError(t, update.Close())
		body = nil
		require.NoError(t, db.QueryRowContext(ctx, "select body from content_client_rows where id=1").Scan(&body))
		require.True(t, bytes.Equal(updated, body), "prepared UPDATE must store the exact committed CONTENT bytes")

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, "update content_client_rows set body = CONTENT_FROM_BYTES(?, 'application/octet-stream') where id = ?", rolledBack, 1)
		require.NoError(t, err)
		var uncommittedLength int64
		require.NoError(t, tx.QueryRowContext(ctx, "select CONTENT_LENGTH(body) from content_client_rows where id=1").Scan(&uncommittedLength))
		require.Equal(t, int64(len(rolledBack)), uncommittedLength, "transaction must expose its updated CONTENT before rollback")
		require.NoError(t, tx.Rollback())
		body = nil
		require.NoError(t, db.QueryRowContext(ctx, "select body from content_client_rows where id=1").Scan(&body))
		if !bytes.Equal(updated, body) {
			var contentLength int64
			var contentDigest string
			diagnosticErr := db.QueryRowContext(ctx, "select CONTENT_LENGTH(body), CONTENT_SHA256(body) from content_client_rows where id=1").Scan(&contentLength, &contentDigest)
			require.NoError(t, diagnosticErr)
			t.Fatalf("rollback must preserve the last committed CONTENT bytes: got %d bytes (CONTENT_LENGTH=%d SHA256=%s), want %d", len(body), contentLength, contentDigest, len(updated))
		}

		deleteStatement, err := db.PrepareContext(ctx, "delete from content_client_rows where id = ?")
		require.NoError(t, err)
		_, err = deleteStatement.ExecContext(ctx, 1)
		require.NoError(t, err)
		require.NoError(t, deleteStatement.Close())
		var count int64
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from content_client_rows").Scan(&count))
		require.Zero(t, count)
	})
	t.Run("prepared_content_long_data_streaming_round_trip", func(t *testing.T) {
		streamConfig := client.Clone()
		streamConfig.MaxAllowedPacket = 1 << 20
		streamConnector, err := mysql.NewConnector(streamConfig)
		require.NoError(t, err)
		streamDB := sql.OpenDB(streamConnector)
		streamDB.SetMaxOpenConns(1)
		t.Cleanup(func() { require.NoError(t, streamDB.Close()) })
		streamCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		require.NoError(t, streamDB.PingContext(streamCtx))

		payload := make([]byte, 2*1024*1024+37)
		for index := range payload {
			payload[index] = byte((index*31 + index/251) % 256)
		}
		expectedDigest := sha256.Sum256(payload)
		insert, err := streamDB.PrepareContext(streamCtx, "insert into content_long_rows values (?, CONTENT_FROM_BYTES(?, 'application/octet-stream'), ?)")
		require.NoError(t, err)
		_, err = insert.ExecContext(streamCtx, 1, payload, 31415)
		require.NoError(t, err)
		require.NoError(t, insert.Close())

		var storedLength int64
		var storedDigest string
		var revision uint64
		require.NoError(t, streamDB.QueryRowContext(streamCtx, "select CONTENT_LENGTH(body), CONTENT_SHA256(body), CONTENT_REVISION(body) from content_long_rows where id = 1").Scan(&storedLength, &storedDigest, &revision))
		require.Equal(t, int64(len(payload)), storedLength)
		require.Equal(t, hex.EncodeToString(expectedDigest[:]), storedDigest)
		var marker int
		require.NoError(t, streamDB.QueryRowContext(streamCtx, "select marker from content_long_rows where id = 1").Scan(&marker))
		require.Equal(t, 31415, marker, "parameters after the long-data value must still be decoded")

		readChunk := 1024 * 1024
		streamDigest := sha256.New()
		for offset := int64(0); offset < storedLength; offset += int64(readChunk) {
			length := int64(readChunk)
			if remaining := storedLength - offset; remaining < length {
				length = remaining
			}
			var chunk []byte
			query := "select CONTENT_SLICE(body, ?, ?) from content_long_rows where id = ? and CONTENT_REVISION(body) = ?"
			require.NoError(t, streamDB.QueryRowContext(streamCtx, query, offset, length, 1, revision).Scan(&chunk))
			require.Equal(t, int(length), len(chunk), "bounded slice at offset %d", offset)
			_, err = streamDigest.Write(chunk)
			require.NoError(t, err)
		}
		require.True(t, bytes.Equal(expectedDigest[:], streamDigest.Sum(nil)), "SQL slices must reconstruct the exact stored CONTENT object")
	})
	t.Run("content_slice_revision_fences_other_connection_changes", func(t *testing.T) {
		for _, transfer := range []struct {
			name      string
			maxPacket int
		}{{"inline_parameter", 16 << 20}, {"long_data_parameter", 1 << 20}} {
			t.Run(transfer.name, func(t *testing.T) {
				streamConfig := client.Clone()
				streamConfig.MaxAllowedPacket = transfer.maxPacket
				streamConnector, err := mysql.NewConnector(streamConfig)
				require.NoError(t, err)
				writer := sql.OpenDB(streamConnector)
				writer.SetMaxOpenConns(1)
				defer writer.Close()
				require.NoError(t, writer.PingContext(ctx))
				original := bytes.Repeat([]byte{0x00, 0xff, 0x31}, 400000)
				replacement := bytes.Repeat([]byte{0x89, 0x50, 0x00}, 400000)
				insertStarted := time.Now()
				_, err = writer.ExecContext(ctx, "insert into content_long_rows values (?, CONTENT_FROM_BYTES(?, 'application/octet-stream'), ?)", 2, original, 0)
				t.Logf("CONTENT %s INSERT elapsed=%s err=%v", transfer.name, time.Since(insertStarted), err)
				require.NoError(t, err)
				var revision uint64
				require.NoError(t, db.QueryRowContext(ctx, "select CONTENT_REVISION(body) from content_long_rows where id=2").Scan(&revision))
				const readSQL = "select CONTENT_SLICE(body, ?, ?) from content_long_rows where id=? and CONTENT_REVISION(body)=?"
				var chunk []byte
				require.NoError(t, db.QueryRowContext(ctx, readSQL, 0, 4096, 2, revision).Scan(&chunk))
				require.Equal(t, original[:4096], chunk)

				_, err = writer.ExecContext(ctx, "update content_long_rows set body=CONTENT_FROM_BYTES(?, 'application/octet-stream') where id=?", replacement, 2)
				require.NoError(t, err)
				var replacementRevision uint64
				require.NoError(t, db.QueryRowContext(ctx, "select CONTENT_REVISION(body) from content_long_rows where id=2").Scan(&replacementRevision))
				require.NotEqual(t, revision, replacementRevision)
				require.ErrorIs(t, db.QueryRowContext(ctx, readSQL, 4096, 4096, 2, revision).Scan(&chunk), sql.ErrNoRows,
					"a committed replacement must stop continuation under the old revision")
				var reconstructed []byte
				for offset := 0; offset < len(replacement); offset += 512 * 1024 {
					size := len(replacement) - offset
					if size > 512*1024 {
						size = 512 * 1024
					}
					require.NoError(t, db.QueryRowContext(ctx, readSQL, offset, size, 2, replacementRevision).Scan(&chunk))
					reconstructed = append(reconstructed, chunk...)
				}
				require.Equal(t, replacement, reconstructed)

				tx, err := writer.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer tx.Rollback()
				_, err = tx.ExecContext(ctx, "update content_long_rows set body=CONTENT_FROM_BYTES(?, 'application/octet-stream') where id=?", original, 2)
				require.NoError(t, err)
				require.NoError(t, tx.Rollback())
				require.NoError(t, db.QueryRowContext(ctx, readSQL, 4096, 4096, 2, replacementRevision).Scan(&chunk))
				require.Equal(t, replacement[4096:8192], chunk, "rollback must retain the committed revision and bytes")
				_, err = writer.ExecContext(ctx, "delete from content_long_rows where id=?", 2)
				require.NoError(t, err)
				require.ErrorIs(t, db.QueryRowContext(ctx, readSQL, 8192, 4096, 2, replacementRevision).Scan(&chunk), sql.ErrNoRows,
					"deletion must stop continuation under the former revision")
			})
		}
	})
	t.Run("kill_query_rejects_late_model_answer", func(t *testing.T) {
		provider := &lateGenerationRuntime{started: make(chan struct{}), release: make(chan struct{})}
		defer close(provider.release)
		srv.xmysqlEngine.QueryExecutor.SetGenerationRuntime(provider)
		defer srv.xmysqlEngine.QueryExecutor.SetGenerationRuntime(nil)
		var connectionID uint64
		require.NoError(t, db.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
		killer := sql.OpenDB(connector)
		defer killer.Close()
		result := make(chan error, 1)
		go func() {
			var answer string
			result <- db.QueryRowContext(ctx, "select AI_GENERATE('prompt', 'model')").Scan(&answer)
		}()
		select {
		case <-provider.started:
		case err := <-result:
			t.Fatalf("model SQL returned before calling provider: %v", err)
		case <-ctx.Done():
			t.Fatal("model SQL did not reach the provider", ctx.Err())
		}
		_, err := killer.ExecContext(ctx, fmt.Sprintf("KILL QUERY %d", connectionID))
		require.NoError(t, err)
		select {
		case err := <-result:
			require.Error(t, err, "a model answer returned after KILL QUERY must not reach the client")
			require.ErrorContains(t, err, "context canceled")
		case <-ctx.Done():
			t.Fatal("KILL QUERY did not cancel model SQL", ctx.Err())
		}
		var value int
		require.NoError(t, db.QueryRowContext(ctx, "select 1").Scan(&value))
		require.Equal(t, 1, value, "KILL QUERY must leave the connection usable")
	})
	t.Run("model_budget_shared_and_reset_for_text_and_prepared_sql", func(t *testing.T) {
		executor := srv.xmysqlEngine.QueryExecutor
		require.NoError(t, executor.SetModelExecutionPolicy(engine.ModelExecutionPolicy{MaxCalls: 1, MaxInputBytes: 100, CallTimeout: time.Second}))
		defer func() { require.NoError(t, executor.SetModelExecutionPolicy(engine.DefaultModelExecutionPolicy())) }()
		defer executor.SetGenerationRuntime(nil)
		var calls atomic.Int64
		executor.SetGenerationRuntime(wireGenerationRuntime(func(context.Context, engine.GenerationRequest) (engine.GenerationResult, error) {
			calls.Add(1)
			return engine.GenerationResult{ModelID: "model", ModelVersion: "1", Text: "answer"}, nil
		}))
		var first, second string
		err := db.QueryRowContext(ctx, "select AI_GENERATE('abc', 'model'), AI_SUMMARIZE('abc', 'model')").Scan(&first, &second)
		require.ErrorContains(t, err, "model query budget exceeded")
		require.Equal(t, int64(1), calls.Load())
		prepared, err := db.PrepareContext(ctx, "select AI_GENERATE(?, 'model'), AI_SUMMARIZE(?, 'model')")
		require.NoError(t, err)
		defer prepared.Close()
		err = prepared.QueryRowContext(ctx, "abc", "abc").Scan(&first, &second)
		require.ErrorContains(t, err, "model query budget exceeded")
		require.Equal(t, int64(2), calls.Load())
		err = db.QueryRowContext(ctx, "select AI_GENERATE('abc', 'model') union all select AI_GENERATE('abc', 'model')").Scan(&first)
		require.ErrorContains(t, err, "model query budget exceeded")
		require.Equal(t, int64(3), calls.Load(), "UNION branches must share the statement budget")
		require.NoError(t, db.QueryRowContext(ctx, "select AI_GENERATE('abc', 'model')").Scan(&first))
		require.Equal(t, "answer", first)
		require.Equal(t, int64(4), calls.Load(), "a new statement receives its own budget")

		require.NoError(t, executor.SetModelExecutionPolicy(engine.ModelExecutionPolicy{MaxCalls: 1, MaxInputBytes: 100, CallTimeout: 20 * time.Millisecond}))
		executor.SetGenerationRuntime(wireGenerationRuntime(func(ctx context.Context, _ engine.GenerationRequest) (engine.GenerationResult, error) {
			<-ctx.Done()
			return engine.GenerationResult{ModelID: "model", ModelVersion: "1", Text: "late answer"}, nil
		}))
		err = db.QueryRowContext(ctx, "select AI_GENERATE('abc', 'model')").Scan(&first)
		require.ErrorContains(t, err, "context deadline exceeded")
	})
	t.Run("prepare_model_sql_does_not_invoke_provider", func(t *testing.T) {
		executor := srv.xmysqlEngine.QueryExecutor
		var calls atomic.Int64
		executor.SetGenerationRuntime(wireGenerationRuntime(func(context.Context, engine.GenerationRequest) (engine.GenerationResult, error) {
			calls.Add(1)
			return engine.GenerationResult{ModelID: "model", ModelVersion: "1", Text: "answer"}, nil
		}))
		defer executor.SetGenerationRuntime(nil)
		// Inspect the very metadata sent at PREPARE, rather than accepting a
		// zero-column response whose types only become known at EXECUTE.
		metadataSession := NewMockSession("prepare-model-metadata")
		mysqlSession := NewMySQLServerSession(metadataSession)
		mysqlSession.SetParamByName("user", "query_wire")
		mysqlSession.SetParamByName("host", "127.0.0.1")
		mysqlSession.SetParamByName("database", "wire_compat")
		metadataStatement := &protocol.PreparedStatement{SQL: "select AI_GENERATE('prompt','model') as answer, AI_EMBED('model','prompt') as embedding"}
		srv.messageHandler.populatePreparedResultMetadata(metadataSession, &mysqlSession, metadataStatement)
		require.Zero(t, calls.Load())
		require.Equal(t, uint16(2), metadataStatement.ColumnCount)
		require.Len(t, metadataStatement.Columns, 2)
		require.Equal(t, "answer", metadataStatement.Columns[0].Name)
		require.Equal(t, "embedding", metadataStatement.Columns[1].Name)
		require.Equal(t, byte(common.COLUMN_TYPE_LONG_BLOB), metadataStatement.Columns[1].Type)
		require.Equal(t, uint16(63), metadataStatement.Columns[1].Charset)
		require.NotZero(t, metadataStatement.Columns[1].Flags&uint16(common.BinaryFlag))
		for _, query := range []string{
			"select AI_GENERATE('prompt','model') as answer",
			"select AI_SUMMARIZE('prompt','model') as answer from query_rows where id=1",
			"select AI_GENERATE('prompt','model') as answer from query_rows n join query_rows l on n.id=l.id where n.id=1",
			"select AI_GENERATE('prompt','model') as answer union all select AI_GENERATE('prompt','model') as answer",
			"select a.answer from (select AI_GENERATE('prompt','model') as answer from query_rows where id=1) a",
		} {
			t.Run(query, func(t *testing.T) {
				before := calls.Load()
				statement, err := db.PrepareContext(ctx, query)
				require.NoError(t, err)
				defer statement.Close()
				require.Equal(t, before, calls.Load(), "PREPARE must describe columns without running inference")
				rows, err := statement.QueryContext(ctx)
				require.NoError(t, err)
				defer rows.Close()
				var count int64
				for rows.Next() {
					var answer string
					require.NoError(t, rows.Scan(&answer))
					require.Equal(t, "answer", answer)
					count++
				}
				require.NoError(t, rows.Err())
				require.Positive(t, count)
				require.Equal(t, before+count, calls.Load(), "EXECUTE must still call the provider once per result row")
			})
		}
	})
	t.Run("prepared_vector_search_metadata_and_empty_result", func(t *testing.T) {
		for _, query := range []string{
			"CREATE TABLE vector_client_rows (id INT PRIMARY KEY, body CONTENT, embedding VECTOR(2))",
			`INSERT INTO vector_client_rows VALUES (1,CONTENT_FROM_BYTES('alpha','text/plain'),VECTOR_FROM_SOURCE('{"model_id":"text-model","model_version":"1","space_id":"text-space","source_modality":"text","elements":[1,0]}',body))`,
			"CREATE VECTOR INDEX embedding_idx ON vector_client_rows (embedding) USING HNSW",
		} {
			_, err := db.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		executor := srv.xmysqlEngine.QueryExecutor
		require.NoError(t, executor.SetModelExecutionPolicy(engine.ModelExecutionPolicy{MaxCalls: 1, MaxInputBytes: 1024, CallTimeout: time.Second}))
		defer func() { require.NoError(t, executor.SetModelExecutionPolicy(engine.DefaultModelExecutionPolicy())) }()
		var calls atomic.Int64
		executor.SetEmbeddingRuntime(wireEmbeddingRuntime(func(_ context.Context, request engine.EmbeddingRequest) (engine.EmbeddingResult, error) {
			calls.Add(1)
			require.Equal(t, "alpha", request.Text)
			return engine.EmbeddingResult{ModelID: "text-model", ModelVersion: "1", SpaceID: "text-space", Elements: []float32{1, 0}}, nil
		}))
		defer executor.SetEmbeddingRuntime(nil)
		query := "SELECT v.primary_key,v.distance,v.approximate,v.may_have_more FROM TABLE(VECTOR_SEARCH('wire_compat','vector_client_rows','embedding_idx',AI_EMBED('text_encoder',?),1,'EXACT')) v"
		metadataSession := NewMockSession("prepare-vector-metadata")
		mysqlSession := NewMySQLServerSession(metadataSession)
		mysqlSession.SetParamByName("user", "query_wire")
		mysqlSession.SetParamByName("host", "127.0.0.1")
		mysqlSession.SetParamByName("database", "wire_compat")
		metadataStatement := &protocol.PreparedStatement{SQL: query}
		srv.messageHandler.populatePreparedResultMetadata(metadataSession, &mysqlSession, metadataStatement)
		require.Zero(t, calls.Load())
		require.Equal(t, uint16(4), metadataStatement.ColumnCount)
		require.Len(t, metadataStatement.Columns, 4)
		require.Equal(t, byte(common.COLUMN_TYPE_LONG_BLOB), metadataStatement.Columns[0].Type)
		require.Equal(t, uint16(63), metadataStatement.Columns[0].Charset)
		require.Equal(t, byte(common.COLUMN_TYPE_DOUBLE), metadataStatement.Columns[1].Type)
		require.Equal(t, byte(common.COLUMN_TYPE_TINY), metadataStatement.Columns[2].Type)
		for _, empty := range []bool{false, true} {
			t.Run(fmt.Sprintf("empty_%v", empty), func(t *testing.T) {
				statementSQL := query
				if empty {
					statementSQL += " WHERE v.distance < 0"
				}
				before := calls.Load()
				statement, err := db.PrepareContext(ctx, statementSQL)
				require.NoError(t, err)
				defer statement.Close()
				require.Equal(t, before, calls.Load())
				rows, err := statement.QueryContext(ctx, "alpha")
				require.NoError(t, err)
				defer rows.Close()
				columns, err := rows.ColumnTypes()
				require.NoError(t, err)
				require.Len(t, columns, 4, "an empty search is still a typed result set")
				require.Equal(t, []string{"LONGBLOB", "DOUBLE", "TINYINT", "TINYINT"}, []string{columns[0].DatabaseTypeName(), columns[1].DatabaseTypeName(), columns[2].DatabaseTypeName(), columns[3].DatabaseTypeName()})
				if empty {
					require.False(t, rows.Next())
				} else {
					require.True(t, rows.Next())
					var key []byte
					var distance float64
					var approximate, more bool
					require.NoError(t, rows.Scan(&key, &distance, &approximate, &more))
					require.Equal(t, []byte("1"), key)
					require.Zero(t, distance)
					require.False(t, approximate)
					require.False(t, more)
					require.False(t, rows.Next())
				}
				require.NoError(t, rows.Err())
				require.Equal(t, before+1, calls.Load(), "only EXECUTE embeds the query, once")
			})
		}
	})
	t.Run("prepared_content_privileges", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "insert into content_client_rows values (1, CONTENT_FROM_BYTES(X'726561646572', 'application/octet-stream'))")
		require.NoError(t, err)
		for _, query := range []string{
			"create user 'query_limited'@'%' identified with mysql_native_password by 'limited-test-only'",
			"grant select on wire_compat.content_client_rows to 'query_limited'@'%'",
		} {
			result := <-srv.xmysqlEngine.ExecuteQuery(nil, query, "")
			require.NoError(t, result.Err, query)
		}
		limitedConfig := client.Clone()
		limitedConfig.User = "query_limited"
		limitedConfig.Passwd = "limited-test-only"
		limitedConnector, err := mysql.NewConnector(limitedConfig)
		require.NoError(t, err)
		limitedDB := sql.OpenDB(limitedConnector)
		limitedDB.SetMaxOpenConns(1)
		t.Cleanup(func() { require.NoError(t, limitedDB.Close()) })
		require.NoError(t, limitedDB.PingContext(ctx))
		metadataSession := NewMockSession("prepare-select-permission")
		mysqlSession := NewMySQLServerSession(metadataSession)
		mysqlSession.SetParamByName("user", "query_limited")
		mysqlSession.SetParamByName("host", "127.0.0.1")
		mysqlSession.SetParamByName("database", "wire_compat")
		for _, test := range []struct {
			query   string
			columns uint16
		}{
			{"select body from content_client_rows", 1},
			{"select id from query_rows", 0},
		} {
			statement := &protocol.PreparedStatement{SQL: test.query}
			srv.messageHandler.populatePreparedResultMetadata(metadataSession, &mysqlSession, statement)
			require.Equal(t, test.columns, statement.ColumnCount, "PREPARE must not disclose unauthorized table metadata")
		}

		read, err := limitedDB.PrepareContext(ctx, "select body from content_client_rows where id = ?")
		require.NoError(t, err)
		rows, err := read.QueryContext(ctx, 1)
		require.NoError(t, err, "a table-level SELECT grant must authorize prepared CONTENT reads")
		require.NoError(t, rows.Close())
		require.NoError(t, read.Close())
		var plainRead []byte
		require.NoError(t, limitedDB.QueryRowContext(ctx, "select body from content_client_rows where id = 1").Scan(&plainRead))
		require.Equal(t, []byte("reader"), plainRead)
		for _, query := range []string{
			"update content_client_rows set body = CONTENT_FROM_BYTES(X'64656e696564', 'application/octet-stream') where id = 1",
			"delete from content_client_rows where id = 1",
			"insert into content_client_rows values (2, CONTENT_FROM_BYTES(X'64656e696564', 'application/octet-stream'))",
		} {
			_, err := limitedDB.ExecContext(ctx, query)
			require.Error(t, err, query)
			var mysqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &mysqlErr, query)
			require.Equal(t, uint16(1142), mysqlErr.Number, query, err.Error())
		}

		for _, statement := range []struct {
			query string
			args  []interface{}
		}{
			{"update content_client_rows set body = CONTENT_FROM_BYTES(?, 'application/octet-stream') where id = ?", []interface{}{[]byte("denied"), 1}},
			{"delete from content_client_rows where id = ?", []interface{}{1}},
			{"insert into content_client_rows values (?, CONTENT_FROM_BYTES(?, 'application/octet-stream'))", []interface{}{2, []byte("denied")}},
		} {
			prepared, err := limitedDB.PrepareContext(ctx, statement.query)
			require.NoError(t, err, statement.query)
			_, err = prepared.ExecContext(ctx, statement.args...)
			require.Error(t, err, statement.query)
			var mysqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &mysqlErr, statement.query)
			require.Equal(t, uint16(1142), mysqlErr.Number, statement.query, err.Error())
			require.NoError(t, prepared.Close())
		}
	})
	t.Run("empty_result_alias_metadata", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, "select id as renamed,k <=> NULL as missing from query_rows where 1=0")
		require.NoError(t, err)
		defer rows.Close()
		columns, err := rows.ColumnTypes()
		require.NoError(t, err)
		require.Equal(t, []string{"INT", "BIGINT"}, []string{columns[0].DatabaseTypeName(), columns[1].DatabaseTypeName()})
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
	})
	t.Run("empty_set", func(t *testing.T) {
		var count int64
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from query_rows where k not in (select k from empty_rows)").Scan(&count))
		require.Equal(t, int64(2), count)
	})
	t.Run("rollback", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, "insert into query_rows values (3,30)")
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())
		var count int64
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from query_rows where id=3").Scan(&count))
		require.Zero(t, count)
	})
	t.Run("scalar_cardinality_error", func(t *testing.T) {
		var value interface{}
		err := db.QueryRowContext(ctx, "select (select k from query_rows) as scalar_value").Scan(&value)
		var mysqlErr *mysql.MySQLError
		require.ErrorAs(t, err, &mysqlErr)
		require.Equal(t, uint16(1242), mysqlErr.Number)
		require.Equal(t, "21000", string(mysqlErr.SQLState[:]))
	})
	t.Run("external_content_clients", func(t *testing.T) {
		runExternalContentClients(t, listener.Addr().String())
	})
	t.Run("mysql84_reference_comparison", func(t *testing.T) {
		runMySQL84ReferenceComparison(t, db)
	})
}
