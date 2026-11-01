package engine

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

// executeParsedAdmin handles the commands whose runtime already accepts
// typed values. A nil result means the legacy compatibility handler still
// owns that command; invalid structured input never falls through silently.
func (e *XMySQLExecutor) executeParsedAdmin(ctx *ExecutionContext, stmt *sqlparser.AdminStatement, session server.MySQLServerSession) (*Result, error) {
	if stmt == nil {
		return nil, fmt.Errorf("admin statement is nil")
	}
	if stmt.Kind == "start" || stmt.Kind == "stop" || stmt.Kind == "reset" || stmt.Kind == "purge" || stmt.Kind == "flush" || stmt.Kind == "change" {
		lower := strings.ToLower(strings.TrimSpace(stmt.Raw))
		requiredPrivilege := replicationControlRequiredPrivilege(lower)
		if requiredPrivilege == "" {
			requiredPrivilege = sourceBinlogRequiredPrivilege(lower)
		}
		if requiredPrivilege != "" {
			if err := e.checkGlobalPrivilege(ctx, requiredPrivilege); err != nil {
				return nil, err
			}
		}
	}
	switch stmt.Kind {
	case "kill":
		id, err := strconv.ParseUint(strings.TrimSpace(stmt.ID), 10, 32)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("invalid KILL thread id")
		}
		if strings.EqualFold(stmt.Scope, "query") {
			if e == nil || e.sessionQueryKill == nil {
				return nil, fmt.Errorf("session query kill control is not configured")
			}
			if err := e.sessionQueryKill(uint32(id), session); err != nil {
				return nil, err
			}
			return &Result{ResultType: common.RESULT_TYPE_QUERY}, nil
		}
		if e == nil || e.sessionKill == nil {
			return nil, fmt.Errorf("session kill control is not configured")
		}
		if err := e.sessionKill(uint32(id), session); err != nil {
			return nil, err
		}
		return &Result{ResultType: common.RESULT_TYPE_QUERY}, nil
	case "unlock":
		e.releaseGlobalReadLock(session)
		e.releaseSessionTableLocks(session)
		return &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "statement executed successfully"}, nil
	case "lock":
		if session == nil {
			return nil, fmt.Errorf("LOCK TABLES requires a session")
		}
		if err := e.prepareDDLImplicitCommit(session); err != nil {
			return nil, err
		}
		database := ctxDatabaseName(ctx)
		if database == "" {
			return nil, fmt.Errorf("no database selected")
		}
		requests := make([]sessionTableLockRequest, 0, len(stmt.Targets))
		seen := make(map[string]struct{}, len(stmt.Targets))
		for index, target := range stmt.Targets {
			schema := target.Qualifier.String()
			if schema == "" {
				schema = database
			}
			name := target.Name.String()
			if schema == "" || name == "" {
				return nil, fmt.Errorf("invalid LOCK TABLES target")
			}
			key := strings.ToLower(schema) + "." + strings.ToLower(name)
			if _, exists := seen[key]; exists {
				return nil, fmt.Errorf("duplicate table in LOCK TABLES: %s", name)
			}
			seen[key] = struct{}{}
			if err := e.checkTablePrivilege(ctx, schema, name, "LOCK TABLES"); err != nil {
				return nil, err
			}
			if err := e.checkTablePrivilege(ctx, schema, name, "SELECT"); err != nil {
				return nil, err
			}
			mode := strings.ToLower(stmt.Options["lock"])
			if index < len(stmt.TargetModes) {
				mode = stmt.TargetModes[index]
			}
			if mode == "" {
				mode = "read"
			}
			requests = append(requests, sessionTableLockRequest{table: key, mode: mode})
		}
		lockContext := context.Background()
		if ctx != nil && ctx.Context != nil {
			lockContext = ctx.Context
		}
		lockContext, cancel := tableLockContext(lockContext, session)
		defer cancel()
		if err := e.acquireSessionTableLocks(lockContext, session, requests); err != nil {
			return nil, tableLockWaitError(err)
		}
		return &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "statement executed successfully"}, nil
	case "start", "stop", "reset", "purge", "flush":
		if handled, err := e.executeParsedReplicationAdmin(stmt); handled {
			if err != nil {
				return nil, err
			}
			return &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "statement executed successfully"}, nil
		}
		return nil, nil
	case "change":
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(stmt.Raw)), "change replication filter") {
			config, err := typedReplicationFilterConfig(stmt)
			if err != nil {
				return nil, err
			}
			if stmt.Channel != "" {
				if e.replicationChangeFilterForChannel == nil {
					return nil, fmt.Errorf("only the default replication channel is supported")
				}
				if err := e.replicationChangeFilterForChannel(stmt.Channel, config); err != nil {
					return nil, err
				}
			} else {
				if e.replicationChangeFilter == nil {
					return nil, fmt.Errorf("replication runtime is not configured")
				}
				if err := e.replicationChangeFilter(config); err != nil {
					return nil, err
				}
			}
			return &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "statement executed successfully"}, nil
		}
		if e == nil {
			return nil, fmt.Errorf("replication runtime is not configured")
		}
		sourceURL, err := typedReplicationSourceURL(stmt)
		if err != nil {
			return nil, err
		}
		if stmt.Channel != "" {
			if e.replicationChangeSourceForChannel == nil {
				return nil, fmt.Errorf("only the default replication channel is supported")
			}
			if err := e.replicationChangeSourceForChannel(stmt.Channel, sourceURL); err != nil {
				return nil, err
			}
		} else {
			if e.replicationChangeSource == nil {
				return nil, fmt.Errorf("replication runtime is not configured")
			}
			if err := e.replicationChangeSource(sourceURL); err != nil {
				return nil, err
			}
		}
		return &Result{ResultType: common.RESULT_TYPE_QUERY, Message: "statement executed successfully"}, nil
	default:
		// Replication and table-maintenance implementations still expose text
		// compatibility entry points; parsing has already validated the shape.
		return nil, nil
	}
}

func typedReplicationFilterConfig(stmt *sqlparser.AdminStatement) (replication.ReplicationFilterConfig, error) {
	var config replication.ReplicationFilterConfig
	if stmt == nil {
		return config, fmt.Errorf("CHANGE REPLICATION FILTER statement is nil")
	}
	parseValues := func(raw string) ([]string, error) {
		raw = strings.TrimSpace(raw)
		if len(raw) < 2 || raw[0] != '(' || raw[len(raw)-1] != ')' {
			return nil, fmt.Errorf("filter option must be parenthesized")
		}
		inner := strings.TrimSpace(raw[1 : len(raw)-1])
		if inner == "" {
			return []string{}, nil
		}
		values := make([]string, 0)
		for _, token := range strings.Split(inner, ",") {
			value := strings.Trim(strings.TrimSpace(token), "'\"")
			value = strings.ReplaceAll(value, "''", "'")
			value = strings.ReplaceAll(value, `""`, `"`)
			if value == "" {
				return nil, fmt.Errorf("filter rule cannot be empty")
			}
			values = append(values, value)
		}
		return values, nil
	}
	for key, raw := range stmt.Options {
		var target *[]string
		switch strings.ToLower(key) {
		case "replicate_do_db":
			target = &config.ReplicateDoDB
		case "replicate_ignore_db":
			target = &config.ReplicateIgnoreDB
		case "replicate_do_table":
			target = &config.ReplicateDoTable
		case "replicate_ignore_table":
			target = &config.ReplicateIgnoreTable
		case "replicate_wild_do_table":
			target = &config.ReplicateWildDoTable
		case "replicate_wild_ignore_table":
			target = &config.ReplicateWildIgnoreTable
		case "replication", "filter":
			continue
		case "replicate_rewrite_db":
			if err := parseTypedReplicationRewrite(raw, &config); err != nil {
				return config, err
			}
			continue
		default:
			return config, fmt.Errorf("unsupported CHANGE REPLICATION FILTER option %q", key)
		}
		values, err := parseValues(raw)
		if err != nil {
			return config, fmt.Errorf("%s: %w", strings.ToUpper(key), err)
		}
		*target = values
	}
	return config, nil
}

func parseTypedReplicationRewrite(raw string, config *replication.ReplicationFilterConfig) error {
	raw = strings.TrimSpace(raw)
	if len(raw) < 4 || raw[0] != '(' || raw[len(raw)-1] != ')' {
		return fmt.Errorf("REPLICATE_REWRITE_DB must be parenthesized")
	}
	inner := strings.TrimSpace(raw[1 : len(raw)-1])
	for _, pair := range strings.Split(inner, "),(") {
		pair = strings.Trim(pair, "() ")
		parts := strings.Split(pair, ",")
		if len(parts) != 2 {
			return fmt.Errorf("REPLICATE_REWRITE_DB requires (from,to) pairs")
		}
		from := strings.Trim(strings.TrimSpace(parts[0]), "'\"")
		to := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
		if from == "" || to == "" {
			return fmt.Errorf("REPLICATE_REWRITE_DB names cannot be empty")
		}
		config.ReplicateRewriteDB = append(config.ReplicateRewriteDB, replication.ReplicationDBRewrite{From: from, To: to})
	}
	if len(config.ReplicateRewriteDB) == 0 {
		return fmt.Errorf("REPLICATE_REWRITE_DB requires at least one pair")
	}
	return nil
}

// typedReplicationSourceURL builds the runtime source URL from lexer-owned
// option values. It never scans the original SQL text and deliberately keeps
// SOURCE_PASSWORD out of logs and AST formatting.
func typedReplicationSourceURL(stmt *sqlparser.AdminStatement) (string, error) {
	if stmt == nil {
		return "", fmt.Errorf("CHANGE REPLICATION SOURCE statement is nil")
	}
	allowed := map[string]struct{}{
		"source_host": {}, "master_host": {}, "source_port": {}, "master_port": {},
		"source_user": {}, "master_user": {}, "source_password": {}, "master_password": {},
		"source_log_file": {}, "master_log_file": {}, "source_log_pos": {}, "master_log_pos": {},
		"source_auto_position": {}, "master_auto_position": {}, "source_ssl": {}, "master_ssl": {},
		"source_ssl_verify_server_cert": {}, "master_ssl_verify_server_cert": {}, "source_ssl_ca": {}, "master_ssl_ca": {},
		"source_ssl_cert": {}, "master_ssl_cert": {}, "source_ssl_key": {}, "master_ssl_key": {},
		"source_connect_retry": {}, "master_connect_retry": {}, "source_retry_count": {}, "master_retry_count": {},
		"source_heartbeat_period": {}, "master_heartbeat_period": {}, "source_compression_algorithms": {}, "master_compression_algorithms": {},
		"source_zstd_compression_level": {}, "master_zstd_compression_level": {},
	}
	keys := make([]string, 0, len(stmt.Options))
	for key := range stmt.Options {
		lower := strings.ToLower(strings.TrimSpace(key))
		if lower == "replication" || lower == "source" || lower == "master" || lower == "to" {
			continue
		}
		if _, ok := allowed[lower]; !ok {
			return "", fmt.Errorf("unsupported CHANGE REPLICATION SOURCE option %q", key)
		}
		keys = append(keys, lower)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		value := strings.ReplaceAll(stmt.Options[key], "'", "''")
		parts = append(parts, strings.ToUpper(key)+"='"+value+"'")
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("CHANGE REPLICATION SOURCE requires SOURCE_HOST")
	}
	return parseReplicationSourceURL("CHANGE REPLICATION SOURCE TO " + strings.Join(parts, ", "))
}

func (e *XMySQLExecutor) executeParsedReplicationAdmin(stmt *sqlparser.AdminStatement) (bool, error) {
	if stmt == nil {
		return false, nil
	}
	channel := stmt.Channel
	switch stmt.Kind {
	case "start":
		if channel != "" {
			if e.replicationStartForChannel == nil {
				return true, fmt.Errorf("only the default replication channel is supported")
			}
			return true, e.replicationStartForChannel(channel)
		}
		if e.replicationStart == nil {
			return true, fmt.Errorf("replication runtime is not configured")
		}
		return true, e.replicationStart()
	case "stop":
		if channel != "" {
			if e.replicationStopForChannel == nil {
				return true, fmt.Errorf("only the default replication channel is supported")
			}
			return true, e.replicationStopForChannel(channel)
		}
		if e.replicationStop == nil {
			return true, fmt.Errorf("replication runtime is not configured")
		}
		return true, e.replicationStop()
	case "reset":
		all := stmt.Options["all"] == "true"
		if all {
			if channel != "" {
				if e.replicationResetAllForChannel == nil {
					return true, fmt.Errorf("only the default replication channel is supported")
				}
				return true, e.replicationResetAllForChannel(channel)
			}
			if e.replicationResetAll == nil {
				return true, fmt.Errorf("replication runtime is not configured")
			}
			return true, e.replicationResetAll()
		}
		if channel != "" {
			if e.replicationResetForChannel == nil {
				return true, fmt.Errorf("only the default replication channel is supported")
			}
			return true, e.replicationResetForChannel(channel)
		}
		if stmt.Options["master"] == "true" {
			if e.replicationResetMaster == nil {
				return true, fmt.Errorf("replication source is not configured")
			}
			return true, e.replicationResetMaster()
		}
		if e.replicationReset == nil {
			return true, fmt.Errorf("replication runtime is not configured")
		}
		return true, e.replicationReset()
	case "purge":
		if stmt.ID == "" {
			return true, fmt.Errorf("PURGE requires a log target")
		}
		if stmt.Options["before"] == "true" {
			if e.replicationPurgeBinaryLogsBefore == nil {
				return true, fmt.Errorf("replication source is not configured")
			}
			cutoff, err := parseBinlogPurgeTime(stmt.ID)
			if err != nil {
				return true, err
			}
			return true, e.replicationPurgeBinaryLogsBefore(cutoff)
		}
		if e.replicationPurgeBinaryLogsTo == nil {
			return true, fmt.Errorf("replication source is not configured")
		}
		return true, e.replicationPurgeBinaryLogsTo(stmt.ID)
	case "flush":
		if stmt.Options["binary"] == "true" && stmt.Options["logs"] == "true" {
			if e.replicationFlushLogs == nil {
				return true, fmt.Errorf("replication source is not configured")
			}
			return true, e.replicationFlushLogs()
		}
	}
	return false, nil
}
