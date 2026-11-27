package engine

import (
	"context"
	"fmt"
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
		requiredPrivilege := sourceBinlogPrivilegeFromAST(stmt)
		if stmt.Kind == "change" || stmt.Kind == "start" || stmt.Kind == "stop" {
			requiredPrivilege = "REPLICATION_SLAVE_ADMIN"
		}
		if stmt.Kind == "reset" && stmt.Options["replica"] != "" {
			requiredPrivilege = "RELOAD"
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
		if stmt.Scope == "filter" {
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
	if stmt == nil || stmt.Kind != "change" || stmt.Scope != "filter" || len(stmt.OptionValues) == 0 {
		return config, fmt.Errorf("CHANGE REPLICATION FILTER requires a filter option")
	}
	for key, option := range stmt.OptionValues {
		if !option.IsList {
			return config, fmt.Errorf("%s must be parenthesized", strings.ToUpper(key))
		}
		var target *[]string
		switch key {
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
		case "replicate_rewrite_db":
			if len(option.List) == 0 {
				return config, fmt.Errorf("rewrite rule list cannot be empty")
			}
			for _, pair := range option.List {
				if !pair.IsList || len(pair.List) != 2 || pair.List[0].IsList || pair.List[1].IsList || strings.TrimSpace(pair.List[0].Text) == "" || strings.TrimSpace(pair.List[1].Text) == "" {
					return config, fmt.Errorf("rewrite rule must contain exactly two database names")
				}
				config.ReplicateRewriteDB = append(config.ReplicateRewriteDB, replication.ReplicationDBRewrite{From: pair.List[0].Text, To: pair.List[1].Text})
			}
			continue
		default:
			return config, fmt.Errorf("unsupported CHANGE REPLICATION FILTER option %q", key)
		}
		values := make([]string, 0, len(option.List))
		for _, item := range option.List {
			if item.IsList || strings.TrimSpace(item.Text) == "" {
				return config, fmt.Errorf("%s: filter rule cannot be empty or nested", strings.ToUpper(key))
			}
			values = append(values, item.Text)
		}
		*target = values
	}
	return config, nil
}

// typedReplicationSourceURL builds the runtime source URL from lexer-owned
// option values. It never scans the original SQL text and deliberately keeps
// SOURCE_PASSWORD out of logs and AST formatting.
func typedReplicationSourceURL(stmt *sqlparser.AdminStatement) (string, error) {
	if stmt == nil || stmt.Kind != "change" || (stmt.Scope != "source" && stmt.Scope != "master") {
		return "", fmt.Errorf("CHANGE REPLICATION SOURCE statement is required")
	}
	for key, value := range stmt.OptionValues {
		if value.IsList {
			return "", fmt.Errorf("unsupported CHANGE REPLICATION SOURCE option %q", key)
		}
	}
	return replicationSourceURLFromOptions(stmt.Options)
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
		if stmt.Options["binary_gtids"] == "true" {
			index := uint64(1)
			if stmt.ID != "" {
				var err error
				index, err = strconv.ParseUint(stmt.ID, 10, 32)
				if err != nil || index == 0 {
					return true, fmt.Errorf("invalid RESET BINARY LOGS AND GTIDS file index %q", stmt.ID)
				}
			}
			if e.replicationResetBinaryLogsAndGTIDs == nil {
				return true, fmt.Errorf("replication source is not configured")
			}
			return true, e.replicationResetBinaryLogsAndGTIDs(uint32(index))
		}
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
