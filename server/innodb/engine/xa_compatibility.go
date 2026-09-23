package engine

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	innodbcommon "github.com/zhukovaskychina/xmysql-server/server/innodb/common"
	"github.com/zhukovaskychina/xmysql-server/server/replication"
)

// xaPreparedTransaction keeps the session-owned change journal alive between
// XA PREPARE and the later XA COMMIT/XA ROLLBACK. The storage-integrated DML
// path already records reversible row images in that journal, so the same
// commit/rollback boundary can be used for a two-phase transaction.
type xaPreparedTransaction struct {
	xid          xaIdentifier
	session      server.MySQLServerSession
	participants []server.MySQLServerSession
	changes      []transactionDMLChange
	statements   []replication.Statement
	journalID    string
	manifestPath string
}

type xaSuspendedTransaction struct {
	xid          xaIdentifier
	state        *sessionTransactionState
	journalID    string
	participants []server.MySQLServerSession
}

type xaIdentifier struct {
	gtrid    string
	bqual    string
	formatID uint32
}

func (x xaIdentifier) key() string {
	return fmt.Sprintf("%d:%x:%x", x.formatID, []byte(x.gtrid), []byte(x.bqual))
}

func (x xaIdentifier) recoverRow(convertXID bool) []interface{} {
	data := x.gtrid + x.bqual
	if convertXID {
		data = hex.EncodeToString([]byte(data))
	}
	return []interface{}{int64(x.formatID), int64(len([]byte(x.gtrid))), int64(len([]byte(x.bqual))), data}
}

// executeXACompatibility handles the common SQL-level XA state machine. It
// is intentionally kept at the raw-SQL seam because the bundled parser does
// not expose XA as an AST statement.
func (e *XMySQLExecutor) executeXACompatibility(ctx *ExecutionContext, session server.MySQLServerSession, query string) (bool, error) {
	op, xid, onePhase, options, ok, err := parseXAStatementOptions(query)
	if !ok {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if session == nil {
		return true, fmt.Errorf("XA requires a session")
	}

	state := strings.ToUpper(strings.TrimSpace(fmt.Sprint(session.GetParamByName("xa_state"))))
	if state == "<NIL>" {
		state = ""
	}
	currentKey := strings.TrimSpace(fmt.Sprint(session.GetParamByName("xa_xid")))
	if currentKey == "<nil>" {
		currentKey = ""
	}

	switch op {
	case "START":
		if hasXAOption(options, "RESUME") || hasXAOption(options, "JOIN") {
			if state == "SUSPENDED" && currentKey == xid.key() {
				suspended := e.takeSuspendedXA(xid.key())
				session.SetParamByName("xa_state", "ACTIVE")
				if suspended != nil {
					if err := e.removeSuspendedXAManifest(suspended); err != nil {
						return true, fmt.Errorf("XAER_RMFAIL: remove suspended XA manifest: %w", err)
					}
				}
				return true, nil
			}
			if state == "" && !sessionBoolParam(session, "in_transaction") {
				if suspended := e.takeSuspendedXA(xid.key()); suspended != nil {
					if err := e.attachSuspendedXA(suspended, session); err != nil {
						e.restoreSuspendedXA(suspended)
						return true, err
					}
					if err := e.removeSuspendedXAManifest(suspended); err != nil {
						return true, fmt.Errorf("XAER_RMFAIL: remove suspended XA manifest: %w", err)
					}
					return true, nil
				}
			}
			return true, fmt.Errorf("XAER_NOTA: XA START %s references an unknown suspended XID", strings.Join(options, " "))
		}
		if state != "" || sessionBoolParam(session, "in_transaction") {
			return true, fmt.Errorf("XAER_OUTSIDE: XA START cannot be executed while another transaction is active")
		}
		if len(options) > 0 {
			return true, fmt.Errorf("XAER_NOTA: XA START %s requires a suspended XA transaction in this session", strings.Join(options, " "))
		}
		e.xaMu.Lock()
		_, duplicatePrepared := e.xaPrepared[xid.key()]
		_, duplicateSuspended := e.xaSuspended[xid.key()]
		e.xaMu.Unlock()
		if duplicatePrepared || duplicateSuspended {
			return true, fmt.Errorf("XAER_DUPID: XA transaction already exists")
		}
		e.clearSessionTransactionState(session)
		session.SetParamByName("xa_state", "ACTIVE")
		session.SetParamByName("xa_xid", xid.key())
		session.SetParamByName("in_transaction", true)
		session.SetParamByName("transaction_journal_active", true)
		session.SessionContext().SetInTransaction(true)
		e.recordActiveTransactionDelta(session, 1)
		return true, nil

	case "END":
		if state != "ACTIVE" || currentKey != xid.key() {
			return true, fmt.Errorf("XAER_NOTA: XA END references an unknown or inactive XID")
		}
		if hasXAOption(options, "SUSPEND") {
			session.SetParamByName("xa_state", "SUSPENDED")
			if err := e.rememberSuspendedXA(xid, session); err != nil {
				session.SetParamByName("xa_state", "ACTIVE")
				return true, fmt.Errorf("XAER_RMFAIL: persist suspended XA transaction: %w", err)
			}
		} else {
			e.takeSuspendedXA(xid.key())
			session.SetParamByName("xa_state", "IDLE")
		}
		return true, nil

	case "PREPARE":
		if state != "IDLE" || currentKey != xid.key() {
			return true, fmt.Errorf("XAER_RMFAIL: XA PREPARE requires an ended XA transaction")
		}
		if pending := session.GetParamByName(pendingAccountFileParam); pending != nil {
			return true, fmt.Errorf("XAER_RMFAIL: XA PREPARE does not support pending account metadata changes")
		}
		transactionState := e.sessionTransactionState(session)
		prepared := &xaPreparedTransaction{
			xid:          xid,
			session:      session,
			participants: xaParticipantsForSession(session),
			changes:      append([]transactionDMLChange(nil), transactionState.Changes...),
			statements:   append([]replication.Statement(nil), transactionState.Statements...),
			journalID:    e.transactionJournalID(session),
		}
		e.xaMu.Lock()
		defer e.xaMu.Unlock()
		if e.xaPrepared == nil {
			e.xaPrepared = make(map[string]*xaPreparedTransaction)
		}
		if _, exists := e.xaPrepared[xid.key()]; exists {
			return true, fmt.Errorf("XAER_DUPID: XA transaction already prepared")
		}
		if err := e.persistPreparedXAManifest(prepared); err != nil {
			return true, fmt.Errorf("XAER_RMFAIL: persist prepared XA transaction: %w", err)
		}
		e.xaPrepared[xid.key()] = prepared
		session.SetParamByName("xa_state", "PREPARED")
		return true, nil

	case "COMMIT":
		if onePhase {
			if state != "ACTIVE" && state != "IDLE" || currentKey != xid.key() {
				return true, fmt.Errorf("XAER_NOTA: XA COMMIT ONE PHASE references an unknown XID")
			}
			return true, e.finishXATransaction(xid, session, false)
		}
		prepared, exists := e.takePreparedXA(xid.key())
		if !exists {
			return true, fmt.Errorf("XAER_NOTA: XA COMMIT references an unknown prepared XID")
		}
		if err := e.finishPreparedXATransaction(prepared, false); err != nil {
			e.restorePreparedXA(prepared)
			return true, err
		}
		return true, nil

	case "ROLLBACK":
		if state == "ACTIVE" || state == "IDLE" {
			if currentKey != xid.key() {
				return true, fmt.Errorf("XAER_NOTA: XA ROLLBACK references an unknown XID")
			}
			return true, e.finishXATransaction(xid, session, true)
		}
		prepared, exists := e.takePreparedXA(xid.key())
		if !exists {
			return true, fmt.Errorf("XAER_NOTA: XA ROLLBACK references an unknown prepared XID")
		}
		if err := e.finishPreparedXATransaction(prepared, true); err != nil {
			e.restorePreparedXA(prepared)
			return true, err
		}
		return true, nil

	case "RECOVER":
		if !sessionHasXARecoverPrivilege(session) {
			return true, fmt.Errorf("Access denied; you need the XA_RECOVER_ADMIN privilege for this operation")
		}
		return true, e.executeXARecover(ctx, hasXARecoverConvertOption(options))
	default:
		return true, fmt.Errorf("XAER_INVAL: unsupported XA operation %s", op)
	}
}

func sessionHasXARecoverPrivilege(session server.MySQLServerSession) bool {
	if session == nil {
		return false
	}
	if privileges, ok := session.GetParamByName("global_privileges").([]common.PrivilegeType); ok {
		for _, privilege := range privileges {
			if privilege == common.SuperPriv || privilege == common.AllPriv {
				return true
			}
		}
	}
	if dynamicPrivileges, ok := session.GetParamByName("dynamic_privileges").([]string); ok {
		for _, privilege := range dynamicPrivileges {
			if strings.EqualFold(strings.TrimSpace(privilege), "XA_RECOVER_ADMIN") {
				return true
			}
		}
	}
	return false
}

func (e *XMySQLExecutor) takePreparedXA(key string) (*xaPreparedTransaction, bool) {
	e.xaMu.Lock()
	defer e.xaMu.Unlock()
	prepared, ok := e.xaPrepared[key]
	if ok {
		delete(e.xaPrepared, key)
	}
	return prepared, ok
}

func (e *XMySQLExecutor) restorePreparedXA(prepared *xaPreparedTransaction) {
	if prepared == nil {
		return
	}
	e.xaMu.Lock()
	if e.xaPrepared == nil {
		e.xaPrepared = make(map[string]*xaPreparedTransaction)
	}
	e.xaPrepared[prepared.xid.key()] = prepared
	e.xaMu.Unlock()
}

func (e *XMySQLExecutor) finishXATransaction(xid xaIdentifier, session server.MySQLServerSession, rollback bool) error {
	if session == nil {
		return fmt.Errorf("XA requires a session")
	}
	state := e.sessionTransactionState(session)
	prepared := &xaPreparedTransaction{
		xid:          xid,
		session:      session,
		participants: xaParticipantsForSession(session),
		changes:      append([]transactionDMLChange(nil), state.Changes...),
		statements:   append([]replication.Statement(nil), state.Statements...),
		journalID:    e.transactionJournalID(session),
	}
	return e.finishPreparedXATransaction(prepared, rollback)
}

func (e *XMySQLExecutor) finishPreparedXATransaction(prepared *xaPreparedTransaction, rollback bool) error {
	if prepared == nil {
		return fmt.Errorf("invalid prepared XA transaction")
	}
	session := prepared.session
	if session == nil {
		if rollback {
			dml, err := e.newStorageIntegratedDMLExecutor()
			if err != nil {
				return err
			}
			if e.tableStorageManager != nil && e.infosSchemaManager != nil {
				if err := e.tableStorageManager.SyncFromInfoSchema(e.infosSchemaManager); err != nil {
					return fmt.Errorf("refresh table storage mapping for prepared XA rollback: %w", err)
				}
			}
			for index := len(prepared.changes) - 1; index >= 0; index-- {
				change := prepared.changes[index]
				schemaName, tableName, ok := strings.Cut(change.tableName, ".")
				if !ok {
					return fmt.Errorf("invalid prepared XA table %q", change.tableName)
				}
				if e.tableStorageManager != nil && e.infosSchemaManager != nil {
					if err := e.tableStorageManager.EnsureTableStorage(context.Background(), e.infosSchemaManager, schemaName, tableName); err != nil {
						return fmt.Errorf("repair storage mapping for prepared XA table %s: %w", change.tableName, err)
					}
				}
				if err := rollbackDMLChange(dml, change); err != nil {
					return fmt.Errorf("rollback prepared XA transaction: %w", err)
				}
			}
		} else if len(prepared.statements) > 0 {
			if e.replicationCommitTransactionHookWithID != nil {
				if err := e.replicationCommitTransactionHookWithID(prepared.xid.key(), replicationRowsFromTransactionChanges(prepared.changes), prepared.statements); err != nil {
					return err
				}
			} else if e.replicationCommitTransactionHook != nil {
				if err := e.replicationCommitTransactionHook(replicationRowsFromTransactionChanges(prepared.changes), prepared.statements); err != nil {
					return err
				}
			} else if e.replicationCommitHook != nil {
				if err := e.replicationCommitHook(prepared.statements); err != nil {
					return err
				}
			}
		}
		return e.removePreparedXAManifest(prepared)
	}
	if rollback {
		if err := e.rollbackSessionTransaction(session, 0); err != nil {
			return err
		}
		e.discardSessionAccountChanges(session)
	} else {
		if err := e.commitSessionAccountChanges(session); err != nil {
			return err
		}
		if err := e.commitReplicationStatementsWithID(session, prepared.xid.key()); err != nil {
			return err
		}
	}
	if sessionBoolParam(session, "in_transaction") {
		state := "COMMITTED"
		if rollback {
			state = "ROLLED BACK"
		}
		e.recordPerformanceSchemaTransactionHistory(session, state)
		e.recordActiveTransactionDelta(session, -1)
	}
	participants := append([]server.MySQLServerSession(nil), prepared.participants...)
	if len(participants) == 0 {
		participants = []server.MySQLServerSession{session}
	}
	for _, participant := range participants {
		if participant == nil {
			continue
		}
		e.clearSessionTransactionState(participant)
		participant.SetParamByName("transaction_journal_active", false)
		participant.SetParamByName("in_transaction", false)
		participant.SetParamByName("xa_state", "")
		participant.SetParamByName("xa_xid", "")
		participant.SetParamByName("xa_participants", nil)
		participant.SetParamByName("transaction_journal_id", "")
		participant.SessionContext().SetInTransaction(false)
	}
	return e.removePreparedXAManifest(prepared)
}

func (e *XMySQLExecutor) rememberSuspendedXA(xid xaIdentifier, session server.MySQLServerSession) error {
	if e == nil || session == nil {
		return fmt.Errorf("XA requires a session")
	}
	participants := xaParticipantsForSession(session)
	if len(participants) == 0 {
		participants = []server.MySQLServerSession{session}
	}
	suspended := &xaSuspendedTransaction{
		xid:          xid,
		state:        e.sessionTransactionState(session),
		journalID:    e.transactionJournalID(session),
		participants: participants,
	}
	if err := e.persistSuspendedXAManifest(suspended); err != nil {
		return err
	}
	e.xaMu.Lock()
	if e.xaSuspended == nil {
		e.xaSuspended = make(map[string]*xaSuspendedTransaction)
	}
	e.xaSuspended[xid.key()] = suspended
	e.xaMu.Unlock()
	return nil
}

func (e *XMySQLExecutor) takeSuspendedXA(key string) *xaSuspendedTransaction {
	if e == nil {
		return nil
	}
	e.xaMu.Lock()
	defer e.xaMu.Unlock()
	if e.xaSuspended == nil {
		return nil
	}
	suspended := e.xaSuspended[key]
	if suspended != nil {
		delete(e.xaSuspended, key)
	}
	return suspended
}

func (e *XMySQLExecutor) restoreSuspendedXA(suspended *xaSuspendedTransaction) {
	if e == nil || suspended == nil {
		return
	}
	e.xaMu.Lock()
	if e.xaSuspended == nil {
		e.xaSuspended = make(map[string]*xaSuspendedTransaction)
	}
	e.xaSuspended[suspended.xid.key()] = suspended
	e.xaMu.Unlock()
}

func (e *XMySQLExecutor) attachSuspendedXA(suspended *xaSuspendedTransaction, session server.MySQLServerSession) error {
	if suspended == nil || suspended.state == nil || session == nil {
		return fmt.Errorf("XAER_RMFAIL: suspended XA transaction is unavailable")
	}
	participants := append([]server.MySQLServerSession(nil), suspended.participants...)
	participants = appendXASession(participants, session)
	for _, participant := range participants {
		if participant == nil {
			continue
		}
		participant.SetParamByName("xa_participants", participants)
		participant.SetParamByName("xa_state", "ACTIVE")
		participant.SetParamByName("xa_xid", suspended.xid.key())
		participant.SetParamByName("transaction_journal_id", suspended.journalID)
		participant.SetParamByName("transaction_dml_state", suspended.state)
		participant.SetParamByName("transaction_journal_active", true)
		participant.SetParamByName("in_transaction", true)
		participant.SessionContext().SetInTransaction(true)
	}
	return nil
}

func xaParticipantsForSession(session server.MySQLServerSession) []server.MySQLServerSession {
	if session == nil {
		return nil
	}
	participants := make([]server.MySQLServerSession, 0, 2)
	if raw := session.GetParamByName("xa_participants"); raw != nil {
		if existing, ok := raw.([]server.MySQLServerSession); ok {
			for _, participant := range existing {
				participants = appendXASession(participants, participant)
			}
		}
	}
	return appendXASession(participants, session)
}

func appendXASession(participants []server.MySQLServerSession, session server.MySQLServerSession) []server.MySQLServerSession {
	if session == nil {
		return participants
	}
	for _, existing := range participants {
		if existing == session {
			return participants
		}
	}
	return append(participants, session)
}

func (e *XMySQLExecutor) executeXARecover(ctx *ExecutionContext, convertXID bool) error {
	columns := []string{"formatID", "gtrid_length", "bqual_length", "data"}
	e.xaMu.Lock()
	rows := make([][]interface{}, 0, len(e.xaPrepared))
	for _, prepared := range e.xaPrepared {
		if prepared != nil {
			rows = append(rows, prepared.xid.recoverRow(convertXID))
		}
	}
	e.xaMu.Unlock()
	sortXARecoverRows(rows)
	ctx.Results <- &Result{Data: newInformationSchemaSelectResult("XA RECOVER", columns, rows), ResultType: innodbcommon.RESULT_TYPE_SELECT, Message: fmt.Sprintf("XA RECOVER returned %d prepared transactions", len(rows))}
	return nil
}

func isXARecoverQuery(query string) bool {
	fields := strings.Fields(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";")))
	return len(fields) >= 2 && strings.EqualFold(fields[0], "XA") && strings.EqualFold(fields[1], "RECOVER")
}

func sortXARecoverRows(rows [][]interface{}) {
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0 && fmt.Sprint(rows[j][3]) < fmt.Sprint(rows[j-1][3]); j-- {
			rows[j], rows[j-1] = rows[j-1], rows[j]
		}
	}
}

func parseXAStatement(query string) (op string, xid xaIdentifier, onePhase, ok bool, err error) {
	op, xid, onePhase, _, ok, err = parseXAStatementOptions(query)
	return op, xid, onePhase, ok, err
}

func parseXAStatementOptions(query string) (op string, xid xaIdentifier, onePhase bool, options []string, ok bool, err error) {
	q := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	fields := strings.Fields(q)
	if len(fields) < 2 || !strings.EqualFold(fields[0], "XA") {
		return "", xaIdentifier{}, false, nil, false, nil
	}
	op = strings.ToUpper(fields[1])
	if op == "BEGIN" {
		op = "START"
	}
	if op == "RECOVER" {
		rest := strings.TrimSpace(q[len(fields[0])+1+len(fields[1]):])
		options := strings.Fields(rest)
		if optionErr := validateXAOptions(op, options); optionErr != nil {
			return op, xaIdentifier{}, false, options, true, optionErr
		}
		return op, xaIdentifier{}, false, options, true, nil
	}
	rest := strings.TrimSpace(q[len(fields[0])+1+len(fields[1]):])
	if op == "COMMIT" && strings.HasSuffix(strings.ToUpper(rest), " ONE PHASE") {
		onePhase = true
		rest = strings.TrimSpace(rest[:len(rest)-len(" ONE PHASE")])
	}
	parts, splitErr := splitXAArguments(rest)
	if splitErr != nil {
		return op, xaIdentifier{}, onePhase, nil, true, splitErr
	}
	if len(parts) < 1 || len(parts) > 3 {
		return op, xaIdentifier{}, onePhase, nil, true, fmt.Errorf("XAER_INVAL: XA %s requires an XID", op)
	}
	if last := len(parts) - 1; last >= 0 {
		identifier, trailing, splitErr := splitXAIdentifierAndOptions(parts[last])
		if splitErr != nil {
			return op, xaIdentifier{}, onePhase, nil, true, splitErr
		}
		parts[last] = identifier
		options = strings.Fields(trailing)
	}
	if optionErr := validateXAOptions(op, options); optionErr != nil {
		return op, xaIdentifier{}, onePhase, options, true, optionErr
	}
	xid.gtrid = unquoteXA(parts[0])
	if len(parts) >= 2 {
		xid.bqual = unquoteXA(parts[1])
	}
	xid.formatID = 1
	if len(parts) == 3 {
		value, parseErr := strconv.ParseUint(strings.TrimSpace(parts[2]), 10, 32)
		if parseErr != nil {
			return op, xaIdentifier{}, onePhase, options, true, fmt.Errorf("XAER_INVAL: invalid XID format ID")
		}
		xid.formatID = uint32(value)
	}
	if xid.gtrid == "" {
		return op, xaIdentifier{}, onePhase, options, true, fmt.Errorf("XAER_INVAL: XID gtrid cannot be empty")
	}
	return op, xid, onePhase, options, true, nil
}

func splitXAIdentifierAndOptions(raw string) (identifier, options string, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", fmt.Errorf("XAER_INVAL: empty XID component")
	}
	if raw[0] != '\'' && raw[0] != '"' {
		fields := strings.Fields(raw)
		if len(fields) == 0 {
			return "", "", fmt.Errorf("XAER_INVAL: empty XID component")
		}
		return fields[0], strings.TrimSpace(raw[len(fields[0]):]), nil
	}
	quote := raw[0]
	escaped := false
	for index := 1; index < len(raw); index++ {
		character := raw[index]
		if escaped {
			escaped = false
			continue
		}
		if character == '\\' {
			escaped = true
			continue
		}
		if character == quote {
			return raw[:index+1], strings.TrimSpace(raw[index+1:]), nil
		}
	}
	return "", "", fmt.Errorf("XAER_INVAL: unterminated XID quote")
}

func validateXAOptions(op string, options []string) error {
	if len(options) == 0 {
		return nil
	}
	switch op {
	case "RECOVER":
		if len(options) != 2 || !strings.EqualFold(options[0], "CONVERT") || !strings.EqualFold(options[1], "XID") {
			return fmt.Errorf("XAER_INVAL: XA RECOVER supports only CONVERT XID")
		}
	case "START":
		if len(options) != 1 || (!strings.EqualFold(options[0], "JOIN") && !strings.EqualFold(options[0], "RESUME")) {
			return fmt.Errorf("XAER_INVAL: XA START supports only JOIN or RESUME")
		}
	case "END":
		if !strings.EqualFold(options[0], "SUSPEND") || len(options) > 3 || (len(options) > 1 && !strings.EqualFold(options[1], "FOR")) || (len(options) > 2 && !strings.EqualFold(options[2], "MIGRATE")) {
			return fmt.Errorf("XAER_INVAL: XA END supports SUSPEND [FOR MIGRATE]")
		}
	default:
		return fmt.Errorf("XAER_INVAL: XA %s does not support options", op)
	}
	return nil
}

func hasXARecoverConvertOption(options []string) bool {
	return len(options) == 2 && strings.EqualFold(options[0], "CONVERT") && strings.EqualFold(options[1], "XID")
}

func hasXAOption(options []string, wanted string) bool {
	return len(options) > 0 && strings.EqualFold(strings.TrimSpace(options[0]), wanted)
}

func splitXAArguments(raw string) ([]string, error) {
	var parts []string
	start := 0
	var quote byte
	escaped := false
	for index := 0; index < len(raw); index++ {
		character := raw[index]
		if escaped {
			escaped = false
			continue
		}
		if quote != 0 && character == '\\' {
			escaped = true
			continue
		}
		if quote != 0 {
			if character == quote {
				quote = 0
			}
			continue
		}
		if character == '\'' || character == '"' {
			quote = character
			continue
		}
		if character == ',' {
			parts = append(parts, strings.TrimSpace(raw[start:index]))
			start = index + 1
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("XAER_INVAL: unterminated XID quote")
	}
	parts = append(parts, strings.TrimSpace(raw[start:]))
	return parts, nil
}

func unquoteXA(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 2 && ((value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"')) {
		value = value[1 : len(value)-1]
	}
	return strings.ReplaceAll(value, "\\'", "'")
}
