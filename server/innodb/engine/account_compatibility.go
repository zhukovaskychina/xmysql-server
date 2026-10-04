package engine

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
)

type persistedAccount struct {
	User                   string                     `json:"user"`
	Host                   string                     `json:"host"`
	Password               string                     `json:"password"`
	Plugin                 string                     `json:"plugin,omitempty"`
	TLSRequired            bool                       `json:"tls_required,omitempty"`
	X509Required           bool                       `json:"x509_required,omitempty"`
	SSLCipher              string                     `json:"ssl_cipher,omitempty"`
	X509Issuer             string                     `json:"x509_issuer,omitempty"`
	X509Subject            string                     `json:"x509_subject,omitempty"`
	MaxQuestions           *int64                     `json:"max_questions,omitempty"`
	MaxUpdates             *int64                     `json:"max_updates,omitempty"`
	MaxConnections         *int64                     `json:"max_connections,omitempty"`
	MaxUserConnections     *int64                     `json:"max_user_connections,omitempty"`
	AccountLocked          bool                       `json:"account_locked"`
	PasswordExpired        bool                       `json:"password_expired"`
	PasswordLastChanged    *string                    `json:"password_last_changed,omitempty"`
	PasswordLifetime       *int64                     `json:"password_lifetime,omitempty"`
	PasswordReuseHistory   *int64                     `json:"password_reuse_history,omitempty"`
	PasswordReuseTime      *int64                     `json:"password_reuse_time,omitempty"`
	PasswordRequireCurrent *bool                      `json:"password_require_current,omitempty"`
	FailedLoginAttempts    *int64                     `json:"failed_login_attempts,omitempty"`
	PasswordLockTime       *int64                     `json:"password_lock_time,omitempty"`
	PasswordLockUnbounded  bool                       `json:"password_lock_unbounded,omitempty"`
	FailedLoginCount       int64                      `json:"failed_login_count,omitempty"`
	PasswordLockedUntil    *string                    `json:"password_locked_until,omitempty"`
	PasswordHistory        []persistedPasswordHistory `json:"password_history,omitempty"`
	Roles                  []string                   `json:"roles,omitempty"`
	RoleAdminOptions       []string                   `json:"role_admin_options,omitempty"`
	DefaultRoles           []string                   `json:"default_roles,omitempty"`
	GlobalGrants           []string                   `json:"global_grants,omitempty"`
	Grants                 map[string][]string        `json:"grants,omitempty"`
	ColumnGrants           map[string][]string        `json:"column_grants,omitempty"`
	Grantors               map[string]string          `json:"grantors,omitempty"`
	ColumnGrantors         map[string]string          `json:"column_grantors,omitempty"`
	RoleGrantors           map[string]string          `json:"role_grantors,omitempty"`
	Restrictions           map[string][]string        `json:"restrictions,omitempty"`
	UserAttributes         string                     `json:"user_attributes,omitempty"`
	UpdatedAt              string                     `json:"updated_at"`
}

type persistedPasswordHistory struct {
	Password          string `json:"password"`
	PasswordTimestamp string `json:"password_timestamp"`
}

type persistedAccountFile struct {
	Accounts []persistedAccount `json:"accounts"`
}

const pendingAccountFileParam = "pending_account_file"

func (e *XMySQLExecutor) accountFilePath() string {
	return filepath.Join(e.getDataDir(), "mysql", "accounts.json")
}

func (e *XMySQLExecutor) loadPersistedAccounts() (persistedAccountFile, error) {
	raw, err := os.ReadFile(e.accountFilePath())
	if os.IsNotExist(err) {
		return persistedAccountFile{Accounts: []persistedAccount{}}, nil
	}
	if err != nil {
		return persistedAccountFile{}, err
	}
	var file persistedAccountFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return persistedAccountFile{}, err
	}
	return file, nil
}

func (e *XMySQLExecutor) savePersistedAccounts(file persistedAccountFile) error {
	dir := filepath.Dir(e.accountFilePath())
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	sort.Slice(file.Accounts, func(i, j int) bool {
		if file.Accounts[i].User == file.Accounts[j].User {
			return file.Accounts[i].Host < file.Accounts[j].Host
		}
		return file.Accounts[i].User < file.Accounts[j].User
	})
	raw, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return err
	}
	return writeMetadataFileAtomic(e.accountFilePath(), raw)
}

func (e *XMySQLExecutor) accountFileForSession(ctx *ExecutionContext) (persistedAccountFile, error) {
	var file persistedAccountFile
	if ctx != nil && ctx.Session != nil && sessionTransactionActive(ctx.Session) {
		if raw := ctx.Session.GetParamByName(pendingAccountFileParam); raw != nil {
			if file, ok := raw.(*persistedAccountFile); ok && file != nil {
				e.ensureMandatoryRolesActive(*file, ctx.Session)
				return *file, nil
			}
		}
	}
	file, err := e.loadPersistedAccounts()
	if err != nil {
		return persistedAccountFile{}, err
	}
	if ctx != nil {
		e.ensureMandatoryRolesActive(file, ctx.Session)
	}
	return file, nil
}

func (e *XMySQLExecutor) stageOrSaveAccountFile(session server.MySQLServerSession, file persistedAccountFile) error {
	if session != nil && sessionTransactionActive(session) {
		session.SetParamByName(pendingAccountFileParam, &file)
		return nil
	}
	return e.savePersistedAccounts(file)
}

func (e *XMySQLExecutor) commitSessionAccountChanges(session server.MySQLServerSession) error {
	if session == nil {
		return nil
	}
	raw := session.GetParamByName(pendingAccountFileParam)
	file, ok := raw.(*persistedAccountFile)
	if ok && file != nil {
		e.lockPerformanceSchemaMutex(&e.accountMu, "wait/synch/mutex/sql/xmysql/account", int64(sessionConnectionID(session)))
		if err := e.savePersistedAccounts(*file); err != nil {
			e.unlockPerformanceSchemaMutex(&e.accountMu, "wait/synch/mutex/sql/xmysql/account")
			return err
		}
		e.unlockPerformanceSchemaMutex(&e.accountMu, "wait/synch/mutex/sql/xmysql/account")
		session.SetParamByName(pendingAccountFileParam, nil)
	}
	return e.commitSessionOptimizerCostChanges(session)
}

func (e *XMySQLExecutor) discardSessionAccountChanges(session server.MySQLServerSession) {
	if session != nil {
		session.SetParamByName(pendingAccountFileParam, nil)
		e.discardSessionOptimizerCostChanges(session)
	}
}

func nativePasswordHash(password string) string {
	if password == "" {
		return ""
	}
	first := sha1.Sum([]byte(password))
	second := sha1.Sum(first[:])
	return fmt.Sprintf("*%X", second)
}

func passwordHashForPlugin(plugin, password string) string {
	if password == "" {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(plugin)) {
	case "caching_sha2_password":
		first := sha256.Sum256([]byte(password))
		second := sha256.Sum256(first[:])
		return hex.EncodeToString(second[:])
	case "sha256_password":
		digest := sha256.Sum256([]byte(password))
		return hex.EncodeToString(digest[:])
	default:
		return nativePasswordHash(password)
	}
}

func appendPasswordHistory(account *persistedAccount) {
	if account == nil || account.Password == "" {
		return
	}
	account.PasswordHistory = append(account.PasswordHistory, persistedPasswordHistory{
		Password:          account.Password,
		PasswordTimestamp: time.Now().UTC().Format(time.RFC3339),
	})
}

func parsePasswordHistoryOption(query string) (*int64, bool, error) {
	match := regexp.MustCompile(`(?is)\bpassword\s+history\s+(default|[0-9]+)\b`).FindStringSubmatch(query)
	if len(match) != 2 {
		return nil, false, nil
	}
	if strings.EqualFold(match[1], "default") {
		return nil, true, nil
	}
	value, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil || value < 0 {
		return nil, true, fmt.Errorf("invalid PASSWORD HISTORY value %q", match[1])
	}
	return &value, true, nil
}

func parsePasswordReuseIntervalOption(query string) (*int64, bool, error) {
	match := regexp.MustCompile(`(?is)\bpassword\s+reuse\s+interval\s+(default|[0-9]+)\s+day\b`).FindStringSubmatch(query)
	if len(match) != 2 {
		return nil, false, nil
	}
	if strings.EqualFold(match[1], "default") {
		return nil, true, nil
	}
	value, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil || value < 0 {
		return nil, true, fmt.Errorf("invalid PASSWORD REUSE INTERVAL value %q", match[1])
	}
	return &value, true, nil
}

func parsePasswordRequireCurrentOption(query string) (*bool, bool, error) {
	match := regexp.MustCompile(`(?is)\bpassword\s+require\s+current\b(?:\s+(default|optional))?`).FindStringSubmatch(query)
	if len(match) != 2 {
		return nil, false, nil
	}
	if strings.EqualFold(match[1], "default") {
		return nil, true, nil
	}
	value := !strings.EqualFold(match[1], "optional")
	return &value, true, nil
}

func parsePasswordLifetimeOption(query string) (*int64, bool, error) {
	match := regexp.MustCompile(`(?is)\bpassword\s+expire\s+interval\s+([0-9]+)\s+day\b`).FindStringSubmatch(query)
	if len(match) != 2 {
		return nil, false, nil
	}
	value, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil || value < 0 || value > 65535 {
		return nil, true, fmt.Errorf("invalid PASSWORD EXPIRE INTERVAL value %q", match[1])
	}
	return &value, true, nil
}

func parseFailedLoginPolicyOptions(query string) (attempts *int64, lockTime *int64, unbounded bool, hasAttempts, hasLockTime bool, err error) {
	if match := regexp.MustCompile(`(?is)\bfailed_login_attempts\s+([0-9]+)\b`).FindStringSubmatch(query); len(match) == 2 {
		value, parseErr := strconv.ParseInt(match[1], 10, 64)
		if parseErr != nil || value < 0 || value > 32767 {
			return nil, nil, false, true, false, fmt.Errorf("invalid FAILED_LOGIN_ATTEMPTS value %q", match[1])
		}
		attempts = &value
		hasAttempts = true
	}
	if match := regexp.MustCompile(`(?is)\bpassword_lock_time\s+(unbounded|[0-9]+)\b`).FindStringSubmatch(query); len(match) == 2 {
		hasLockTime = true
		if strings.EqualFold(match[1], "unbounded") {
			unbounded = true
			return attempts, nil, unbounded, hasAttempts, hasLockTime, nil
		}
		value, parseErr := strconv.ParseInt(match[1], 10, 64)
		if parseErr != nil || value < 0 || value > 32767 {
			return nil, nil, false, hasAttempts, true, fmt.Errorf("invalid PASSWORD_LOCK_TIME value %q", match[1])
		}
		lockTime = &value
	}
	return attempts, lockTime, unbounded, hasAttempts, hasLockTime, nil
}

type accountResourceOptions struct {
	MaxQuestions       *int64
	MaxUpdates         *int64
	MaxConnections     *int64
	MaxUserConnections *int64
}

func parseAccountResourceOptions(query string) (accountResourceOptions, bool, error) {
	options := accountResourceOptions{}
	hasOption := false
	definitions := []struct {
		name   string
		target **int64
	}{
		{"max_queries_per_hour", &options.MaxQuestions},
		{"max_updates_per_hour", &options.MaxUpdates},
		{"max_connections_per_hour", &options.MaxConnections},
		{"max_user_connections", &options.MaxUserConnections},
	}
	for _, definition := range definitions {
		pattern := regexp.MustCompile(`(?is)\b` + definition.name + `\s+([0-9]+)\b`)
		match := pattern.FindStringSubmatch(query)
		if len(match) != 2 {
			continue
		}
		value, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil || value < 0 || value > 4294967295 {
			return accountResourceOptions{}, true, fmt.Errorf("invalid %s value %q", strings.ToUpper(definition.name), match[1])
		}
		valueCopy := value
		*definition.target = &valueCopy
		hasOption = true
	}
	return options, hasOption, nil
}

func parseTLSAccountOptions(query string) (tlsRequired, x509Required bool, cipher, issuer, subject string, hasRequirement bool) {
	lower := strings.ToLower(query)
	requirement := regexp.MustCompile(`(?is)\brequire\s+(none|ssl|x509)\b`).FindStringSubmatch(lower)
	if len(requirement) == 2 {
		hasRequirement = true
		switch requirement[1] {
		case "ssl":
			tlsRequired = true
		case "x509":
			tlsRequired, x509Required = true, true
		}
	}
	for _, item := range []struct {
		name   string
		target *string
	}{
		{"cipher", &cipher},
		{"issuer", &issuer},
		{"subject", &subject},
	} {
		pattern := regexp.MustCompile(`(?is)\b` + item.name + `\s+'((?:''|[^'])*)'`)
		match := pattern.FindStringSubmatch(query)
		if len(match) == 2 {
			*item.target = strings.ReplaceAll(match[1], "''", "'")
			tlsRequired = true
			hasRequirement = true
		}
	}
	return
}

// parseUserAttributeOption parses the JSON object accepted by CREATE/ALTER
// USER ... ATTRIBUTE.  MySQL stores this value in mysql.user.User_attributes
// and exposes the same document through INFORMATION_SCHEMA.USER_ATTRIBUTES.
func parseUserAttributeOption(query string) (string, bool, error) {
	if !regexp.MustCompile(`(?is)\battribute\s+default\b`).MatchString(query) {
		match := regexp.MustCompile(`(?is)\battribute\s+'((?:''|[^'])*)'`).FindStringSubmatch(query)
		if len(match) != 2 {
			return "", false, nil
		}
		value := strings.ReplaceAll(match[1], "''", "'")
		var document interface{}
		if err := json.Unmarshal([]byte(value), &document); err != nil {
			return "", true, fmt.Errorf("invalid user ATTRIBUTE JSON: %w", err)
		}
		if _, ok := document.(map[string]interface{}); !ok {
			return "", true, fmt.Errorf("user ATTRIBUTE must be a JSON object")
		}
		return value, true, nil
	}
	return "", true, nil
}

func parseUserCommentOption(query string) (string, bool, error) {
	match := regexp.MustCompile(`(?is)\bcomment\s+'((?:''|[^'])*)'`).FindStringSubmatch(query)
	if len(match) != 2 {
		return "", false, nil
	}
	return strings.ReplaceAll(match[1], "''", "'"), true, nil
}

func userAttributesWithComment(existing, comment string) (string, error) {
	attributes := map[string]interface{}{}
	if strings.TrimSpace(existing) != "" {
		if err := json.Unmarshal([]byte(existing), &attributes); err != nil {
			return "", err
		}
	}
	attributes["comment"] = comment
	raw, err := json.Marshal(attributes)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func passwordChangedAt(password string) *string {
	if password == "" {
		return nil
	}
	value := time.Now().UTC().Format(time.RFC3339)
	return &value
}

func (e *XMySQLExecutor) globalPasswordReusePolicy() (int64, int64) {
	if e == nil || e.storageManager == nil || e.storageManager.GetSystemVariablesManager() == nil {
		return 0, 0
	}
	managerValue := e.storageManager.GetSystemVariablesManager()
	read := func(name string) int64 {
		value, err := managerValue.GetVariable("", name, manager.GlobalScope)
		if err != nil {
			return 0
		}
		switch typed := value.(type) {
		case int64:
			if typed > 0 {
				return typed
			}
		case int:
			if typed > 0 {
				return int64(typed)
			}
		case uint64:
			if typed > 0 {
				return int64(typed)
			}
		case string:
			parsed, _ := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
			if parsed > 0 {
				return parsed
			}
		}
		return 0
	}
	return read("password_history"), read("password_reuse_interval")
}

func (e *XMySQLExecutor) globalPasswordRequireCurrent() bool {
	if e == nil || e.storageManager == nil || e.storageManager.GetSystemVariablesManager() == nil {
		return false
	}
	value, err := e.storageManager.GetSystemVariablesManager().GetVariable("", "password_require_current", manager.GlobalScope)
	if err != nil {
		return false
	}
	return sessionBoolValue(value)
}

func (e *XMySQLExecutor) passwordRequireCurrent(account persistedAccount) bool {
	if account.PasswordRequireCurrent != nil {
		return *account.PasswordRequireCurrent
	}
	return e.globalPasswordRequireCurrent()
}

func parsePasswordReplace(query string) (string, bool) {
	match := regexp.MustCompile(`(?is)\breplace\s+'([^']*)'`).FindStringSubmatch(query)
	if len(match) != 2 {
		return "", false
	}
	return match[1], true
}

func (e *XMySQLExecutor) validateCurrentPasswordForSelf(session server.MySQLServerSession, account persistedAccount, query string) error {
	if session == nil || !sessionOwnsAccount(session, account.User, account.Host) || strings.EqualFold(strings.TrimSpace(fmt.Sprint(session.GetParamByName("user"))), "root") {
		return nil
	}
	if !e.passwordRequireCurrent(account) {
		return nil
	}
	current, ok := parsePasswordReplace(query)
	if !ok || passwordHashForPlugin(account.Plugin, current) != account.Password {
		return fmt.Errorf("Current password is required for '%s'@'%s'", account.User, account.Host)
	}
	return nil
}

func (e *XMySQLExecutor) passwordReusePolicy(account persistedAccount) (int64, int64) {
	history, interval := e.globalPasswordReusePolicy()
	if account.PasswordReuseHistory != nil {
		history = *account.PasswordReuseHistory
	}
	if account.PasswordReuseTime != nil {
		interval = *account.PasswordReuseTime
	}
	return history, interval
}

func (e *XMySQLExecutor) validatePasswordReuse(account persistedAccount, passwordHash string) error {
	if passwordHash == "" {
		return nil
	}
	historyLimit, intervalDays := e.passwordReusePolicy(account)
	if historyLimit <= 0 && intervalDays <= 0 {
		return nil
	}
	if account.Password != "" && account.Password == passwordHash {
		return fmt.Errorf("Password reuse is not allowed for '%s'@'%s'", account.User, account.Host)
	}
	if historyLimit > 0 {
		seen := int64(0)
		for index := len(account.PasswordHistory) - 1; index >= 0 && seen < historyLimit; index-- {
			seen++
			if account.PasswordHistory[index].Password == passwordHash {
				return fmt.Errorf("Password reuse is not allowed for '%s'@'%s'", account.User, account.Host)
			}
		}
	}
	if intervalDays > 0 {
		cutoff := time.Now().UTC().Add(-time.Duration(intervalDays) * 24 * time.Hour)
		for _, history := range account.PasswordHistory {
			if history.Password != passwordHash {
				continue
			}
			when, err := time.Parse(time.RFC3339, history.PasswordTimestamp)
			if err == nil && !when.Before(cutoff) {
				return fmt.Errorf("Password reuse is not allowed for '%s'@'%s'", account.User, account.Host)
			}
		}
	}
	return nil
}

func accountTargetFromQuery(query string) (user, host string, ok bool) {
	match := regexp.MustCompile(`(?is)'([^']*)'\s*@\s*'([^']*)'`).FindStringSubmatch(query)
	if len(match) != 3 {
		return "", "", false
	}
	return match[1], match[2], true
}

func accountTargetsFromQuery(query string) [][2]string {
	matches := regexp.MustCompile(`(?is)'([^']*)'\s*@\s*'([^']*)'`).FindAllStringSubmatch(query, -1)
	result := make([][2]string, 0, len(matches))
	for _, match := range matches {
		if len(match) == 3 {
			result = append(result, [2]string{match[1], match[2]})
		}
	}
	return result
}

func accountGranteeTargetsFromQuery(query string) [][2]string {
	match := regexp.MustCompile(`(?is)\b(?:to|from)\b\s+(.+?)(?:\s+with\s+(?:grant|admin)\s+option)?\s*;?\s*$`).FindStringSubmatch(query)
	if len(match) != 2 {
		return nil
	}
	return accountTargetsFromQuery(match[1])
}

func roleReferences(raw string) []string {
	matches := regexp.MustCompile(`(?is)'([^']*)'\s*@\s*'([^']*)'`).FindAllStringSubmatch(raw, -1)
	roles := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) == 3 {
			roles = append(roles, match[1]+"@"+match[2])
		}
	}
	return roles
}

func accountHasRole(account persistedAccount, role string) bool {
	for _, assigned := range account.Roles {
		if strings.EqualFold(assigned, role) {
			return true
		}
	}
	return false
}

func accountHostMatches(host, pattern string) bool {
	if pattern == host {
		return true
	}
	var builder strings.Builder
	builder.WriteString("^")
	for _, char := range pattern {
		switch char {
		case '%':
			builder.WriteString(".*")
		case '_':
			builder.WriteByte('.')
		default:
			builder.WriteString(regexp.QuoteMeta(string(char)))
		}
	}
	builder.WriteString("$")
	matched, _ := regexp.MatchString(builder.String(), host)
	return matched
}

func sessionAccount(file persistedAccountFile, session server.MySQLServerSession) *persistedAccount {
	if session == nil {
		return nil
	}
	user, _ := session.GetParamByName("user").(string)
	host, _ := session.GetParamByName("host").(string)
	if user == "" {
		return nil
	}
	best := (*persistedAccount)(nil)
	// A '%' host pattern is intentionally scored below zero so an exact host
	// or a more specific wildcard wins. It must still be eligible when it is
	// the only matching account, however.
	bestScore := -1 << 60
	for index := range file.Accounts {
		account := &file.Accounts[index]
		if !strings.EqualFold(account.User, user) || !accountHostMatches(host, account.Host) {
			continue
		}
		score := 0
		if account.Host == host {
			score = 1_000_000 + len(account.Host)
		} else {
			for _, char := range account.Host {
				if char == '%' {
					score--
				} else if char == '_' {
					score++
				} else {
					score += 10
				}
			}
		}
		if score > bestScore {
			candidate := *account
			best = &candidate
			bestScore = score
		}
	}
	return best
}

func effectiveAccountGrants(file persistedAccountFile, account persistedAccount, session server.MySQLServerSession) map[string][]string {
	grants := make(map[string][]string, len(account.Grants)+len(account.Restrictions))
	for scope, privileges := range account.Grants {
		grants[scope] = append([]string(nil), privileges...)
	}
	// Dynamic privileges are persisted separately from mysql.user-style static
	// grants, but they are still global (*.*) privileges for authorization and
	// INFORMATION_SCHEMA visibility checks.
	grants["*.*"] = appendUniqueStrings(grants["*.*"], account.GlobalGrants...)
	addPartialRevokeRestrictions(grants, account.Restrictions)
	roles := append([]string(nil), account.Roles...)
	if session != nil {
		if active, ok := session.GetParamByName("active_roles").([]string); ok {
			roles = active
		}
	}
	visited := map[string]bool{}
	var mergeRole func(string, int)
	mergeRole = func(roleName string, depth int) {
		if depth > 16 || visited[strings.ToLower(roleName)] {
			return
		}
		visited[strings.ToLower(roleName)] = true
		parts := strings.SplitN(roleName, "@", 2)
		if len(parts) != 2 {
			return
		}
		for _, role := range file.Accounts {
			if !strings.EqualFold(role.User, parts[0]) || !strings.EqualFold(role.Host, parts[1]) {
				continue
			}
			for scope, privileges := range role.Grants {
				grants[scope] = appendUniqueStrings(grants[scope], privileges...)
			}
			grants["*.*"] = appendUniqueStrings(grants["*.*"], role.GlobalGrants...)
			addPartialRevokeRestrictions(grants, role.Restrictions)
			for _, nested := range role.Roles {
				mergeRole(nested, depth+1)
			}
			break
		}
	}
	for _, role := range roles {
		mergeRole(role, 1)
	}
	return grants
}

func scopeCovers(requested, granted string) bool {
	requested = strings.ToLower(strings.TrimSpace(requested))
	granted = strings.ToLower(strings.TrimSpace(granted))
	if granted == "*.*" {
		return true
	}
	requestedParts := strings.Split(requested, ".")
	grantedParts := strings.Split(granted, ".")
	if len(requestedParts) != 2 || len(grantedParts) != 2 {
		return requested == granted
	}
	return (grantedParts[0] == requestedParts[0] || grantedParts[0] == "*") &&
		(grantedParts[1] == requestedParts[1] || grantedParts[1] == "*")
}

func grantsContain(grants map[string][]string, scope, wanted string) bool {
	for grantedScope, privileges := range grants {
		if strings.HasPrefix(grantedScope, partialRevokeScopePrefix) {
			continue
		}
		if !scopeCovers(scope, grantedScope) {
			continue
		}
		for _, privilege := range privileges {
			if strings.EqualFold(privilege, wanted) || strings.EqualFold(privilege, "ALL") || strings.EqualFold(privilege, "ALL PRIVILEGES") {
				// A partial revoke restricts the global grant only. Explicit
				// schema/table/column grants can still restore access inside the
				// restricted schema, matching MySQL's privilege aggregation rules.
				if grantedScope == "*.*" && partialRevokeDenied(grants, scope, wanted) {
					continue
				}
				return true
			}
		}
	}
	return false
}

// checkGlobalPrivilege applies a global static or dynamic privilege gate to a
// real authenticated session. Nil sessions are internal execution paths and
// intentionally remain unrestricted, matching the other compatibility gates.
func (e *XMySQLExecutor) checkGlobalPrivilege(ctx *ExecutionContext, required string) error {
	if e == nil || ctx == nil || ctx.Session == nil {
		return nil
	}
	user, _ := ctx.Session.GetParamByName("user").(string)
	user = strings.TrimSpace(user)
	if user == "" || strings.EqualFold(user, "root") {
		return nil
	}
	allowSuper := strings.EqualFold(strings.TrimSpace(required), "REPLICATION_SLAVE_ADMIN")
	allowReload := strings.EqualFold(strings.TrimSpace(required), "RELOAD") || strings.EqualFold(strings.TrimSpace(required), "FLUSH_OPTIMIZER_COSTS")
	if privileges, ok := ctx.Session.GetParamByName("global_privileges").([]common.PrivilegeType); ok {
		for _, privilege := range privileges {
			if privilege == common.AllPriv || (allowSuper && privilege == common.SuperPriv) ||
				(allowReload && privilege == common.ReloadPriv) {
				return nil
			}
		}
	}
	if dynamicPrivileges, ok := ctx.Session.GetParamByName("dynamic_privileges").([]string); ok {
		for _, privilege := range dynamicPrivileges {
			if strings.EqualFold(strings.TrimSpace(privilege), required) {
				return nil
			}
		}
	}

	host, _ := ctx.Session.GetParamByName("host").(string)
	host = strings.TrimSpace(host)
	if host == "" {
		host = "localhost"
	}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		return err
	}
	account := sessionAccount(file, ctx.Session)
	if account == nil {
		return fmt.Errorf("Access denied; user '%s'@'%s' does not exist", user, host)
	}
	grants := effectiveAccountGrants(file, *account, ctx.Session)
	if grantsContain(grants, "*.*", required) ||
		(allowSuper && grantsContain(grants, "*.*", "SUPER")) {
		return nil
	}
	if allowSuper {
		return fmt.Errorf("Access denied; you need the %s or SUPER privilege for this operation", required)
	}
	return fmt.Errorf("Access denied; you need the %s privilege for this operation", required)
}

const partialRevokeScopePrefix = "__xmysql_partial_revoke__:"

func addPartialRevokeRestrictions(grants map[string][]string, restrictions map[string][]string) {
	for database, privileges := range restrictions {
		key := partialRevokeScopePrefix + strings.ToLower(strings.TrimSpace(database))
		grants[key] = appendUniqueStrings(grants[key], privileges...)
	}
}

func partialRevokeDenied(grants map[string][]string, scope, wanted string) bool {
	parts := strings.Split(strings.ToLower(strings.TrimSpace(scope)), ".")
	if len(parts) != 2 || parts[0] == "" || parts[0] == "*" {
		return false
	}
	key := partialRevokeScopePrefix + parts[0]
	for _, privilege := range grants[key] {
		if strings.EqualFold(strings.TrimSpace(privilege), wanted) || strings.EqualFold(strings.TrimSpace(privilege), "ALL") || strings.EqualFold(strings.TrimSpace(privilege), "ALL PRIVILEGES") {
			return true
		}
	}
	return false
}

func accountCanGrant(file persistedAccountFile, session server.MySQLServerSession, scope string, privileges []string) bool {
	if session == nil {
		return true
	}
	user, _ := session.GetParamByName("user").(string)
	if user == "" || strings.EqualFold(user, "root") {
		return true
	}
	account := sessionAccount(file, session)
	if account == nil {
		return false
	}
	grants := effectiveAccountGrants(file, *account, session)
	if !grantsContain(grants, scope, "GRANT OPTION") {
		return false
	}
	for _, privilege := range privileges {
		privilege = strings.TrimSpace(privilege)
		if privilege == "" || strings.EqualFold(privilege, "GRANT OPTION") {
			continue
		}
		if !grantsContain(grants, scope, privilege) {
			return false
		}
	}
	return true
}

func accountCanGrantRole(file persistedAccountFile, session server.MySQLServerSession, role string) bool {
	if session == nil {
		return true
	}
	user, _ := session.GetParamByName("user").(string)
	if user == "" || strings.EqualFold(user, "root") {
		return true
	}
	account := sessionAccount(file, session)
	if account == nil {
		return false
	}
	for _, adminRole := range account.RoleAdminOptions {
		if strings.EqualFold(adminRole, role) {
			return true
		}
	}
	return grantsContain(effectiveAccountGrants(file, *account, session), "*.*", "GRANT OPTION")
}

func sessionOwnsAccount(session server.MySQLServerSession, user, host string) bool {
	if session == nil {
		return true
	}
	currentUser, _ := session.GetParamByName("user").(string)
	currentHost, _ := session.GetParamByName("host").(string)
	return strings.EqualFold(currentUser, user) && (currentHost == "" || strings.EqualFold(currentHost, host))
}

// showCreateUserVisibility applies the two separate visibility rules used by
// SHOW CREATE USER: looking up another account requires SELECT on the mysql
// system schema, while the current account may inspect its own definition.
// The authentication hash is an additional protected field and is only
// rendered when the session has SELECT on mysql.user (or an equivalent
// broader grant). Nil sessions are internal/root execution paths.
func showCreateUserVisibility(file persistedAccountFile, session server.MySQLServerSession, targetUser, targetHost string) (bool, bool, error) {
	if session == nil {
		return true, true, nil
	}
	if sessionOwnsAccount(session, targetUser, targetHost) {
		account := sessionAccount(file, session)
		if account == nil {
			return true, false, nil
		}
		grants := effectiveAccountGrants(file, *account, session)
		return true, grantsContain(grants, "mysql.user", "SELECT"), nil
	}
	account := sessionAccount(file, session)
	if account == nil {
		return false, false, fmt.Errorf("Access denied; user does not have the SELECT privilege for this operation")
	}
	grants := effectiveAccountGrants(file, *account, session)
	if !grantsContain(grants, "mysql.*", "SELECT") {
		return false, false, fmt.Errorf("Access denied; you need the SELECT privilege for this operation")
	}
	return true, grantsContain(grants, "mysql.user", "SELECT"), nil
}

func accountCanManage(file persistedAccountFile, session server.MySQLServerSession, privilege string, targetUser, targetHost string) bool {
	if session == nil {
		return true
	}
	currentUser, _ := session.GetParamByName("user").(string)
	if currentUser == "" || strings.EqualFold(currentUser, "root") {
		return true
	}
	if sessionOwnsAccount(session, targetUser, targetHost) && strings.EqualFold(privilege, "ALTER USER") {
		return true
	}
	account := sessionAccount(file, session)
	if account == nil {
		return false
	}
	return grantsContain(effectiveAccountGrants(file, *account, session), "*.*", privilege) ||
		grantsContain(effectiveAccountGrants(file, *account, session), "*.*", "GRANT OPTION")
}

func normalizeRoleList(roles []string) []string {
	result := make([]string, 0, len(roles))
	seen := map[string]struct{}{}
	for _, role := range roles {
		role = strings.TrimSpace(role)
		if role == "" {
			continue
		}
		key := strings.ToLower(role)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, role)
	}
	return result
}

// configuredRoleList parses the comma-separated role syntax used by MySQL's
// mandatory_roles variable.  A missing host part means '%', matching MySQL's
// account-name rules for roles.
func configuredRoleList(raw string) []string {
	roles := make([]string, 0)
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		item = strings.Trim(item, "`\" ")
		parts := strings.SplitN(item, "@", 2)
		user := strings.Trim(strings.TrimSpace(parts[0]), "'`\"")
		host := "%"
		if len(parts) == 2 {
			host = strings.Trim(strings.TrimSpace(parts[1]), "'`\"")
			if host == "" {
				host = "%"
			}
		}
		if user != "" {
			roles = append(roles, user+"@"+host)
		}
	}
	return normalizeRoleList(roles)
}

func (e *XMySQLExecutor) mandatoryRoles() []string {
	if e == nil || e.storageManager == nil {
		return nil
	}
	value, err := e.storageManager.GetSystemVariablesManager().GetVariable("", "mandatory_roles", manager.GlobalScope)
	if err != nil {
		return nil
	}
	return configuredRoleList(fmt.Sprint(value))
}

func containsConfiguredRole(roles []string, wanted string) bool {
	for _, role := range roles {
		if strings.EqualFold(role, wanted) {
			return true
		}
	}
	return false
}

func (e *XMySQLExecutor) isMandatoryRole(user, host string) bool {
	return containsConfiguredRole(e.mandatoryRoles(), user+"@"+host)
}

func (e *XMySQLExecutor) allGrantedRoles(file persistedAccountFile, account persistedAccount) []string {
	roles := append([]string(nil), account.Roles...)
	return e.addMandatoryRoles(file, roles)
}

func (e *XMySQLExecutor) addMandatoryRoles(file persistedAccountFile, roles []string) []string {
	for _, mandatory := range e.mandatoryRoles() {
		for _, candidate := range file.Accounts {
			if strings.EqualFold(candidate.User+"@"+candidate.Host, mandatory) {
				roles = appendUniqueStrings(roles, mandatory)
				break
			}
		}
	}
	return normalizeRoleList(roles)
}

// ensureMandatoryRolesActive mirrors MySQL's mandatory_roles behavior: a
// configured role is active for every session and cannot be removed by SET
// ROLE NONE or SET ROLE ... EXCEPT. Keep the session parameter authoritative
// so all existing privilege paths observe the same role set.
func (e *XMySQLExecutor) ensureMandatoryRolesActive(file persistedAccountFile, session server.MySQLServerSession) {
	if session == nil || len(e.mandatoryRoles()) == 0 {
		return
	}
	account := sessionAccount(file, session)
	if account == nil {
		return
	}
	roles := append([]string(nil), account.Roles...)
	if active, ok := session.GetParamByName("active_roles").([]string); ok {
		roles = append([]string(nil), active...)
	}
	session.SetParamByName("active_roles", e.addMandatoryRoles(file, roles))
}

func (e *XMySQLExecutor) allGrantedRolesForMandatoryRoles() []string {
	file, err := e.loadPersistedAccounts()
	if err != nil {
		return nil
	}
	roles := make([]string, 0)
	for _, mandatory := range e.mandatoryRoles() {
		for _, candidate := range file.Accounts {
			if strings.EqualFold(candidate.User+"@"+candidate.Host, mandatory) {
				roles = appendUniqueStrings(roles, mandatory)
				break
			}
		}
	}
	return normalizeRoleList(roles)
}

func (e *XMySQLExecutor) executeRoleStatement(ctx *ExecutionContext, lower, query string) {
	if ctx == nil {
		return
	}
	if ctx.Session == nil {
		ctx.Results <- &Result{Err: fmt.Errorf("role statement requires a session"), ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	file, err := e.accountFileForSession(ctx)
	if err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	if strings.HasPrefix(lower, "set default role") {
		targets := accountTargetsFromQuery(query)
		granteeTargets := accountGranteeTargetsFromQuery(query)
		if len(granteeTargets) == 0 && len(targets) > 0 {
			granteeTargets = targets[len(targets)-1:]
		}
		if len(granteeTargets) == 0 {
			ctx.Results <- &Result{Err: fmt.Errorf("SET DEFAULT ROLE requires a target account"), ResultType: common.RESULT_TYPE_QUERY}
			return
		}
		spec := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(lower, "set default role"), ";"))
		toIndex := strings.Index(strings.ToLower(spec), " to ")
		if toIndex >= 0 {
			spec = strings.TrimSpace(spec[:toIndex])
		}
		indices := make([]int, len(granteeTargets))
		for targetIndex, target := range granteeTargets {
			if !accountCanManage(file, ctx.Session, "CREATE USER", target[0], target[1]) && !sessionOwnsAccount(ctx.Session, target[0], target[1]) {
				ctx.Results <- &Result{Err: fmt.Errorf("access denied: current account cannot set the default role for '%s'@'%s'", target[0], target[1]), ResultType: common.RESULT_TYPE_QUERY}
				return
			}
			idx := -1
			for i := range file.Accounts {
				if file.Accounts[i].User == target[0] && file.Accounts[i].Host == target[1] {
					idx = i
					break
				}
			}
			if idx < 0 {
				ctx.Results <- &Result{Err: fmt.Errorf("account '%s'@'%s' does not exist", target[0], target[1]), ResultType: common.RESULT_TYPE_QUERY}
				return
			}
			indices[targetIndex] = idx
		}
		for targetIndex, idx := range indices {
			target := granteeTargets[targetIndex]
			var roles []string
			switch {
			case strings.EqualFold(spec, "none"):
				roles = []string{}
			case strings.EqualFold(spec, "all"):
				roles = append([]string(nil), file.Accounts[idx].Roles...)
			case strings.HasPrefix(strings.ToLower(spec), "all except "):
				roles = removeStrings(file.Accounts[idx].Roles, roleReferences(strings.TrimSpace(spec[len("all except "):]))...)
			default:
				roles = roleReferences(spec)
				for _, role := range roles {
					if !accountHasRole(file.Accounts[idx], role) {
						ctx.Results <- &Result{Err: fmt.Errorf("role '%s' is not granted to '%s'@'%s'", role, target[0], target[1]), ResultType: common.RESULT_TYPE_QUERY}
						return
					}
				}
			}
			file.Accounts[idx].DefaultRoles = normalizeRoleList(roles)
		}
		if err := e.stageOrSaveAccountFile(ctx.Session, file); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return
		}
		accountResult(ctx, nil, nil, "Default roles updated")
		return
	}

	user, _ := ctx.Session.GetParamByName("user").(string)
	host, _ := ctx.Session.GetParamByName("host").(string)
	if user == "" {
		ctx.Results <- &Result{Err: fmt.Errorf("SET ROLE requires an authenticated session"), ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	idx := -1
	for i := range file.Accounts {
		if file.Accounts[i].User == user && (file.Accounts[i].Host == host || host == "") {
			idx = i
			break
		}
	}
	if idx < 0 {
		ctx.Results <- &Result{Err: fmt.Errorf("account '%s'@'%s' does not exist", user, host), ResultType: common.RESULT_TYPE_QUERY}
		return
	}
	spec := strings.TrimSpace(strings.TrimPrefix(lower, "set role"))
	var roles []string
	switch {
	case strings.EqualFold(spec, "none"):
		roles = []string{}
	case strings.EqualFold(spec, "all"):
		roles = e.allGrantedRoles(file, file.Accounts[idx])
	case strings.EqualFold(spec, "default"):
		roles = append([]string(nil), file.Accounts[idx].DefaultRoles...)
	case strings.HasPrefix(strings.ToLower(spec), "all except "):
		excluded := roleReferences(strings.TrimSpace(spec[len("all except "):]))
		roles = removeStrings(e.allGrantedRoles(file, file.Accounts[idx]), excluded...)
	default:
		roles = roleReferences(spec)
		for _, role := range roles {
			if !accountHasRole(file.Accounts[idx], role) && !containsConfiguredRole(e.mandatoryRoles(), role) {
				ctx.Results <- &Result{Err: fmt.Errorf("role '%s' is not granted to '%s'@'%s'", role, user, host), ResultType: common.RESULT_TYPE_QUERY}
				return
			}
		}
	}
	roles = e.addMandatoryRoles(file, roles)
	ctx.Session.SetParamByName("active_roles", roles)
	accountResult(ctx, nil, nil, "Roles set")
}

func accountResult(ctx *ExecutionContext, columns []string, rows [][]interface{}, message string) {
	ctx.Results <- &Result{ResultType: common.RESULT_TYPE_QUERY, Data: newInformationSchemaSelectResult("account", columns, rows), Message: message}
}

func grantorIdentity(session server.MySQLServerSession) string {
	if session == nil {
		return "root@localhost"
	}
	user, _ := session.GetParamByName("user").(string)
	host, _ := session.GetParamByName("host").(string)
	if strings.TrimSpace(user) == "" {
		user = "root"
	}
	if strings.TrimSpace(host) == "" {
		host = "localhost"
	}
	return strings.TrimSpace(user) + "@" + strings.TrimSpace(host)
}

func persistedGrantorKey(scope, privilege string) string {
	return strings.ToLower(strings.TrimSpace(scope)) + "\x00" + strings.ToUpper(strings.TrimSpace(privilege))
}

func setPersistedGrantor(account *persistedAccount, scope, privilege, grantor string) {
	if account == nil {
		return
	}
	if account.Grantors == nil {
		account.Grantors = map[string]string{}
	}
	account.Grantors[persistedGrantorKey(scope, privilege)] = grantor
}

func deletePersistedGrantor(account *persistedAccount, scope, privilege string) {
	if account == nil {
		return
	}
	delete(account.Grantors, persistedGrantorKey(scope, privilege))
}

func deletePersistedGrantorsForScope(account *persistedAccount, scope string) {
	if account == nil {
		return
	}
	prefix := strings.ToLower(strings.TrimSpace(scope)) + "\x00"
	for key := range account.Grantors {
		if strings.HasPrefix(key, prefix) {
			delete(account.Grantors, key)
		}
	}
}

func setPersistedColumnGrantor(account *persistedAccount, scope, privilege, grantor string) {
	if account == nil {
		return
	}
	if account.ColumnGrantors == nil {
		account.ColumnGrantors = map[string]string{}
	}
	account.ColumnGrantors[persistedGrantorKey(scope, privilege)] = grantor
}

func deletePersistedColumnGrantor(account *persistedAccount, scope, privilege string) {
	if account == nil {
		return
	}
	delete(account.ColumnGrantors, persistedGrantorKey(scope, privilege))
}

func deletePersistedColumnGrantorsForScope(account *persistedAccount, scope string) {
	if account == nil {
		return
	}
	prefix := strings.ToLower(strings.TrimSpace(scope)) + "\x00"
	for key := range account.ColumnGrantors {
		if strings.HasPrefix(key, prefix) {
			delete(account.ColumnGrantors, key)
		}
	}
}

func persistedGrantor(account persistedAccount, scope, privilege string) string {
	if grantor := account.Grantors[persistedGrantorKey(scope, privilege)]; strings.TrimSpace(grantor) != "" {
		return grantor
	}
	for _, fallback := range []string{"ALL", "ALL PRIVILEGES"} {
		if grantor := account.Grantors[persistedGrantorKey(scope, fallback)]; strings.TrimSpace(grantor) != "" {
			return grantor
		}
	}
	return "root@localhost"
}

func persistedColumnGrantor(account persistedAccount, scope, privilege string) string {
	if grantor := account.ColumnGrantors[persistedGrantorKey(scope, privilege)]; strings.TrimSpace(grantor) != "" {
		return grantor
	}
	for _, fallback := range []string{"ALL", "ALL PRIVILEGES"} {
		if grantor := account.ColumnGrantors[persistedGrantorKey(scope, fallback)]; strings.TrimSpace(grantor) != "" {
			return grantor
		}
	}
	return "root@localhost"
}

func persistedRoleGrantor(account persistedAccount, role string) string {
	if grantor := account.RoleGrantors[strings.ToLower(strings.TrimSpace(role))]; strings.TrimSpace(grantor) != "" {
		return grantor
	}
	return "root@localhost"
}

func escapeAccountSQL(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func (e *XMySQLExecutor) partialRevokesEnabled() bool {
	if e == nil || e.storageManager == nil {
		return false
	}
	value, err := e.storageManager.GetSystemVariablesManager().GetVariable("", "partial_revokes", manager.GlobalScope)
	if err != nil {
		return false
	}
	return sessionBoolValue(value)
}

func partialRevokeSchema(scope string) (string, bool) {
	parts := strings.Split(strings.Trim(strings.TrimSpace(scope), "`"), ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] != "*" || parts[0] == "*" {
		return "", false
	}
	return strings.ToLower(strings.Trim(parts[0], "` ")), true
}

func isPartialRevokeEligible(privilege string) bool {
	switch strings.ToUpper(strings.TrimSpace(privilege)) {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "REFERENCES", "INDEX", "ALTER",
		"CREATE TEMPORARY TABLES", "LOCK TABLES", "EXECUTE", "CREATE VIEW", "SHOW VIEW", "CREATE ROUTINE",
		"ALTER ROUTINE", "EVENT", "TRIGGER":
		return true
	default:
		return false
	}
}

func grantNameContains(privileges []string, wanted string) bool {
	for _, privilege := range privileges {
		if strings.EqualFold(strings.TrimSpace(privilege), wanted) ||
			strings.EqualFold(strings.TrimSpace(privilege), "ALL") ||
			strings.EqualFold(strings.TrimSpace(privilege), "ALL PRIVILEGES") {
			return true
		}
	}
	return false
}

func partialRevokePrivileges(account *persistedAccount, requested []string) []string {
	result := make([]string, 0, len(requested))
	for _, raw := range requested {
		privilege := strings.ToUpper(strings.TrimSpace(raw))
		if privilege != "ALL" && privilege != "ALL PRIVILEGES" {
			if isPartialRevokeEligible(privilege) {
				result = appendUniqueStrings(result, privilege)
			}
			continue
		}
		for _, globalPrivilege := range account.Grants["*.*"] {
			globalPrivilege = strings.ToUpper(strings.TrimSpace(globalPrivilege))
			if isPartialRevokeEligible(globalPrivilege) {
				result = appendUniqueStrings(result, globalPrivilege)
			}
		}
		if grantNameContains(account.Grants["*.*"], privilege) && len(result) == 0 {
			for _, candidate := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "REFERENCES", "INDEX", "ALTER", "CREATE TEMPORARY TABLES", "LOCK TABLES", "EXECUTE", "CREATE VIEW", "SHOW VIEW", "CREATE ROUTINE", "ALTER ROUTINE", "EVENT", "TRIGGER"} {
				result = appendUniqueStrings(result, candidate)
			}
		}
	}
	return result
}

func addPartialRevoke(account *persistedAccount, schema string, requested []string) bool {
	if account == nil || schema == "" {
		return false
	}
	privileges := partialRevokePrivileges(account, requested)
	if len(privileges) == 0 {
		return false
	}
	if account.Restrictions == nil {
		account.Restrictions = map[string][]string{}
	}
	changed := false
	for _, privilege := range privileges {
		if grantNameContains(account.Grants["*.*"], privilege) && !grantNameContains(account.Grants[schema+".*"], privilege) {
			updated := appendUniqueStrings(account.Restrictions[schema], privilege)
			if len(updated) != len(account.Restrictions[schema]) {
				changed = true
			}
			account.Restrictions[schema] = updated
		}
	}
	return changed
}

func removePartialRevoke(account *persistedAccount, schema string, requested []string) bool {
	if account == nil || len(account.Restrictions) == 0 {
		return false
	}
	privileges := partialRevokePrivileges(account, requested)
	if len(privileges) == 0 {
		return false
	}
	changed := false
	removeFrom := func(database string) {
		before := len(account.Restrictions[database])
		account.Restrictions[database] = removeStrings(account.Restrictions[database], privileges...)
		if len(account.Restrictions[database]) != before {
			changed = true
		}
		if len(account.Restrictions[database]) == 0 {
			delete(account.Restrictions, database)
		}
	}
	if schema != "" {
		removeFrom(schema)
	} else {
		for database := range account.Restrictions {
			removeFrom(database)
		}
	}
	return changed
}

func syncPartialRevokeUserAttributes(account *persistedAccount) {
	if account == nil {
		return
	}
	attributes := map[string]interface{}{}
	if strings.TrimSpace(account.UserAttributes) != "" {
		_ = json.Unmarshal([]byte(account.UserAttributes), &attributes)
	}
	if len(account.Restrictions) == 0 {
		delete(attributes, "Restrictions")
	} else {
		databases := make([]string, 0, len(account.Restrictions))
		for database := range account.Restrictions {
			databases = append(databases, database)
		}
		sort.Strings(databases)
		restrictions := make([]map[string]interface{}, 0, len(databases))
		for _, database := range databases {
			privileges := append([]string(nil), account.Restrictions[database]...)
			sort.Strings(privileges)
			restrictions = append(restrictions, map[string]interface{}{"Database": database, "Privileges": privileges})
		}
		attributes["Restrictions"] = restrictions
	}
	if len(attributes) == 0 {
		account.UserAttributes = ""
		return
	}
	raw, err := json.Marshal(attributes)
	if err == nil {
		account.UserAttributes = string(raw)
	}
}

// executeAccountStatement implements the persistent account-management subset
// needed by clients and authentication. It deliberately keeps grant state in a
// small durable file until mysql system-table writes are transaction-aware.
func (e *XMySQLExecutor) executeAccountStatement(ctx *ExecutionContext, query string) bool {
	owner := int64(0)
	if ctx != nil {
		owner = int64(sessionConnectionID(ctx.Session))
	}
	e.lockPerformanceSchemaMutex(&e.accountMu, "wait/synch/mutex/sql/xmysql/account", owner)
	defer e.unlockPerformanceSchemaMutex(&e.accountMu, "wait/synch/mutex/sql/xmysql/account")
	lower := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(query, ";")))
	if lower == "flush privileges" {
		accountResult(ctx, nil, nil, "Privileges flushed")
		return true
	}
	if lower == "show privileges" {
		e.executeShowPrivileges(ctx)
		return true
	}
	if strings.HasPrefix(lower, "set role") || strings.HasPrefix(lower, "set default role") {
		e.executeRoleStatement(ctx, lower, query)
		return true
	}
	if strings.HasPrefix(lower, "show create user") {
		user, host, ok := accountTargetFromQuery(query)
		file, err := e.accountFileForSession(ctx)
		if err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		if !ok && regexp.MustCompile(`(?is)^show\s+create\s+user\s+current_user\s*\(?(?:\s*\))?\s*$`).MatchString(lower) {
			if current := sessionAccount(file, ctx.Session); current != nil {
				user, host, ok = current.User, current.Host, true
			}
		}
		if !ok {
			ctx.Results <- &Result{Err: fmt.Errorf("SHOW CREATE USER requires an account"), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		idx := -1
		for index := range file.Accounts {
			if strings.EqualFold(file.Accounts[index].User, user) && strings.EqualFold(file.Accounts[index].Host, host) {
				idx = index
				break
			}
		}
		if idx < 0 {
			ctx.Results <- &Result{Err: fmt.Errorf("SHOW CREATE USER failed for '%s'@'%s': user does not exist", user, host), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		visible, showHash, visibilityErr := showCreateUserVisibility(file, ctx.Session, user, host)
		if !visible {
			ctx.Results <- &Result{Err: visibilityErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		account := file.Accounts[idx]
		plugin := account.Plugin
		if plugin == "" {
			plugin = "mysql_native_password"
		}
		password := "<secret>"
		if showHash {
			password = account.Password
		}
		createSQL := fmt.Sprintf("CREATE USER '%s'@'%s' IDENTIFIED WITH '%s' AS '%s'", escapeAccountSQL(account.User), escapeAccountSQL(account.Host), escapeAccountSQL(plugin), escapeAccountSQL(password))
		if account.X509Required {
			createSQL += " REQUIRE X509"
		} else if account.TLSRequired {
			createSQL += " REQUIRE SSL"
		} else {
			createSQL += " REQUIRE NONE"
		}
		if account.SSLCipher != "" {
			createSQL += fmt.Sprintf(" AND CIPHER '%s'", escapeAccountSQL(account.SSLCipher))
		}
		if account.X509Issuer != "" {
			createSQL += fmt.Sprintf(" AND ISSUER '%s'", escapeAccountSQL(account.X509Issuer))
		}
		if account.X509Subject != "" {
			createSQL += fmt.Sprintf(" AND SUBJECT '%s'", escapeAccountSQL(account.X509Subject))
		}
		resourceClauses := make([]string, 0, 4)
		for _, resource := range []struct {
			name  string
			value *int64
		}{
			{"MAX_QUERIES_PER_HOUR", account.MaxQuestions},
			{"MAX_UPDATES_PER_HOUR", account.MaxUpdates},
			{"MAX_CONNECTIONS_PER_HOUR", account.MaxConnections},
			{"MAX_USER_CONNECTIONS", account.MaxUserConnections},
		} {
			if resource.value != nil {
				resourceClauses = append(resourceClauses, fmt.Sprintf("%s %d", resource.name, *resource.value))
			}
		}
		if len(resourceClauses) > 0 {
			createSQL += " WITH " + strings.Join(resourceClauses, " ")
		}
		if account.AccountLocked {
			createSQL += " ACCOUNT LOCK"
		} else {
			createSQL += " ACCOUNT UNLOCK"
		}
		if account.PasswordLifetime != nil {
			createSQL += fmt.Sprintf(" PASSWORD EXPIRE INTERVAL %d DAY", *account.PasswordLifetime)
		} else if account.PasswordExpired {
			createSQL += " PASSWORD EXPIRE"
		} else {
			createSQL += " PASSWORD EXPIRE DEFAULT"
		}
		if account.PasswordReuseHistory != nil {
			createSQL += fmt.Sprintf(" PASSWORD HISTORY %d", *account.PasswordReuseHistory)
		} else {
			createSQL += " PASSWORD HISTORY DEFAULT"
		}
		if account.PasswordReuseTime != nil {
			createSQL += fmt.Sprintf(" PASSWORD REUSE INTERVAL %d DAY", *account.PasswordReuseTime)
		} else {
			createSQL += " PASSWORD REUSE INTERVAL DEFAULT"
		}
		if account.PasswordRequireCurrent == nil {
			createSQL += " PASSWORD REQUIRE CURRENT DEFAULT"
		} else if *account.PasswordRequireCurrent {
			createSQL += " PASSWORD REQUIRE CURRENT"
		} else {
			createSQL += " PASSWORD REQUIRE CURRENT OPTIONAL"
		}
		if account.FailedLoginAttempts != nil {
			createSQL += fmt.Sprintf(" FAILED_LOGIN_ATTEMPTS %d", *account.FailedLoginAttempts)
		}
		if account.PasswordLockUnbounded {
			createSQL += " PASSWORD_LOCK_TIME UNBOUNDED"
		} else if account.PasswordLockTime != nil {
			createSQL += fmt.Sprintf(" PASSWORD_LOCK_TIME %d", *account.PasswordLockTime)
		}
		if strings.TrimSpace(account.UserAttributes) != "" {
			createSQL += fmt.Sprintf(" ATTRIBUTE '%s'", escapeAccountSQL(account.UserAttributes))
		}
		accountResult(ctx, []string{"User", "Create User"}, [][]interface{}{{fmt.Sprintf("'%s'@'%s'", account.User, account.Host), createSQL}}, "SHOW CREATE USER completed")
		return true
	}
	if !strings.HasPrefix(lower, "create user") && !strings.HasPrefix(lower, "alter user") &&
		!strings.HasPrefix(lower, "create role") && !strings.HasPrefix(lower, "drop role") &&
		!strings.HasPrefix(lower, "drop user") && !strings.HasPrefix(lower, "grant ") &&
		!strings.HasPrefix(lower, "revoke ") && !strings.HasPrefix(lower, "show grants") &&
		!strings.HasPrefix(lower, "set password") && !strings.HasPrefix(lower, "rename user") {
		return false
	}
	// The account layer intentionally stages GRANT/REVOKE in the session so
	// existing COMMIT/ROLLBACK semantics remain transactional. User/role
	// definition and password statements follow the MySQL implicit-commit
	// boundary; privilege mutations are handled by the account staging layer.
	if !strings.HasPrefix(lower, "grant ") && !strings.HasPrefix(lower, "revoke ") {
		if err := e.prepareDDLImplicitCommit(ctx.Session); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
	}

	file, err := e.accountFileForSession(ctx)
	if err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	if strings.HasPrefix(lower, "set password") {
		user, host, ok := accountTargetFromQuery(query)
		if !ok && regexp.MustCompile(`(?is)\bset\s+password\s+for\s+(?:user|current_user)\s*\(\s*\)`).MatchString(lower) {
			if current := sessionAccount(file, ctx.Session); current != nil {
				user, host, ok = current.User, current.Host, true
			}
		}
		if !ok && ctx.Session != nil {
			if current := sessionAccount(file, ctx.Session); current != nil {
				user, host, ok = current.User, current.Host, true
			} else {
				user, _ = ctx.Session.GetParamByName("user").(string)
				host, _ = ctx.Session.GetParamByName("host").(string)
				ok = user != ""
			}
		}
		if !ok {
			ctx.Results <- &Result{Err: fmt.Errorf("SET PASSWORD requires an account"), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		if !accountCanManage(file, ctx.Session, "ALTER USER", user, host) {
			ctx.Results <- &Result{Err: fmt.Errorf("access denied: current account cannot change password for '%s'@'%s'", user, host), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		passwordMatch := regexp.MustCompile(`(?is)set\s+password(?:\s+for\s+(?:'[^']*'\s*@\s*'[^']*'|(?:user|current_user)\s*\(\s*\)))?\s*=\s*'([^']*)'`).FindStringSubmatch(query)
		if len(passwordMatch) != 2 {
			ctx.Results <- &Result{Err: fmt.Errorf("invalid SET PASSWORD syntax"), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		idx := -1
		for i := range file.Accounts {
			if file.Accounts[i].User == user && file.Accounts[i].Host == host {
				idx = i
				break
			}
		}
		if idx < 0 {
			ctx.Results <- &Result{Err: fmt.Errorf("account '%s'@'%s' does not exist", user, host), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		if err := e.validateCurrentPasswordForSelf(ctx.Session, file.Accounts[idx], query); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		newPassword := passwordHashForPlugin(file.Accounts[idx].Plugin, passwordMatch[1])
		if err := e.validatePasswordReuse(file.Accounts[idx], newPassword); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		appendPasswordHistory(&file.Accounts[idx])
		file.Accounts[idx].Password = newPassword
		file.Accounts[idx].PasswordLastChanged = passwordChangedAt(newPassword)
		file.Accounts[idx].PasswordExpired = false
		file.Accounts[idx].FailedLoginCount = 0
		file.Accounts[idx].PasswordLockedUntil = nil
		if err := e.stageOrSaveAccountFile(ctx.Session, file); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		accountResult(ctx, nil, nil, "Password changed")
		return true
	}
	if strings.HasPrefix(lower, "rename user") {
		if ctx.Session != nil {
			currentUser, _ := ctx.Session.GetParamByName("user").(string)
			if currentUser != "" && !strings.EqualFold(currentUser, "root") {
				account := sessionAccount(file, ctx.Session)
				if account == nil || !grantsContain(effectiveAccountGrants(file, *account, ctx.Session), "*.*", "CREATE USER") {
					ctx.Results <- &Result{Err: fmt.Errorf("access denied: current account cannot rename users"), ResultType: common.RESULT_TYPE_QUERY}
					return true
				}
			}
		}
		pairs := regexp.MustCompile(`(?is)'([^']*)'\s*@\s*'([^']*)'\s+to\s+'([^']*)'\s*@\s*'([^']*)'`).FindAllStringSubmatch(query, -1)
		if len(pairs) == 0 {
			ctx.Results <- &Result{Err: fmt.Errorf("invalid RENAME USER syntax"), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		sources := make(map[string]struct{}, len(pairs))
		destinations := make(map[string]struct{}, len(pairs))
		for _, pair := range pairs {
			sourceKey := strings.ToLower(pair[1] + "@" + pair[2])
			destinationKey := strings.ToLower(pair[3] + "@" + pair[4])
			if _, duplicate := sources[sourceKey]; duplicate {
				ctx.Results <- &Result{Err: fmt.Errorf("account '%s'@'%s' is renamed more than once", pair[1], pair[2]), ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
			if _, duplicate := destinations[destinationKey]; duplicate {
				ctx.Results <- &Result{Err: fmt.Errorf("account '%s'@'%s' is a duplicate rename target", pair[3], pair[4]), ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
			sources[sourceKey] = struct{}{}
			destinations[destinationKey] = struct{}{}
			foundSource := false
			for _, account := range file.Accounts {
				if account.User == pair[1] && account.Host == pair[2] {
					foundSource = true
				}
				if account.User == pair[3] && account.Host == pair[4] {
					ctx.Results <- &Result{Err: fmt.Errorf("account '%s'@'%s' already exists", pair[3], pair[4]), ResultType: common.RESULT_TYPE_QUERY}
					return true
				}
			}
			if !foundSource {
				ctx.Results <- &Result{Err: fmt.Errorf("account '%s'@'%s' does not exist", pair[1], pair[2]), ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
		}
		for _, pair := range pairs {
			idx := -1
			for i := range file.Accounts {
				if file.Accounts[i].User == pair[1] && file.Accounts[i].Host == pair[2] {
					idx = i
					break
				}
			}
			if idx < 0 {
				ctx.Results <- &Result{Err: fmt.Errorf("account '%s'@'%s' does not exist", pair[1], pair[2]), ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
			oldRole := pair[1] + "@" + pair[2]
			newRole := pair[3] + "@" + pair[4]
			file.Accounts[idx].User, file.Accounts[idx].Host = pair[3], pair[4]
			for accountIndex := range file.Accounts {
				file.Accounts[accountIndex].Roles = replaceString(file.Accounts[accountIndex].Roles, oldRole, newRole)
				file.Accounts[accountIndex].DefaultRoles = replaceString(file.Accounts[accountIndex].DefaultRoles, oldRole, newRole)
				file.Accounts[accountIndex].RoleAdminOptions = replaceString(file.Accounts[accountIndex].RoleAdminOptions, oldRole, newRole)
				if file.Accounts[accountIndex].Grants != nil {
					oldProxy := fmt.Sprintf("'%s'@'%s'", pair[1], pair[2])
					newProxy := fmt.Sprintf("'%s'@'%s'", pair[3], pair[4])
					if privileges, exists := file.Accounts[accountIndex].Grants[oldProxy]; exists {
						delete(file.Accounts[accountIndex].Grants, oldProxy)
						file.Accounts[accountIndex].Grants[newProxy] = appendUniqueStrings(file.Accounts[accountIndex].Grants[newProxy], privileges...)
					}
				}
			}
		}
		if err := e.stageOrSaveAccountFile(ctx.Session, file); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		accountResult(ctx, nil, nil, "User renamed")
		return true
	}
	user, host, ok := accountTargetFromQuery(query)
	if !ok && regexp.MustCompile(`(?is)^alter\s+user\s+(?:user|current_user)\s*\(\s*\)`).MatchString(lower) {
		if current := sessionAccount(file, ctx.Session); current != nil {
			user, host, ok = current.User, current.Host, true
		}
	}
	if !ok {
		ctx.Results <- &Result{Err: fmt.Errorf("account must use 'user'@'host' syntax"), ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	find := func() int {
		for i := range file.Accounts {
			if file.Accounts[i].User == user && file.Accounts[i].Host == host {
				return i
			}
		}
		return -1
	}

	switch {
	case strings.HasPrefix(lower, "create user"):
		accountTargets := accountTargetsFromQuery(query)
		if len(accountTargets) == 0 {
			ctx.Results <- &Result{Err: fmt.Errorf("CREATE USER requires an account"), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		for _, target := range accountTargets {
			if !accountCanManage(file, ctx.Session, "CREATE USER", target[0], target[1]) {
				ctx.Results <- &Result{Err: fmt.Errorf("access denied: current account cannot create users"), ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
			for _, account := range file.Accounts {
				if account.User == target[0] && account.Host == target[1] && !strings.Contains(lower, "if not exists") {
					ctx.Results <- &Result{Err: fmt.Errorf("Operation CREATE USER failed for '%s'@'%s': user exists", target[0], target[1]), ResultType: common.RESULT_TYPE_QUERY}
					return true
				}
			}
		}
		password := ""
		if match := regexp.MustCompile(`(?is)identified(?:\s+with\s+[a-z0-9_]+)?\s+by\s+'([^']*)'`).FindStringSubmatch(query); len(match) == 2 {
			password = match[1]
		}
		plugin := "mysql_native_password"
		if match := regexp.MustCompile(`(?is)identified\s+with\s+([a-z0-9_]+)`).FindStringSubmatch(query); len(match) == 2 {
			plugin = strings.ToLower(match[1])
		}
		tlsRequired, parsedX509Required, sslCipher, x509Issuer, x509Subject, hasTLSRequirement := parseTLSAccountOptions(query)
		x509Required := parsedX509Required || strings.Contains(lower, "require x509")
		passwordExpired := strings.Contains(lower, "password expire") && !strings.Contains(lower, "password expire never") && !strings.Contains(lower, "password expire default") && !strings.Contains(lower, "password expire interval")
		passwordHistory, hasPasswordHistory, policyErr := parsePasswordHistoryOption(query)
		if policyErr != nil {
			ctx.Results <- &Result{Err: policyErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		passwordReuseTime, hasPasswordReuseTime, policyErr := parsePasswordReuseIntervalOption(query)
		if policyErr != nil {
			ctx.Results <- &Result{Err: policyErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		passwordRequireCurrent, hasPasswordRequireCurrent, policyErr := parsePasswordRequireCurrentOption(query)
		if policyErr != nil {
			ctx.Results <- &Result{Err: policyErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		passwordLifetime, hasPasswordLifetime, policyErr := parsePasswordLifetimeOption(query)
		if policyErr != nil {
			ctx.Results <- &Result{Err: policyErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		resources, hasResources, resourceErr := parseAccountResourceOptions(query)
		if resourceErr != nil {
			ctx.Results <- &Result{Err: resourceErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		failedLoginAttempts, passwordLockTime, passwordLockUnbounded, hasFailedLoginAttempts, hasPasswordLockTime, policyErr := parseFailedLoginPolicyOptions(query)
		if policyErr != nil {
			ctx.Results <- &Result{Err: policyErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		userAttributes, hasUserAttributes, attributeErr := parseUserAttributeOption(query)
		if attributeErr != nil {
			ctx.Results <- &Result{Err: attributeErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		userComment, hasUserComment, commentErr := parseUserCommentOption(query)
		if commentErr != nil {
			ctx.Results <- &Result{Err: commentErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		if hasUserAttributes && hasUserComment {
			ctx.Results <- &Result{Err: fmt.Errorf("COMMENT and ATTRIBUTE cannot be used together"), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		if hasUserComment {
			userAttributes, commentErr = userAttributesWithComment("", userComment)
			if commentErr != nil {
				ctx.Results <- &Result{Err: commentErr, ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
			hasUserAttributes = true
		}
		for _, target := range accountTargets {
			alreadyExists := false
			for _, account := range file.Accounts {
				if account.User == target[0] && account.Host == target[1] {
					alreadyExists = true
					break
				}
			}
			if !alreadyExists {
				passwordHash := passwordHashForPlugin(plugin, password)
				account := persistedAccount{User: target[0], Host: target[1], Password: passwordHash, Plugin: plugin, TLSRequired: tlsRequired || strings.Contains(lower, "require ssl") || x509Required, X509Required: x509Required, SSLCipher: sslCipher, X509Issuer: x509Issuer, X509Subject: x509Subject, PasswordExpired: passwordExpired, PasswordLastChanged: passwordChangedAt(passwordHash), Grants: map[string][]string{}, ColumnGrants: map[string][]string{}, UpdatedAt: time.Now().UTC().Format(time.RFC3339)}
				if hasResources {
					account.MaxQuestions = resources.MaxQuestions
					account.MaxUpdates = resources.MaxUpdates
					account.MaxConnections = resources.MaxConnections
					account.MaxUserConnections = resources.MaxUserConnections
				}
				if hasFailedLoginAttempts {
					account.FailedLoginAttempts = failedLoginAttempts
				}
				if hasPasswordLockTime {
					account.PasswordLockTime = passwordLockTime
					account.PasswordLockUnbounded = passwordLockUnbounded
				}
				if hasTLSRequirement && !tlsRequired && !x509Required {
					account.TLSRequired = false
				}
				if hasUserAttributes {
					account.UserAttributes = userAttributes
				}
				if hasPasswordHistory {
					account.PasswordReuseHistory = passwordHistory
				}
				if hasPasswordReuseTime {
					account.PasswordReuseTime = passwordReuseTime
				}
				if hasPasswordRequireCurrent {
					account.PasswordRequireCurrent = passwordRequireCurrent
				}
				if hasPasswordLifetime {
					account.PasswordLifetime = passwordLifetime
				}
				file.Accounts = append(file.Accounts, account)
			}
		}
	case strings.HasPrefix(lower, "create role"):
		accountTargets := accountTargetsFromQuery(query)
		if len(accountTargets) == 0 {
			ctx.Results <- &Result{Err: fmt.Errorf("CREATE ROLE requires a role"), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		for _, target := range accountTargets {
			if !accountCanManage(file, ctx.Session, "CREATE ROLE", target[0], target[1]) {
				ctx.Results <- &Result{Err: fmt.Errorf("access denied: current account cannot create roles"), ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
			for _, account := range file.Accounts {
				if account.User == target[0] && account.Host == target[1] && !strings.Contains(lower, "if not exists") {
					ctx.Results <- &Result{Err: fmt.Errorf("Operation CREATE ROLE failed for '%s'@'%s': role exists", target[0], target[1]), ResultType: common.RESULT_TYPE_QUERY}
					return true
				}
			}
		}
		for _, target := range accountTargets {
			alreadyExists := false
			for _, account := range file.Accounts {
				if account.User == target[0] && account.Host == target[1] {
					alreadyExists = true
					break
				}
			}
			if !alreadyExists {
				file.Accounts = append(file.Accounts, persistedAccount{User: target[0], Host: target[1], Plugin: "mysql_native_password", Grants: map[string][]string{}, ColumnGrants: map[string][]string{}, UpdatedAt: time.Now().UTC().Format(time.RFC3339)})
			}
		}
	case strings.HasPrefix(lower, "alter user"):
		accountTargets := accountTargetsFromQuery(query)
		if len(accountTargets) == 0 && regexp.MustCompile(`(?is)^alter\s+user\s+(?:user|current_user)\s*\(\s*\)`).MatchString(lower) && ok {
			accountTargets = [][2]string{{user, host}}
		}
		if len(accountTargets) == 0 {
			ctx.Results <- &Result{Err: fmt.Errorf("ALTER USER requires an account"), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		indices := make([]int, len(accountTargets))
		for targetIndex, target := range accountTargets {
			if !accountCanManage(file, ctx.Session, "ALTER USER", target[0], target[1]) {
				ctx.Results <- &Result{Err: fmt.Errorf("access denied: current account cannot alter user '%s'@'%s'", target[0], target[1]), ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
			indices[targetIndex] = -1
			for accountIndex := range file.Accounts {
				if file.Accounts[accountIndex].User == target[0] && file.Accounts[accountIndex].Host == target[1] {
					indices[targetIndex] = accountIndex
					break
				}
			}
			if indices[targetIndex] < 0 && !strings.Contains(lower, "if exists") {
				ctx.Results <- &Result{Err: fmt.Errorf("Operation ALTER USER failed for '%s'@'%s': user does not exist", target[0], target[1]), ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
		}
		plugin := ""
		if match := regexp.MustCompile(`(?is)identified\s+with\s+([a-z0-9_]+)`).FindStringSubmatch(query); len(match) == 2 {
			plugin = strings.ToLower(match[1])
		}
		passwordHistory, hasPasswordHistory, policyErr := parsePasswordHistoryOption(query)
		if policyErr != nil {
			ctx.Results <- &Result{Err: policyErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		passwordReuseTime, hasPasswordReuseTime, policyErr := parsePasswordReuseIntervalOption(query)
		if policyErr != nil {
			ctx.Results <- &Result{Err: policyErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		passwordRequireCurrent, hasPasswordRequireCurrent, policyErr := parsePasswordRequireCurrentOption(query)
		if policyErr != nil {
			ctx.Results <- &Result{Err: policyErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		passwordLifetime, hasPasswordLifetime, policyErr := parsePasswordLifetimeOption(query)
		if policyErr != nil {
			ctx.Results <- &Result{Err: policyErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		resources, hasResources, resourceErr := parseAccountResourceOptions(query)
		if resourceErr != nil {
			ctx.Results <- &Result{Err: resourceErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		failedLoginAttempts, passwordLockTime, passwordLockUnbounded, hasFailedLoginAttempts, hasPasswordLockTime, policyErr := parseFailedLoginPolicyOptions(query)
		if policyErr != nil {
			ctx.Results <- &Result{Err: policyErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		tlsRequired, x509Required, sslCipher, x509Issuer, x509Subject, hasTLSRequirement := parseTLSAccountOptions(query)
		userAttributes, hasUserAttributes, attributeErr := parseUserAttributeOption(query)
		if attributeErr != nil {
			ctx.Results <- &Result{Err: attributeErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		userComment, hasUserComment, commentErr := parseUserCommentOption(query)
		if commentErr != nil {
			ctx.Results <- &Result{Err: commentErr, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		if hasUserAttributes && hasUserComment {
			ctx.Results <- &Result{Err: fmt.Errorf("COMMENT and ATTRIBUTE cannot be used together"), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		if hasUserComment {
			userAttributes, commentErr = userAttributesWithComment("", userComment)
			if commentErr != nil {
				ctx.Results <- &Result{Err: commentErr, ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
			hasUserAttributes = true
		}
		if match := regexp.MustCompile(`(?is)identified(?:\s+with\s+[a-z0-9_]+)?\s+by\s+'([^']*)'`).FindStringSubmatch(query); len(match) == 2 {
			for _, idx := range indices {
				if idx >= 0 {
					if err := e.validateCurrentPasswordForSelf(ctx.Session, file.Accounts[idx], query); err != nil {
						ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
						return true
					}
					hashPlugin := plugin
					if hashPlugin == "" {
						hashPlugin = file.Accounts[idx].Plugin
					}
					newPassword := passwordHashForPlugin(hashPlugin, match[1])
					if err := e.validatePasswordReuse(file.Accounts[idx], newPassword); err != nil {
						ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
						return true
					}
					appendPasswordHistory(&file.Accounts[idx])
					file.Accounts[idx].Password = newPassword
					file.Accounts[idx].PasswordLastChanged = passwordChangedAt(newPassword)
					file.Accounts[idx].PasswordExpired = false
					file.Accounts[idx].FailedLoginCount = 0
					file.Accounts[idx].PasswordLockedUntil = nil
				}
			}
		}
		if hasPasswordHistory || hasPasswordReuseTime || hasPasswordRequireCurrent || hasPasswordLifetime || hasResources || hasFailedLoginAttempts || hasPasswordLockTime {
			for _, idx := range indices {
				if idx < 0 {
					continue
				}
				if hasPasswordHistory {
					file.Accounts[idx].PasswordReuseHistory = passwordHistory
				}
				if hasPasswordReuseTime {
					file.Accounts[idx].PasswordReuseTime = passwordReuseTime
				}
				if hasPasswordRequireCurrent {
					file.Accounts[idx].PasswordRequireCurrent = passwordRequireCurrent
				}
				if hasPasswordLifetime {
					file.Accounts[idx].PasswordLifetime = passwordLifetime
				}
				if hasResources {
					file.Accounts[idx].MaxQuestions = resources.MaxQuestions
					file.Accounts[idx].MaxUpdates = resources.MaxUpdates
					file.Accounts[idx].MaxConnections = resources.MaxConnections
					file.Accounts[idx].MaxUserConnections = resources.MaxUserConnections
				}
				if hasFailedLoginAttempts {
					file.Accounts[idx].FailedLoginAttempts = failedLoginAttempts
				}
				if hasPasswordLockTime {
					file.Accounts[idx].PasswordLockTime = passwordLockTime
					file.Accounts[idx].PasswordLockUnbounded = passwordLockUnbounded
				}
			}
		}
		if hasUserAttributes {
			for _, idx := range indices {
				if idx >= 0 {
					value := userAttributes
					if hasUserComment {
						value, attributeErr = userAttributesWithComment(file.Accounts[idx].UserAttributes, userComment)
						if attributeErr != nil {
							ctx.Results <- &Result{Err: attributeErr, ResultType: common.RESULT_TYPE_QUERY}
							return true
						}
					}
					file.Accounts[idx].UserAttributes = value
					syncPartialRevokeUserAttributes(&file.Accounts[idx])
				}
			}
		}
		if plugin != "" {
			for _, idx := range indices {
				if idx >= 0 {
					file.Accounts[idx].Plugin = plugin
				}
			}
		}
		// ALTER USER replaces the transport requirement.  In particular,
		// REQUIRE NONE must be able to clear a previous REQUIRE SSL/X509
		// setting; otherwise a user cannot be brought back to the default
		// password-authentication policy without editing the grant tables.
		if hasTLSRequirement {
			for _, idx := range indices {
				if idx < 0 {
					continue
				}
				file.Accounts[idx].TLSRequired = tlsRequired
				file.Accounts[idx].X509Required = x509Required
				file.Accounts[idx].SSLCipher = sslCipher
				file.Accounts[idx].X509Issuer = x509Issuer
				file.Accounts[idx].X509Subject = x509Subject
			}
		}
		for _, idx := range indices {
			if idx < 0 {
				continue
			}
			file.Accounts[idx].AccountLocked = strings.Contains(lower, "account lock") && !strings.Contains(lower, "account unlock")
			if strings.Contains(lower, "account unlock") {
				file.Accounts[idx].AccountLocked = false
				file.Accounts[idx].FailedLoginCount = 0
				file.Accounts[idx].PasswordLockedUntil = nil
			}
			if strings.Contains(lower, "password expire never") || strings.Contains(lower, "password expire default") || strings.Contains(lower, "password expire interval") {
				file.Accounts[idx].PasswordExpired = false
				if !hasPasswordLifetime {
					file.Accounts[idx].PasswordLifetime = nil
				}
			} else if strings.Contains(lower, "password expire") {
				file.Accounts[idx].PasswordExpired = true
			}
		}
	case strings.HasPrefix(lower, "drop user"):
		if !accountCanManage(file, ctx.Session, "CREATE USER", user, host) {
			ctx.Results <- &Result{Err: fmt.Errorf("access denied: current account cannot drop users"), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		targets := accountTargetsFromQuery(query)
		if len(targets) == 0 {
			ctx.Results <- &Result{Err: fmt.Errorf("DROP USER requires an account"), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		dropSet := make(map[string]struct{}, len(targets))
		for _, target := range targets {
			if e.isMandatoryRole(target[0], target[1]) {
				ctx.Results <- &Result{Err: fmt.Errorf("Operation DROP USER failed for '%s'@'%s': role is mandatory", target[0], target[1]), ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
			key := strings.ToLower(target[0] + "@" + target[1])
			dropSet[key] = struct{}{}
			found := false
			for _, account := range file.Accounts {
				if strings.EqualFold(account.User, target[0]) && strings.EqualFold(account.Host, target[1]) {
					found = true
					break
				}
			}
			if !found && !strings.Contains(lower, "if exists") {
				ctx.Results <- &Result{Err: fmt.Errorf("Operation DROP USER failed for '%s'@'%s': user does not exist", target[0], target[1]), ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
		}
		filtered := make([]persistedAccount, 0, len(file.Accounts))
		for _, account := range file.Accounts {
			if _, drop := dropSet[strings.ToLower(account.User+"@"+account.Host)]; !drop {
				for _, target := range targets {
					roleName := target[0] + "@" + target[1]
					account.Roles = removeStrings(account.Roles, roleName)
					account.RoleAdminOptions = removeStrings(account.RoleAdminOptions, roleName)
					account.DefaultRoles = removeStrings(account.DefaultRoles, roleName)
					delete(account.RoleGrantors, strings.ToLower(roleName))
					if account.Grants != nil {
						delete(account.Grants, fmt.Sprintf("'%s'@'%s'", target[0], target[1]))
					}
				}
				filtered = append(filtered, account)
			}
		}
		file.Accounts = filtered
	case strings.HasPrefix(lower, "drop role"):
		if !accountCanManage(file, ctx.Session, "DROP ROLE", user, host) {
			ctx.Results <- &Result{Err: fmt.Errorf("access denied: current account cannot drop roles"), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		targets := accountTargetsFromQuery(query)
		if len(targets) == 0 {
			ctx.Results <- &Result{Err: fmt.Errorf("DROP ROLE requires a role"), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		dropSet := make(map[string]struct{}, len(targets))
		for _, target := range targets {
			if e.isMandatoryRole(target[0], target[1]) {
				ctx.Results <- &Result{Err: fmt.Errorf("Operation DROP ROLE failed for '%s'@'%s': role is mandatory", target[0], target[1]), ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
			key := strings.ToLower(target[0] + "@" + target[1])
			dropSet[key] = struct{}{}
			found := false
			for _, account := range file.Accounts {
				if strings.EqualFold(account.User, target[0]) && strings.EqualFold(account.Host, target[1]) {
					found = true
					break
				}
			}
			if !found && !strings.Contains(lower, "if exists") {
				ctx.Results <- &Result{Err: fmt.Errorf("Operation DROP ROLE failed for '%s'@'%s': role does not exist", target[0], target[1]), ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
		}
		filtered := make([]persistedAccount, 0, len(file.Accounts))
		for _, account := range file.Accounts {
			if _, drop := dropSet[strings.ToLower(account.User+"@"+account.Host)]; drop {
				continue
			}
			for _, target := range targets {
				roleName := target[0] + "@" + target[1]
				account.Roles = removeStrings(account.Roles, roleName)
				account.RoleAdminOptions = removeStrings(account.RoleAdminOptions, roleName)
				account.DefaultRoles = removeStrings(account.DefaultRoles, roleName)
				delete(account.RoleGrantors, strings.ToLower(roleName))
			}
			filtered = append(filtered, account)
		}
		file.Accounts = filtered
	case strings.HasPrefix(lower, "grant ") || strings.HasPrefix(lower, "revoke "):
		targets := accountTargetsFromQuery(query)
		isRoleGrant := strings.HasPrefix(lower, "grant '") && len(targets) >= 2 && strings.Contains(lower, " to ")
		isRoleRevoke := strings.HasPrefix(lower, "revoke '") && len(targets) >= 2 && strings.Contains(lower, " from ")
		if isRoleGrant || isRoleRevoke {
			granteeTargets := accountGranteeTargetsFromQuery(query)
			if len(granteeTargets) == 0 {
				granteeTargets = targets[len(targets)-1:]
			}
			roleCount := len(targets) - len(granteeTargets)
			if roleCount <= 0 {
				ctx.Results <- &Result{Err: fmt.Errorf("role GRANT/REVOKE requires a role and target account"), ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
			indices := make([]int, len(granteeTargets))
			for targetIndex, target := range granteeTargets {
				idx := -1
				for accountIndex := range file.Accounts {
					if file.Accounts[accountIndex].User == target[0] && file.Accounts[accountIndex].Host == target[1] {
						idx = accountIndex
						break
					}
				}
				if idx < 0 {
					ctx.Results <- &Result{Err: fmt.Errorf("grant target '%s'@'%s' does not exist", target[0], target[1]), ResultType: common.RESULT_TYPE_QUERY}
					return true
				}
				indices[targetIndex] = idx
			}
			for _, roleTarget := range targets[:roleCount] {
				role := roleTarget[0] + "@" + roleTarget[1]
				if isRoleRevoke && e.isMandatoryRole(roleTarget[0], roleTarget[1]) {
					ctx.Results <- &Result{Err: fmt.Errorf("REVOKE failed for '%s': role is mandatory", role), ResultType: common.RESULT_TYPE_QUERY}
					return true
				}
				if !accountCanGrantRole(file, ctx.Session, role) {
					ctx.Results <- &Result{Err: fmt.Errorf("access denied: current account cannot grant role '%s'", role), ResultType: common.RESULT_TYPE_QUERY}
					return true
				}
				for _, idx := range indices {
					if isRoleGrant {
						file.Accounts[idx].Roles = appendUniqueStrings(file.Accounts[idx].Roles, role)
						if file.Accounts[idx].RoleGrantors == nil {
							file.Accounts[idx].RoleGrantors = map[string]string{}
						}
						file.Accounts[idx].RoleGrantors[strings.ToLower(role)] = grantorIdentity(ctx.Session)
						if strings.Contains(lower, "with admin option") {
							file.Accounts[idx].RoleAdminOptions = appendUniqueStrings(file.Accounts[idx].RoleAdminOptions, role)
						}
					} else {
						file.Accounts[idx].Roles = removeStrings(file.Accounts[idx].Roles, role)
						file.Accounts[idx].RoleAdminOptions = removeStrings(file.Accounts[idx].RoleAdminOptions, role)
						file.Accounts[idx].DefaultRoles = removeStrings(file.Accounts[idx].DefaultRoles, role)
						delete(file.Accounts[idx].RoleGrantors, strings.ToLower(role))
					}
				}
			}
			if err := e.stageOrSaveAccountFile(ctx.Session, file); err != nil {
				ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
			message := "Role granted"
			if isRoleRevoke {
				message = "Role revoked"
			}
			accountResult(ctx, nil, nil, message)
			return true
		}
		grantMatch := regexp.MustCompile(`(?is)^(?:grant|revoke)\s+(.+?)\s+on\s+(?:(?:procedure|function)\s+)?([^\s]+)\s+(?:to|from)\s+`).FindStringSubmatch(query)
		if len(grantMatch) != 3 {
			ctx.Results <- &Result{Err: fmt.Errorf("invalid GRANT/REVOKE syntax"), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		key := strings.Trim(grantMatch[2], "` ")
		revokeGrantOptionFor := regexp.MustCompile(`(?is)^revoke\s+grant\s+option\s+for\b`).MatchString(query)
		privilegeSpec := grantMatch[1]
		if revokeGrantOptionFor {
			privilegeSpec = regexp.MustCompile(`(?is)^grant\s+option\s+for\s+`).ReplaceAllString(privilegeSpec, "")
		}
		privileges, columnGrants := parseGrantPrivileges(privilegeSpec, key)
		if strings.Contains(strings.ToLower(query), "with grant option") {
			if len(privileges) > 0 {
				privileges = appendUniqueStrings(privileges, "GRANT OPTION")
			}
			for column, names := range columnGrants {
				columnGrants[column] = appendUniqueStrings(names, "GRANT OPTION")
			}
		}
		if err := validateGrantPrivileges(key, privileges); err != nil {
			ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		if !accountCanGrant(file, ctx.Session, key, privileges) {
			ctx.Results <- &Result{Err: fmt.Errorf("access denied: current account cannot grant or revoke privileges on %s", key), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		granteeTargets := accountGranteeTargetsFromQuery(query)
		if len(granteeTargets) == 0 && len(targets) > 0 {
			granteeTargets = targets[len(targets)-1:]
		}
		if len(granteeTargets) == 0 {
			ctx.Results <- &Result{Err: fmt.Errorf("GRANT/REVOKE requires a target account"), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		indices := make([]int, len(granteeTargets))
		for targetIndex, target := range granteeTargets {
			idx := -1
			for accountIndex := range file.Accounts {
				if file.Accounts[accountIndex].User == target[0] && file.Accounts[accountIndex].Host == target[1] {
					idx = accountIndex
					break
				}
			}
			if idx < 0 {
				ctx.Results <- &Result{Err: fmt.Errorf("grant target '%s'@'%s' does not exist", target[0], target[1]), ResultType: common.RESULT_TYPE_QUERY}
				return true
			}
			indices[targetIndex] = idx
		}
		if strings.HasPrefix(lower, "grant ") {
			if strings.EqualFold(key, "*.*") && regexp.MustCompile(`(?is)\ball(?:\s+privileges)?\b`).MatchString(grantMatch[1]) {
				privileges = appendUniqueStrings(privileges, common.RegisteredDynamicPrivileges()...)
			}
			for _, idx := range indices {
				if file.Accounts[idx].Grants == nil {
					file.Accounts[idx].Grants = map[string][]string{}
				}
				if file.Accounts[idx].ColumnGrants == nil {
					file.Accounts[idx].ColumnGrants = map[string][]string{}
				}
				if len(privileges) > 0 {
					file.Accounts[idx].Grants[key] = appendUniqueStrings(file.Accounts[idx].Grants[key], privileges...)
					for _, privilege := range privileges {
						setPersistedGrantor(&file.Accounts[idx], key, privilege, grantorIdentity(ctx.Session))
					}
				}
				for column, names := range columnGrants {
					file.Accounts[idx].ColumnGrants[column] = appendUniqueStrings(file.Accounts[idx].ColumnGrants[column], names...)
					for _, privilege := range names {
						setPersistedColumnGrantor(&file.Accounts[idx], column, privilege, grantorIdentity(ctx.Session))
					}
				}
				if schema, ok := partialRevokeSchema(key); ok {
					removePartialRevoke(&file.Accounts[idx], schema, privileges)
				} else if strings.EqualFold(key, "*.*") {
					removePartialRevoke(&file.Accounts[idx], "", privileges)
				}
				syncPartialRevokeUserAttributes(&file.Accounts[idx])
			}
		} else {
			partialSchema, hasPartialSchema := partialRevokeSchema(key)
			if !revokeGrantOptionFor && hasPartialSchema && e.partialRevokesEnabled() {
				for _, idx := range indices {
					addPartialRevoke(&file.Accounts[idx], partialSchema, privileges)
				}
			} else if !revokeGrantOptionFor && hasPartialSchema && !e.partialRevokesEnabled() {
				for _, idx := range indices {
					account := &file.Accounts[idx]
					for _, privilege := range partialRevokePrivileges(account, privileges) {
						if grantNameContains(account.Grants["*.*"], privilege) && !grantNameContains(account.Grants[key], privilege) {
							ctx.Results <- &Result{Err: fmt.Errorf("partial revokes is disabled; cannot revoke %s on %s", privilege, key), ResultType: common.RESULT_TYPE_QUERY}
							return true
						}
					}
				}
			} else if !revokeGrantOptionFor && strings.EqualFold(key, "*.*") {
				for _, idx := range indices {
					removePartialRevoke(&file.Accounts[idx], "", privileges)
				}
			}
			if revokeGrantOptionFor {
				for _, idx := range indices {
					if len(privileges) > 0 {
						file.Accounts[idx].Grants[key] = removeStrings(file.Accounts[idx].Grants[key], "GRANT OPTION")
						deletePersistedGrantor(&file.Accounts[idx], key, "GRANT OPTION")
					}
					for column := range columnGrants {
						file.Accounts[idx].ColumnGrants[column] = removeStrings(file.Accounts[idx].ColumnGrants[column], "GRANT OPTION")
						deletePersistedColumnGrantor(&file.Accounts[idx], column, "GRANT OPTION")
					}
				}
			} else if regexp.MustCompile(`(?is)\ball(?:\s+privileges)?\b`).MatchString(grantMatch[1]) {
				for _, idx := range indices {
					delete(file.Accounts[idx].Grants, key)
					deletePersistedGrantorsForScope(&file.Accounts[idx], key)
					prefix := key + "."
					for column := range file.Accounts[idx].ColumnGrants {
						if strings.HasPrefix(strings.ToLower(column), strings.ToLower(prefix)) {
							delete(file.Accounts[idx].ColumnGrants, column)
							deletePersistedColumnGrantorsForScope(&file.Accounts[idx], column)
						}
					}
				}
			} else {
				for _, idx := range indices {
					file.Accounts[idx].Grants[key] = removeStrings(file.Accounts[idx].Grants[key], privileges...)
					for _, privilege := range privileges {
						deletePersistedGrantor(&file.Accounts[idx], key, privilege)
					}
					for column, names := range columnGrants {
						file.Accounts[idx].ColumnGrants[column] = removeStrings(file.Accounts[idx].ColumnGrants[column], names...)
						for _, privilege := range names {
							deletePersistedColumnGrantor(&file.Accounts[idx], column, privilege)
						}
					}
				}
			}
			for _, idx := range indices {
				syncPartialRevokeUserAttributes(&file.Accounts[idx])
			}
		}
	case strings.HasPrefix(lower, "show grants"):
		idx := find()
		if idx < 0 {
			ctx.Results <- &Result{Err: fmt.Errorf("user '%s'@'%s' does not exist", user, host), ResultType: common.RESULT_TYPE_QUERY}
			return true
		}
		rows := make([][]interface{}, 0)
		for key, privileges := range file.Accounts[idx].Grants {
			if len(privileges) == 0 {
				continue
			}
			grantText := strings.Join(displayGrantPrivileges(privileges), ", ")
			if hasGrantOption(privileges) {
				grantText += " WITH GRANT OPTION"
			}
			rows = append(rows, []interface{}{fmt.Sprintf("GRANT %s ON %s TO '%s'@'%s'", grantText, key, user, host)})
		}
		for key, privileges := range file.Accounts[idx].ColumnGrants {
			if len(privileges) > 0 {
				parts := strings.Split(key, ".")
				if len(parts) == 3 {
					grantText := strings.Join(displayGrantPrivileges(privileges), ", ")
					grantOptionSuffix := ""
					if hasGrantOption(privileges) {
						grantOptionSuffix = " WITH GRANT OPTION"
					}
					rows = append(rows, []interface{}{fmt.Sprintf("GRANT %s (%s) ON %s TO '%s'@'%s'%s", grantText, parts[2], strings.Join(parts[:2], "."), user, host, grantOptionSuffix)})
				}
			}
		}
		for database, privileges := range file.Accounts[idx].Restrictions {
			if len(privileges) == 0 {
				continue
			}
			grantText := strings.Join(displayGrantPrivileges(privileges), ", ")
			rows = append(rows, []interface{}{fmt.Sprintf("REVOKE %s ON %s.* FROM '%s'@'%s'", grantText, database, user, host)})
		}
		sort.Slice(rows, func(i, j int) bool { return fmt.Sprint(rows[i][0]) < fmt.Sprint(rows[j][0]) })
		for _, role := range file.Accounts[idx].Roles {
			parts := strings.SplitN(role, "@", 2)
			if len(parts) == 2 {
				grantText := fmt.Sprintf("GRANT '%s'@'%s' TO '%s'@'%s'", parts[0], parts[1], user, host)
				for _, adminRole := range file.Accounts[idx].RoleAdminOptions {
					if strings.EqualFold(adminRole, role) {
						grantText += " WITH ADMIN OPTION"
						break
					}
				}
				rows = append(rows, []interface{}{grantText})
			}
		}
		if len(rows) == 0 {
			rows = append(rows, []interface{}{fmt.Sprintf("GRANT USAGE ON *.* TO '%s'@'%s'", user, host)})
		}
		accountResult(ctx, []string{fmt.Sprintf("Grants for %s@%s", user, host)}, rows, "SHOW GRANTS completed")
		return true
	}
	if err := e.stageOrSaveAccountFile(ctx.Session, file); err != nil {
		ctx.Results <- &Result{Err: err, ResultType: common.RESULT_TYPE_QUERY}
		return true
	}
	accountResult(ctx, nil, nil, "Account statement completed")
	return true
}

func hasGrantOption(privileges []string) bool {
	for _, privilege := range privileges {
		if strings.EqualFold(strings.TrimSpace(privilege), "GRANT OPTION") {
			return true
		}
	}
	return false
}

func displayGrantPrivileges(privileges []string) []string {
	result := make([]string, 0, len(privileges))
	for _, privilege := range privileges {
		if !strings.EqualFold(strings.TrimSpace(privilege), "GRANT OPTION") {
			result = append(result, privilege)
		}
	}
	return result
}

func (e *XMySQLExecutor) executeShowPrivileges(ctx *ExecutionContext) {
	rows := make([][]interface{}, 0, len(common.Priv2SetStr)+len(common.RegisteredDynamicPrivileges()))
	static := []struct {
		name    string
		context string
		comment string
	}{
		{"ALL", "Server administration", "All privileges available at a given privilege level"},
		{"Alter", "Tables", "To alter existing tables"},
		{"Alter routine", "Functions,Procedures", "To alter or drop stored functions/procedures"},
		{"Create", "Databases, tables, or indexes", "To create new databases and tables"},
		{"Create role", "Server Admin", "To create new roles"},
		{"Create routine", "Databases", "To use CREATE FUNCTION/PROCEDURE"},
		{"Create tablespace", "Server Admin", "To create, alter, or drop tablespaces and log file groups"},
		{"Create temporary tables", "Databases", "To use CREATE TEMPORARY TABLE"},
		{"Create User", "Server Admin", "To create new users"},
		{"Create view", "Tables", "To create new views"},
		{"Delete", "Tables", "To delete existing rows"},
		{"Drop", "Databases, tables", "To drop databases, tables, and views"},
		{"Drop role", "Server Admin", "To drop roles"},
		{"Execute", "Server Admin", "To execute stored routines"},
		{"Event", "Databases", "To create events for the Event Scheduler"},
		{"File", "File access on server", "To read and write files on the server"},
		{"Grant Option", "Databases, tables, and routines", "To give to other users those privileges you possess"},
		{"Index", "Tables", "To create or drop indexes"},
		{"Insert", "Tables", "To insert data into tables"},
		{"Lock Tables", "Databases", "To use LOCK TABLES"},
		{"Process", "Server Admin", "To view the plain text of currently executing queries"},
		{"Proxy", "Server Admin", "To make proxy users possible"},
		{"References", "Databases, tables", "To have references on tables"},
		{"Reload", "Server Admin", "To reload or refresh tables, logs and privileges"},
		{"Replication client", "Server Admin", "To ask where the source or replica is"},
		{"Replication slave", "Server Admin", "To read binary logs from the source"},
		{"Select", "Tables", "To retrieve rows from table"},
		{"Show Databases", "Server Admin", "To see all databases"},
		{"Show View", "Tables", "To see views"},
		{"Shutdown", "Server Admin", "To shut down the server"},
		{"Super", "Server Admin", "To use KILL thread, SET GLOBAL, and other admin commands"},
		{"Trigger", "Tables", "To use triggers"},
		{"Update", "Tables", "To update existing rows"},
		{"Usage", "Server Admin", "Synonym for no privileges"},
	}
	for _, privilege := range static {
		rows = append(rows, []interface{}{privilege.name, privilege.context, privilege.comment})
	}
	for _, privilege := range common.RegisteredDynamicPrivileges() {
		rows = append(rows, []interface{}{privilege, "Global", "Dynamic privilege"})
	}
	sort.Slice(rows, func(i, j int) bool { return fmt.Sprint(rows[i][0]) < fmt.Sprint(rows[j][0]) })
	accountResult(ctx, []string{"Privilege", "Context", "Comment"}, rows, "SHOW PRIVILEGES completed")
}

func validateGrantPrivileges(scope string, privileges []string) error {
	for _, raw := range privileges {
		privilege := strings.ToUpper(strings.TrimSpace(raw))
		if privilege == "" || isStaticGrantPrivilege(privilege) {
			continue
		}
		if !common.IsRegisteredDynamicPrivilege(privilege) {
			return fmt.Errorf("unknown dynamic privilege %s", privilege)
		}
		if !strings.EqualFold(strings.TrimSpace(scope), "*.*") {
			return fmt.Errorf("dynamic privilege %s is only valid at global scope", privilege)
		}
	}
	return nil
}

func isStaticGrantPrivilege(privilege string) bool {
	switch privilege {
	case "ALL", "ALL PRIVILEGES", "GRANT OPTION", "USAGE",
		"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "RELOAD", "SHUTDOWN",
		"PROCESS", "FILE", "REFERENCES", "INDEX", "ALTER", "SHOW DATABASES", "SUPER",
		"CREATE TEMPORARY TABLES", "LOCK TABLES", "EXECUTE", "REPLICATION SLAVE", "REPLICATION CLIENT",
		"CREATE VIEW", "SHOW VIEW", "CREATE ROLE", "DROP ROLE", "CREATE USER", "CREATE TABLESPACE",
		"TRIGGER", "EVENT", "CREATE ROUTINE", "ALTER ROUTINE", "PROXY":
		return true
	default:
		return false
	}
}

func splitGrantPrivileges(raw string) []string {
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, strings.ToUpper(part))
		}
	}
	return result
}

func parseGrantPrivileges(raw, scope string) ([]string, map[string][]string) {
	columns := map[string][]string{}
	if regexp.MustCompile(`(?is)^\s*grant\s+option\s+for\b`).MatchString(raw) {
		return []string{"GRANT OPTION"}, columns
	}
	grantOption := regexp.MustCompile(`(?is)\s+with\s+grant\s+option\s*$`).MatchString(raw)
	if grantOption {
		raw = regexp.MustCompile(`(?is)\s+with\s+grant\s+option\s*$`).ReplaceAllString(raw, "")
	}
	columnPattern := regexp.MustCompile(`(?is)([a-z][a-z0-9 _]*)\s*\(([^)]*)\)`)
	plain := columnPattern.ReplaceAllStringFunc(raw, func(match string) string {
		parts := columnPattern.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}
		privilege := strings.ToUpper(strings.TrimSpace(parts[1]))
		for _, column := range strings.Split(parts[2], ",") {
			column = strings.Trim(strings.TrimSpace(column), "` ")
			if column != "" {
				columns[scope+"."+column] = appendUniqueStrings(columns[scope+"."+column], privilege)
			}
		}
		return ""
	})
	privileges := splitGrantPrivileges(plain)
	if grantOption {
		// A column grant's WITH GRANT OPTION belongs to the column
		// privilege itself. Do not manufacture a table-level GRANT OPTION
		// row when the statement contains only column privileges. For mixed
		// table/column grants, retain the option at both scopes because it
		// applies to each granted privilege.
		if len(privileges) > 0 {
			privileges = appendUniqueStrings(privileges, "GRANT OPTION")
		}
		for column, names := range columns {
			columns[column] = appendUniqueStrings(names, "GRANT OPTION")
		}
	}
	return privileges, columns
}

func appendUniqueStrings(target []string, values ...string) []string {
	seen := make(map[string]struct{}, len(target)+len(values))
	for _, value := range target {
		seen[value] = struct{}{}
	}
	for _, value := range values {
		if _, exists := seen[value]; !exists {
			target = append(target, value)
			seen[value] = struct{}{}
		}
	}
	return target
}

func removeStrings(target []string, values ...string) []string {
	remove := make(map[string]struct{}, len(values))
	for _, value := range values {
		remove[value] = struct{}{}
	}
	result := target[:0]
	for _, value := range target {
		if _, exists := remove[value]; !exists {
			result = append(result, value)
		}
	}
	return result
}

func replaceString(target []string, oldValue, newValue string) []string {
	result := append([]string(nil), target...)
	for index, value := range result {
		if strings.EqualFold(value, oldValue) {
			result[index] = newValue
		}
	}
	return result
}
