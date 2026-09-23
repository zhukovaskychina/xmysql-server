package auth

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
)

type persistedAccountForAuth struct {
	User            string              `json:"user"`
	Host            string              `json:"host"`
	Password        string              `json:"password"`
	Plugin          string              `json:"plugin,omitempty"`
	TLSRequired     bool                `json:"tls_required,omitempty"`
	X509Required    bool                `json:"x509_required,omitempty"`
	AccountLocked   bool                `json:"account_locked"`
	PasswordExpired bool                `json:"password_expired"`
	Roles           []string            `json:"roles,omitempty"`
	DefaultRoles    []string            `json:"default_roles,omitempty"`
	Grants          map[string][]string `json:"grants,omitempty"`
	ColumnGrants    map[string][]string `json:"column_grants,omitempty"`
	GlobalGrants    []string            `json:"global_grants,omitempty"`
	Restrictions    map[string][]string `json:"restrictions,omitempty"`
}

type persistedAccountFileForAuth struct {
	Accounts []persistedAccountForAuth `json:"accounts"`
}

func (ea *InnoDBEngineAccess) loadPersistedAccount(user, host string) (*persistedAccountForAuth, bool, error) {
	if ea == nil || ea.engine == nil {
		return nil, false, nil
	}
	path := filepath.Join(ea.engine.GetDataDir(), "mysql", "accounts.json")
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var file persistedAccountFileForAuth
	if err := json.Unmarshal(raw, &file); err != nil {
		return nil, false, err
	}
	var best *persistedAccountForAuth
	bestScore := -1
	for _, account := range file.Accounts {
		if account.User != user {
			continue
		}
		if score := authHostMatchSpecificity(ea, host, account.Host); score > bestScore {
			candidate := account
			best = &candidate
			bestScore = score
		}
	}
	if best == nil {
		return nil, false, nil
	}
	return best, true, nil
}

// authHostMatchSpecificity mirrors MySQL's account selection rule: an exact
// host wins over a wildcard, and among wildcard matches the pattern with more
// literal characters wins. File order must never decide which account gets
// authenticated.
func authHostMatchSpecificity(ea *InnoDBEngineAccess, host, pattern string) int {
	if ea == nil || !ea.matchHost(host, pattern) {
		return -1
	}
	if pattern == host {
		return 1_000_000 + len(pattern)
	}
	score := 0
	for _, char := range pattern {
		switch char {
		case '%':
			score -= 1
		case '_':
			score += 1
		default:
			score += 10
		}
	}
	if score < 0 {
		score = 0
	}
	return score
}

func persistedAccountToUserInfo(account *persistedAccountForAuth) *UserInfo {
	if account == nil {
		return nil
	}
	globalGrantNames := append([]string(nil), account.Grants["*.*"]...)
	globalGrantNames = appendUniqueGrantNames(globalGrantNames, account.GlobalGrants...)
	info := &UserInfo{
		User:               account.User,
		Host:               account.Host,
		Password:           account.Password,
		AuthPlugin:         account.Plugin,
		TLSRequired:        account.TLSRequired,
		X509Required:       account.X509Required,
		AccountLocked:      account.AccountLocked,
		PasswordExpired:    account.PasswordExpired,
		GlobalPrivileges:   grantNamesToPrivileges(globalGrantNames),
		DynamicPrivileges:  dynamicGrantNames(globalGrantNames),
		DatabasePrivileges: make(map[string][]common.PrivilegeType),
		TablePrivileges:    make(map[string]map[string][]common.PrivilegeType),
		ColumnPrivileges:   make(map[string][]common.PrivilegeType),
		Roles:              append([]string(nil), account.Roles...),
		DefaultRoles:       append([]string(nil), account.DefaultRoles...),
		Restrictions:       persistedRestrictionPrivileges(account.Restrictions),
	}
	for scope, names := range account.Grants {
		privileges := grantNamesToPrivileges(names)
		parts := strings.Split(scope, ".")
		switch len(parts) {
		case 2:
			if parts[0] == "*" && parts[1] == "*" {
				continue
			}
			if parts[1] == "*" {
				info.DatabasePrivileges[parts[0]] = appendUniquePrivileges(info.DatabasePrivileges[parts[0]], privileges...)
			} else {
				if info.TablePrivileges[parts[0]] == nil {
					info.TablePrivileges[parts[0]] = map[string][]common.PrivilegeType{}
				}
				info.TablePrivileges[parts[0]][parts[1]] = appendUniquePrivileges(info.TablePrivileges[parts[0]][parts[1]], privileges...)
			}
		}
	}
	for scope, names := range account.ColumnGrants {
		info.ColumnPrivileges[scope] = appendUniquePrivileges(info.ColumnPrivileges[scope], grantNamesToPrivileges(names)...)
	}
	return info
}

func persistedRestrictionPrivileges(restrictions map[string][]string) map[string][]common.PrivilegeType {
	result := make(map[string][]common.PrivilegeType, len(restrictions))
	for database, names := range restrictions {
		privileges := grantNamesToPrivileges(names)
		if len(privileges) > 0 {
			result[database] = append([]common.PrivilegeType(nil), privileges...)
		}
	}
	return result
}

func persistedPrivilegeRestricted(restrictions map[string][]string, database string, privilege common.PrivilegeType) bool {
	for name, privileges := range restrictions {
		if !strings.EqualFold(name, database) {
			continue
		}
		for _, restricted := range privileges {
			if strings.EqualFold(strings.TrimSpace(restricted), privilege.String()) || strings.EqualFold(strings.TrimSpace(restricted), "ALL") {
				return true
			}
		}
	}
	return false
}

func dynamicGrantNames(names []string) []string {
	result := make([]string, 0)
	seen := make(map[string]struct{})
	for _, raw := range names {
		name := strings.ToUpper(strings.TrimSpace(raw))
		if name == "" || !isDynamicGrantName(name) {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		result = append(result, name)
	}
	return result
}

func isDynamicGrantName(name string) bool {
	return common.IsRegisteredDynamicPrivilege(name)
}

func appendUniquePrivileges(target []common.PrivilegeType, values ...common.PrivilegeType) []common.PrivilegeType {
	seen := make(map[common.PrivilegeType]struct{}, len(target)+len(values))
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

func grantNamesToPrivileges(names []string) []common.PrivilegeType {
	result := make([]common.PrivilegeType, 0, len(names))
	for _, name := range names {
		name = strings.ToUpper(strings.TrimSpace(name))
		if name == "ALL" || name == "ALL PRIVILEGES" {
			result = append(result, common.AllGlobalPrivs...)
			continue
		}
		mapping := map[string]common.PrivilegeType{
			"SELECT": common.SelectPriv, "INSERT": common.InsertPriv, "UPDATE": common.UpdatePriv, "DELETE": common.DeletePriv,
			"CREATE": common.CreatePriv, "DROP": common.DropPriv, "ALTER": common.AlterPriv, "INDEX": common.IndexPriv,
			"EXECUTE": common.ExecutePriv, "CREATE VIEW": common.CreateViewPriv, "SHOW VIEW": common.ShowViewPriv,
			"CREATE USER": common.CreateUserPriv, "GRANT OPTION": common.GrantPriv, "TRIGGER": common.TriggerPriv,
			"EVENT": common.EventPriv, "REFERENCES": common.ReferencesPriv, "USAGE": common.UsagePriv,
		}
		if privilege, ok := mapping[name]; ok {
			result = append(result, privilege)
		}
	}
	return result
}

func (ea *InnoDBEngineAccess) queryPersistedGrantPrivileges(ctx context.Context, user, host, database, table string) ([]common.PrivilegeType, bool, error) {
	account, found, err := ea.loadPersistedAccount(user, host)
	if err != nil || !found {
		return nil, found, err
	}
	roles, explicitRoles := server.ActiveRoles(ctx)
	ea.expandPersistedAccountGrants(account, roles, explicitRoles)
	if account.Grants == nil {
		return []common.PrivilegeType{}, true, nil
	}
	keys := []string{"*.*"}
	if database != "" {
		keys = append(keys, database+".*")
	}
	if table != "" {
		keys = append(keys, database+"."+table)
	}
	result := make([]common.PrivilegeType, 0)
	seen := map[common.PrivilegeType]struct{}{}
	for _, key := range keys {
		for _, privilege := range grantNamesToPrivileges(account.Grants[key]) {
			if key == "*.*" && persistedPrivilegeRestricted(account.Restrictions, database, privilege) {
				continue
			}
			if _, exists := seen[privilege]; !exists {
				result = append(result, privilege)
				seen[privilege] = struct{}{}
			}
		}
	}
	return result, true, nil
}

func (ea *InnoDBEngineAccess) queryPersistedColumnPrivileges(ctx context.Context, user, host, database, table, column string) ([]common.PrivilegeType, bool, error) {
	account, found, err := ea.loadPersistedAccount(user, host)
	if err != nil || !found {
		return nil, found, err
	}
	roles, explicitRoles := server.ActiveRoles(ctx)
	ea.expandPersistedAccountGrants(account, roles, explicitRoles)
	if account.ColumnGrants == nil || database == "" || table == "" || column == "" {
		return []common.PrivilegeType{}, true, nil
	}
	keys := []string{
		database + "." + table + "." + column,
		database + "." + table + ".*",
		"*.*." + column,
	}
	result := make([]common.PrivilegeType, 0)
	seen := map[common.PrivilegeType]struct{}{}
	for _, key := range keys {
		for _, privilege := range grantNamesToPrivileges(account.ColumnGrants[key]) {
			if _, exists := seen[privilege]; !exists {
				result = append(result, privilege)
				seen[privilege] = struct{}{}
			}
		}
	}
	return result, true, nil
}

func (ea *InnoDBEngineAccess) expandPersistedAccountGrants(account *persistedAccountForAuth, activeRoles []string, explicitRoles bool) {
	if account == nil || (len(account.Roles) == 0 && !explicitRoles) {
		return
	}
	visited := map[string]bool{}
	var merge func(user, host string, depth int)
	merge = func(user, host string, depth int) {
		if depth > 16 {
			return
		}
		key := user + "@" + host
		if visited[key] {
			return
		}
		visited[key] = true
		role, found, err := ea.loadPersistedAccount(user, host)
		if err != nil || !found || role == nil {
			return
		}
		if account.Grants == nil {
			account.Grants = map[string][]string{}
		}
		for scope, names := range role.Grants {
			account.Grants[scope] = appendUniqueGrantNames(account.Grants[scope], names...)
		}
		if account.Restrictions == nil {
			account.Restrictions = map[string][]string{}
		}
		for database, names := range role.Restrictions {
			account.Restrictions[database] = appendUniqueGrantNames(account.Restrictions[database], names...)
		}
		account.GlobalGrants = appendUniqueGrantNames(account.GlobalGrants, role.GlobalGrants...)
		if account.ColumnGrants == nil {
			account.ColumnGrants = map[string][]string{}
		}
		for scope, names := range role.ColumnGrants {
			account.ColumnGrants[scope] = appendUniqueGrantNames(account.ColumnGrants[scope], names...)
		}
		for _, nested := range role.Roles {
			parts := strings.SplitN(nested, "@", 2)
			if len(parts) == 2 {
				merge(parts[0], parts[1], depth+1)
			}
		}
	}
	roles := account.Roles
	if explicitRoles {
		roles = activeRoles
	}
	for _, role := range roles {
		parts := strings.SplitN(role, "@", 2)
		if len(parts) == 2 {
			merge(parts[0], parts[1], 1)
		}
	}
}

func appendUniqueGrantNames(target []string, values ...string) []string {
	seen := make(map[string]struct{}, len(target)+len(values))
	for _, value := range target {
		seen[strings.ToUpper(strings.TrimSpace(value))] = struct{}{}
	}
	for _, value := range values {
		key := strings.ToUpper(strings.TrimSpace(value))
		if key != "" {
			if _, exists := seen[key]; !exists {
				target = append(target, value)
				seen[key] = struct{}{}
			}
		}
	}
	return target
}

func (ea *InnoDBEngineAccess) queryPersistedUserInfo(ctx context.Context, user, host string) (*UserInfo, bool, error) {
	account, found, err := ea.loadPersistedAccount(user, host)
	if err != nil || !found {
		return nil, found, err
	}
	roles, explicitRoles := server.ActiveRoles(ctx)
	ea.expandPersistedAccountGrants(account, roles, explicitRoles)
	return persistedAccountToUserInfo(account), true, nil
}
