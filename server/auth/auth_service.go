package auth

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server"
	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/conf"
)

// AuthService 认证服务接口
type AuthService interface {
	// 验证用户认证
	AuthenticateUser(ctx context.Context, user, password, host, database string) (*AuthResult, error)

	// 验证数据库是否存在
	ValidateDatabase(ctx context.Context, database string) error

	// 检查用户权限
	CheckPrivilege(ctx context.Context, user, host, database, table string, privilege common.PrivilegeType) error

	// 获取用户信息
	GetUserInfo(ctx context.Context, user, host string) (*UserInfo, error)

	// 刷新权限缓存
	FlushPrivileges(ctx context.Context) error

	// 生成认证挑战
	GenerateChallenge(sessionID string) ([]byte, error)

	// 获取认证挑战
	GetChallenge(sessionID string) []byte
}

// AuthResult 认证结果
type AuthResult struct {
	Success           bool
	User              string
	Host              string
	Database          string
	Privileges        []common.PrivilegeType
	DynamicPrivileges []string
	ActiveRoles       []string
	ErrorCode         uint16
	ErrorMessage      string
}

// ResolveProxyUser resolves a PROXY grant for an authenticated account.  It
// is intentionally additive to AuthService so lightweight test doubles and
// external authentication adapters remain source compatible.
func (as *AuthServiceImpl) ResolveProxyUser(ctx context.Context, user, host string) (string, string, bool, error) {
	resolver, ok := as.engineAccess.(interface {
		QueryProxyUser(context.Context, string, string) (string, string, bool, error)
	})
	if !ok {
		return "", "", false, nil
	}
	return resolver.QueryProxyUser(ctx, user, host)
}

// UserInfo 用户信息
type UserInfo struct {
	User               string
	Host               string
	Password           string
	AuthPlugin         string
	TLSRequired        bool
	X509Required       bool
	PasswordExpired    bool
	AccountLocked      bool
	MaxConnections     int
	MaxUserConnections int
	GlobalPrivileges   []common.PrivilegeType
	DynamicPrivileges  []string
	DatabasePrivileges map[string][]common.PrivilegeType
	TablePrivileges    map[string]map[string][]common.PrivilegeType
	ColumnPrivileges   map[string][]common.PrivilegeType
	Restrictions       map[string][]common.PrivilegeType
	Roles              []string
	DefaultRoles       []string
}

// DatabaseInfo 数据库信息
type DatabaseInfo struct {
	Name      string
	Charset   string
	Collation string
	Exists    bool
}

// AuthServiceImpl 认证服务实现
type AuthServiceImpl struct {
	config            *conf.Cfg
	engineAccess      EngineAccess
	userCache         map[string]*UserInfo
	dbCache           map[string]*DatabaseInfo
	cacheExpiry       time.Time
	passwordValidator PasswordValidator
	challengeCache    map[string][]byte // 存储会话的挑战字符串
}

// EngineAccess 引擎访问接口
type EngineAccess interface {
	// 查询用户信息
	QueryUser(ctx context.Context, user, host string) (*UserInfo, error)

	// 查询数据库信息
	QueryDatabase(ctx context.Context, database string) (*DatabaseInfo, error)

	// 查询用户权限
	QueryUserPrivileges(ctx context.Context, user, host string) ([]common.PrivilegeType, error)

	// 查询数据库权限
	QueryDatabasePrivileges(ctx context.Context, user, host, database string) ([]common.PrivilegeType, error)

	// 查询表权限
	QueryTablePrivileges(ctx context.Context, user, host, database, table string) ([]common.PrivilegeType, error)
}

// NewAuthService 创建认证服务
func NewAuthService(config *conf.Cfg, engineAccess EngineAccess) AuthService {
	factory := &PasswordValidatorFactory{}
	validator := factory.CreateValidator("mysql_native_password")

	return &AuthServiceImpl{
		config:            config,
		engineAccess:      engineAccess,
		userCache:         make(map[string]*UserInfo),
		dbCache:           make(map[string]*DatabaseInfo),
		cacheExpiry:       time.Now().Add(5 * time.Minute), // 缓存5分钟
		passwordValidator: validator,
		challengeCache:    make(map[string][]byte),
	}
}

// AuthenticateUser 验证用户认证
func (as *AuthServiceImpl) AuthenticateUser(ctx context.Context, user, password, host, database string) (*AuthResult, error) {
	// 1. 获取用户信息
	userInfo, err := as.getUserInfo(ctx, user, host)
	if err != nil {
		return &AuthResult{
			Success:      false,
			ErrorCode:    common.ER_ACCESS_DENIED_ERROR,
			ErrorMessage: fmt.Sprintf("Access denied for user '%s'@'%s'", user, host),
		}, nil
	}

	// 2. 验证密码
	sessionKey := fmt.Sprintf("%s@%s", user, host)
	challenge := as.challengeCache[sessionKey]
	usedExactSessionKey := challenge != nil
	challengeSessionKey := sessionKey
	if len(challenge) == 0 && len(as.challengeCache) > 0 {
		for k, c := range as.challengeCache {
			if len(c) > 0 {
				challenge = c
				challengeSessionKey = k
				usedExactSessionKey = false
				break
			}
		}
	}

	validator := as.passwordValidator
	if userInfo.AuthPlugin != "" {
		candidate := (&PasswordValidatorFactory{}).CreateValidator(userInfo.AuthPlugin)
		if candidate == nil {
			return &AuthResult{Success: false, ErrorCode: common.ErrPluginIsNotLoaded, ErrorMessage: fmt.Sprintf("Authentication plugin '%s' is not loaded", userInfo.AuthPlugin)}, nil
		}
		validator = candidate
	}
	if !validator.ValidatePassword(password, userInfo.Password, challenge) {
		return &AuthResult{
			Success:      false,
			ErrorCode:    common.ER_ACCESS_DENIED_ERROR,
			ErrorMessage: fmt.Sprintf("Access denied for user '%s'@'%s' (using password: YES)", user, host),
		}, nil
	}

	// 3. 检查账户状态
	if userInfo.AccountLocked {
		return &AuthResult{
			Success:      false,
			ErrorCode:    common.ER_ACCESS_DENIED_ERROR,
			ErrorMessage: fmt.Sprintf("Account '%s'@'%s' is locked", user, host),
		}, nil
	}

	if userInfo.PasswordExpired {
		return &AuthResult{
			Success:      false,
			ErrorCode:    common.ER_ACCESS_DENIED_ERROR,
			ErrorMessage: "Your password has expired. To log in you must change it using a client that supports expired passwords.",
		}, nil
	}

	// Password authentication belongs to the proxy account, while database
	// access and the resulting session identity belong to the proxied account.
	// Resolve this before validating an optional default database so both the
	// legacy AuthService API and the decoupled protocol path agree on PROXY
	// semantics.
	if proxiedUser, proxiedHost, found, proxyErr := as.ResolveProxyUser(ctx, user, host); proxyErr != nil {
		return &AuthResult{Success: false, ErrorCode: common.ER_ACCESS_DENIED_ERROR, ErrorMessage: proxyErr.Error()}, nil
	} else if found {
		proxiedInfo, lookupErr := as.getUserInfo(ctx, proxiedUser, proxiedHost)
		if lookupErr != nil || proxiedInfo == nil {
			if lookupErr == nil {
				lookupErr = fmt.Errorf("PROXY target '%s'@'%s' does not exist", proxiedUser, proxiedHost)
			}
			return &AuthResult{Success: false, ErrorCode: common.ER_ACCESS_DENIED_ERROR, ErrorMessage: lookupErr.Error()}, nil
		}
		if proxiedInfo.AccountLocked {
			return &AuthResult{Success: false, ErrorCode: common.ER_ACCESS_DENIED_ERROR, ErrorMessage: fmt.Sprintf("PROXY target '%s'@'%s' is locked", proxiedUser, proxiedHost)}, nil
		}
		user, host, userInfo = proxiedUser, proxiedHost, proxiedInfo
	}

	// 4. 验证数据库（如果指定了数据库）
	if database != "" {
		if err := as.ValidateDatabase(ctx, database); err != nil {
			return &AuthResult{
				Success:      false,
				ErrorCode:    common.ER_BAD_DB_ERROR,
				ErrorMessage: fmt.Sprintf("Unknown database '%s'", database),
			}, nil
		}

		// 检查数据库访问权限
		if err := as.CheckPrivilege(ctx, user, host, database, "", common.SelectPriv); err != nil {
			return &AuthResult{
				Success:      false,
				ErrorCode:    common.ER_SPECIFIC_ACCESS_DENIED_ERROR,
				ErrorMessage: fmt.Sprintf("Access denied for user '%s'@'%s' to database '%s'", user, host, database),
			}, nil
		}
	}

	// 5. 认证成功，清理挑战缓存
	if len(challengeSessionKey) > 0 {
		if usedExactSessionKey {
			delete(as.challengeCache, challengeSessionKey)
		} else {
			for k := range as.challengeCache {
				if len(as.challengeCache[k]) > 0 {
					delete(as.challengeCache, k)
				}
			}
		}
	}

	// 6. 获取用户权限
	privileges := userInfo.GlobalPrivileges
	if database != "" && userInfo.DatabasePrivileges[database] != nil {
		privileges = append(privileges, userInfo.DatabasePrivileges[database]...)
	}

	activeRoles := append([]string(nil), userInfo.DefaultRoles...)
	if roleProvider, ok := as.engineAccess.(interface {
		MandatoryRoles(context.Context) []string
		ActivateAllRolesOnLogin(context.Context) bool
	}); ok && roleProvider.ActivateAllRolesOnLogin(ctx) {
		activeRoles = append(activeRoles[:0], userInfo.Roles...)
		activeRoles = append(activeRoles, roleProvider.MandatoryRoles(ctx)...)
	}

	return &AuthResult{
		Success:           true,
		User:              user,
		Host:              host,
		Database:          database,
		Privileges:        privileges,
		DynamicPrivileges: append([]string(nil), userInfo.DynamicPrivileges...),
		ActiveRoles:       normalizeAuthRoleList(activeRoles),
	}, nil
}

func normalizeAuthRoleList(roles []string) []string {
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

// CheckDynamicPrivilege checks a named MySQL dynamic privilege without
// forcing it into the fixed common.PrivilegeType bitmask. Callers that gate
// features such as BACKUP_ADMIN or SYSTEM_USER can use this additive API;
// existing static privilege callers remain source-compatible.
func (as *AuthServiceImpl) CheckDynamicPrivilege(ctx context.Context, user, host, privilege string) error {
	userInfo, err := as.getUserInfo(ctx, user, host)
	if err != nil {
		return err
	}
	wanted := strings.ToUpper(strings.TrimSpace(privilege))
	if !common.IsRegisteredDynamicPrivilege(wanted) {
		return fmt.Errorf("unknown dynamic privilege %s", wanted)
	}
	for _, granted := range userInfo.DynamicPrivileges {
		if strings.EqualFold(granted, wanted) {
			return nil
		}
	}
	return fmt.Errorf("access denied: user '%s'@'%s' lacks dynamic privilege %s", user, host, wanted)
}

// ValidateDatabase 验证数据库是否存在
func (as *AuthServiceImpl) ValidateDatabase(ctx context.Context, database string) error {
	// 检查缓存
	if dbInfo, exists := as.dbCache[database]; exists && time.Now().Before(as.cacheExpiry) {
		if !dbInfo.Exists {
			return fmt.Errorf("database '%s' does not exist", database)
		}
		return nil
	}

	// 从引擎查询
	dbInfo, err := as.engineAccess.QueryDatabase(ctx, database)
	if err != nil {
		return err
	}

	// 更新缓存
	as.dbCache[database] = dbInfo

	if !dbInfo.Exists {
		return fmt.Errorf("database '%s' does not exist", database)
	}

	return nil
}

// CheckPrivilege 检查用户权限
func (as *AuthServiceImpl) CheckPrivilege(ctx context.Context, user, host, database, table string, privilege common.PrivilegeType) error {
	userInfo, err := as.getUserInfo(ctx, user, host)
	if err != nil {
		return err
	}

	// 检查全局权限
	for _, p := range userInfo.GlobalPrivileges {
		if (p == privilege || p == common.AllPriv) && !hasPrivilege(userInfo.Restrictions[database], privilege) {
			return nil
		}
	}

	// 检查数据库权限。权限表是独立于 mysql.user 的 grant scope，不能只
	// 依赖 QueryUser 返回的全局权限快照。
	if database != "" {
		if dbPrivs, exists := userInfo.DatabasePrivileges[database]; exists {
			if hasPrivilege(dbPrivs, privilege) {
				return nil
			}
		}
		dbPrivs, dbErr := as.engineAccess.QueryDatabasePrivileges(ctx, user, host, database)
		if dbErr != nil {
			return dbErr
		}
		if hasPrivilege(dbPrivs, privilege) {
			return nil
		}
	}

	// 检查表权限
	if table != "" && database != "" {
		if dbTables, exists := userInfo.TablePrivileges[database]; exists {
			if tablePrivs, exists := dbTables[table]; exists {
				if hasPrivilege(tablePrivs, privilege) {
					return nil
				}
			}
		}
		tablePrivs, tableErr := as.engineAccess.QueryTablePrivileges(ctx, user, host, database, table)
		if tableErr != nil {
			return tableErr
		}
		if hasPrivilege(tablePrivs, privilege) {
			return nil
		}
	}

	return fmt.Errorf("access denied: user '%s'@'%s' lacks %s privilege", user, host, privilege.String())
}

// CheckColumnPrivilege applies the same global/database/table precedence as
// CheckPrivilege and then evaluates the persisted column grant. It is kept as
// an additive API so existing dispatcher integrations remain source compatible.
func (as *AuthServiceImpl) CheckColumnPrivilege(ctx context.Context, user, host, database, table, column string, privilege common.PrivilegeType) error {
	if err := as.CheckPrivilege(ctx, user, host, database, table, privilege); err == nil {
		return nil
	}
	if userInfo, err := as.getUserInfo(ctx, user, host); err == nil {
		if hasPrivilege(userInfo.ColumnPrivileges[database+"."+table+"."+column], privilege) {
			return nil
		}
	}
	if access, ok := as.engineAccess.(interface {
		queryPersistedColumnPrivileges(context.Context, string, string, string, string, string) ([]common.PrivilegeType, bool, error)
	}); ok {
		privileges, found, err := access.queryPersistedColumnPrivileges(ctx, user, host, database, table, column)
		if err != nil {
			return err
		}
		if found && hasPrivilege(privileges, privilege) {
			return nil
		}
	}
	return fmt.Errorf("access denied: user '%s'@'%s' lacks %s privilege on column '%s'", user, host, privilege.String(), column)
}

func hasPrivilege(privileges []common.PrivilegeType, required common.PrivilegeType) bool {
	for _, privilege := range privileges {
		if privilege == required || privilege == common.AllPriv {
			return true
		}
	}
	return false
}

// GetUserInfo 获取用户信息
func (as *AuthServiceImpl) GetUserInfo(ctx context.Context, user, host string) (*UserInfo, error) {
	return as.getUserInfo(ctx, user, host)
}

// FlushPrivileges 刷新权限缓存
func (as *AuthServiceImpl) FlushPrivileges(ctx context.Context) error {
	as.userCache = make(map[string]*UserInfo)
	as.dbCache = make(map[string]*DatabaseInfo)
	as.cacheExpiry = time.Now().Add(5 * time.Minute)
	return nil
}

// getUserInfo 获取用户信息（内部方法）
func (as *AuthServiceImpl) getUserInfo(ctx context.Context, user, host string) (*UserInfo, error) {
	key := fmt.Sprintf("%s@%s", user, host)
	if roles, explicit := server.ActiveRoles(ctx); explicit {
		key += "#roles=" + fmt.Sprint(roles)
	}

	// 检查缓存
	if userInfo, exists := as.userCache[key]; exists && time.Now().Before(as.cacheExpiry) {
		return userInfo, nil
	}

	// 从引擎查询
	userInfo, err := as.engineAccess.QueryUser(ctx, user, host)
	if err != nil {
		return nil, err
	}

	// 查询权限
	globalPrivs, err := as.engineAccess.QueryUserPrivileges(ctx, user, host)
	if err == nil {
		userInfo.GlobalPrivileges = globalPrivs
	}

	// 更新缓存
	as.userCache[key] = userInfo

	return userInfo, nil
}

// GenerateChallenge 为会话生成挑战字符串
func (as *AuthServiceImpl) GenerateChallenge(sessionID string) ([]byte, error) {
	challenge, err := as.passwordValidator.GenerateChallenge()
	if err != nil {
		return nil, err
	}

	// 缓存挑战字符串
	as.challengeCache[sessionID] = challenge

	return challenge, nil
}

// GetChallenge 获取会话的挑战字符串
func (as *AuthServiceImpl) GetChallenge(sessionID string) []byte {
	return as.challengeCache[sessionID]
}
