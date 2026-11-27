package auth

import (
	"slices"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

// Cached values are immutable snapshots. Callers receive their own copy so
// changing a result cannot race with another connection reading the cache.
func cloneCachedUserInfo(info *UserInfo) *UserInfo {
	if info == nil {
		return nil
	}
	copy := *info
	copy.PasswordLifetime = cloneAuthPointer(info.PasswordLifetime)
	copy.FailedLoginAttempts = cloneAuthPointer(info.FailedLoginAttempts)
	copy.PasswordLockTime = cloneAuthPointer(info.PasswordLockTime)
	copy.PasswordLockedUntil = cloneAuthPointer(info.PasswordLockedUntil)
	copy.GlobalPrivileges = slices.Clone(info.GlobalPrivileges)
	copy.DynamicPrivileges = slices.Clone(info.DynamicPrivileges)
	copy.Roles = slices.Clone(info.Roles)
	copy.DefaultRoles = slices.Clone(info.DefaultRoles)
	copy.DatabasePrivileges = cloneAuthPrivilegeMap(info.DatabasePrivileges)
	copy.ColumnPrivileges = cloneAuthPrivilegeMap(info.ColumnPrivileges)
	copy.Restrictions = cloneAuthPrivilegeMap(info.Restrictions)
	if info.TablePrivileges != nil {
		copy.TablePrivileges = make(map[string]map[string][]common.PrivilegeType, len(info.TablePrivileges))
		for database, tables := range info.TablePrivileges {
			copy.TablePrivileges[database] = cloneAuthPrivilegeMap(tables)
		}
	}
	return &copy
}

func cloneAuthPrivilegeMap(source map[string][]common.PrivilegeType) map[string][]common.PrivilegeType {
	if source == nil {
		return nil
	}
	copy := make(map[string][]common.PrivilegeType, len(source))
	for key, privileges := range source {
		copy[key] = slices.Clone(privileges)
	}
	return copy
}

func cloneAuthPointer[T any](source *T) *T {
	if source == nil {
		return nil
	}
	copy := *source
	return &copy
}
