// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package common

// AllPrivilegeLiteral is the string literal for All Privilege.
const AllPrivilegeLiteral = "ALL PRIVILEGES"

// Priv2Str is the map for privilege to string.
var Priv2Str = privilegeStringMap(0)

// Priv2SetStr is the map for privilege to string.
var Priv2SetStr = privilegeStringMap(1)

// SetStr2Priv is the map for privilege set string to privilege type.
var SetStr2Priv = privilegeReverseMap(false)

// Priv2UserCol is the privilege to mysql.user table column name.
var Priv2UserCol = privilegeStringMap(2)

// Col2PrivType is the privilege tables column name to privilege type.
var Col2PrivType = privilegeReverseMap(true)

// PrivilegeType privilege
type PrivilegeType uint64

// NewPrivFromColumn constructs priv from a column name. False means invalid priv column name.
func NewPrivFromColumn(col string) (PrivilegeType, bool) {
	p, o := Col2PrivType[col]
	return p, o
}

// NewPrivFromSetEnum constructs priv from a set enum. False means invalid priv enum.
func NewPrivFromSetEnum(e string) (PrivilegeType, bool) {
	p, o := SetStr2Priv[e]
	return p, o
}

// String returns the corresponding identifier in SQLs.
func (p PrivilegeType) String() string {
	if s, ok := Priv2Str[p]; ok {
		return s
	}
	return ""
}

// ColumnString returns the corresponding name of columns in mysql.user/mysql.db.
func (p PrivilegeType) ColumnString() string {
	if s, ok := Priv2UserCol[p]; ok {
		return s
	}
	return ""
}

// SetString returns the corresponding set enum string in Table_priv/Column_priv of mysql.tables_priv/mysql.columns_priv.
func (p PrivilegeType) SetString() string {
	if s, ok := Priv2SetStr[p]; ok {
		return s
	}
	return ""
}

const (
	// UsagePriv is a synonym for “no privileges”
	UsagePriv PrivilegeType = 1 << iota
	// CreatePriv is the privilege to create schema/table.
	CreatePriv
	// SelectPriv is the privilege to read from table.
	SelectPriv
	// InsertPriv is the privilege to insert data into table.
	InsertPriv
	// UpdatePriv is the privilege to update data in table.
	UpdatePriv
	// DeletePriv is the privilege to delete data from table.
	DeletePriv
	// ShowDBPriv is the privilege to run show databases statement.
	ShowDBPriv
	// SuperPriv enables many operations and server behaviors.
	SuperPriv
	// CreateUserPriv is the privilege to create user.
	CreateUserPriv
	// TriggerPriv is not checked yet.
	TriggerPriv
	// DropPriv is the privilege to drop schema/table.
	DropPriv
	// ProcessPriv pertains to display of information about the threads executing within the server.
	ProcessPriv
	// GrantPriv is the privilege to grant privilege to user.
	GrantPriv
	// ReferencesPriv is not checked yet.
	ReferencesPriv
	// AlterPriv is the privilege to run alter statement.
	AlterPriv
	// ExecutePriv is the privilege to run execute statement.
	ExecutePriv
	// IndexPriv is the privilege to create/drop index.
	IndexPriv
	// CreateViewPriv is the privilege to create view.
	CreateViewPriv
	// ShowViewPriv is the privilege to show create view.
	ShowViewPriv
	// CreateRolePriv the privilege to create a role.
	CreateRolePriv
	// DropRolePriv is the privilege to drop a role.
	DropRolePriv
	// CreateTMPTablePriv is the privilege to create a temporary table.
	CreateTMPTablePriv
	// LockTablesPriv is the privilege to lock tables.
	LockTablesPriv
	// CreateRoutinePriv is the privilege to create a stored routine.
	CreateRoutinePriv
	// AlterRoutinePriv is the privilege to alter a stored routine.
	AlterRoutinePriv
	// EventPriv is the privilege to event.
	EventPriv

	// ShutdownPriv the privilege to shutdown a server.
	ShutdownPriv
	// ReloadPriv is the privilege to enable the use of the FLUSH statement.
	ReloadPriv
	// FilePriv is the privilege to enable the use of LOAD DATA and SELECT ... INTO OUTFILE.
	FilePriv
	// ConfigPriv is the privilege to enable the use SET CONFIG statements.
	ConfigPriv

	// CreateTablespacePriv is the privilege to create tablespace.
	CreateTablespacePriv

	// ReplicationClientPriv is used in MySQL replication
	ReplicationClientPriv
	// ReplicationSlavePriv is used in MySQL replication
	ReplicationSlavePriv

	// AllPriv is the privilege for all actions.
	AllPriv
	/*
	 *  Please add the new priv before AllPriv to keep the values consistent across versions.
	 */

	// ExtendedPriv is used to successful parse privileges not included above.
	// these are dynamic privileges in MySQL 8.0 and other extended privileges like LOAD FROM S3 in Aurora.
	ExtendedPriv
)

// AllPrivMask is the mask for PrivilegeType with all bits set to 1.
// If it's passed to RequestVerification, it means any privilege would be OK.
const AllPrivMask = AllPriv - 1

// Privileges is the list of all privileges.
type Privileges []PrivilegeType

// Has checks whether PrivilegeType has the privilege.
func (privs Privileges) Has(p PrivilegeType) bool {
	for _, cp := range privs {
		if cp == p {
			return true
		}
	}
	return false
}

// AllGlobalPrivs is all the privileges in global scope.
var AllGlobalPrivs = privilegeScope(0)

// AllDBPrivs is all the privileges in database scope.
var AllDBPrivs = privilegeScope(1)

// AllTablePrivs is all the privileges in table scope.
var AllTablePrivs = privilegeScope(2)

// AllColumnPrivs is all the privileges in column scope.
var AllColumnPrivs = privilegeScope(3)

// StaticGlobalOnlyPrivs is all the privileges only in global scope and different from dynamic privileges.
var StaticGlobalOnlyPrivs = privilegeScope(4)
