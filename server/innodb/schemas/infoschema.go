// Copyright 2015 PingCAP, Inc.
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

package schemas

import (
	"xmysql-server/server/innodb/model"
	"xmysql-server/server/innodb/terror"
	"xmysql-server/server/mysql"
)

var (
	// ErrDatabaseDropExists returns for dropping a non-existent database.
	ErrDatabaseDropExists = terror.ClassSchema.New(codeDBDropExists, "Can't drop database '%s'; database doesn't exist")
	// ErrDatabaseNotExists returns for database not exists.
	ErrDatabaseNotExists = terror.ClassSchema.New(codeDatabaseNotExists, "Unknown database '%s'")
	// ErrTableNotExists returns for table not exists.
	ErrTableNotExists = terror.ClassSchema.New(codeTableNotExists, "Table '%s.%s' doesn't exist")
	// ErrColumnNotExists returns for column not exists.
	ErrColumnNotExists = terror.ClassSchema.New(codeColumnNotExists, "Unknown column '%s' in '%s'")
	// ErrForeignKeyNotMatch returns for foreign key not match.
	ErrForeignKeyNotMatch = terror.ClassSchema.New(codeWrongFkDef, "Incorrect foreign key definition for '%s': Key reference and table reference don't match")
	// ErrCannotAddForeign returns for foreign key exists.
	ErrCannotAddForeign = terror.ClassSchema.New(codeCannotAddForeign, "Cannot add foreign key constraint")
	// ErrForeignKeyNotExists returns for foreign key not exists.
	ErrForeignKeyNotExists = terror.ClassSchema.New(codeForeignKeyNotExists, "Can't DROP '%s'; check that column/key exists")
	// ErrDatabaseExists returns for database already exists.
	ErrDatabaseExists = terror.ClassSchema.New(codeDatabaseExists, "Can't create database '%s'; database exists")
	// ErrTableExists returns for table already exists.
	ErrTableExists = terror.ClassSchema.New(codeTableExists, "Table '%s' already exists")
	// ErrTableDropExists returns for dropping a non-existent table.
	ErrTableDropExists = terror.ClassSchema.New(codeBadTable, "Unknown table '%s'")
	// ErrColumnExists returns for column already exists.
	ErrColumnExists = terror.ClassSchema.New(codeColumnExists, "Duplicate column name '%s'")
	// ErrIndexExists returns for index already exists.
	ErrIndexExists = terror.ClassSchema.New(codeIndexExists, "Duplicate Index")
	// ErrMultiplePriKey returns for multiple primary keys.
	ErrMultiplePriKey = terror.ClassSchema.New(codeMultiplePriKey, "Multiple primary key defined")
	// ErrTooManyKeyParts returns for too many key parts.
	ErrTooManyKeyParts = terror.ClassSchema.New(codeTooManyKeyParts, "Too many key parts specified; max %d parts allowed")
)

// InfoSchema is the interface used to retrieve the schema information.
// It works as a in memory cache and doesn't handle any schema change.
// InfoSchema is read-only, and the returned value is a copy.
// TODO: add more methods to retrieve tables and columns.

// 启动时加载
type InfoSchema interface {
	SchemaByName(schema model.CIStr) (*model.DBInfo, bool)

	SchemaExists(schema model.CIStr) bool

	TableByName(schema, table model.CIStr) (Table, error)

	TableExists(schema, table model.CIStr) bool

	AllSchemaNames() []string

	AllSchemas() []*model.DBInfo

	Clone() (result []*model.DBInfo)

	SchemaTables(schema model.CIStr) []Table

	//获取数据库对象
	GetSchemaByName(schemaName string) (Database, error)

	//判断数据库是否存在
	GetSchemaExist(schemaName string) bool

	GetTableByName(schema string, tableName string) (Table, error)

	GetTableExist(schemaName string, tableName string) bool

	SchemaByID(id int64) (*model.DBInfo, bool)

	TableByID(id int64) (Table, bool)

	AllocByID(id int64) (Allocator, bool)

	SchemaMetaVersion() int64
}

// Information Schema Name.
const (
	Name = "INFORMATION_SCHEMA"
)

type schemaTables struct {
	dbInfo *model.DBInfo
	tables map[string]Table
}

//
//type infoSchema struct {
//	schemaMap map[string]*schemaTables
//
//	// We should check version when change schema.
//	schemaMetaVersion int64
//}
//
//func NewInfoSchema(schemaName string, tbList []*model.TableInfo) InfoSchema {
//	result := &infoSchema{}
//	result.schemaMap = make(map[string]*schemaTables)
//	dbInfo := &model.DBInfo{Name: model.NewCIStr(schemaName), Tables: tbList}
//	tableNames := &schemaTables{
//		dbInfo: dbInfo,
//		tables: make(map[string]schemas.Table),
//	}
//	result.schemaMap[schemaName] = tableNames
//	for _, tb := range tbList {
//		tbl := schemas.MockTableFromMeta(tb)
//		tableNames.tables[tb.Name.L] = tbl
//	}
//	return result
//}
//
//var _ InfoSchema = (*infoSchema)(nil)
//
//func (is *infoSchema) SchemaByName(schema model.CIStr) (val *model.DBInfo, ok bool) {
//	tableNames, ok := is.schemaMap[schema.L]
//	if !ok {
//		return
//	}
//	return tableNames.dbInfo, true
//}
//
//func (is *infoSchema) SchemaMetaVersion() int64 {
//	return is.schemaMetaVersion
//}
//
//func (is *infoSchema) SchemaExists(schema model.CIStr) bool {
//	_, ok := is.schemaMap[schema.L]
//	return ok
//}
//
//func (is *infoSchema) TableByName(schema, table model.CIStr) (t schemas.Table, err error) {
//	if tbNames, ok := is.schemaMap[schema.L]; ok {
//		if t, ok = tbNames.tables[table.L]; ok {
//			return
//		}
//	}
//	return nil, ErrTableNotExists.GenByArgs(schema, table)
//}
//
//func (is *infoSchema) TableExists(schema, table model.CIStr) bool {
//	if tbNames, ok := is.schemaMap[schema.L]; ok {
//		if _, ok = tbNames.tables[table.L]; ok {
//			return true
//		}
//	}
//	return false
//}
//
//func (is *infoSchema) AllSchemaNames() (names []string) {
//	for _, v := range is.schemaMap {
//		names = append(names, v.dbInfo.Name.O)
//	}
//	return
//}
//
//func (is *infoSchema) AllSchemas() (schemas []*model.DBInfo) {
//	for _, v := range is.schemaMap {
//		schemas = append(schemas, v.dbInfo)
//	}
//	return
//}
//
//func (is *infoSchema) SchemaTables(schema model.CIStr) (tables []schemas.Table) {
//	schemaTables, ok := is.schemaMap[schema.L]
//	if !ok {
//		return
//	}
//	for _, tbl := range schemaTables.tables {
//		tables = append(tables, tbl)
//	}
//	return
//}
//
//func (is *infoSchema) Clone() (result []*model.DBInfo) {
//	for _, v := range is.schemaMap {
//		result = append(result, v.dbInfo.Clone())
//	}
//	return
//}

// Schema error codes.
const (
	codeDBDropExists      terror.ErrCode = 1008
	codeDatabaseNotExists                = 1049
	codeTableNotExists                   = 1146
	codeColumnNotExists                  = 1054

	codeCannotAddForeign    = 1215
	codeForeignKeyNotExists = 1091
	codeWrongFkDef          = 1239

	codeDatabaseExists = 1007
	codeTableExists    = 1050
	codeBadTable       = 1051
	codeColumnExists   = 1060
	codeIndexExists    = 1831
	codeMultiplePriKey = 1068

	codeTooManyKeyParts = 1070
)

func init() {
	schemaMySQLErrCodes := map[terror.ErrCode]uint16{
		codeDBDropExists:        mysql.ErrDBDropExists,
		codeDatabaseNotExists:   mysql.ErrBadDB,
		codeTableNotExists:      mysql.ErrNoSuchTable,
		codeColumnNotExists:     mysql.ErrBadField,
		codeCannotAddForeign:    mysql.ErrCannotAddForeign,
		codeWrongFkDef:          mysql.ErrWrongFkDef,
		codeForeignKeyNotExists: mysql.ErrCantDropFieldOrKey,
		codeDatabaseExists:      mysql.ErrDBCreateExists,
		codeTableExists:         mysql.ErrTableExists,
		codeBadTable:            mysql.ErrBadTable,
		codeColumnExists:        mysql.ErrDupFieldName,
		codeIndexExists:         mysql.ErrDupIndex,
		codeMultiplePriKey:      mysql.ErrMultiplePriKey,
	}
	terror.ErrClassToMySQLCodes[terror.ClassSchema] = schemaMySQLErrCodes
}
