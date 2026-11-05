package sqlparser

// ForeignKeysInCreateTable returns table-level and inline foreign keys from a
// CREATE TABLE statement. The definitions come from the structured parser, not
// from a regular expression over the clause text.
func ForeignKeysInCreateTable(sql string) []*ForeignKeyDefinition {
	stmt, ok := parseCreateTableWithForeignKeys(sql)
	if !ok || stmt == nil {
		return nil
	}
	ddl, ok := stmt.(*DDL)
	if !ok || ddl.TableSpec == nil {
		return nil
	}
	return ddl.TableSpec.ForeignKeys
}
