package engine

// NormalizeTableConstructorQuery prepares MySQL TABLE and VALUES query forms
// for the existing SELECT, set-operation, and CREATE ... AS code paths.
func NormalizeTableConstructorQuery(query string) string {
	query = rewriteCreateTableConstructorSource(query)
	query = rewriteParenthesizedTableConstructors(query)
	if rewritten, ok := rewriteTableConstructorsInSetQuery(query); ok {
		query = rewritten
	}
	if rewritten, ok := rewriteStandaloneValuesStatement(query); ok {
		query = rewritten
	}
	if rewritten, ok := rewriteStandaloneTableStatement(query); ok {
		query = rewritten
	}
	return query
}
