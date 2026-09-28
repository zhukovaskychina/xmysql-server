package replication

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ReplicationFilterConfig contains the replica-side filters supported by the
// native and logical appliers.  Filters are evaluated before the storage
// callback, while the source transaction/GTID is still recorded as consumed.
type ReplicationFilterConfig struct {
	ReplicateDoDB            []string               `json:"replicate_do_db,omitempty"`
	ReplicateIgnoreDB        []string               `json:"replicate_ignore_db,omitempty"`
	ReplicateDoTable         []string               `json:"replicate_do_table,omitempty"`
	ReplicateIgnoreTable     []string               `json:"replicate_ignore_table,omitempty"`
	ReplicateWildDoTable     []string               `json:"replicate_wild_do_table,omitempty"`
	ReplicateWildIgnoreTable []string               `json:"replicate_wild_ignore_table,omitempty"`
	ReplicateRewriteDB       []ReplicationDBRewrite `json:"replicate_rewrite_db,omitempty"`
}

// ReplicationDBRewrite is one ordered source-database to replica-database
// translation. MySQL applies the first matching rewrite before evaluating the
// other replication filters.
type ReplicationDBRewrite struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// ReplicationFilterStatus is the observable P_S representation of one
// configured filter rule.
type ReplicationFilterStatus struct {
	Name         string    `json:"name"`
	Rule         string    `json:"rule"`
	ConfiguredBy string    `json:"configured_by"`
	ActiveSince  time.Time `json:"active_since"`
	Counter      uint64    `json:"counter"`
}

type persistedReplicationFilters struct {
	Config       ReplicationFilterConfig `json:"config"`
	ConfiguredBy string                  `json:"configured_by"`
	ActiveSince  time.Time               `json:"active_since"`
	Counters     map[string]uint64       `json:"counters,omitempty"`
}

func normalizeReplicationFilterConfig(config ReplicationFilterConfig) ReplicationFilterConfig {
	return ReplicationFilterConfig{
		ReplicateDoDB:            normalizeFilterValues(config.ReplicateDoDB),
		ReplicateIgnoreDB:        normalizeFilterValues(config.ReplicateIgnoreDB),
		ReplicateDoTable:         normalizeFilterValues(config.ReplicateDoTable),
		ReplicateIgnoreTable:     normalizeFilterValues(config.ReplicateIgnoreTable),
		ReplicateWildDoTable:     normalizeFilterValues(config.ReplicateWildDoTable),
		ReplicateWildIgnoreTable: normalizeFilterValues(config.ReplicateWildIgnoreTable),
		ReplicateRewriteDB:       normalizeReplicationDBRewrites(config.ReplicateRewriteDB),
	}
}

func normalizeReplicationDBRewrites(values []ReplicationDBRewrite) []ReplicationDBRewrite {
	result := make([]ReplicationDBRewrite, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		from := normalizeFilterIdentifier(value.From)
		to := normalizeFilterIdentifier(value.To)
		if from == "" || to == "" {
			continue
		}
		key := strings.ToLower(from)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, ReplicationDBRewrite{From: from, To: to})
	}
	return result
}

func normalizeFilterValues(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		value := normalizeFilterIdentifier(raw)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, value)
	}
	return result
}

func normalizeFilterIdentifier(raw string) string {
	value := strings.TrimSpace(raw)
	value = strings.Trim(value, "`\"")
	value = strings.TrimSpace(value)
	return value
}

func (config ReplicationFilterConfig) AllowsRowChange(change RowChange) bool {
	return config.allowsRewrittenRowChange(config.RewriteRowChange(change))
}

func (config ReplicationFilterConfig) allowsRewrittenRowChange(change RowChange) bool {
	database, table := splitReplicationTable(change.Table)
	if len(config.ReplicateDoDB) > 0 && !filterListContains(config.ReplicateDoDB, database) {
		return false
	}
	if filterListContains(config.ReplicateIgnoreDB, database) {
		return false
	}
	if len(config.ReplicateDoTable) > 0 || len(config.ReplicateWildDoTable) > 0 {
		if !filterListContains(config.ReplicateDoTable, database+"."+table) &&
			!filterWildcardContains(config.ReplicateWildDoTable, database+"."+table) {
			return false
		}
	}
	if filterListContains(config.ReplicateIgnoreTable, database+"."+table) ||
		filterWildcardContains(config.ReplicateWildIgnoreTable, database+"."+table) {
		return false
	}
	return true
}

func (config ReplicationFilterConfig) AllowsStatement(statement Statement) bool {
	return config.allowsRewrittenStatement(config.RewriteStatement(statement))
}

func (config ReplicationFilterConfig) allowsRewrittenStatement(statement Statement) bool {
	database := normalizeFilterIdentifier(statement.Database)
	if len(config.ReplicateDoDB) > 0 && !filterListContains(config.ReplicateDoDB, database) {
		return false
	}
	if filterListContains(config.ReplicateIgnoreDB, database) {
		return false
	}
	// Statement events do not carry a TABLE_MAP identity. For the common,
	// unambiguous SQL forms, recover qualified table names lexically so table
	// filters have the same effect for statement-based replay. If the SQL does
	// not expose a reliable table reference (for example a dynamic statement or
	// a subquery-only source), retain it rather than guessing and dropping data.
	tables, known := replicationStatementTables(statement.SQL, database)
	if !known || len(tables) == 0 {
		return true
	}
	for _, table := range tables {
		if len(config.ReplicateDoTable) > 0 || len(config.ReplicateWildDoTable) > 0 {
			if !filterListContains(config.ReplicateDoTable, table) &&
				!filterWildcardContains(config.ReplicateWildDoTable, table) {
				return false
			}
		}
		if filterListContains(config.ReplicateIgnoreTable, table) ||
			filterWildcardContains(config.ReplicateWildIgnoreTable, table) {
			return false
		}
	}
	return true
}

type replicationSQLToken struct {
	text   string
	quoted bool
}

// replicationStatementTables extracts only table references introduced by the
// SQL keywords whose target is unambiguous in MySQL statement events. It is a
// lexer, not a SQL parser: comments and literals are skipped, qualified names
// are normalized, and parenthesized/subquery targets make the result unknown.
func replicationStatementTables(sql, defaultDatabase string) ([]string, bool) {
	tokens, ok := tokenizeReplicationSQL(sql)
	if !ok {
		return nil, false
	}
	tables := make([]string, 0)
	seen := make(map[string]struct{})
	for index := 0; index < len(tokens); index++ {
		keyword := strings.ToLower(tokens[index].text)
		if keyword != "from" && keyword != "join" && keyword != "update" && keyword != "into" {
			continue
		}
		candidate := index + 1
		for candidate < len(tokens) && isReplicationStatementModifier(tokens[candidate].text) {
			candidate++
		}
		if candidate >= len(tokens) || tokens[candidate].text == "(" {
			continue
		}
		name, next, parsed := parseReplicationSQLTableName(tokens, candidate, defaultDatabase)
		if !parsed {
			continue
		}
		if name == "" {
			return nil, false
		}
		if _, exists := seen[name]; !exists {
			seen[name] = struct{}{}
			tables = append(tables, name)
		}
		index = next - 1
	}
	sort.Strings(tables)
	return tables, true
}

func tokenizeReplicationSQL(sql string) ([]replicationSQLToken, bool) {
	tokens := make([]replicationSQLToken, 0)
	for index := 0; index < len(sql); {
		if isReplicationSQLSpace(sql[index]) {
			index++
			continue
		}
		if sql[index] == '#' || (sql[index] == '-' && index+2 < len(sql) && sql[index+1] == '-' && isReplicationSQLSpace(sql[index+2])) {
			index = skipReplicationSQLLineComment(sql, index)
			continue
		}
		if sql[index] == '/' && index+1 < len(sql) && sql[index+1] == '*' {
			end := strings.Index(sql[index+2:], "*/")
			if end < 0 {
				return nil, false
			}
			index += end + 4
			continue
		}
		if sql[index] == '\'' || sql[index] == '"' || sql[index] == '`' {
			quote := sql[index]
			end := index + 1
			for end < len(sql) {
				if sql[end] == quote {
					if end+1 < len(sql) && sql[end+1] == quote {
						end += 2
						continue
					}
					break
				}
				if sql[end] == '\\' && quote != '`' && end+1 < len(sql) {
					end += 2
					continue
				}
				end++
			}
			if end >= len(sql) || sql[end] != quote {
				return nil, false
			}
			raw := sql[index+1 : end]
			raw = strings.ReplaceAll(raw, string([]byte{quote, quote}), string([]byte{quote}))
			tokens = append(tokens, replicationSQLToken{text: raw, quoted: true})
			index = end + 1
			continue
		}
		if isReplicationSQLIdentifierStart(sql[index]) {
			start := index
			for index < len(sql) && isReplicationSQLIdentifierPart(sql[index]) {
				index++
			}
			tokens = append(tokens, replicationSQLToken{text: sql[start:index]})
			continue
		}
		tokens = append(tokens, replicationSQLToken{text: string(sql[index])})
		index++
	}
	return tokens, true
}

func skipReplicationSQLLineComment(sql string, start int) int {
	for index := start; index < len(sql); index++ {
		if sql[index] == '\n' || sql[index] == '\r' {
			return index + 1
		}
	}
	return len(sql)
}

func isReplicationStatementModifier(value string) bool {
	switch strings.ToLower(value) {
	case "low_priority", "ignore", "straight_join":
		return true
	default:
		return false
	}
}

func parseReplicationSQLTableName(tokens []replicationSQLToken, start int, defaultDatabase string) (string, int, bool) {
	if start >= len(tokens) || tokens[start].text == "(" || tokens[start].text == "," {
		return "", start, false
	}
	first := normalizeFilterIdentifier(tokens[start].text)
	if first == "" || strings.EqualFold(first, "select") || strings.EqualFold(first, "values") {
		return "", start, false
	}
	next := start + 1
	database, table := defaultDatabase, first
	if next+1 < len(tokens) && tokens[next].text == "." {
		second := normalizeFilterIdentifier(tokens[next+1].text)
		if second == "" {
			return "", start, false
		}
		database, table = first, second
		next += 2
	}
	if database == "" {
		return "", next, false
	}
	return strings.ToLower(normalizeFilterIdentifier(database)) + "." + strings.ToLower(normalizeFilterIdentifier(table)), next, true
}

// RewriteRowChange applies the first matching database rewrite while retaining
// the rest of the row image unchanged. The rewritten table is then the target
// used by table filters and the storage callback.
func (config ReplicationFilterConfig) RewriteRowChange(change RowChange) RowChange {
	database, table := splitReplicationTable(change.Table)
	if database == "" || table == "" {
		return change
	}
	if rewritten, ok := config.rewriteDatabase(database); ok {
		change.Table = rewritten + "." + table
	}
	return change
}

// RewriteStatement translates the statement's current/default database. SQL
// text is intentionally preserved; the engine receives the rewritten default
// database and continues to apply the statement through its normal parser.
func (config ReplicationFilterConfig) RewriteStatement(statement Statement) Statement {
	if rewritten, ok := config.rewriteDatabase(statement.Database); ok {
		statement.Database = rewritten
	}
	statement.SQL = rewriteReplicationStatementSQL(statement.SQL, config.ReplicateRewriteDB)
	return statement
}

// rewriteReplicationStatementSQL rewrites database qualifiers in statement
// text while leaving literals, comments, and unqualified identifiers alone.
// The binlog decoder supplies SQL text rather than a parsed AST, so this
// deliberately performs only the safe lexical operation required by
// REPLICATE_REWRITE_DB: replacing an identifier immediately followed by a dot.
func rewriteReplicationStatementSQL(sql string, rewrites []ReplicationDBRewrite) string {
	if strings.TrimSpace(sql) == "" || len(rewrites) == 0 {
		return sql
	}
	var out strings.Builder
	out.Grow(len(sql))
	for index := 0; index < len(sql); {
		switch sql[index] {
		case '\'':
			index = copyReplicationSQLQuoted(&out, sql, index, sql[index])
			continue
		case '`', '"':
			if next, ok := rewriteReplicationSQLQuotedIdentifier(&out, sql, index, sql[index], rewrites); ok {
				index = next
				continue
			}
			index = copyReplicationSQLQuoted(&out, sql, index, sql[index])
			continue
		case '#':
			index = copyReplicationSQLLineComment(&out, sql, index)
			continue
		case '-':
			if index+2 < len(sql) && sql[index+1] == '-' && isReplicationSQLSpace(sql[index+2]) {
				index = copyReplicationSQLLineComment(&out, sql, index)
				continue
			}
		case '/':
			if index+1 < len(sql) && sql[index+1] == '*' {
				index = copyReplicationSQLBlockComment(&out, sql, index)
				continue
			}
		}

		if !isReplicationSQLIdentifierStart(sql[index]) {
			out.WriteByte(sql[index])
			index++
			continue
		}
		start := index
		for index < len(sql) && isReplicationSQLIdentifierPart(sql[index]) {
			index++
		}
		token := sql[start:index]
		lookahead := index
		for lookahead < len(sql) && isReplicationSQLSpace(sql[lookahead]) {
			lookahead++
		}
		if lookahead < len(sql) && sql[lookahead] == '.' {
			if rewritten, ok := rewriteReplicationDatabase(token, rewrites); ok {
				out.WriteString(renderReplicationSQLIdentifier(rewritten, false))
				continue
			}
		}
		out.WriteString(token)
	}
	return out.String()
}

func rewriteReplicationSQLQuotedIdentifier(out *strings.Builder, sql string, start int, quote byte, rewrites []ReplicationDBRewrite) (int, bool) {
	end := start + 1
	for end < len(sql) {
		if sql[end] != quote {
			end++
			continue
		}
		if end+1 < len(sql) && sql[end+1] == quote {
			end += 2
			continue
		}
		break
	}
	if end >= len(sql) || sql[end] != quote {
		return start, false
	}
	lookahead := end + 1
	for lookahead < len(sql) && isReplicationSQLSpace(sql[lookahead]) {
		lookahead++
	}
	if lookahead >= len(sql) || sql[lookahead] != '.' {
		return start, false
	}
	raw := strings.ReplaceAll(sql[start+1:end], string([]byte{quote, quote}), string([]byte{quote}))
	rewritten, ok := rewriteReplicationDatabase(raw, rewrites)
	if !ok {
		return start, false
	}
	out.WriteString(renderReplicationSQLIdentifier(rewritten, true))
	return end + 1, true
}

func rewriteReplicationDatabase(database string, rewrites []ReplicationDBRewrite) (string, bool) {
	for _, rewrite := range rewrites {
		if strings.EqualFold(normalizeFilterIdentifier(rewrite.From), normalizeFilterIdentifier(database)) {
			return normalizeFilterIdentifier(rewrite.To), true
		}
	}
	return "", false
}

func renderReplicationSQLIdentifier(identifier string, quoted bool) string {
	identifier = normalizeFilterIdentifier(identifier)
	if quoted || !isReplicationSQLBareIdentifier(identifier) {
		return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
	}
	return identifier
}

func isReplicationSQLBareIdentifier(value string) bool {
	if value == "" || !isReplicationSQLIdentifierStart(value[0]) {
		return false
	}
	for index := 1; index < len(value); index++ {
		if !isReplicationSQLIdentifierPart(value[index]) {
			return false
		}
	}
	return true
}

func isReplicationSQLIdentifierStart(value byte) bool {
	return value == '_' || value == '$' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}

func isReplicationSQLIdentifierPart(value byte) bool {
	return isReplicationSQLIdentifierStart(value) || value >= '0' && value <= '9'
}

func isReplicationSQLSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n'
}

func copyReplicationSQLQuoted(out *strings.Builder, sql string, start int, quote byte) int {
	if start >= len(sql) {
		return start
	}
	out.WriteByte(sql[start])
	index := start + 1
	for index < len(sql) {
		value := sql[index]
		out.WriteByte(value)
		index++
		if value == '\\' && index < len(sql) {
			out.WriteByte(sql[index])
			index++
			continue
		}
		if value == quote {
			if index < len(sql) && sql[index] == quote {
				out.WriteByte(sql[index])
				index++
				continue
			}
			break
		}
	}
	return index
}

func copyReplicationSQLLineComment(out *strings.Builder, sql string, start int) int {
	index := start
	for index < len(sql) {
		out.WriteByte(sql[index])
		if sql[index] == '\n' {
			return index + 1
		}
		index++
	}
	return index
}

func copyReplicationSQLBlockComment(out *strings.Builder, sql string, start int) int {
	index := start
	for index < len(sql) {
		out.WriteByte(sql[index])
		if sql[index] == '*' && index+1 < len(sql) && sql[index+1] == '/' {
			out.WriteByte(sql[index+1])
			return index + 2
		}
		index++
	}
	return index
}

func (config ReplicationFilterConfig) rewriteDatabase(database string) (string, bool) {
	database = normalizeFilterIdentifier(database)
	for _, rewrite := range config.ReplicateRewriteDB {
		if strings.EqualFold(normalizeFilterIdentifier(rewrite.From), database) {
			return normalizeFilterIdentifier(rewrite.To), true
		}
	}
	return database, false
}

// filterAppliedImagesLocked rewrites source identities before applying the
// remaining filters. The caller holds replica.mu because rejected-rule
// counters are part of the same durable replica state.
func (replica *Replica) filterAppliedImagesLocked(changes []RowChange, statements []Statement) ([]RowChange, []Statement) {
	filteredChanges := make([]RowChange, 0, len(changes))
	for _, change := range changes {
		rewritten := replica.filterConfig.RewriteRowChange(change)
		if replica.filterConfig.allowsRewrittenRowChange(rewritten) {
			filteredChanges = append(filteredChanges, rewritten)
		} else {
			replica.incrementReplicationFilterCounterLocked(rewritten)
		}
	}
	filteredStatements := make([]Statement, 0, len(statements))
	for _, statement := range statements {
		rewritten := replica.filterConfig.RewriteStatement(statement)
		if replica.filterConfig.allowsRewrittenStatement(rewritten) {
			filteredStatements = append(filteredStatements, rewritten)
		} else {
			replica.incrementReplicationFilterCounterLocked(rewritten)
		}
	}
	return filteredChanges, filteredStatements
}

func splitReplicationTable(raw string) (string, string) {
	value := normalizeFilterIdentifier(raw)
	parts := strings.SplitN(value, ".", 2)
	if len(parts) != 2 {
		return "", strings.ToLower(value)
	}
	return strings.ToLower(normalizeFilterIdentifier(parts[0])), strings.ToLower(normalizeFilterIdentifier(parts[1]))
}

func filterListContains(values []string, wanted string) bool {
	wanted = strings.ToLower(normalizeFilterIdentifier(wanted))
	for _, value := range values {
		if strings.EqualFold(normalizeFilterIdentifier(value), wanted) {
			return true
		}
	}
	return false
}

func filterWildcardContains(values []string, wanted string) bool {
	for _, pattern := range values {
		if mysqlReplicationFilterLike(pattern, wanted) {
			return true
		}
	}
	return false
}

func mysqlReplicationFilterLike(pattern, value string) bool {
	pattern = strings.ToLower(normalizeFilterIdentifier(pattern))
	value = strings.ToLower(normalizeFilterIdentifier(value))
	var match func(int, int) bool
	match = func(patternIndex, valueIndex int) bool {
		if patternIndex == len(pattern) {
			return valueIndex == len(value)
		}
		switch pattern[patternIndex] {
		case '%':
			for next := valueIndex; next <= len(value); next++ {
				if match(patternIndex+1, next) {
					return true
				}
			}
			return false
		case '_':
			return valueIndex < len(value) && match(patternIndex+1, valueIndex+1)
		case '\\':
			if patternIndex+1 < len(pattern) {
				return valueIndex < len(value) && pattern[patternIndex+1] == value[valueIndex] && match(patternIndex+2, valueIndex+1)
			}
		}
		return valueIndex < len(value) && pattern[patternIndex] == value[valueIndex] && match(patternIndex+1, valueIndex+1)
	}
	return match(0, 0)
}

func (replica *Replica) SetReplicationFilters(config ReplicationFilterConfig, configuredBy string) error {
	if replica == nil {
		return fmt.Errorf("replica is nil")
	}
	config = normalizeReplicationFilterConfig(config)
	configuredBy = strings.TrimSpace(configuredBy)
	if configuredBy == "" {
		configuredBy = "CHANGE_REPLICATION_FILTER"
	}
	replica.mu.Lock()
	defer replica.mu.Unlock()
	replica.filterConfig = config
	replica.filterConfiguredBy = configuredBy
	replica.filterActiveSince = time.Now().UTC()
	replica.filterCounters = make(map[string]uint64)
	return replica.persistFiltersLocked()
}

func (replica *Replica) ReplicationFilterStatus() []ReplicationFilterStatus {
	if replica == nil {
		return nil
	}
	replica.mu.Lock()
	defer replica.mu.Unlock()
	return replica.replicationFilterStatusLocked()
}

func (replica *Replica) replicationFilterStatusLocked() []ReplicationFilterStatus {
	if replica == nil {
		return nil
	}
	type filterRule struct{ name, rule string }
	rules := make([]filterRule, 0)
	appendRules := func(name string, values []string) {
		for _, value := range values {
			rules = append(rules, filterRule{name: name, rule: value})
		}
	}
	appendRules("REPLICATE_DO_DB", replica.filterConfig.ReplicateDoDB)
	appendRules("REPLICATE_IGNORE_DB", replica.filterConfig.ReplicateIgnoreDB)
	appendRules("REPLICATE_DO_TABLE", replica.filterConfig.ReplicateDoTable)
	appendRules("REPLICATE_IGNORE_TABLE", replica.filterConfig.ReplicateIgnoreTable)
	appendRules("REPLICATE_WILD_DO_TABLE", replica.filterConfig.ReplicateWildDoTable)
	appendRules("REPLICATE_WILD_IGNORE_TABLE", replica.filterConfig.ReplicateWildIgnoreTable)
	for _, rewrite := range replica.filterConfig.ReplicateRewriteDB {
		rules = append(rules, filterRule{name: "REPLICATE_REWRITE_DB", rule: rewrite.From + " -> " + rewrite.To})
	}
	sort.Slice(rules, func(i, j int) bool {
		if rules[i].name != rules[j].name {
			return rules[i].name < rules[j].name
		}
		return rules[i].rule < rules[j].rule
	})
	result := make([]ReplicationFilterStatus, 0, len(rules))
	for _, rule := range rules {
		key := rule.name + "\x00" + rule.rule
		result = append(result, ReplicationFilterStatus{
			Name: rule.name, Rule: rule.rule, ConfiguredBy: replica.filterConfiguredBy,
			ActiveSince: replica.filterActiveSince, Counter: replica.filterCounters[key],
		})
	}
	return result
}

func (replica *Replica) persistFiltersLocked() error {
	if replica == nil || replica.filtersPath == "" {
		return nil
	}
	state := persistedReplicationFilters{
		Config: replica.filterConfig, ConfiguredBy: replica.filterConfiguredBy,
		ActiveSince: replica.filterActiveSince, Counters: replica.filterCounters,
	}
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(replica.filtersPath), 0755); err != nil {
		return err
	}
	return writeReplicationFileAtomic(replica.filtersPath, raw)
}
