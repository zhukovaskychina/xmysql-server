package sqlparser

import (
	"fmt"
	"strings"
)

type AccountName struct {
	User string
	Host string
}

type AccountStatement struct {
	Kind        string
	Accounts    []AccountName
	RoleSources []AccountName
	Privileges  []string
	// PrivilegeSpec preserves the lexical privilege list for GRANT/REVOKE,
	// including column lists. Privileges remains populated for callers that
	// only need individual names.
	PrivilegeSpec string
	Scope         string
	Options       map[string]string
	Role          bool
	Password      string
	Raw           string
	RenamePairs   []AccountRenamePair
}

type AccountRenamePair struct {
	Source      AccountName
	Destination AccountName
}

func (node *AccountStatement) iStatement() {}

func (node *AccountStatement) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	buf.Myprintf("%s", node.Kind)
	for _, account := range node.Accounts {
		buf.Myprintf(" '%s'@'%s'", account.User, account.Host)
	}
}

func (node *AccountStatement) walkSubtree(visit Visit) error { return nil }

func tryParseAccountForExecution(query string) (*AccountStatement, bool, error) {
	tokens, err := scanShowExecutionTokens(query)
	if err != nil || len(tokens) == 0 {
		return nil, false, err
	}
	first := strings.ToLower(tokens[0].text)
	kind := first
	index := 1
	if first == "create" || first == "alter" || first == "drop" || first == "rename" {
		if index >= len(tokens) || (!strings.EqualFold(tokens[index].text, "user") && !strings.EqualFold(tokens[index].text, "role")) {
			return nil, false, nil
		}
		kind += " " + strings.ToLower(tokens[index].text)
		index++
	} else if first == "grant" || first == "revoke" {
		// account targets are located after TO/FROM below
	} else if first == "show" && index+1 < len(tokens) && strings.EqualFold(tokens[index].text, "create") && strings.EqualFold(tokens[index+1].text, "user") {
		kind = "show create user"
		index += 2
	} else if first == "set" && index < len(tokens) && strings.EqualFold(tokens[index].text, "password") {
		kind = "set password"
		index++
	} else if first == "set" && index+1 < len(tokens) && strings.EqualFold(tokens[index].text, "default") && strings.EqualFold(tokens[index+1].text, "role") {
		kind = "set default role"
		index += 2
	} else {
		return nil, false, nil
	}
	statement := &AccountStatement{Kind: kind, Raw: query, Options: make(map[string]string), Role: strings.HasSuffix(kind, " role")}
	if kind == "rename user" {
		for index < len(tokens) {
			source, next, ok := parseAccountNameTokens(tokens, index)
			if !ok {
				return nil, true, fmt.Errorf("%w: RENAME USER source missing", ErrInvalidStatement)
			}
			index = next
			if index >= len(tokens) || !strings.EqualFold(tokens[index].text, "to") {
				return nil, true, fmt.Errorf("%w: RENAME USER TO expected", ErrInvalidStatement)
			}
			index++
			destination, after, ok := parseAccountNameTokens(tokens, index)
			if !ok {
				return nil, true, fmt.Errorf("%w: RENAME USER destination missing", ErrInvalidStatement)
			}
			statement.RenamePairs = append(statement.RenamePairs, AccountRenamePair{Source: source, Destination: destination})
			index = after
			if index == len(tokens) {
				break
			}
			if tokens[index].text != "," {
				return nil, true, fmt.Errorf("%w: unexpected RENAME USER token", ErrInvalidStatement)
			}
			index++
		}
		if len(statement.RenamePairs) == 0 {
			return nil, true, fmt.Errorf("%w: RENAME USER target missing", ErrInvalidStatement)
		}
		return statement, true, nil
	}
	if kind == "set default role" {
		toIndex := -1
		for cursor := index; cursor < len(tokens); cursor++ {
			if strings.EqualFold(tokens[cursor].text, "to") {
				toIndex = cursor
				break
			}
		}
		if toIndex < 0 {
			return nil, true, fmt.Errorf("%w: SET DEFAULT ROLE target missing", ErrInvalidStatement)
		}
		for cursor := toIndex + 1; cursor < len(tokens); {
			if tokens[cursor].text == "," {
				cursor++
				continue
			}
			account, next, ok := parseAccountNameTokens(tokens, cursor)
			if !ok {
				return nil, true, fmt.Errorf("%w: SET DEFAULT ROLE account target invalid", ErrInvalidStatement)
			}
			statement.Accounts = append(statement.Accounts, account)
			cursor = next
		}
		if len(statement.Accounts) == 0 {
			return nil, true, fmt.Errorf("%w: SET DEFAULT ROLE account target missing", ErrInvalidStatement)
		}
		return statement, true, nil
	}
	if first == "grant" || first == "revoke" {
		return parseGrantAccountForExecution(tokens, statement)
	}
	for index < len(tokens) {
		text := strings.ToLower(tokens[index].text)
		if (first == "grant" || first == "revoke") && text == "on" {
			index++
			if index < len(tokens) && (strings.EqualFold(tokens[index].text, "procedure") || strings.EqualFold(tokens[index].text, "function")) {
				index++
			}
			if index < len(tokens) {
				statement.Scope = tokens[index].text
				index++
			}
			continue
		}
		if text == "to" || text == "from" || text == "for" || text == "user" || text == "role" {
			index++
			continue
		}
		if tokens[index].text == "@" && index > 0 {
			index++
			continue
		}
		if account, next, ok := parseAccountNameTokens(tokens, index); ok {
			statement.Accounts = append(statement.Accounts, account)
			index = next
			continue
		}
		if text == "identified" || text == "with" || text == "as" || text == "on" || text == "require" || text == "password" || text == "account" || text == "default" {
			statement.Scope = text
			if text == "identified" && index+2 < len(tokens) && strings.EqualFold(tokens[index+1].text, "with") {
				statement.Options["identified_with"] = tokens[index+2].text
				if index+4 < len(tokens) && strings.EqualFold(tokens[index+3].text, "by") {
					statement.Options["identified_by"] = tokens[index+4].text
				}
			} else if text == "identified" && index+1 < len(tokens) && strings.EqualFold(tokens[index+1].text, "by") && index+2 < len(tokens) {
				statement.Options["identified_by"] = tokens[index+2].text
			}
			if index+1 < len(tokens) && tokens[index+1].text != "," {
				statement.Options[text] = tokens[index+1].text
			}
			index++
			continue
		}
		if first == "set" && strings.EqualFold(tokens[index].text, "=") && index+1 < len(tokens) {
			statement.Password = tokens[index+1].text
			index += 2
			continue
		}
		if first == "grant" || first == "revoke" {
			if tokens[index].text != "," && text != "to" && text != "from" {
				statement.Privileges = append(statement.Privileges, tokens[index].text)
			}
		}
		index++
	}
	if kind == "set password" && len(statement.Accounts) > 1 {
		return nil, true, fmt.Errorf("%w: multiple SET PASSWORD accounts", ErrInvalidStatement)
	}
	populateAccountSecurityOptions(tokens, statement)
	return statement, true, nil
}

func parseGrantAccountForExecution(tokens []showExecutionToken, statement *AccountStatement) (*AccountStatement, bool, error) {
	if statement == nil || len(tokens) < 2 {
		return statement, true, fmt.Errorf("%w: GRANT/REVOKE statement is incomplete", ErrInvalidStatement)
	}
	targetKeyword := -1
	onKeyword := -1
	for index := 1; index < len(tokens); index++ {
		word := strings.ToLower(tokens[index].text)
		if onKeyword < 0 && word == "on" {
			onKeyword = index
		}
		if word == "to" || word == "from" {
			targetKeyword = index
			break
		}
	}
	if targetKeyword < 0 {
		return statement, true, fmt.Errorf("%w: GRANT/REVOKE target is missing", ErrInvalidStatement)
	}
	privilegeEnd := targetKeyword
	if onKeyword >= 0 && onKeyword < targetKeyword {
		privilegeEnd = onKeyword
	}
	statement.PrivilegeSpec = formatAccountTokenSequence(tokens[1:privilegeEnd])
	if statement.PrivilegeSpec != "" {
		statement.Privileges = []string{statement.PrivilegeSpec}
	}
	if onKeyword >= 0 && onKeyword+1 < targetKeyword {
		scopeStart := onKeyword + 1
		if scopeStart < targetKeyword && (strings.EqualFold(tokens[scopeStart].text, "procedure") || strings.EqualFold(tokens[scopeStart].text, "function")) {
			scopeStart++
		}
		if account, _, ok := parseAccountNameTokens(tokens, scopeStart); ok {
			statement.Scope = fmt.Sprintf("'%s'@'%s'", strings.ReplaceAll(account.User, "'", "''"), strings.ReplaceAll(account.Host, "'", "''"))
		} else {
			statement.Scope = formatAccountTokenSequence(tokens[scopeStart:targetKeyword])
		}
	} else {
		for index := 1; index < targetKeyword; {
			if tokens[index].text == "," {
				index++
				continue
			}
			if account, next, ok := parseAccountNameTokens(tokens, index); ok {
				statement.RoleSources = append(statement.RoleSources, account)
				index = next
				continue
			}
			index++
		}
	}
	for index := targetKeyword + 1; index < len(tokens); {
		if tokens[index].text == "," {
			index++
			continue
		}
		if account, next, ok := parseAccountNameTokens(tokens, index); ok {
			statement.Accounts = append(statement.Accounts, account)
			index = next
			continue
		}
		index++
	}
	return statement, true, nil
}

func formatAccountTokenSequence(tokens []showExecutionToken) string {
	var builder strings.Builder
	for _, token := range tokens {
		text := strings.TrimSpace(token.text)
		if text == "" {
			continue
		}
		if builder.Len() > 0 && text != "," && text != ")" && text != "." && text != "@" && !strings.HasSuffix(builder.String(), "(") && !strings.HasSuffix(builder.String(), ".") && !strings.HasSuffix(builder.String(), "@") {
			builder.WriteByte(' ')
		}
		builder.WriteString(text)
	}
	return strings.TrimSpace(builder.String())
}

func populateAccountSecurityOptions(tokens []showExecutionToken, statement *AccountStatement) {
	if statement == nil || statement.Options == nil {
		return
	}
	for index := 0; index < len(tokens); index++ {
		word := strings.ToLower(tokens[index].text)
		key := ""
		valueIndex := index + 1
		switch word {
		case "password":
			if index+2 < len(tokens) && strings.EqualFold(tokens[index+1].text, "history") {
				key = "password history"
				valueIndex = index + 2
			} else if index+3 < len(tokens) && strings.EqualFold(tokens[index+1].text, "reuse") && strings.EqualFold(tokens[index+2].text, "interval") {
				key = "password reuse interval"
				valueIndex = index + 3
			} else if index+2 < len(tokens) && strings.EqualFold(tokens[index+1].text, "require") && strings.EqualFold(tokens[index+2].text, "current") {
				key = "password require current"
				valueIndex = index + 3
			} else if index+2 < len(tokens) && strings.EqualFold(tokens[index+1].text, "expire") && strings.EqualFold(tokens[index+2].text, "interval") {
				key = "password expire interval"
				valueIndex = index + 3
			}
		case "failed_login_attempts", "password_lock_time", "max_queries_per_hour", "max_updates_per_hour", "max_connections_per_hour", "max_user_connections", "cipher", "issuer", "subject", "replace":
			key = word
		case "attribute", "comment":
			if index+1 < len(tokens) && tokens[index+1].text == "=" {
				valueIndex = index + 2
			}
			if valueIndex < len(tokens) && strings.EqualFold(tokens[valueIndex].text, "default") {
				key = word + " default"
			} else {
				key = word
			}
		}
		if key != "" {
			if valueIndex < len(tokens) {
				statement.Options[key] = tokens[valueIndex].text
			} else {
				statement.Options[key] = ""
			}
		}
	}
}

func parseAccountNameTokens(tokens []showExecutionToken, index int) (AccountName, int, bool) {
	if index+2 >= len(tokens) || tokens[index].text == "@" || tokens[index+1].text != "@" {
		return AccountName{}, index, false
	}
	return AccountName{User: tokens[index].text, Host: tokens[index+2].text}, index + 3, true
}

func IsSelfServicePasswordChange(statement Statement, account AccountName) bool {
	parsed, ok := statement.(*AccountStatement)
	if !ok || parsed.Kind != "set password" {
		return false
	}
	if len(parsed.Accounts) == 0 {
		return true
	}
	return len(parsed.Accounts) == 1 && strings.EqualFold(parsed.Accounts[0].User, account.User) && strings.EqualFold(parsed.Accounts[0].Host, account.Host)
}
