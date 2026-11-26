package engine

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
	"golang.org/x/text/cases"
	"golang.org/x/text/collate"
	"golang.org/x/text/language"
	"golang.org/x/text/unicode/norm"
)

const columnCollationContextPrefix = "__xmysql_column_collation:"

var swedishCollatorPool = sync.Pool{New: func() interface{} {
	return collate.New(language.Swedish, collate.IgnoreCase)
}}

func columnCollationContextKey(name string) string {
	return columnCollationContextPrefix + strings.ToLower(strings.Trim(strings.TrimSpace(name), "`"))
}

func collationFromValueContext(values map[string]interface{}, columnName string) string {
	columnName = strings.Trim(strings.TrimSpace(columnName), "` ")
	for candidate := columnName; candidate != ""; {
		if collation, ok := values[columnCollationContextKey(candidate)].(string); ok && strings.TrimSpace(collation) != "" {
			return collation
		}
		dot := strings.Index(candidate, ".")
		if dot < 0 || dot+1 >= len(candidate) {
			break
		}
		candidate = candidate[dot+1:]
	}
	return ""
}

func addColumnCollationContext(values map[string]interface{}, tableMeta *metadata.TableMeta) {
	if values == nil || tableMeta == nil {
		return
	}
	for _, column := range tableMeta.Columns {
		if column == nil {
			continue
		}
		collation := strings.TrimSpace(column.Collation)
		if collation == "" {
			if !isTextualCollationType(column.Type) {
				continue
			}
			collation = strings.TrimSpace(tableMeta.Collation)
		}
		if collation != "" {
			values[columnCollationContextKey(column.Name)] = collation
		}
	}
}

func isTextualCollationType(dataType metadata.DataType) bool {
	switch dataType {
	case metadata.TypeChar, metadata.TypeVarchar, metadata.TypeTinyText, metadata.TypeText,
		metadata.TypeMediumText, metadata.TypeLongText, metadata.TypeEnum, metadata.TypeSet:
		return true
	default:
		return false
	}
}

func collationForExpression(expression sqlparser.Expr, values map[string]interface{}) string {
	switch typed := expression.(type) {
	case *sqlparser.CollateExpr:
		return strings.Trim(strings.TrimSpace(typed.Charset), "` ")
	case *sqlparser.ParenExpr:
		return collationForExpression(typed.Expr, values)
	case *sqlparser.ColName:
		if typed == nil {
			return ""
		}
		name := strings.Trim(typed.Name.String(), "` ")
		if !typed.Qualifier.IsEmpty() {
			qualified := strings.Trim(sqlparser.String(typed.Qualifier), "` ") + "." + name
			if collation := collationFromValueContext(values, qualified); collation != "" {
				return collation
			}
		}
		if collation := collationFromValueContext(values, name); collation != "" {
			return collation
		}
	}
	return ""
}

func comparisonCollation(left, right sqlparser.Expr, values map[string]interface{}) string {
	collation, _ := resolveComparisonCollation(left, right, values, "comparison")
	return collation
}

type collationOperand struct {
	name         string
	coercibility int
}

func resolveComparisonCollation(left, right sqlparser.Expr, values map[string]interface{}, operation string) (string, error) {
	leftOperand := collationOperandForExpression(left, values)
	rightOperand := collationOperandForExpression(right, values)
	if leftOperand.name == "" {
		return rightOperand.name, nil
	}
	if rightOperand.name == "" {
		return leftOperand.name, nil
	}
	if strings.EqualFold(leftOperand.name, rightOperand.name) {
		return leftOperand.name, nil
	}
	if leftOperand.coercibility < rightOperand.coercibility {
		return leftOperand.name, nil
	}
	if rightOperand.coercibility < leftOperand.coercibility {
		return rightOperand.name, nil
	}
	if leftOperand.coercibility == 0 {
		return "", common.NewErr(common.ErrCantAggregate2collations,
			leftOperand.name, "EXPLICIT", rightOperand.name, "EXPLICIT", operation)
	}
	leftInfo, leftKnown := common.CollationMetadataByName(leftOperand.name)
	rightInfo, rightKnown := common.CollationMetadataByName(rightOperand.name)
	if leftKnown && rightKnown {
		if strings.EqualFold(leftInfo.Charset, rightInfo.Charset) {
			leftBinary := strings.HasSuffix(strings.ToLower(leftInfo.Name), "_bin")
			rightBinary := strings.HasSuffix(strings.ToLower(rightInfo.Name), "_bin")
			if leftBinary != rightBinary {
				if leftBinary {
					return leftOperand.name, nil
				}
				return rightOperand.name, nil
			}
		} else if isUnicodeCharset(leftInfo.Charset) != isUnicodeCharset(rightInfo.Charset) {
			if isUnicodeCharset(leftInfo.Charset) {
				return leftOperand.name, nil
			}
			return rightOperand.name, nil
		}
	}
	return "", common.NewErr(common.ErrCantAggregate2collations,
		leftOperand.name, collationDerivationName(leftOperand.coercibility),
		rightOperand.name, collationDerivationName(rightOperand.coercibility), operation)
}

func collationOperandForExpression(expression sqlparser.Expr, values map[string]interface{}) collationOperand {
	switch typed := expression.(type) {
	case *sqlparser.CollateExpr:
		return collationOperand{name: strings.Trim(strings.TrimSpace(typed.Charset), "` "), coercibility: 0}
	case *sqlparser.ParenExpr:
		return collationOperandForExpression(typed.Expr, values)
	case *sqlparser.ColName:
		return collationOperand{name: collationForExpression(typed, values), coercibility: 2}
	default:
		return collationOperand{}
	}
}

func collationDerivationName(coercibility int) string {
	switch coercibility {
	case 0:
		return "EXPLICIT"
	case 2:
		return "IMPLICIT"
	default:
		return "COERCIBLE"
	}
}

func isUnicodeCharset(charset string) bool {
	switch strings.ToLower(strings.TrimSpace(charset)) {
	case "utf8", "utf8mb3", "utf8mb4", "ucs2", "utf16", "utf16le", "utf32":
		return true
	default:
		return false
	}
}

func explicitCollationForExpression(expression sqlparser.Expr) string {
	switch typed := expression.(type) {
	case *sqlparser.CollateExpr:
		return strings.Trim(strings.TrimSpace(typed.Charset), "` ")
	case *sqlparser.ParenExpr:
		return explicitCollationForExpression(typed.Expr)
	default:
		return ""
	}
}

// compareScalarValuesWithCollation applies PAD_ATTRIBUTE and the common _ci /
// _ai_ci distinctions carried by the declared collation name. The resulting
// text order is a normalized lexical fallback, not MySQL's complete collation
// weight table; locale-specific and legacy weight rules remain a known gap.
func compareScalarValuesWithCollation(left, right interface{}, collationName string) int {
	leftText, leftIsText := left.(string)
	rightText, rightIsText := right.(string)
	if leftIsText && rightIsText && strings.TrimSpace(collationName) != "" {
		if collation, found := common.CollationMetadataByName(collationName); found {
			if isSwedishCollation(collation.Name) {
				if strings.EqualFold(collation.PadAttribute, "PAD SPACE") {
					leftText = strings.TrimRight(leftText, " ")
					rightText = strings.TrimRight(rightText, " ")
				}
				collator := swedishCollatorPool.Get().(*collate.Collator)
				comparison := collator.CompareString(leftText, rightText)
				swedishCollatorPool.Put(collator)
				return comparison
			}
			return compareTextValues(normalizeTextForCollation(leftText, collationName), normalizeTextForCollation(rightText, collationName))
		}
	}
	return compareScalarValues(left, right)
}

func isSwedishCollation(name string) bool {
	return strings.Contains(strings.ToLower(strings.TrimSpace(name)), "_swedish_ci")
}

func isGerman2Collation(name string) bool {
	return strings.Contains(strings.ToLower(strings.TrimSpace(name)), "_german2_ci")
}

func normalizeTextForCollation(value, collationName string) string {
	collation, found := common.CollationMetadataByName(collationName)
	if !found {
		return value
	}
	if strings.EqualFold(collation.PadAttribute, "PAD SPACE") {
		value = strings.TrimRight(value, " ")
	}
	lowerName := strings.ToLower(collation.Name)
	if isGerman2Collation(lowerName) {
		value = normalizeGerman2Text(value, collation.Charset)
	}
	if strings.Contains(lowerName, "_ai_") || (strings.HasSuffix(lowerName, "_ci") && !strings.Contains(lowerName, "_as_") && !isSwedishCollation(lowerName)) {
		value = removeCollationDiacritics(value)
	}
	if !strings.EqualFold(collation.Charset, "binary") && strings.HasSuffix(lowerName, "_ci") {
		value = cases.Fold().String(value)
	}
	return value
}

func normalizeGerman2Text(value, charset string) string {
	replacements := []string{
		"Ä", "ae", "ä", "ae",
		"Ö", "oe", "ö", "oe",
		"Ü", "ue", "ü", "ue",
		"ß", "ss",
	}
	if !strings.EqualFold(charset, "latin1") {
		replacements = append(replacements, "Æ", "ae", "æ", "ae", "Œ", "oe", "œ", "oe")
	}
	return strings.NewReplacer(replacements...).Replace(value)
}

func sqlRegexpMatchWithCollation(pattern, value, collationName string) (bool, error) {
	if collation, found := common.CollationMetadataByName(collationName); found {
		lowerName := strings.ToLower(collation.Name)
		if !strings.EqualFold(collation.Charset, "binary") && strings.HasSuffix(lowerName, "_ci") {
			pattern = "(?i)" + pattern
		}
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return false, err
	}
	return compiled.MatchString(value), nil
}

func removeCollationDiacritics(value string) string {
	decomposed := norm.NFD.String(value)
	var normalized strings.Builder
	for _, char := range decomposed {
		if !unicode.Is(unicode.Mn, char) && !unicode.Is(unicode.Me, char) && !unicode.Is(unicode.Mc, char) {
			normalized.WriteRune(char)
		}
	}
	return normalized.String()
}

func aggregateGroupKeyWithCollations(values []interface{}, collations []string) string {
	normalized := append([]interface{}(nil), values...)
	for index := range normalized {
		if index >= len(collations) || collations[index] == "" {
			continue
		}
		text, ok := normalized[index].(string)
		if !ok {
			continue
		}
		if _, found := common.CollationMetadataByName(collations[index]); found {
			normalized[index] = normalizeTextForCollation(text, collations[index])
		}
	}
	return aggregateGroupKey(normalized)
}

func recordColumnCollation(record Record, columnName string) string {
	typed, ok := record.(*EngineExecutorRecord)
	if !ok || typed.GetTableMeta() == nil {
		return ""
	}
	columnName = strings.Trim(strings.TrimSpace(columnName), "` ")
	if ordinal, err := strconv.Atoi(columnName); err == nil && ordinal > 0 && ordinal <= len(typed.GetTableMeta().Columns) {
		column := typed.GetTableMeta().Columns[ordinal-1]
		if column != nil {
			if strings.TrimSpace(column.Collation) != "" {
				return column.Collation
			}
			return typed.GetTableMeta().Collation
		}
	}
	if dot := strings.LastIndex(columnName, "."); dot >= 0 {
		columnName = strings.Trim(columnName[dot+1:], "` ")
	}
	meta := typed.GetTableMeta()
	for _, column := range meta.Columns {
		if column == nil || !strings.EqualFold(column.Name, columnName) {
			continue
		}
		if strings.TrimSpace(column.Collation) != "" {
			return column.Collation
		}
		return meta.Collation
	}
	return ""
}
