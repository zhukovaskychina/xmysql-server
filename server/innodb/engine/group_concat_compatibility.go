package engine

import (
	"strings"
	"unicode/utf8"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

const defaultGroupConcatMaxBytes int64 = 1024

// truncateGroupConcatResult applies MySQL's default GROUP_CONCAT byte limit.
// For known UTF-8 results, it keeps the output on a character boundary, as the
// server does when the limit falls in the middle of a multibyte character.
func truncateGroupConcatResult(value string, maxBytes int64, collationName string) string {
	if maxBytes <= 0 {
		maxBytes = defaultGroupConcatMaxBytes
	}
	if int64(len(value)) <= maxBytes {
		return value
	}
	limit := int(maxBytes)
	if collation, ok := common.CollationMetadataByName(strings.TrimSpace(collationName)); ok &&
		(strings.EqualFold(collation.Charset, "utf8mb4") || strings.EqualFold(collation.Charset, "utf8mb3") || strings.EqualFold(collation.Charset, "utf8")) &&
		utf8.ValidString(value) {
		for limit > 0 && !utf8.RuneStart(value[limit]) {
			limit--
		}
	}
	return value[:limit]
}
