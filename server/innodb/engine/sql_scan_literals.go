package engine

import (
	"encoding/hex"
	"fmt"
)

// asciiLowerSQL folds SQL keywords and identifiers without changing byte
// offsets. SQL scanners must keep offsets aligned with the original input,
// which may contain arbitrary bytes inside a quoted binary literal or bound
// parameter. strings.ToLower can expand invalid UTF-8 and break that alignment.
func asciiLowerSQL(value string) string {
	bytes := []byte(value)
	for index, current := range bytes {
		if current >= 'A' && current <= 'Z' {
			bytes[index] = current + ('a' - 'A')
		}
	}
	return string(bytes)
}

func decodeSQLHexLiteral(raw []byte) ([]byte, error) {
	decoded := make([]byte, hex.DecodedLen(len(raw)))
	if _, err := hex.Decode(decoded, raw); err != nil {
		return nil, fmt.Errorf("invalid SQL hex literal: %w", err)
	}
	return decoded, nil
}

// sqlCodePositions excludes string and comment contents from compatibility
// scanners. Backtick identifiers remain visible to the identifier scanner.
func sqlCodePositions(sql string) []bool {
	code := make([]bool, len(sql))
	for index := 0; index < len(sql); {
		ch := sql[index]
		if ch == '\'' || ch == '"' {
			quote := ch
			index++
			for index < len(sql) {
				if sql[index] == '\\' {
					index += 2
					continue
				}
				if sql[index] == quote {
					index++
					if index < len(sql) && sql[index] == quote {
						index++
						continue
					}
					break
				}
				index++
			}
			continue
		}
		if ch == '#' || (ch == '-' && index+2 < len(sql) && sql[index+1] == '-' && sql[index+2] <= ' ') {
			for index < len(sql) && sql[index] != '\n' {
				index++
			}
			continue
		}
		if ch == '/' && index+1 < len(sql) && sql[index+1] == '*' {
			index += 2
			for index+1 < len(sql) && (sql[index] != '*' || sql[index+1] != '/') {
				index++
			}
			index += 2
			continue
		}
		code[index] = true
		index++
	}
	return code
}
