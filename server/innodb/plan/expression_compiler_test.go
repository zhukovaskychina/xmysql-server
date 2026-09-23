package plan

import (
	"reflect"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestCompiledConvertUsingPreservesCharsetSemantics(t *testing.T) {
	expression := &Function{
		FuncName:     "CONVERT",
		UsingCharset: "latin1",
		FuncArgs:     []Expression{&Constant{Value: "é"}},
	}
	compiled, ok := CompileExpression(expression)
	if !ok {
		t.Fatal("CONVERT USING should use the compiled evaluator")
	}
	got, err := compiled(&EvalContext{})
	if err != nil {
		t.Fatalf("compiled CONVERT USING failed: %v", err)
	}
	if got != string([]byte{0xe9}) {
		t.Fatalf("compiled CONVERT('é' USING latin1) = %#v, want latin1 byte 0xe9", got)
	}
}

func TestCompiledRowCountReadsSessionValue(t *testing.T) {
	compiled, ok := CompileExpression(&Function{FuncName: "ROW_COUNT"})
	if !ok || compiled == nil {
		t.Fatal("CompileExpression should support ROW_COUNT")
	}
	got, err := compiled(&EvalContext{SessionValues: map[string]interface{}{"row_count": int64(-1)}})
	if err != nil {
		t.Fatalf("compiled ROW_COUNT() failed: %v", err)
	}
	if got != int64(-1) {
		t.Fatalf("compiled ROW_COUNT() = %#v, want int64(-1)", got)
	}
}

func TestCompiledSessionMetadataFunctionsReadSessionValues(t *testing.T) {
	values := map[string]interface{}{
		"database":      "app",
		"user":          "alice@127.0.0.1",
		"current_user":  "alice@127.0.0.1",
		"version":       "8.0.32",
		"connection_id": int64(42),
	}
	for name, want := range map[string]interface{}{
		"DATABASE":      "app",
		"SCHEMA":        "app",
		"USER":          "alice@127.0.0.1",
		"CURRENT_USER":  "alice@127.0.0.1",
		"VERSION":       "8.0.32",
		"CONNECTION_ID": int64(42),
	} {
		compiled, ok := CompileExpression(&Function{FuncName: name})
		if !ok || compiled == nil {
			t.Fatalf("CompileExpression should support %s", name)
		}
		got, err := compiled(&EvalContext{SessionValues: values})
		if err != nil {
			t.Fatalf("compiled %s() failed: %v", name, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("compiled %s() = %#v, want %#v", name, got, want)
		}
	}
}

func TestCompiledCastHonorsCharacterLength(t *testing.T) {
	expression := &Function{
		FuncName:      "CONVERT",
		CastType:      "CHAR",
		CastLength:    3,
		HasCastLength: true,
		FuncArgs:      []Expression{&Constant{Value: "abcdef"}, &Constant{Value: "CHAR"}},
	}
	compiled, ok := CompileExpression(expression)
	if !ok {
		t.Fatal("CONVERT CHAR(N) should use the compiled evaluator")
	}
	got, err := compiled(&EvalContext{})
	if err != nil {
		t.Fatalf("compiled CONVERT CHAR(3) failed: %v", err)
	}
	if got != "abc" {
		t.Fatalf("compiled CONVERT('abcdef', CHAR(3)) = %#v, want abc", got)
	}
}

func TestCompileExpressionSupportsStructuredInExpression(t *testing.T) {
	compiled, ok := CompileExpression(&InExpression{
		Column: &Column{Name: "status"},
		Values: []interface{}{"ready", "done"},
	})
	if !ok || compiled == nil {
		t.Fatal("CompileExpression should support structured IN expressions")
	}

	value, err := compiled(&EvalContext{Row: map[string]interface{}{"status": "done"}})
	if err != nil {
		t.Fatalf("compiled IN evaluation failed: %v", err)
	}
	if value != true {
		t.Fatalf("compiled IN value = %#v, want true", value)
	}

	value, err = compiled(&EvalContext{Row: map[string]interface{}{"status": "pending"}})
	if err != nil {
		t.Fatalf("compiled IN non-match evaluation failed: %v", err)
	}
	if value != false {
		t.Fatalf("compiled IN non-match value = %#v, want false", value)
	}
}

func TestCompileExpressionResolvesColumnNamesCaseInsensitively(t *testing.T) {
	compiled, ok := CompileExpression(&Column{Name: "NAME"})
	if !ok || compiled == nil {
		t.Fatal("CompileExpression should support column expressions")
	}

	value, err := compiled(&EvalContext{Row: map[string]interface{}{"name": "Alice"}})
	if err != nil {
		t.Fatalf("compiled column evaluation failed: %v", err)
	}
	if value != "Alice" {
		t.Fatalf("compiled column value = %#v, want Alice", value)
	}
}

func TestCompileExpressionSupportsStructuredLikeExpression(t *testing.T) {
	compiled, ok := CompileExpression(&LikeExpression{
		Column:  &Column{Name: "name"},
		Pattern: "ab%",
	})
	if !ok || compiled == nil {
		t.Fatal("CompileExpression should support structured LIKE expressions")
	}

	value, err := compiled(&EvalContext{Row: map[string]interface{}{"name": "about"}})
	if err != nil {
		t.Fatalf("compiled LIKE evaluation failed: %v", err)
	}
	if value != true {
		t.Fatalf("compiled LIKE value = %#v, want true", value)
	}

	value, err = compiled(&EvalContext{Row: map[string]interface{}{"name": nil}})
	if err != nil {
		t.Fatalf("compiled LIKE NULL evaluation failed: %v", err)
	}
	if value != nil {
		t.Fatalf("compiled LIKE NULL value = %#v, want UNKNOWN", value)
	}
}

func TestCompileExpressionSupportsNotExpression(t *testing.T) {
	compiled, ok := CompileExpression(&NotExpression{
		Operand: &Column{Name: "enabled"},
	})
	if !ok || compiled == nil {
		t.Fatal("CompileExpression should support NOT expressions")
	}

	cases := []struct {
		name string
		row  map[string]interface{}
		want interface{}
	}{
		{name: "true", row: map[string]interface{}{"enabled": true}, want: false},
		{name: "false", row: map[string]interface{}{"enabled": false}, want: true},
		{name: "null", row: map[string]interface{}{"enabled": nil}, want: nil},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			value, err := compiled(&EvalContext{Row: testCase.row})
			if err != nil {
				t.Fatalf("compiled NOT evaluation failed: %v", err)
			}
			if !reflect.DeepEqual(value, testCase.want) {
				t.Fatalf("compiled NOT value = %#v, want %#v", value, testCase.want)
			}
		})
	}
}

func TestBuildExpressionSupportsModuloAndIntegerDivision(t *testing.T) {
	cases := []struct {
		name     string
		operator string
		want     interface{}
	}{
		{name: "modulo", operator: sqlparser.ModStr, want: int64(1)},
		{name: "integer division", operator: sqlparser.IntDivStr, want: int64(2)},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			expression := BuildExpression(&sqlparser.BinaryExpr{
				Operator: testCase.operator,
				Left:     sqlparser.NewIntVal([]byte("7")),
				Right:    sqlparser.NewIntVal([]byte("3")),
			})
			value, err := expression.Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("%s evaluation failed: %v", testCase.operator, err)
			}
			if value != testCase.want {
				t.Fatalf("%s value = %#v, want %#v", testCase.operator, value, testCase.want)
			}
		})
	}
}

func TestBuildExpressionSupportsBitwiseAndShiftOperators(t *testing.T) {
	cases := []struct {
		name     string
		operator string
		left     string
		right    string
		want     interface{}
	}{
		{name: "bit and", operator: sqlparser.BitAndStr, left: "6", right: "3", want: int64(2)},
		{name: "bit or", operator: sqlparser.BitOrStr, left: "4", right: "1", want: int64(5)},
		{name: "bit xor", operator: sqlparser.BitXorStr, left: "6", right: "3", want: int64(5)},
		{name: "shift left", operator: sqlparser.ShiftLeftStr, left: "3", right: "2", want: int64(12)},
		{name: "shift right", operator: sqlparser.ShiftRightStr, left: "12", right: "2", want: int64(3)},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			expression := BuildExpression(&sqlparser.BinaryExpr{
				Operator: testCase.operator,
				Left:     sqlparser.NewIntVal([]byte(testCase.left)),
				Right:    sqlparser.NewIntVal([]byte(testCase.right)),
			})
			value, err := expression.Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("%s evaluation failed: %v", testCase.operator, err)
			}
			if value != testCase.want {
				t.Fatalf("%s value = %#v, want %#v", testCase.operator, value, testCase.want)
			}
		})
	}
}

func TestCompileExpressionSupportsDeterministicNetworkDigestFunctions(t *testing.T) {
	cases := []struct {
		name string
		fn   string
		args []Expression
		want interface{}
	}{
		{name: "md5", fn: "MD5", args: []Expression{&Constant{Value: "abc"}}, want: "900150983cd24fb0d6963f7d28e17f72"},
		{name: "inet aton", fn: "INET_ATON", args: []Expression{&Constant{Value: "127.0.0.1"}}, want: uint64(2130706433)},
		{name: "inet6 aton", fn: "INET6_ATON", args: []Expression{&Constant{Value: "2001:db8::1"}}, want: []byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}},
		{name: "is uuid", fn: "IS_UUID", args: []Expression{&Constant{Value: "6ccd780c-baba-1026-9564-5b8c656024db"}}, want: int64(1)},
		{name: "bit count", fn: "BIT_COUNT", args: []Expression{&Constant{Value: int64(7)}}, want: int64(3)},
		{name: "json merge preserve", fn: "JSON_MERGE_PRESERVE", args: []Expression{&Constant{Value: `{"a":1}`}, &Constant{Value: `{"a":2}`}}, want: `{"a":[1,2]}`},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			compiled, ok := CompileExpression(&Function{FuncName: testCase.fn, FuncArgs: testCase.args})
			if !ok || compiled == nil {
				t.Fatalf("CompileExpression should support %s", testCase.fn)
			}
			value, err := compiled(&EvalContext{})
			if err != nil {
				t.Fatalf("compiled %s evaluation failed: %v", testCase.fn, err)
			}
			if !reflect.DeepEqual(value, testCase.want) {
				t.Fatalf("compiled %s value = %#v, want %#v", testCase.fn, value, testCase.want)
			}
		})
	}
}

func TestCompileExpressionSupportsDeterministicDateFunctions(t *testing.T) {
	cases := []struct {
		name string
		fn   string
		want interface{}
	}{
		{name: "year", fn: "YEAR", want: int64(2024)},
		{name: "month", fn: "MONTH", want: int64(2)},
		{name: "date", fn: "DATE", want: "2024-02-29"},
		{name: "last day", fn: "LAST_DAY", want: "2024-02-29"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			compiled, ok := CompileExpression(&Function{
				FuncName: testCase.fn,
				FuncArgs: []Expression{&Constant{Value: "2024-02-29 12:34:56"}},
			})
			if !ok || compiled == nil {
				t.Fatalf("CompileExpression should support %s", testCase.fn)
			}
			value, err := compiled(&EvalContext{})
			if err != nil {
				t.Fatalf("compiled %s evaluation failed: %v", testCase.fn, err)
			}
			if value != testCase.want {
				t.Fatalf("compiled %s value = %#v, want %#v", testCase.fn, value, testCase.want)
			}
		})
	}
}

func TestCompileExpressionSupportsOctetLength(t *testing.T) {
	compiled, ok := CompileExpression(&Function{
		FuncName: "OCTET_LENGTH",
		FuncArgs: []Expression{&Constant{Value: "你好"}},
	})
	if !ok || compiled == nil {
		t.Fatal("CompileExpression should support OCTET_LENGTH")
	}
	value, err := compiled(&EvalContext{})
	if err != nil {
		t.Fatalf("compiled OCTET_LENGTH evaluation failed: %v", err)
	}
	if value != int64(6) {
		t.Fatalf("compiled OCTET_LENGTH value = %#v, want 6", value)
	}
}

func TestBuildExpressionSupportsJSONExtractionOperators(t *testing.T) {
	cases := []struct {
		name     string
		operator string
		want     interface{}
	}{
		{name: "json extract", operator: sqlparser.JSONExtractOp, want: float64(7)},
		{name: "json unquote extract", operator: sqlparser.JSONUnquoteExtractOp, want: "7"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			expression := BuildExpression(&sqlparser.BinaryExpr{
				Operator: testCase.operator,
				Left: &sqlparser.ColName{
					Name: sqlparser.NewColIdent("doc"),
				},
				Right: sqlparser.NewStrVal([]byte("$.value")),
			})
			value, err := expression.Eval(&EvalContext{Row: map[string]interface{}{
				"doc": `{"value":7}`,
			}})
			if err != nil {
				t.Fatalf("%s evaluation failed: %v", testCase.operator, err)
			}
			if value != testCase.want {
				t.Fatalf("%s value = %#v, want %#v", testCase.operator, value, testCase.want)
			}
		})
	}
}

func TestBuildExpressionSupportsRegexpComparisons(t *testing.T) {
	cases := []struct {
		name     string
		operator string
		pattern  string
		want     interface{}
	}{
		{name: "regexp match", operator: sqlparser.RegexpStr, pattern: `^[a-z]+$`, want: true},
		{name: "regexp non-match", operator: sqlparser.NotRegexpStr, pattern: `^[0-9]+$`, want: true},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			expression := BuildExpression(&sqlparser.ComparisonExpr{
				Operator: testCase.operator,
				Left:     sqlparser.NewStrVal([]byte("abc")),
				Right:    sqlparser.NewStrVal([]byte(testCase.pattern)),
			})
			value, err := expression.Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("%s evaluation failed: %v", testCase.operator, err)
			}
			if value != testCase.want {
				t.Fatalf("%s value = %#v, want %#v", testCase.operator, value, testCase.want)
			}
		})
	}
}

func TestCompileExpressionSupportsDeterministicRegexpFunction(t *testing.T) {
	compiled, ok := CompileExpression(&Function{
		FuncName: "REGEXP_LIKE",
		FuncArgs: []Expression{
			&Constant{Value: "abc"},
			&Constant{Value: "^a"},
		},
	})
	if !ok || compiled == nil {
		t.Fatal("CompileExpression should support REGEXP_LIKE")
	}

	value, err := compiled(&EvalContext{})
	if err != nil {
		t.Fatalf("compiled REGEXP_LIKE evaluation failed: %v", err)
	}
	if value != true {
		t.Fatalf("compiled REGEXP_LIKE value = %#v, want true", value)
	}
}

func TestCompileExpressionSupportsTimeConversionFunctions(t *testing.T) {
	for _, expression := range []Expression{
		&Function{FuncName: "TIME_TO_SEC", FuncArgs: []Expression{&Constant{Value: "01:02:03"}}},
		&Function{FuncName: "SEC_TO_TIME", FuncArgs: []Expression{&Constant{Value: int64(3723)}}},
	} {
		compiled, ok := CompileExpression(expression)
		if !ok || compiled == nil {
			t.Fatalf("CompileExpression should support %s", expression.String())
		}
		interpreted, err := expression.Eval(&EvalContext{})
		if err != nil {
			t.Fatalf("interpreted %s evaluation failed: %v", expression.String(), err)
		}
		value, err := compiled(&EvalContext{})
		if err != nil {
			t.Fatalf("compiled %s evaluation failed: %v", expression.String(), err)
		}
		if !reflect.DeepEqual(value, interpreted) {
			t.Fatalf("compiled %s = %#v, interpreted = %#v", expression.String(), value, interpreted)
		}
	}
}

func TestCompileExpressionSupportsRuntimeCompatibilityFunctions(t *testing.T) {
	for _, name := range []string{
		"CURDATE", "CURRENT_DATE", "UTC_DATE", "CURTIME", "CURRENT_TIME", "LOCALTIME", "UTC_TIME",
		"NOW", "CURRENT_TIMESTAMP", "LOCALTIMESTAMP", "UTC_TIMESTAMP", "RAND", "UUID",
		"GET_FORMAT", "CONVERT_TZ", "STR_TO_DATE",
		"TO_DAYS", "FROM_DAYS", "TO_SECONDS", "PERIOD_ADD", "PERIOD_DIFF", "SYSDATE", "TIMEDIFF", "WEEKOFYEAR", "TO_BASE64", "FROM_BASE64", "RANDOM_BYTES",
		"JSON_SCHEMA_VALID", "JSON_SCHEMA_VALIDATION_REPORT",
		"ST_CONVEXHULL", "ST_VALIDATE", "ST_POINTATDISTANCE", "ST_LINEINTERPOLATEPOINT", "ST_LINEINTERPOLATEPOINTS", "ST_GEOHASH", "ST_LATFROMGEOHASH", "ST_LONGFROMGEOHASH", "ST_POINTFROMGEOHASH",
		"ST_LINESTRING", "ST_MAKEENVELOPE", "ST_SIMPLIFY", "ST_LINEFROMTEXT", "ST_POLYFROMTEXT", "ST_MPOINTFROMTEXT", "ST_MLINEFROMTEXT", "ST_MPOLYFROMTEXT", "ST_GEOMCOLLFROMTEXT",
		"ST_TRANSFORM",
		"ST_HAUSDORFFDISTANCE", "ST_FRECHETDISTANCE",
	} {
		expression := &Function{FuncName: name}
		compiled, ok := CompileExpression(expression)
		if !ok || compiled == nil {
			t.Fatalf("CompileExpression should support %s", name)
		}
		if _, err := compiled(&EvalContext{}); err != nil {
			t.Fatalf("compiled %s evaluation failed: %v", name, err)
		}
	}
}

func TestCompileExpressionSupportsConv(t *testing.T) {
	expression := &Function{FuncName: "CONV", FuncArgs: []Expression{
		&Constant{Value: "10"}, &Constant{Value: int64(10)}, &Constant{Value: int64(16)},
	}}
	compiled, ok := CompileExpression(expression)
	if !ok || compiled == nil {
		t.Fatal("CompileExpression should support CONV")
	}
	value, err := compiled(&EvalContext{})
	if err != nil {
		t.Fatalf("compiled CONV evaluation failed: %v", err)
	}
	if value != "A" {
		t.Fatalf("compiled CONV(10, 10, 16) = %#v, want A", value)
	}

	invalid := &Function{FuncName: "CONV", FuncArgs: []Expression{
		&Constant{Value: "10"}, &Constant{Value: int64(1)}, &Constant{Value: int64(16)},
	}}
	if _, err := invalid.Eval(&EvalContext{}); err == nil {
		t.Fatal("CONV should reject bases outside 2..36")
	}
}

func TestCompileExpressionSupportsInterval(t *testing.T) {
	expression := &Function{FuncName: "INTERVAL", FuncArgs: []Expression{
		&Constant{Value: int64(25)}, &Constant{Value: int64(10)}, &Constant{Value: int64(20)}, &Constant{Value: int64(30)},
	}}
	interpreted, err := expression.Eval(&EvalContext{})
	if err != nil {
		t.Fatalf("interpreted INTERVAL evaluation failed: %v", err)
	}
	compiled, ok := CompileExpression(expression)
	if !ok || compiled == nil {
		t.Fatal("CompileExpression should support INTERVAL")
	}
	value, err := compiled(&EvalContext{})
	if err != nil {
		t.Fatalf("compiled INTERVAL evaluation failed: %v", err)
	}
	if !reflect.DeepEqual(value, interpreted) || value != int64(2) {
		t.Fatalf("compiled INTERVAL = %#v, interpreted = %#v, want 2", value, interpreted)
	}
}

func TestCompileExpressionSupportsIsNullFunction(t *testing.T) {
	expression := &Function{FuncName: "ISNULL", FuncArgs: []Expression{&Constant{Value: nil}}}
	compiled, ok := CompileExpression(expression)
	if !ok || compiled == nil {
		t.Fatal("CompileExpression should support ISNULL")
	}
	value, err := compiled(&EvalContext{})
	if err != nil {
		t.Fatalf("compiled ISNULL evaluation failed: %v", err)
	}
	if value != int64(1) {
		t.Fatalf("compiled ISNULL(NULL) = %#v, want 1", value)
	}
}

func TestCompileExpressionSupportsDateNameFunctions(t *testing.T) {
	for _, name := range []string{"DAYOFMONTH", "MONTHNAME", "DAYNAME"} {
		expression := &Function{FuncName: name, FuncArgs: []Expression{&Constant{Value: "2024-02-29"}}}
		compiled, ok := CompileExpression(expression)
		if !ok || compiled == nil {
			t.Fatalf("CompileExpression should support %s", name)
		}
		interpreted, err := expression.Eval(&EvalContext{})
		if err != nil {
			t.Fatalf("interpreted %s evaluation failed: %v", name, err)
		}
		value, err := compiled(&EvalContext{})
		if err != nil {
			t.Fatalf("compiled %s evaluation failed: %v", name, err)
		}
		if value != interpreted {
			t.Fatalf("compiled %s = %#v, interpreted = %#v", name, value, interpreted)
		}
	}
}

func TestCompileExpressionSupportsAdditionalNumericFunctions(t *testing.T) {
	for _, name := range []string{"RADIANS", "DEGREES", "LOG2", "COT"} {
		expression := &Function{FuncName: name, FuncArgs: []Expression{&Constant{Value: 1.0}}}
		compiled, ok := CompileExpression(expression)
		if !ok || compiled == nil {
			t.Fatalf("CompileExpression should support %s", name)
		}
	}
}

func TestCompileExpressionSupportsWeekFunctions(t *testing.T) {
	for _, name := range []string{"WEEK", "YEARWEEK"} {
		expression := &Function{FuncName: name, FuncArgs: []Expression{&Constant{Value: "2024-01-01"}}}
		compiled, ok := CompileExpression(expression)
		if !ok || compiled == nil {
			t.Fatalf("CompileExpression should support %s", name)
		}
	}
}

func TestCompileExpressionSupportsTimeFormat(t *testing.T) {
	expression := &Function{FuncName: "TIME_FORMAT", FuncArgs: []Expression{
		&Constant{Value: "13:04:05"}, &Constant{Value: "%H:%i:%s"},
	}}
	compiled, ok := CompileExpression(expression)
	if !ok || compiled == nil {
		t.Fatal("CompileExpression should support TIME_FORMAT")
	}
}
