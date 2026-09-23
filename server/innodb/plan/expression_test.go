package plan

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser"
)

func TestCompileExpressionMatchesEvalForHotPath(t *testing.T) {
	expression := &BinaryOperation{
		Op: OpGT,
		Left: &BinaryOperation{
			Op:    OpAdd,
			Left:  &Column{Name: "amount"},
			Right: &Constant{Value: int64(2)},
		},
		Right: &Constant{Value: int64(10)},
	}
	ctx := &EvalContext{Row: map[string]interface{}{"amount": int64(9)}}
	want, err := expression.Eval(ctx)
	if err != nil {
		t.Fatalf("interpreted Eval() error = %v", err)
	}
	compiled, ok := CompileExpression(expression)
	if !ok {
		t.Fatal("hot-path expression was not compiled")
	}
	got, err := compiled(ctx)
	if err != nil {
		t.Fatalf("compiled expression error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("compiled expression = %#v, interpreted = %#v", got, want)
	}
}

func TestColumnEvalIsCaseInsensitive(t *testing.T) {
	value, err := (&Column{Name: "userid"}).Eval(&EvalContext{Row: map[string]interface{}{"UserID": int64(7)}})
	if err != nil {
		t.Fatalf("case-insensitive Column.Eval() error = %v", err)
	}
	if got := value.(int64); got != 7 {
		t.Fatalf("case-insensitive Column.Eval() = %d, want 7", got)
	}
}

func TestColumnEvalNormalizesQuotedQualifiedReferences(t *testing.T) {
	expression := &Column{Name: "`orders`.`ID`"}
	ctx := &EvalContext{Row: map[string]interface{}{"orders.id": int64(9)}}

	value, err := expression.Eval(ctx)
	if err != nil {
		t.Fatalf("quoted qualified Column.Eval() error = %v", err)
	}
	if got := value.(int64); got != 9 {
		t.Fatalf("quoted qualified Column.Eval() = %d, want 9", got)
	}

	compiled, ok := CompileExpression(expression)
	if !ok {
		t.Fatal("quoted qualified column should use the compiled evaluator")
	}
	value, err = compiled(ctx)
	if err != nil {
		t.Fatalf("compiled quoted qualified Column.Eval() error = %v", err)
	}
	if got := value.(int64); got != 9 {
		t.Fatalf("compiled quoted qualified Column.Eval() = %d, want 9", got)
	}
}

func TestCurrentTemporalFunctionAliasesEvaluate(t *testing.T) {
	dateAliases := []string{"CURDATE", "CURRENT_DATE", "UTC_DATE"}
	for _, alias := range dateAliases {
		value, err := (&Function{FuncName: alias}).Eval(&EvalContext{})
		if err != nil {
			t.Fatalf("%s() error = %v", alias, err)
		}
		if _, err := time.Parse("2006-01-02", fmt.Sprint(value)); err != nil {
			t.Fatalf("%s() = %v, want YYYY-MM-DD", alias, value)
		}
	}

	timeAliases := []string{"CURTIME", "CURRENT_TIME", "LOCALTIME", "UTC_TIME"}
	for _, alias := range timeAliases {
		value, err := (&Function{FuncName: alias}).Eval(&EvalContext{})
		if err != nil {
			t.Fatalf("%s() error = %v", alias, err)
		}
		if _, err := time.Parse("15:04:05", fmt.Sprint(value)); err != nil {
			t.Fatalf("%s() = %v, want HH:MM:SS", alias, value)
		}
	}

	for _, alias := range []string{"CURRENT_TIMESTAMP", "LOCALTIMESTAMP", "UTC_TIMESTAMP"} {
		value, err := (&Function{FuncName: alias}).Eval(&EvalContext{})
		if err != nil {
			t.Fatalf("%s() error = %v", alias, err)
		}
		if _, ok := value.(time.Time); !ok {
			t.Fatalf("%s() = %T, want time.Time", alias, value)
		}
	}
}

func TestTimeConversionFunctionsEvaluateMySQLForms(t *testing.T) {
	cases := []struct {
		name string
		expr *Function
		want interface{}
	}{
		{name: "time to seconds", expr: &Function{FuncName: "TIME_TO_SEC", FuncArgs: []Expression{&Constant{Value: "12:34:56"}}}, want: int64(45296)},
		{name: "negative time to seconds", expr: &Function{FuncName: "TIME_TO_SEC", FuncArgs: []Expression{&Constant{Value: "-01:02:03"}}}, want: int64(-3723)},
		{name: "seconds to time", expr: &Function{FuncName: "SEC_TO_TIME", FuncArgs: []Expression{&Constant{Value: int64(45296)}}}, want: "12:34:56"},
		{name: "fractional seconds to time", expr: &Function{FuncName: "SEC_TO_TIME", FuncArgs: []Expression{&Constant{Value: 45296.125}}}, want: "12:34:56.125000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.expr.Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("%s error = %v", tc.name, err)
			}
			if got != tc.want {
				t.Fatalf("%s = %#v, want %#v", tc.name, got, tc.want)
			}
		})
	}
}

func TestCalendarAndPeriodFunctionsEvaluateMySQLForms(t *testing.T) {
	cases := []struct {
		name string
		expr *Function
		want interface{}
	}{
		{name: "to days", expr: &Function{FuncName: "TO_DAYS", FuncArgs: []Expression{&Constant{Value: "2008-10-07"}}}, want: int64(733687)},
		{name: "from days", expr: &Function{FuncName: "FROM_DAYS", FuncArgs: []Expression{&Constant{Value: int64(733687)}}}, want: "2008-10-07"},
		{name: "to seconds", expr: &Function{FuncName: "TO_SECONDS", FuncArgs: []Expression{&Constant{Value: "1970-01-01 00:00:00"}}}, want: int64(62167219200)},
		{name: "period add", expr: &Function{FuncName: "PERIOD_ADD", FuncArgs: []Expression{&Constant{Value: int64(200801)}, &Constant{Value: int64(2)}}}, want: int64(200803)},
		{name: "period diff", expr: &Function{FuncName: "PERIOD_DIFF", FuncArgs: []Expression{&Constant{Value: int64(200802)}, &Constant{Value: int64(200801)}}}, want: int64(1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.expr.Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("%s error = %v", tc.name, err)
			}
			if got != tc.want {
				t.Fatalf("%s = %#v, want %#v", tc.name, got, tc.want)
			}
		})
	}
	sysdate, err := (&Function{FuncName: "SYSDATE"}).Eval(&EvalContext{})
	if err != nil {
		t.Fatalf("SYSDATE error = %v", err)
	}
	if _, ok := sysdate.(time.Time); !ok {
		t.Fatalf("SYSDATE = %T, want time.Time", sysdate)
	}
	for _, tc := range []struct {
		fsp     int64
		quantum time.Duration
	}{
		{fsp: 0, quantum: time.Second},
		{fsp: 3, quantum: time.Millisecond},
		{fsp: 6, quantum: time.Microsecond},
	} {
		value, err := (&Function{FuncName: "SYSDATE", FuncArgs: []Expression{&Constant{Value: tc.fsp}}}).Eval(&EvalContext{})
		got, ok := value.(time.Time)
		if err != nil || !ok {
			t.Fatalf("SYSDATE(%d) = %#v, %v; want time.Time", tc.fsp, value, err)
		}
		if got.Nanosecond()%int(tc.quantum) != 0 {
			t.Fatalf("SYSDATE(%d) nanoseconds = %d, want quantum %s", tc.fsp, got.Nanosecond(), tc.quantum)
		}
	}
}

func TestFractionalSecondTruncationUsesMySQLPrecision(t *testing.T) {
	base := time.Date(2026, time.March, 5, 14, 6, 7, 123456789, time.UTC)
	for _, tc := range []struct {
		fsp  int64
		want int
	}{
		{fsp: 0, want: 0},
		{fsp: 3, want: 123000000},
		{fsp: 6, want: 123456000},
	} {
		if got := truncateTimeFraction(base, tc.fsp).Nanosecond(); got != tc.want {
			t.Fatalf("fsp %d truncation = %d, want %d", tc.fsp, got, tc.want)
		}
	}
}

func TestTimeDifferenceAndWeekOfYearFunctionsEvaluate(t *testing.T) {
	timediff, err := (&Function{FuncName: "TIMEDIFF", FuncArgs: []Expression{
		&Constant{Value: "2008-12-31 23:59:59"}, &Constant{Value: "2008-12-31 22:58:58"},
	}}).Eval(&EvalContext{})
	if err != nil || timediff != "01:01:01" {
		t.Fatalf("TIMEDIFF = %#v, %v; want 01:01:01", timediff, err)
	}
	negative, err := (&Function{FuncName: "TIMEDIFF", FuncArgs: []Expression{
		&Constant{Value: "01:00:00"}, &Constant{Value: "02:30:00"},
	}}).Eval(&EvalContext{})
	if err != nil || negative != "-01:30:00" {
		t.Fatalf("negative TIMEDIFF = %#v, %v; want -01:30:00", negative, err)
	}
	weekOfYear, err := (&Function{FuncName: "WEEKOFYEAR", FuncArgs: []Expression{&Constant{Value: "2008-02-20"}}}).Eval(&EvalContext{})
	if err != nil || weekOfYear != int64(8) {
		t.Fatalf("WEEKOFYEAR = %#v, %v; want 8", weekOfYear, err)
	}
}

func TestBase64AndRandomBytesFunctionsEvaluate(t *testing.T) {
	encoded, err := (&Function{FuncName: "TO_BASE64", FuncArgs: []Expression{&Constant{Value: "hello"}}}).Eval(&EvalContext{})
	if err != nil || encoded != "aGVsbG8=" {
		t.Fatalf("TO_BASE64 = %#v, %v; want aGVsbG8=", encoded, err)
	}
	decoded, err := (&Function{FuncName: "FROM_BASE64", FuncArgs: []Expression{&Constant{Value: encoded}}}).Eval(&EvalContext{})
	if err != nil || !bytes.Equal(decoded.([]byte), []byte("hello")) {
		t.Fatalf("FROM_BASE64 = %#v, %v; want hello", decoded, err)
	}
	random, err := (&Function{FuncName: "RANDOM_BYTES", FuncArgs: []Expression{&Constant{Value: int64(24)}}}).Eval(&EvalContext{})
	if err != nil || len(random.([]byte)) != 24 {
		t.Fatalf("RANDOM_BYTES = %#v, %v; want 24 bytes", random, err)
	}
}

func TestJSONSchemaValidationFunctionsEvaluate(t *testing.T) {
	schema := `{"type":"object","required":["name"],"properties":{"name":{"type":"string","minLength":2},"age":{"type":"integer","minimum":0}}}`
	valid, err := (&Function{FuncName: "JSON_SCHEMA_VALID", FuncArgs: []Expression{
		&Constant{Value: schema}, &Constant{Value: `{"name":"Ada","age":37}`},
	}}).Eval(&EvalContext{})
	if err != nil || valid != int64(1) {
		t.Fatalf("JSON_SCHEMA_VALID(valid) = %#v, %v; want 1", valid, err)
	}
	invalid, err := (&Function{FuncName: "JSON_SCHEMA_VALID", FuncArgs: []Expression{
		&Constant{Value: schema}, &Constant{Value: `{"name":"A","age":-1}`},
	}}).Eval(&EvalContext{})
	if err != nil || invalid != int64(0) {
		t.Fatalf("JSON_SCHEMA_VALID(invalid) = %#v, %v; want 0", invalid, err)
	}
	report, err := (&Function{FuncName: "JSON_SCHEMA_VALIDATION_REPORT", FuncArgs: []Expression{
		&Constant{Value: schema}, &Constant{Value: `{"age":37}`},
	}}).Eval(&EvalContext{})
	if err != nil || !strings.Contains(fmt.Sprint(report), `"valid":false`) || !strings.Contains(fmt.Sprint(report), "required") {
		t.Fatalf("JSON_SCHEMA_VALIDATION_REPORT = %#v, %v; want invalid required report", report, err)
	}
}

func TestDateAndTimeConstructorFunctionsEvaluate(t *testing.T) {
	cases := []struct {
		name string
		expr *Function
		want interface{}
	}{
		{name: "make date", expr: &Function{FuncName: "MAKEDATE", FuncArgs: []Expression{&Constant{Value: int64(2024)}, &Constant{Value: int64(60)}}}, want: "2024-02-29"},
		{name: "make time", expr: &Function{FuncName: "MAKETIME", FuncArgs: []Expression{&Constant{Value: int64(12)}, &Constant{Value: int64(34)}, &Constant{Value: int64(56)}}}, want: "12:34:56"},
		{name: "make time fractional", expr: &Function{FuncName: "MAKETIME", FuncArgs: []Expression{&Constant{Value: int64(1)}, &Constant{Value: int64(2)}, &Constant{Value: 3.125}}}, want: "1:02:03.125000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.expr.Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("%s error = %v", tc.name, err)
			}
			if got != tc.want {
				t.Fatalf("%s = %#v, want %#v", tc.name, got, tc.want)
			}
		})
	}

	invalidDate, err := (&Function{FuncName: "MAKEDATE", FuncArgs: []Expression{&Constant{Value: int64(2023)}, &Constant{Value: int64(366)}}}).Eval(&EvalContext{})
	if err != nil || invalidDate != nil {
		t.Fatalf("invalid MAKEDATE() = %#v, err=%v, want NULL", invalidDate, err)
	}
	invalidTime, err := (&Function{FuncName: "MAKETIME", FuncArgs: []Expression{&Constant{Value: int64(1)}, &Constant{Value: int64(60)}, &Constant{Value: int64(0)}}}).Eval(&EvalContext{})
	if err != nil || invalidTime != nil {
		t.Fatalf("invalid MAKETIME() = %#v, err=%v, want NULL", invalidTime, err)
	}
}

func TestDateTimeArithmeticCompatibilityFunctionsEvaluate(t *testing.T) {
	cases := []struct {
		name string
		expr *Function
		want interface{}
	}{
		{name: "addtime datetime", expr: &Function{FuncName: "ADDTIME", FuncArgs: []Expression{&Constant{Value: "2024-01-01 23:59:59.500000"}, &Constant{Value: "00:00:00.500000"}}}, want: "2024-01-02 00:00:00"},
		{name: "subtime time", expr: &Function{FuncName: "SUBTIME", FuncArgs: []Expression{&Constant{Value: "12:00:00"}, &Constant{Value: "01:02:03"}}}, want: "10:57:57"},
		{name: "timestamp date and time", expr: &Function{FuncName: "TIMESTAMP", FuncArgs: []Expression{&Constant{Value: "2024-01-01"}, &Constant{Value: "12:34:56"}}}, want: "2024-01-01 12:34:56"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.expr.Eval(&EvalContext{})
			if err != nil || got != tc.want {
				t.Fatalf("%s = %#v, %v; want %#v", tc.name, got, err, tc.want)
			}
		})
	}
}

func TestStrcmpFunctionEvaluatesThreeWayResult(t *testing.T) {
	cases := []struct {
		left, right interface{}
		want        interface{}
	}{
		{left: "a", right: "a", want: int64(0)},
		{left: "a", right: "b", want: int64(-1)},
		{left: "b", right: "a", want: int64(1)},
		{left: nil, right: "a", want: nil},
	}
	for _, tc := range cases {
		got, err := (&Function{FuncName: "STRCMP", FuncArgs: []Expression{
			&Constant{Value: tc.left}, &Constant{Value: tc.right},
		}}).Eval(&EvalContext{})
		if err != nil {
			t.Fatalf("STRCMP(%#v, %#v) error = %v", tc.left, tc.right, err)
		}
		if got != tc.want {
			t.Fatalf("STRCMP(%#v, %#v) = %#v, want %#v", tc.left, tc.right, got, tc.want)
		}
	}
}

func TestRandFunctionSupportsUnseededAndSeededForms(t *testing.T) {
	value, err := (&Function{FuncName: "RAND"}).Eval(&EvalContext{})
	if err != nil {
		t.Fatalf("RAND() error = %v", err)
	}
	random, ok := value.(float64)
	if !ok || random < 0 || random >= 1 {
		t.Fatalf("RAND() = %#v, want float64 in [0, 1)", value)
	}

	seeded := &Function{FuncName: "RAND", FuncArgs: []Expression{&Constant{Value: int64(42)}}}
	first, err := seeded.Eval(&EvalContext{})
	if err != nil {
		t.Fatalf("RAND(42) error = %v", err)
	}
	second, err := seeded.Eval(&EvalContext{})
	if err != nil {
		t.Fatalf("second RAND(42) error = %v", err)
	}
	if first != second {
		t.Fatalf("RAND(42) values = %#v and %#v, want deterministic seeded value", first, second)
	}
}

func TestUUIDFunctionReturnsVersionFourIdentifier(t *testing.T) {
	value, err := (&Function{FuncName: "UUID"}).Eval(&EvalContext{})
	if err != nil {
		t.Fatalf("UUID() error = %v", err)
	}
	uuid := fmt.Sprint(value)
	if len(uuid) != 36 || uuid[8] != '-' || uuid[13] != '-' || uuid[18] != '-' || uuid[23] != '-' {
		t.Fatalf("UUID() = %q, want canonical UUID format", uuid)
	}
	if uuid[14] != '4' {
		t.Fatalf("UUID() version nibble = %q, want 4", uuid[14])
	}
	if !strings.Contains("89ab", strings.ToLower(uuid[19:20])) {
		t.Fatalf("UUID() variant nibble = %q, want RFC 4122 variant", uuid[19])
	}
}

func TestIsNullFunctionReturnsMySQLBoolean(t *testing.T) {
	cases := []struct {
		name string
		arg  interface{}
		want int64
	}{
		{name: "null", arg: nil, want: 1},
		{name: "value", arg: "present", want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			value, err := (&Function{FuncName: "ISNULL", FuncArgs: []Expression{
				&Constant{Value: tc.arg},
			}}).Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("ISNULL(%#v) error = %v", tc.arg, err)
			}
			if value != tc.want {
				t.Fatalf("ISNULL(%#v) = %#v, want %#v", tc.arg, value, tc.want)
			}
		})
	}
}

func TestDateNameAndDayOfMonthFunctionsEvaluate(t *testing.T) {
	cases := []struct {
		name string
		want interface{}
	}{
		{name: "DAYOFMONTH", want: int64(29)},
		{name: "MONTHNAME", want: "February"},
		{name: "DAYNAME", want: "Thursday"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			value, err := (&Function{FuncName: tc.name, FuncArgs: []Expression{
				&Constant{Value: "2024-02-29"},
			}}).Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("%s() error = %v", tc.name, err)
			}
			if value != tc.want {
				t.Fatalf("%s() = %#v, want %#v", tc.name, value, tc.want)
			}
		})
	}
}

func TestAdditionalNumericCompatibilityFunctionsEvaluate(t *testing.T) {
	cases := []struct {
		name string
		arg  float64
		want float64
	}{
		{name: "RADIANS", arg: 180, want: math.Pi},
		{name: "DEGREES", arg: math.Pi, want: 180},
		{name: "LOG2", arg: 8, want: 3},
		{name: "COT", arg: math.Pi / 4, want: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			value, err := (&Function{FuncName: tc.name, FuncArgs: []Expression{
				&Constant{Value: tc.arg},
			}}).Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("%s() error = %v", tc.name, err)
			}
			got, ok := value.(float64)
			if !ok || math.Abs(got-tc.want) > 1e-9 {
				t.Fatalf("%s(%v) = %#v, want %v", tc.name, tc.arg, value, tc.want)
			}
		})
	}
}

func TestIntervalFunctionReturnsLastLessOrEqualBoundary(t *testing.T) {
	tests := []struct {
		name string
		args []Expression
		want interface{}
	}{
		{name: "below first", args: []Expression{&Constant{Value: int64(5)}, &Constant{Value: int64(10)}, &Constant{Value: int64(20)}, &Constant{Value: int64(30)}}, want: int64(0)},
		{name: "on boundary", args: []Expression{&Constant{Value: int64(20)}, &Constant{Value: int64(10)}, &Constant{Value: int64(20)}, &Constant{Value: int64(30)}}, want: int64(2)},
		{name: "between boundaries", args: []Expression{&Constant{Value: int64(25)}, &Constant{Value: int64(10)}, &Constant{Value: int64(20)}, &Constant{Value: int64(30)}}, want: int64(2)},
		{name: "above last", args: []Expression{&Constant{Value: int64(35)}, &Constant{Value: int64(10)}, &Constant{Value: int64(20)}, &Constant{Value: int64(30)}}, want: int64(3)},
		{name: "null expression", args: []Expression{&Constant{Value: nil}, &Constant{Value: int64(10)}, &Constant{Value: int64(20)}}, want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, err := (&Function{FuncName: "INTERVAL", FuncArgs: test.args}).Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("INTERVAL() error = %v", err)
			}
			if value != test.want {
				t.Fatalf("INTERVAL() = %#v, want %#v", value, test.want)
			}
		})
	}
}

func TestLastInsertIDUsesAndUpdatesSessionValue(t *testing.T) {
	sessionValues := map[string]interface{}{"last_insert_id": uint64(17)}
	ctx := &EvalContext{SessionValues: sessionValues}

	got, err := (&Function{FuncName: "LAST_INSERT_ID"}).Eval(ctx)
	if err != nil {
		t.Fatalf("LAST_INSERT_ID() returned error: %v", err)
	}
	if got != int64(17) {
		t.Fatalf("LAST_INSERT_ID() = %#v, want int64(17)", got)
	}

	got, err = (&Function{
		FuncName: "LAST_INSERT_ID",
		FuncArgs: []Expression{&Constant{Value: int64(42)}},
	}).Eval(ctx)
	if err != nil {
		t.Fatalf("LAST_INSERT_ID(expr) returned error: %v", err)
	}
	if got != int64(42) {
		t.Fatalf("LAST_INSERT_ID(42) = %#v, want int64(42)", got)
	}
	if sessionValues["last_insert_id"] != uint64(42) {
		t.Fatalf("session last_insert_id = %#v, want uint64(42)", sessionValues["last_insert_id"])
	}

	got, err = (&Function{FuncName: "LAST_INSERT_ID"}).Eval(ctx)
	if err != nil {
		t.Fatalf("LAST_INSERT_ID() after setter returned error: %v", err)
	}
	if got != int64(42) {
		t.Fatalf("LAST_INSERT_ID() after setter = %#v, want int64(42)", got)
	}
}

func TestRowCountReadsStatementSessionValue(t *testing.T) {
	ctx := &EvalContext{SessionValues: map[string]interface{}{"row_count": int64(3)}}
	got, err := (&Function{FuncName: "ROW_COUNT"}).Eval(ctx)
	if err != nil {
		t.Fatalf("ROW_COUNT() returned error: %v", err)
	}
	if got != int64(3) {
		t.Fatalf("ROW_COUNT() = %#v, want int64(3)", got)
	}
	_, err = (&Function{FuncName: "ROW_COUNT", FuncArgs: []Expression{&Constant{Value: int64(1)}}}).Eval(ctx)
	if err == nil {
		t.Fatal("ROW_COUNT(expr) should reject an argument")
	}
}

func TestRegexpLikeHonorsMatchTypeAndRejectsUnknownFlags(t *testing.T) {
	tests := []struct {
		name    string
		args    []Expression
		want    interface{}
		wantErr bool
	}{
		{name: "dot matches newline", args: []Expression{&Constant{Value: "a\nb"}, &Constant{Value: "a.b"}, &Constant{Value: "n"}}, want: true},
		{name: "anchors match each line", args: []Expression{&Constant{Value: "a\nb"}, &Constant{Value: "^b"}, &Constant{Value: "m"}}, want: true},
		{name: "case flag follows last case option", args: []Expression{&Constant{Value: "Abc"}, &Constant{Value: "^a"}, &Constant{Value: "ic"}}, want: false},
		{name: "unknown flag", args: []Expression{&Constant{Value: "abc"}, &Constant{Value: "a"}, &Constant{Value: "x"}}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, err := (&Function{FuncName: "REGEXP_LIKE", FuncArgs: test.args}).Eval(&EvalContext{})
			if test.wantErr {
				if err == nil {
					t.Fatal("REGEXP_LIKE should reject an unknown match_type flag")
				}
				return
			}
			if err != nil {
				t.Fatalf("REGEXP_LIKE error = %v", err)
			}
			if value != test.want {
				t.Fatalf("REGEXP_LIKE = %#v, want %#v", value, test.want)
			}
		})
	}
}

func TestWeekAndYearWeekFunctionsRespectMySQLWeekBoundaries(t *testing.T) {
	cases := []struct {
		name string
		args []Expression
		want int64
	}{
		{name: "WEEK", args: []Expression{&Constant{Value: "2024-01-01"}}, want: 0},
		{name: "YEARWEEK", args: []Expression{&Constant{Value: "2024-01-01"}}, want: 202353},
		{name: "WEEK", args: []Expression{&Constant{Value: "2024-01-01"}, &Constant{Value: int64(3)}}, want: 1},
		{name: "YEARWEEK", args: []Expression{&Constant{Value: "2024-01-01"}, &Constant{Value: int64(3)}}, want: 202401},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			value, err := (&Function{FuncName: tc.name, FuncArgs: tc.args}).Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("%s() error = %v", tc.name, err)
			}
			if value != tc.want {
				t.Fatalf("%s() = %#v, want %#v", tc.name, value, tc.want)
			}
		})
	}
}

func TestTimeFormatFunctionFormatsTimeValues(t *testing.T) {
	value, err := (&Function{FuncName: "TIME_FORMAT", FuncArgs: []Expression{
		&Constant{Value: "2024-02-29 13:04:05"},
		&Constant{Value: "%H:%i:%s"},
	}}).Eval(&EvalContext{})
	if err != nil {
		t.Fatalf("TIME_FORMAT() error = %v", err)
	}
	if value != "13:04:05" {
		t.Fatalf("TIME_FORMAT() = %#v, want 13:04:05", value)
	}
}

func TestCompositeExtractDateFunctionsMatchCompiledEvaluation(t *testing.T) {
	tests := []struct {
		name string
		want interface{}
	}{
		{name: "EXTRACT_YEAR_MONTH", want: int64(202401)},
		{name: "EXTRACT_DAY_SECOND", want: int64(2030405)},
		{name: "EXTRACT_DAY_MICROSECOND", want: 2030405.123456},
		{name: "EXTRACT_SECOND_MICROSECOND", want: 5.123456},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expression := &Function{FuncName: test.name, FuncArgs: []Expression{&Constant{Value: "2024-01-02 03:04:05.123456"}}}
			interpreted, err := expression.Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("interpreted evaluation: %v", err)
			}
			compiled, ok := CompileExpression(expression)
			if !ok {
				t.Fatal("composite EXTRACT function was not compiled")
			}
			compiledValue, err := compiled(&EvalContext{})
			if err != nil {
				t.Fatalf("compiled evaluation: %v", err)
			}
			if fmt.Sprint(interpreted) != fmt.Sprint(test.want) || fmt.Sprint(compiledValue) != fmt.Sprint(test.want) {
				t.Fatalf("values = interpreted %v, compiled %v, want %v", interpreted, compiledValue, test.want)
			}
		})
	}
}

func TestGetFormatFunctionReturnsMySQLFormatStrings(t *testing.T) {
	tests := []struct {
		name string
		args []Expression
		want string
	}{
		{name: "date usa", args: []Expression{&Constant{Value: "DATE"}, &Constant{Value: "USA"}}, want: "%m.%d.%Y"},
		{name: "datetime iso", args: []Expression{&Constant{Value: "DATETIME"}, &Constant{Value: "ISO"}}, want: "%Y-%m-%d %H:%i:%s"},
		{name: "time internal", args: []Expression{&Constant{Value: "TIME"}, &Constant{Value: "INTERNAL"}}, want: "%H%i%s"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := (&Function{FuncName: "GET_FORMAT", FuncArgs: test.args}).Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("GET_FORMAT() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("GET_FORMAT() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestConvertTZFunctionAppliesFixedOffsetZones(t *testing.T) {
	got, err := (&Function{FuncName: "CONVERT_TZ", FuncArgs: []Expression{
		&Constant{Value: "2024-01-01 12:00:00"},
		&Constant{Value: "+00:00"},
		&Constant{Value: "+08:00"},
	}}).Eval(&EvalContext{})
	if err != nil {
		t.Fatalf("CONVERT_TZ() error = %v", err)
	}
	if got != "2024-01-01 20:00:00" {
		t.Fatalf("CONVERT_TZ() = %#v, want 2024-01-01 20:00:00", got)
	}
}

func TestConvertTZFunctionAppliesNamedTimezoneDSTRules(t *testing.T) {
	tests := []struct {
		name string
		date string
		want string
	}{
		{name: "winter", date: "2024-01-15 12:00:00", want: "2024-01-15 17:00:00"},
		{name: "summer", date: "2024-07-15 12:00:00", want: "2024-07-15 16:00:00"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := (&Function{FuncName: "CONVERT_TZ", FuncArgs: []Expression{
				&Constant{Value: test.date},
				&Constant{Value: "America/New_York"},
				&Constant{Value: "UTC"},
			}}).Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("CONVERT_TZ() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("CONVERT_TZ() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestCompileExpressionSupportsRuntimeExpression(t *testing.T) {
	if _, ok := CompileExpression(&Function{FuncName: "NOW"}); !ok {
		t.Fatal("NOW should use the compiled runtime path")
	}
}

func TestCompileExpressionSupportsUnaryHotPath(t *testing.T) {
	expression := &UnaryOperation{Operator: "-", Operand: &Column{Name: "amount"}}
	ctx := &EvalContext{Row: map[string]interface{}{"amount": int64(7)}}
	want, err := expression.Eval(ctx)
	if err != nil {
		t.Fatalf("interpreted Eval() error = %v", err)
	}
	compiled, ok := CompileExpression(expression)
	if !ok {
		t.Fatal("unary hot-path expression was not compiled")
	}
	got, err := compiled(ctx)
	if err != nil {
		t.Fatalf("compiled unary expression error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("compiled unary expression = %#v, interpreted = %#v", got, want)
	}
}

func TestCompileExpressionSupportsCaseHotPath(t *testing.T) {
	expression := &CaseExpression{
		Whens: []CaseWhen{{
			Condition: &BinaryOperation{Op: OpGT, Left: &Column{Name: "amount"}, Right: &Constant{Value: int64(10)}},
			Value:     &Constant{Value: "high"},
		}},
		Else: &Constant{Value: "low"},
	}
	ctx := &EvalContext{Row: map[string]interface{}{"amount": int64(12)}}
	want, err := expression.Eval(ctx)
	if err != nil {
		t.Fatalf("interpreted CASE Eval() error = %v", err)
	}
	compiled, ok := CompileExpression(expression)
	if !ok {
		t.Fatal("CASE hot-path expression was not compiled")
	}
	got, err := compiled(ctx)
	if err != nil {
		t.Fatalf("compiled CASE expression error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("compiled CASE expression = %#v, interpreted = %#v", got, want)
	}
}

func TestCompileExpressionSupportsTupleMembershipHotPath(t *testing.T) {
	expression := &BinaryOperation{
		Op:    OpIn,
		Left:  &Column{Name: "status"},
		Right: &TupleExpression{Exprs: []Expression{&Constant{Value: "ready"}, &Column{Name: "fallback_status"}}},
	}
	ctx := &EvalContext{Row: map[string]interface{}{"status": "queued", "fallback_status": "queued"}}
	want, err := expression.Eval(ctx)
	if err != nil {
		t.Fatalf("interpreted IN Eval() error = %v", err)
	}
	compiled, ok := CompileExpression(expression)
	if !ok {
		t.Fatal("tuple membership hot-path expression was not compiled")
	}
	got, err := compiled(ctx)
	if err != nil {
		t.Fatalf("compiled IN expression error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("compiled IN expression = %#v, interpreted = %#v", got, want)
	}
}

func TestCompileExpressionSupportsBooleanPredicateHotPath(t *testing.T) {
	expression := &IsTruthExpression{
		Expr:     &NotExpression{Operand: &Column{Name: "active"}},
		Operator: "is true",
	}
	ctx := &EvalContext{Row: map[string]interface{}{"active": int64(0)}}
	want, err := expression.Eval(ctx)
	if err != nil {
		t.Fatalf("interpreted boolean predicate Eval() error = %v", err)
	}
	compiled, ok := CompileExpression(expression)
	if !ok {
		t.Fatal("boolean predicate hot-path expression was not compiled")
	}
	got, err := compiled(ctx)
	if err != nil {
		t.Fatalf("compiled boolean predicate error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("compiled boolean predicate = %#v, interpreted = %#v", got, want)
	}
}

func TestCompileExpressionSupportsDeterministicFunctionHotPath(t *testing.T) {
	expression := &Function{
		FuncName: "CONCAT",
		FuncArgs: []Expression{&Constant{Value: "id="}, &Column{Name: "id"}},
	}
	ctx := &EvalContext{Row: map[string]interface{}{"id": int64(7)}}
	want, err := expression.Eval(ctx)
	if err != nil {
		t.Fatalf("interpreted function Eval() error = %v", err)
	}
	compiled, ok := CompileExpression(expression)
	if !ok {
		t.Fatal("deterministic function hot-path expression was not compiled")
	}
	got, err := compiled(ctx)
	if err != nil {
		t.Fatalf("compiled function expression error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("compiled function expression = %#v, interpreted = %#v", got, want)
	}
}

func TestCompileExpressionSupportsRecentCompatibilityFunctions(t *testing.T) {
	tests := []struct {
		name string
		expr Expression
	}{
		{name: "JSON_VALUE", expr: &Function{FuncName: "JSON_VALUE", FuncArgs: []Expression{
			&Constant{Value: `{"id":7}`}, &Constant{Value: "$.id"},
		}}},
		{name: "JSON_STORAGE_SIZE", expr: &Function{FuncName: "JSON_STORAGE_SIZE", FuncArgs: []Expression{
			&Constant{Value: `{"id":7}`},
		}}},
		{name: "JSON_STORAGE_FREE", expr: &Function{FuncName: "JSON_STORAGE_FREE", FuncArgs: []Expression{
			&Constant{Value: `{"id":7}`},
		}}},
		{name: "REGEXP_INSTR", expr: &Function{FuncName: "REGEXP_INSTR", FuncArgs: []Expression{
			&Constant{Value: "abc123"}, &Constant{Value: "[0-9]+"},
		}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want, err := test.expr.Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("interpreted Eval() error = %v", err)
			}
			compiled, ok := CompileExpression(test.expr)
			if !ok {
				t.Fatalf("%s was not compiled", test.name)
			}
			got, err := compiled(&EvalContext{})
			if err != nil {
				t.Fatalf("compiled Eval() error = %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("compiled value = %#v, interpreted = %#v", got, want)
			}
		})
	}
}

func TestJSONCompatibilityFunctions(t *testing.T) {
	valid, err := evalJSONFunction("JSON_VALID", []interface{}{`{"a":1}`})
	if err != nil || valid != int64(1) {
		t.Fatalf("JSON_VALID = %#v, %v", valid, err)
	}
	invalid, err := evalJSONFunction("JSON_VALID", []interface{}{`{"a":}`})
	if err != nil || invalid != int64(0) {
		t.Fatalf("JSON_VALID invalid = %#v, %v", invalid, err)
	}
	quoted, err := evalJSONFunction("JSON_QUOTE", []interface{}{`a"b`})
	if err != nil || quoted != `"a\"b"` {
		t.Fatalf("JSON_QUOTE = %#v, %v", quoted, err)
	}
	overlaps, err := evalJSONFunction("JSON_OVERLAPS", []interface{}{`[1,2]`, `[3,2]`})
	if err != nil || overlaps != int64(1) {
		t.Fatalf("JSON_OVERLAPS = %#v, %v", overlaps, err)
	}
	merged, err := evalJSONFunction("JSON_MERGE_PATCH", []interface{}{`{"a":1,"b":{"x":1}}`, `{"a":2,"b":{"y":3},"x":null}`})
	if err != nil || merged != `{"a":2,"b":{"x":1,"y":3}}` {
		t.Fatalf("JSON_MERGE_PATCH = %#v, %v", merged, err)
	}
	appended, err := evalJSONFunction("JSON_ARRAY_APPEND", []interface{}{`{"items":[1]}`, "$.items", int64(2), "$.missing", "x"})
	if err != nil || appended != `{"items":[1,2]}` {
		t.Fatalf("JSON_ARRAY_APPEND = %#v, %v", appended, err)
	}
	inserted, err := evalJSONFunction("JSON_INSERT", []interface{}{`{"a":1}`, "$.a", int64(2), "$.b", int64(3)})
	if err != nil || inserted != `{"a":1,"b":3}` {
		t.Fatalf("JSON_INSERT = %#v, %v", inserted, err)
	}
	arrayInserted, err := evalJSONFunction("JSON_ARRAY_INSERT", []interface{}{`{"items":[1,3]}`, "$.items[1]", int64(2)})
	if err != nil || arrayInserted != `{"items":[1,2,3]}` {
		t.Fatalf("JSON_ARRAY_INSERT = %#v, %v", arrayInserted, err)
	}
	searchOne, err := evalJSONFunction("JSON_SEARCH", []interface{}{`{"a":"hello","b":"world"}`, "one", "%ell%"})
	if err != nil || searchOne != "$.a" {
		t.Fatalf("JSON_SEARCH one = %#v, %v", searchOne, err)
	}
	searchAll, err := evalJSONFunction("JSON_SEARCH", []interface{}{`{"a":"hello","b":"hello"}`, "all", "hello"})
	if err != nil || searchAll != `["$.a","$.b"]` {
		t.Fatalf("JSON_SEARCH all = %#v, %v", searchAll, err)
	}
	contains, err := evalJSONFunction("JSON_CONTAINS_PATH", []interface{}{`{"a":{"b":1}}`, "all", "$.a", "$.a.b"})
	if err != nil || contains != int64(1) {
		t.Fatalf("JSON_CONTAINS_PATH = %#v, %v", contains, err)
	}
	keys, err := evalJSONFunction("JSON_KEYS", []interface{}{`{"z":1,"a":2}`})
	if err != nil || keys != `["a","z"]` {
		t.Fatalf("JSON_KEYS = %#v, %v", keys, err)
	}
	depth, err := evalJSONFunction("JSON_DEPTH", []interface{}{`{"a":{"b":1}}`})
	if err != nil || depth != int64(3) {
		t.Fatalf("JSON_DEPTH = %#v, %v", depth, err)
	}
	pretty, err := evalJSONFunction("JSON_PRETTY", []interface{}{`{"a":1}`})
	if err != nil || !strings.Contains(fmt.Sprint(pretty), "\n  \"a\": 1") {
		t.Fatalf("JSON_PRETTY = %#v, %v", pretty, err)
	}
	storageSize, err := evalJSONFunction("JSON_STORAGE_SIZE", []interface{}{`{"a":1}`})
	if err != nil || storageSize != int64(len(`{"a":1}`)) {
		t.Fatalf("JSON_STORAGE_SIZE = %#v, %v", storageSize, err)
	}
	storageFree, err := evalJSONFunction("JSON_STORAGE_FREE", []interface{}{`{"a":1}`})
	if err != nil || storageFree != int64(0) {
		t.Fatalf("JSON_STORAGE_FREE = %#v, %v", storageFree, err)
	}
	jsonValue, err := evalJSONFunction("JSON_VALUE", []interface{}{`{"a":1,"s":"ok"}`, "$.a"})
	if err != nil || jsonValue != float64(1) {
		t.Fatalf("JSON_VALUE number = %#v, %v", jsonValue, err)
	}
	jsonString, err := evalJSONFunction("JSON_VALUE", []interface{}{`{"a":1,"s":"ok"}`, "$.s"})
	if err != nil || jsonString != "ok" {
		t.Fatalf("JSON_VALUE string = %#v, %v", jsonString, err)
	}
	functionValue, err := (&Function{FuncName: "JSON_VALUE", FuncArgs: []Expression{
		&Constant{Value: `{"a":1}`},
		&Constant{Value: "$.a"},
	}}).Eval(&EvalContext{})
	if err != nil || functionValue != float64(1) {
		t.Fatalf("Function JSON_VALUE = %#v, %v", functionValue, err)
	}
}

func TestJSONExtractSupportsArrayIndexPath(t *testing.T) {
	value, err := evalJSONFunction("JSON_EXTRACT", []interface{}{`{"items":[10,20]}`, "$.items[1]"})
	if err != nil {
		t.Fatalf("JSON_EXTRACT array path: %v", err)
	}
	if fmt.Sprint(value) != "20" {
		t.Fatalf("JSON_EXTRACT array path = %#v, want 20", value)
	}
}

func TestRegexpInstrCompatibility(t *testing.T) {
	value, err := (&Function{FuncName: "REGEXP_INSTR", FuncArgs: []Expression{
		&Constant{Value: "abc123def456"},
		&Constant{Value: "[0-9]+"},
	}}).Eval(&EvalContext{})
	if err != nil || value != int64(4) {
		t.Fatalf("REGEXP_INSTR = %#v, %v", value, err)
	}
	end, err := (&Function{FuncName: "REGEXP_INSTR", FuncArgs: []Expression{
		&Constant{Value: "abc123def456"},
		&Constant{Value: "[0-9]+"},
		&Constant{Value: int64(1)},
		&Constant{Value: int64(2)},
		&Constant{Value: int64(1)},
	}}).Eval(&EvalContext{})
	if err != nil || end != int64(13) {
		t.Fatalf("REGEXP_INSTR return end = %#v, %v", end, err)
	}
}

func TestJSONExtractSupportsMultiplePaths(t *testing.T) {
	value, err := evalJSONFunction("JSON_EXTRACT", []interface{}{`{"a":1,"b":2}`, "$.a", "$.b"})
	if err != nil {
		t.Fatalf("JSON_EXTRACT multiple paths: %v", err)
	}
	if fmt.Sprint(value) != `[1,2]` {
		t.Fatalf("JSON_EXTRACT multiple paths = %#v, want [1,2]", value)
	}
}

func TestJSONExtractSupportsArrayWildcardPath(t *testing.T) {
	value, err := evalJSONFunction("JSON_EXTRACT", []interface{}{`{"items":[1,2]}`, "$.items[*]"})
	if err != nil {
		t.Fatalf("JSON_EXTRACT wildcard path: %v", err)
	}
	if fmt.Sprint(value) != `[1,2]` {
		t.Fatalf("JSON_EXTRACT wildcard path = %#v, want [1,2]", value)
	}
}

func TestJSONExtractSupportsObjectWildcardPath(t *testing.T) {
	value, err := evalJSONFunction("JSON_EXTRACT", []interface{}{`{"obj":{"b":2,"a":1}}`, "$.obj.*"})
	if err != nil {
		t.Fatalf("JSON_EXTRACT object wildcard path: %v", err)
	}
	if fmt.Sprint(value) != `[1,2]` {
		t.Fatalf("JSON_EXTRACT object wildcard path = %#v, want [1,2]", value)
	}
}

func TestJSONExtractSupportsNestedArrayWildcardPath(t *testing.T) {
	value, err := evalJSONFunction("JSON_EXTRACT", []interface{}{`{"items":[{"id":10},{"id":20}]}`, "$.items[*].id"})
	if err != nil {
		t.Fatalf("JSON_EXTRACT nested wildcard path: %v", err)
	}
	if fmt.Sprint(value) != `[10,20]` {
		t.Fatalf("JSON_EXTRACT nested wildcard path = %#v, want [10,20]", value)
	}
}

func TestJSONExtractSupportsArrayRangePath(t *testing.T) {
	value, err := evalJSONFunction("JSON_EXTRACT", []interface{}{`{"items":[10,20,30,40]}`, "$.items[1 to 2]"})
	if err != nil {
		t.Fatalf("JSON_EXTRACT array range path: %v", err)
	}
	if fmt.Sprint(value) != `[20,30]` {
		t.Fatalf("JSON_EXTRACT array range path = %#v, want [20,30]", value)
	}
}

func TestJSONExtractSupportsLastArrayIndexPath(t *testing.T) {
	last, err := evalJSONFunction("JSON_EXTRACT", []interface{}{`{"items":[10,20,30]}`, "$.items[last]"})
	if err != nil {
		t.Fatalf("JSON_EXTRACT last path: %v", err)
	}
	if fmt.Sprint(last) != "30" {
		t.Fatalf("JSON_EXTRACT last path = %#v, want 30", last)
	}
	previous, err := evalJSONFunction("JSON_EXTRACT", []interface{}{`{"items":[10,20,30]}`, "$.items[last-1]"})
	if err != nil {
		t.Fatalf("JSON_EXTRACT last-1 path: %v", err)
	}
	if fmt.Sprint(previous) != "20" {
		t.Fatalf("JSON_EXTRACT last-1 path = %#v, want 20", previous)
	}
}

func TestJSONExtractSupportsRecursiveDescentPath(t *testing.T) {
	value, err := evalJSONFunction("JSON_EXTRACT", []interface{}{`{"id":1,"nested":{"id":2,"deep":{"id":3}}}`, "$**.id"})
	if err != nil {
		t.Fatalf("JSON_EXTRACT recursive descent path: %v", err)
	}
	if fmt.Sprint(value) != `[1,2,3]` {
		t.Fatalf("JSON_EXTRACT recursive descent path = %#v, want [1,2,3]", value)
	}
}

func TestConstantExpression(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		wantType DataType
	}{
		{"Int", int64(42), TypeInt},
		{"Float", 3.14, TypeFloat},
		{"String", "hello", TypeString},
		{"Bool", true, TypeBoolean},
		{"Null", nil, TypeNull},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &Constant{
				BaseExpression: BaseExpression{resultType: tt.wantType},
				Value:          tt.value,
			}

			// Test type
			if got := expr.GetType(); got != tt.wantType {
				t.Errorf("Constant.GetType() = %v, want %v", got, tt.wantType)
			}

			// Test evaluation
			ctx := &EvalContext{}
			got, err := expr.Eval(ctx)
			if err != nil {
				t.Errorf("Constant.Eval() error = %v", err)
				return
			}
			if got != tt.value {
				t.Errorf("Constant.Eval() = %v, want %v", got, tt.value)
			}
		})
	}
}

func TestColumnExpression(t *testing.T) {
	tests := []struct {
		name    string
		col     string
		row     map[string]interface{}
		want    interface{}
		wantErr bool
	}{
		{
			name: "ExistingColumn",
			col:  "id",
			row:  map[string]interface{}{"id": int64(1)},
			want: int64(1),
		},
		{
			name:    "NonExistingColumn",
			col:     "unknown",
			row:     map[string]interface{}{"id": int64(1)},
			wantErr: true,
		},
		{
			name: "NullValue",
			col:  "name",
			row:  map[string]interface{}{"name": nil},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &Column{
				BaseExpression: BaseExpression{},
				Name:           tt.col,
			}

			ctx := &EvalContext{Row: tt.row}
			got, err := expr.Eval(ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Column.Eval() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Column.Eval() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinaryOperation(t *testing.T) {
	tests := []struct {
		name    string
		op      BinaryOp
		left    interface{}
		right   interface{}
		want    interface{}
		wantErr bool
	}{
		// 算术运算
		{"Add_Int", OpAdd, int64(1), int64(2), int64(3), false},
		{"Add_Float", OpAdd, float64(1.5), float64(2.5), float64(4.0), false},
		{"Sub_Int", OpSub, int64(5), int64(3), int64(2), false},
		{"Mul_Int", OpMul, int64(4), int64(3), int64(12), false},
		{"Div_Int", OpDiv, int64(10), int64(2), int64(5), false},
		{"Div_Zero", OpDiv, int64(1), int64(0), nil, true},

		// 比较运算
		{"EQ_Int", OpEQ, int64(1), int64(1), true, false},
		{"NE_Int", OpNE, int64(1), int64(2), true, false},
		{"LT_Int", OpLT, int64(1), int64(2), true, false},
		{"LE_Int", OpLE, int64(2), int64(2), true, false},
		{"GT_Int", OpGT, int64(3), int64(2), true, false},
		{"GE_Int", OpGE, int64(2), int64(2), true, false},

		// 逻辑运算
		{"And_True", OpAnd, true, true, true, false},
		{"And_False", OpAnd, true, false, false, false},
		{"Or_True", OpOr, true, false, true, false},
		{"Or_False", OpOr, false, false, false, false},

		// 字符串运算
		{"Like_Match", OpLike, "hello", "%ell%", true, false},
		{"Like_NoMatch", OpLike, "hello", "%xyz%", false, false},

		// NULL 处理：MySQL 的 NULL 算术结果为 SQL NULL，而不是错误。
		{"Add_Null", OpAdd, nil, int64(1), nil, false},
		{"EQ_Null", OpEQ, nil, nil, nil, false},
		{"LT_Null", OpLT, nil, int64(1), nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			left := &Constant{Value: tt.left}
			right := &Constant{Value: tt.right}
			expr := &BinaryOperation{
				BaseExpression: BaseExpression{},
				Op:             tt.op,
				Left:           left,
				Right:          right,
			}

			ctx := &EvalContext{}
			got, err := expr.Eval(ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("BinaryOperation.Eval() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("BinaryOperation.Eval() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFunction(t *testing.T) {
	tests := []struct {
		name    string
		fn      string
		args    []interface{}
		want    interface{}
		wantErr bool
	}{
		// COUNT
		{"Count_List", "COUNT", []interface{}{[]interface{}{1, nil, 3}}, int64(2), false},
		{"Count_Null", "COUNT", []interface{}{nil}, int64(0), false},
		{"Count_Single", "COUNT", []interface{}{42}, int64(1), false},

		// SUM
		{"Sum_List", "SUM", []interface{}{[]interface{}{int64(1), int64(2), int64(3)}}, float64(6), false},
		{"Sum_Null", "SUM", []interface{}{nil}, nil, false},
		{"Sum_Mixed", "SUM", []interface{}{[]interface{}{int64(1), nil, int64(3)}}, float64(4), false},

		// AVG
		{"Avg_List", "AVG", []interface{}{[]interface{}{int64(1), int64(2), int64(3)}}, float64(2), false},
		{"Avg_Null", "AVG", []interface{}{nil}, nil, false},
		{"Avg_Mixed", "AVG", []interface{}{[]interface{}{int64(1), nil, int64(3)}}, float64(2), false},

		// MAX
		{"Max_List", "MAX", []interface{}{[]interface{}{int64(1), int64(3), int64(2)}}, int64(3), false},
		{"Max_Null", "MAX", []interface{}{nil}, nil, false},
		{"Max_Mixed", "MAX", []interface{}{[]interface{}{int64(1), nil, int64(3)}}, int64(3), false},

		// MIN
		{"Min_List", "MIN", []interface{}{[]interface{}{int64(2), int64(1), int64(3)}}, int64(1), false},
		{"Min_Null", "MIN", []interface{}{nil}, nil, false},
		{"Min_Mixed", "MIN", []interface{}{[]interface{}{int64(2), nil, int64(1)}}, int64(1), false},

		// CONCAT
		{"Concat_Strings", "CONCAT", []interface{}{"Hello", " ", "World"}, "Hello World", false},
		{"Concat_Mixed", "CONCAT", []interface{}{"Value: ", int64(42)}, "Value: 42", false},
		{"Concat_Null", "CONCAT", []interface{}{"Hello", nil, "World"}, nil, false},

		// SUBSTRING
		{"Substring_Basic", "SUBSTRING", []interface{}{"Hello", int64(1), int64(2)}, "He", false},
		{"Substring_From", "SUBSTRING", []interface{}{"Hello", int64(2)}, "ello", false},
		{"Substring_Invalid", "SUBSTRING", []interface{}{"Hello", int64(10)}, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := make([]Expression, len(tt.args))
			for i, arg := range tt.args {
				args[i] = &Constant{Value: arg}
			}

			expr := &Function{
				BaseExpression: BaseExpression{},
				FuncName:       tt.fn,
				FuncArgs:       args,
			}

			ctx := &EvalContext{}
			got, err := expr.Eval(ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Function.Eval() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Function.Eval() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompatibilityFunctions(t *testing.T) {
	tests := []struct {
		name string
		fn   string
		args []interface{}
		want interface{}
	}{
		{"greatest numeric", "GREATEST", []interface{}{int64(2), int64(7), int64(4)}, int64(7)},
		{"least string", "LEAST", []interface{}{"beta", "alpha"}, "alpha"},
		{"greatest null", "GREATEST", []interface{}{int64(2), nil}, nil},
		{"date format", "DATE_FORMAT", []interface{}{"2024-03-05 14:06:07", "%Y-%m-%d %H:%i:%s"}, "2024-03-05 14:06:07"},
		{"str to date", "STR_TO_DATE", []interface{}{"2024/03/05", "%Y/%m/%d"}, "2024-03-05 00:00:00"},
		{"unix timestamp", "UNIX_TIMESTAMP", []interface{}{"1970-01-01 00:00:01"}, int64(1)},
		{"from unix time", "FROM_UNIXTIME", []interface{}{int64(1)}, "1970-01-01 00:00:01"},
		{"cast signed", "CAST", []interface{}{"42", "SIGNED"}, int64(42)},
		{"cast decimal", "CAST", []interface{}{"1.25", "DECIMAL"}, float64(1.25)},
		{"truncate", "TRUNCATE", []interface{}{float64(1.239), int64(2)}, float64(1.23)},
		{"format number", "FORMAT", []interface{}{float64(1234.5), int64(2)}, "1,234.50"},
		{"lpad", "LPAD", []interface{}{"7", int64(3), "0"}, "007"},
		{"find in set", "FIND_IN_SET", []interface{}{"b", "a,b,c"}, int64(2)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := make([]Expression, len(tt.args))
			for i, arg := range tt.args {
				args[i] = &Constant{Value: arg}
			}
			got, err := (&Function{FuncName: tt.fn, FuncArgs: args}).Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("Eval() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("Eval() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestExpressionString(t *testing.T) {
	tests := []struct {
		name string
		expr Expression
		want string
	}{
		{
			name: "Constant",
			expr: &Constant{Value: 42},
			want: "42",
		},
		{
			name: "Column",
			expr: &Column{Name: "id"},
			want: "id",
		},
		{
			name: "BinaryOperation",
			expr: &BinaryOperation{
				Op:    OpAdd,
				Left:  &Constant{Value: 1},
				Right: &Constant{Value: 2},
			},
			want: "(1 + 2)",
		},
		{
			name: "Function",
			expr: &Function{
				FuncName: "COUNT",
				FuncArgs: []Expression{&Column{Name: "id"}},
			},
			want: "COUNT(id)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.expr.String(); got != tt.want {
				t.Errorf("Expression.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInAndNotInPreserveSQLUnknownSemantics(t *testing.T) {
	tests := []struct {
		name string
		op   BinaryOp
		left interface{}
		list []interface{}
		want interface{}
	}{
		{name: "in with null is unknown", op: OpIn, left: int64(2), list: []interface{}{int64(1), nil}, want: nil},
		{name: "not in with null is unknown", op: OpNotIn, left: int64(2), list: []interface{}{int64(1), nil}, want: nil},
		{name: "not in without match", op: OpNotIn, left: int64(2), list: []interface{}{int64(1), int64(3)}, want: true},
		{name: "not in matching value", op: OpNotIn, left: int64(1), list: []interface{}{int64(1), nil}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := (&BinaryOperation{
				Op:    tt.op,
				Left:  &Constant{Value: tt.left},
				Right: &Constant{Value: tt.list},
			}).Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("Eval() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("Eval() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestComparisonsApplyMySQLNumericCoercion(t *testing.T) {
	tests := []struct {
		name  string
		op    BinaryOp
		left  interface{}
		right interface{}
		want  interface{}
	}{
		{name: "numeric string equality", op: OpEQ, left: "1", right: int64(1), want: true},
		{name: "numeric bytes equality", op: OpEQ, left: []byte("1"), right: int64(1), want: true},
		{name: "numeric string ordering", op: OpLT, left: "2", right: int64(10), want: true},
		{name: "mixed integer float equality", op: OpEQ, left: int64(2), right: float64(2), want: true},
		{name: "text remains lexical", op: OpLT, left: "10", right: "2", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := (&BinaryOperation{
				Op:    tt.op,
				Left:  &Constant{Value: tt.left},
				Right: &Constant{Value: tt.right},
			}).Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("Eval() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("Eval() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestArithmeticAppliesMySQLNumericCoercion(t *testing.T) {
	tests := []struct {
		name  string
		op    BinaryOp
		left  interface{}
		right interface{}
		want  interface{}
	}{
		{name: "numeric string addition", op: OpAdd, left: "2", right: int64(3), want: int64(5)},
		{name: "numeric bytes addition", op: OpAdd, left: []byte("2"), right: int64(3), want: int64(5)},
		{name: "decimal string multiplication", op: OpMul, left: "2.5", right: int64(2), want: float64(5)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := (&BinaryOperation{
				Op:    tt.op,
				Left:  &Constant{Value: tt.left},
				Right: &Constant{Value: tt.right},
			}).Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("Eval() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("Eval() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestLikeTreatsPatternTextAsLiteralAndHonorsEscapes(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		pattern string
		want    bool
	}{
		{name: "regex metacharacter is literal", value: "axb", pattern: "a.b", want: false},
		{name: "literal dot matches", value: "a.b", pattern: "a.b", want: true},
		{name: "escaped percent matches literal", value: "a%b", pattern: `a\%b`, want: true},
		{name: "percent remains wildcard", value: "aXXb", pattern: "a%b", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := (&BinaryOperation{
				Op:    OpLike,
				Left:  &Constant{Value: tt.value},
				Right: &Constant{Value: tt.pattern},
			}).Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("Eval() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("Eval() = %#v, want %#v", got, tt.want)
			}
		})
	}

	got, err := (&LikeExpression{Column: &Constant{Value: "a%b"}, Pattern: `a\%b`}).Eval(&EvalContext{})
	if err != nil || got != true {
		t.Fatalf("LikeExpression escaped percent = %#v, error=%v; want true", got, err)
	}
}

func TestLikeUsesDefaultCaseInsensitiveTextSemantics(t *testing.T) {
	expression := BuildExpression(&sqlparser.ComparisonExpr{
		Operator: sqlparser.LikeStr,
		Left:     sqlparser.NewStrVal([]byte("Alice")),
		Right:    sqlparser.NewStrVal([]byte("a%")),
	})
	if expression == nil {
		t.Fatal("LIKE expression was not built")
	}
	got, err := expression.Eval(&EvalContext{})
	if err != nil {
		t.Fatalf("LIKE evaluation failed: %v", err)
	}
	if got != true {
		t.Fatalf("LIKE evaluation = %#v, want true", got)
	}
}

func TestLikeEscapeSurvivesPlanConstructionAndCompilation(t *testing.T) {
	expression := BuildExpression(&sqlparser.ComparisonExpr{
		Operator: sqlparser.LikeStr,
		Left:     sqlparser.NewStrVal([]byte("a%b")),
		Right:    sqlparser.NewStrVal([]byte("a#%b")),
		Escape:   sqlparser.NewStrVal([]byte("#")),
	})
	if expression == nil {
		t.Fatal("LIKE ESCAPE expression was not built")
	}

	interpreted, err := expression.Eval(&EvalContext{})
	if err != nil {
		t.Fatalf("interpreted LIKE ESCAPE evaluation failed: %v", err)
	}
	if interpreted != true {
		t.Fatalf("interpreted LIKE ESCAPE value = %#v, want true", interpreted)
	}

	compiled, ok := CompileExpression(expression)
	if !ok || compiled == nil {
		t.Fatal("LIKE ESCAPE should be compilable")
	}
	compiledValue, err := compiled(&EvalContext{})
	if err != nil {
		t.Fatalf("compiled LIKE ESCAPE evaluation failed: %v", err)
	}
	if compiledValue != interpreted {
		t.Fatalf("compiled LIKE ESCAPE value = %#v, interpreted = %#v", compiledValue, interpreted)
	}
	if got := expression.String(); got != "(a%b LIKE a#%b ESCAPE #)" {
		t.Fatalf("LIKE ESCAPE String() = %q", got)
	}

	for name, normalized := range map[string]Expression{
		"cnf":        NewCNFConverter().ConvertToCNF(expression),
		"normalizer": NewExpressionNormalizer().Normalize(expression),
	} {
		t.Run(name, func(t *testing.T) {
			value, evalErr := normalized.Eval(&EvalContext{})
			if evalErr != nil {
				t.Fatalf("normalized LIKE ESCAPE evaluation failed: %v", evalErr)
			}
			if value != true {
				t.Fatalf("normalized LIKE ESCAPE value = %#v, want true; expression=%s", value, normalized.String())
			}
		})
	}
}

func TestNullSafeEqualityPreservesNullSemantics(t *testing.T) {
	tests := []struct {
		name  string
		left  interface{}
		right interface{}
		want  bool
	}{
		{name: "both null", left: nil, right: nil, want: true},
		{name: "one null", left: nil, right: int64(1), want: false},
		{name: "equal values", left: int64(1), right: int64(1), want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := (&BinaryOperation{
				Op:    OpNullSafeEQ,
				Left:  &Constant{Value: tt.left},
				Right: &Constant{Value: tt.right},
			}).Eval(&EvalContext{})
			if err != nil {
				t.Fatalf("Eval() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("Eval() = %#v, want %#v", got, tt.want)
			}
		})
	}
	if got := (&PlanBuilder{}).convertComparisonOp("<=>"); got != OpNullSafeEQ {
		t.Fatalf("convertComparisonOp(<=>) = %v, want OpNullSafeEQ", got)
	}
}

func TestNotExpressionPreservesUnknownSemantics(t *testing.T) {
	expression := &NotExpression{Operand: &BinaryOperation{
		Op:    OpEQ,
		Left:  &Constant{Value: nil},
		Right: &Constant{Value: int64(1)},
	}}
	got, err := expression.Eval(&EvalContext{Row: map[string]interface{}{}})
	if err != nil {
		t.Fatalf("NOT UNKNOWN returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("NOT UNKNOWN = %#v, want nil", got)
	}
}

func TestNotExpressionAcceptsNumericTruthValues(t *testing.T) {
	expression := &NotExpression{Operand: &Constant{Value: int64(0)}}
	got, err := expression.Eval(&EvalContext{Row: map[string]interface{}{}})
	if err != nil {
		t.Fatalf("NOT 0 returned error: %v", err)
	}
	if got != true {
		t.Fatalf("NOT 0 = %#v, want true", got)
	}
}

func TestNotLikePreservesPatternSemantics(t *testing.T) {
	expression := &BinaryOperation{
		Op:    OpNotLike,
		Left:  &Constant{Value: "abc"},
		Right: &Constant{Value: "a%"},
	}
	got, err := expression.Eval(&EvalContext{Row: map[string]interface{}{}})
	if err != nil {
		t.Fatalf("NOT LIKE returned error: %v", err)
	}
	if got != false {
		t.Fatalf("NOT LIKE matching value = %#v, want false", got)
	}
	if (&PlanBuilder{}).convertComparisonOp("not like") != OpNotLike {
		t.Fatal("PlanBuilder did not map NOT LIKE to OpNotLike")
	}
}

func TestNotBetweenBuildsAndEvaluatesWithNullSemantics(t *testing.T) {
	statement, err := sqlparser.Parse("select * from users where amount not between 1 and 2")
	if err != nil {
		t.Fatalf("parse NOT BETWEEN: %v", err)
	}
	condition := (&PlanBuilder{}).buildExpr(statement.(*sqlparser.Select).Where.Expr)
	between, ok := condition.(*BetweenExpression)
	if !ok {
		t.Fatalf("NOT BETWEEN expression = %T, want *BetweenExpression", condition)
	}
	if !between.Not {
		t.Fatal("NOT BETWEEN flag was not preserved")
	}

	for value, want := range map[interface{}]interface{}{
		int64(0): true,
		int64(2): false,
		nil:      nil,
	} {
		got, err := between.Eval(&EvalContext{Row: map[string]interface{}{"amount": value}})
		if err != nil {
			t.Fatalf("NOT BETWEEN value %#v returned error: %v", value, err)
		}
		if got != want {
			t.Errorf("NOT BETWEEN value %#v = %#v, want %#v", value, got, want)
		}
	}
}

func TestIsNullBuildsFromSQLPredicate(t *testing.T) {
	statement, err := sqlparser.Parse("select * from users where active is not null")
	if err != nil {
		t.Fatalf("parse IS NOT NULL: %v", err)
	}
	condition := (&PlanBuilder{}).buildExpr(statement.(*sqlparser.Select).Where.Expr)
	isNull, ok := condition.(*IsNullExpression)
	if !ok {
		t.Fatalf("IS NOT NULL expression = %T, want *IsNullExpression", condition)
	}
	if isNull.IsNull {
		t.Fatal("IS NOT NULL was built as IS NULL")
	}
}

func TestNotPredicateBuildsFromSQL(t *testing.T) {
	statement, err := sqlparser.Parse("select * from users where not (active = 1)")
	if err != nil {
		t.Fatalf("parse NOT predicate: %v", err)
	}
	condition := (&PlanBuilder{}).buildExpr(statement.(*sqlparser.Select).Where.Expr)
	if _, ok := condition.(*NotExpression); !ok {
		t.Fatalf("NOT predicate expression = %T, want *NotExpression", condition)
	}
}

func TestBooleanOperatorsAcceptSQLTruthValues(t *testing.T) {
	tests := []struct {
		name     string
		op       BinaryOp
		left     interface{}
		right    interface{}
		expected interface{}
	}{
		{name: "numeric AND true", op: OpAnd, left: int64(1), right: true, expected: true},
		{name: "zero OR true", op: OpOr, left: int64(0), right: true, expected: true},
		{name: "numeric string AND true", op: OpAnd, left: "2", right: true, expected: true},
		{name: "NULL AND false", op: OpAnd, left: nil, right: false, expected: false},
		{name: "NULL OR false", op: OpOr, left: nil, right: false, expected: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := (&BinaryOperation{
				Op:    tt.op,
				Left:  &Constant{Value: tt.left},
				Right: &Constant{Value: tt.right},
			}).Eval(&EvalContext{Row: map[string]interface{}{}})
			if err != nil {
				t.Fatalf("boolean expression error = %v", err)
			}
			if result != tt.expected {
				t.Fatalf("boolean expression = %#v, want %#v", result, tt.expected)
			}
		})
	}
}

func TestInExpressionPreservesUnknownForNull(t *testing.T) {
	tests := []struct {
		name     string
		column   interface{}
		values   []interface{}
		expected interface{}
	}{
		{name: "NULL candidate", column: int64(2), values: []interface{}{nil, int64(3)}, expected: nil},
		{name: "match dominates NULL", column: int64(2), values: []interface{}{nil, int64(2)}, expected: true},
		{name: "NULL column", column: nil, values: []interface{}{int64(2)}, expected: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := (&InExpression{
				Column: &Constant{Value: tt.column},
				Values: tt.values,
			}).Eval(&EvalContext{Row: map[string]interface{}{}})
			if err != nil {
				t.Fatalf("IN expression error = %v", err)
			}
			if result != tt.expected {
				t.Fatalf("IN expression = %#v, want %#v", result, tt.expected)
			}
		})
	}
}

func TestLikeExpressionsCoerceScalarValuesToText(t *testing.T) {
	for _, expr := range []Expression{
		&BinaryOperation{Op: OpLike, Left: &Constant{Value: int64(123)}, Right: &Constant{Value: "12%"}},
		&LikeExpression{Column: &Constant{Value: int64(123)}, Pattern: "12%"},
	} {
		result, err := expr.Eval(&EvalContext{Row: map[string]interface{}{}})
		if err != nil {
			t.Fatalf("LIKE expression error = %v", err)
		}
		if result != true {
			t.Fatalf("LIKE expression = %#v, want true", result)
		}
	}
}

func TestIsTruthPredicatesBuildAndEvaluate(t *testing.T) {
	tests := []struct {
		name     string
		operator string
		value    interface{}
		want     interface{}
	}{
		{name: "is true", operator: "is true", value: int64(1), want: true},
		{name: "is true null", operator: "is true", value: nil, want: false},
		{name: "is false", operator: "is false", value: int64(0), want: true},
		{name: "is false null", operator: "is false", value: nil, want: false},
		{name: "is not true", operator: "is not true", value: nil, want: true},
		{name: "is not false", operator: "is not false", value: nil, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statement, err := sqlparser.Parse(fmt.Sprintf("select * from users where active %s", tt.operator))
			if err != nil {
				t.Fatalf("parse %s: %v", tt.operator, err)
			}
			condition := (&PlanBuilder{}).buildExpr(statement.(*sqlparser.Select).Where.Expr)
			truth, ok := condition.(*IsTruthExpression)
			if !ok {
				t.Fatalf("%s expression = %T, want *IsTruthExpression", tt.operator, condition)
			}
			got, err := truth.Eval(&EvalContext{Row: map[string]interface{}{"active": tt.value}})
			if err != nil {
				t.Fatalf("Eval %s: %v", tt.operator, err)
			}
			if got != tt.want {
				t.Fatalf("%#v %s = %#v, want %#v", tt.value, tt.operator, got, tt.want)
			}
		})
	}
}

func TestUnaryAndCaseExpressionsBuildForProjection(t *testing.T) {
	statement, err := sqlparser.Parse("select -amount, case when amount > 10 then 'high' else 'low' end from users")
	if err != nil {
		t.Fatalf("parse unary and CASE expressions: %v", err)
	}
	selectStmt := statement.(*sqlparser.Select)
	if len(selectStmt.SelectExprs) != 2 {
		t.Fatalf("select expression count = %d, want 2", len(selectStmt.SelectExprs))
	}

	negated := (&PlanBuilder{}).buildExpr(selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr).Expr)
	if _, ok := negated.(*UnaryOperation); !ok {
		t.Fatalf("unary expression = %T, want *UnaryOperation", negated)
	}
	got, err := negated.Eval(&EvalContext{Row: map[string]interface{}{"amount": int64(12)}})
	if err != nil || got != int64(-12) {
		t.Fatalf("-amount = %#v, %v; want -12", got, err)
	}

	caseExpr := (&PlanBuilder{}).buildExpr(selectStmt.SelectExprs[1].(*sqlparser.AliasedExpr).Expr)
	casePlan, ok := caseExpr.(*CaseExpression)
	if !ok {
		t.Fatalf("CASE expression = %T, want *CaseExpression", caseExpr)
	}
	got, err = casePlan.Eval(&EvalContext{Row: map[string]interface{}{"amount": int64(12)}})
	if err != nil || got != "high" {
		t.Fatalf("CASE high = %#v, %v; want high", got, err)
	}
	got, err = casePlan.Eval(&EvalContext{Row: map[string]interface{}{"amount": int64(3)}})
	if err != nil || got != "low" {
		t.Fatalf("CASE low = %#v, %v; want low", got, err)
	}
}

func TestPlanBuilderCoversSubstringAndLiteralForms(t *testing.T) {
	substring := (&PlanBuilder{}).buildExpr(&sqlparser.SubstrExpr{
		Name: &sqlparser.ColName{Name: sqlparser.NewColIdent("name")},
		From: sqlparser.NewIntVal([]byte("2")),
	})
	got, err := substring.Eval(&EvalContext{Row: map[string]interface{}{"name": "abcd"}})
	if err != nil || got != "bcd" {
		t.Fatalf("SUBSTRING = %#v, %v; want bcd", got, err)
	}

	hexLiteral := (&PlanBuilder{}).buildExpr(sqlparser.NewHexNum([]byte("0x10")))
	if constant, ok := hexLiteral.(*Constant); !ok || constant.Value != int64(16) {
		t.Fatalf("hex literal = %#v, want int64(16)", hexLiteral)
	}
	bitLiteral := (&PlanBuilder{}).buildExpr(sqlparser.NewBitVal([]byte("1010")))
	if constant, ok := bitLiteral.(*Constant); !ok || constant.Value != int64(10) {
		t.Fatalf("bit literal = %#v, want int64(10)", bitLiteral)
	}
}

func TestDynamicBetweenBoundsRemainInPlan(t *testing.T) {
	statement, err := sqlparser.Parse("select * from users where amount between lower_bound and upper_bound")
	if err != nil {
		t.Fatalf("parse dynamic BETWEEN: %v", err)
	}
	condition := (&PlanBuilder{}).buildExpr(statement.(*sqlparser.Select).Where.Expr)
	between, ok := condition.(*BetweenExpression)
	if !ok {
		t.Fatalf("dynamic BETWEEN expression = %T, want *BetweenExpression", condition)
	}
	if between.LowerExpr == nil || between.UpperExpr == nil {
		t.Fatal("dynamic BETWEEN bounds were dropped from the plan")
	}

	got, err := between.Eval(&EvalContext{Row: map[string]interface{}{
		"amount":      int64(5),
		"lower_bound": int64(1),
		"upper_bound": int64(10),
	}})
	if err != nil || got != true {
		t.Fatalf("dynamic BETWEEN = %#v, %v; want true", got, err)
	}
}

func TestDynamicInTupleRemainsInPlan(t *testing.T) {
	statement, err := sqlparser.Parse("select * from users where amount in (lower_bound, upper_bound)")
	if err != nil {
		t.Fatalf("parse dynamic IN: %v", err)
	}
	condition := (&PlanBuilder{}).buildExpr(statement.(*sqlparser.Select).Where.Expr)
	inOperation, ok := condition.(*BinaryOperation)
	if !ok || inOperation.Op != OpIn {
		t.Fatalf("dynamic IN expression = %#v, want OpIn BinaryOperation", condition)
	}
	if _, ok := inOperation.Right.(*TupleExpression); !ok {
		t.Fatalf("dynamic IN right operand = %T, want *TupleExpression", inOperation.Right)
	}

	got, err := inOperation.Eval(&EvalContext{Row: map[string]interface{}{
		"amount":      int64(10),
		"lower_bound": int64(1),
		"upper_bound": int64(10),
	}})
	if err != nil || got != true {
		t.Fatalf("dynamic IN = %#v, %v; want true", got, err)
	}
}
