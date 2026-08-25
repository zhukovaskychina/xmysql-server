package sqltypes

import querypb "github.com/zhukovaskychina/xmysql-server/server/innodb/sqlparser/dependency/querypb"

func testVal(typ querypb.Type, val string) Value {
	return MakeTrusted(typ, []byte(val))
}

func makePretty(v Value) string {
	return v.String()
}
