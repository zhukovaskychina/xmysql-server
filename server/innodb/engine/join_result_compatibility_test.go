package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMySQLJoinResultShapeCompatibility(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, executor, "", "create database join_compat")
	mustExecSQL(t, executor, "join_compat", "create table l (lid int primary key, k int, a varchar(20))")
	mustExecSQL(t, executor, "join_compat", "create table r (rid int primary key, k int, b varchar(20))")
	mustExecSQL(t, executor, "join_compat", "create table e (lid int primary key, k int, a varchar(20))")
	mustExecSQL(t, executor, "join_compat", "insert into l values (1,10,'A'),(2,20,'B'),(3,NULL,'C')")
	mustExecSQL(t, executor, "join_compat", "insert into r values (10,10,'X'),(40,40,'Y'),(50,NULL,'Z')")
	for _, test := range []struct {
		name, sql string
		columns   []string
		rows      [][]interface{}
	}{
		{"using_star", "select * from l left join r using (k) order by l.lid", []string{"k", "lid", "a", "rid", "b"}, [][]interface{}{{"10", "1", "A", "10", "X"}, {"20", "2", "B", nil, nil}, {nil, "3", "C", nil, nil}}},
		{"natural_left", "select * from l natural left join r order by l.lid", []string{"k", "lid", "a", "rid", "b"}, [][]interface{}{{"10", "1", "A", "10", "X"}, {"20", "2", "B", nil, nil}, {nil, "3", "C", nil, nil}}},
		{"qualified_star", "select l.*, r.* from l left join r using(k) where l.lid=2", []string{"lid", "k", "a", "rid", "k", "b"}, [][]interface{}{{"2", "20", "B", nil, nil, nil}}},
		{"empty_left_right_join", "select e.lid,r.rid from e right join r using(k) order by r.rid", []string{"lid", "rid"}, [][]interface{}{{nil, "10"}, {nil, "40"}, {nil, "50"}}},
		{"join_alias_order", "select l.lid, -l.lid as rev from l left join r using(k) order by rev limit 2", []string{"lid", "rev"}, [][]interface{}{{"3", "-3"}, {"2", "-2"}}},
		{"join_ordinal_order", "select l.lid, -l.lid as rev from l left join r using(k) order by 2 limit 2", []string{"lid", "rev"}, [][]interface{}{{"3", "-3"}, {"2", "-2"}}},
		{"join_boolean", "select l.k = r.k as matched from l left join r using(k) order by l.lid", []string{"matched"}, [][]interface{}{{"1"}, {nil}, {nil}}},
		{"right_on_star", "select * from l right join r on l.k=r.k where r.rid=40", []string{"lid", "k", "a", "rid", "k", "b"}, [][]interface{}{{nil, nil, nil, "40", "40", "Y"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := mustSelectResultSQL(t, executor, "join_compat", test.sql)
			require.Equal(t, test.columns, result.Columns)
			require.Equal(t, test.rows, mustQuerySQL(t, executor, "join_compat", test.sql))
		})
	}
}
