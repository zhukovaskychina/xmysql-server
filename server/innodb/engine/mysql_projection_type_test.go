package engine

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMySQLProjectionTypesSurviveNullAndEmptyMaterialization(t *testing.T) {
	db := newTestStorageIntegratedExecutor(t, t.TempDir())
	mustExecSQL(t, db, "", "CREATE DATABASE app")
	mustExecSQL(t, db, "app", "CREATE TABLE lhs(id INT PRIMARY KEY,k INT,flag TINYINT)")
	mustExecSQL(t, db, "app", "CREATE TABLE rhs(id INT PRIMARY KEY,k INT)")
	mustExecSQL(t, db, "app", "INSERT INTO lhs VALUES(1,10,1),(2,NULL,0)")
	mustExecSQL(t, db, "app", "INSERT INTO rhs VALUES(1,10)")
	for _, test := range []struct {
		query string
		types []string
		rows  int
	}{
		{"SELECT flag,k <=> NULL AS missing FROM lhs ORDER BY id", []string{"tinyint", "bigint"}, 2},
		{"SELECT flag,k=10 AS matches FROM lhs WHERE id<0", []string{"tinyint", "bigint"}, 0},
		{"SELECT TRUE AS yes,FALSE AS no,1=1 AS equal_value", []string{"bigint", "bigint", "bigint"}, 1},
		{"SELECT l.k=r.k AS same_value FROM lhs l JOIN rhs r ON l.id=r.id", []string{"bigint"}, 1},
		{"SELECT l.k=r.k AS same_value FROM lhs l JOIN rhs r ON l.id=r.id ORDER BY l.id", []string{"int"}, 1},
		{"SELECT l.k IS NULL AS missing FROM lhs l JOIN rhs r ON l.id=r.id ORDER BY l.id", []string{"int"}, 1},
		{"SELECT q.id,q.k,q.flag FROM (SELECT id,k,flag FROM lhs) q ORDER BY q.id DESC", []string{"int", "int", "tinyint"}, 2},
		{"SELECT q.id,q.k FROM (SELECT id,k FROM lhs) q WHERE q.id<0", []string{"int", "int"}, 0},
		{"SELECT q.* FROM (SELECT id,k,flag FROM lhs) q WHERE q.id<0", []string{"int", "int", "tinyint"}, 0},
		{"WITH a AS(SELECT id,k FROM lhs) SELECT id,k FROM a ORDER BY id DESC", []string{"int", "int"}, 2},
		{"WITH a(x,y) AS(SELECT id,k FROM lhs) SELECT x,y FROM a WHERE x<0", []string{"int", "int"}, 0},
		{"SELECT l.id,(SELECT r.k FROM rhs r WHERE r.id=l.id) AS matching FROM lhs l ORDER BY l.id DESC", []string{"int", "int"}, 2},
		{"SELECT l.id,(SELECT r.k FROM rhs r WHERE r.id=l.id) AS matching FROM lhs l WHERE l.id<0", []string{"int", "int"}, 0},
	} {
		t.Run(test.query, func(t *testing.T) {
			result := <-db.ExecuteQuery(nil, test.query, "app")
			require.NoError(t, result.Err)
			selected, ok := result.Data.(*SelectResult)
			require.True(t, ok)
			types := append([]string(nil), selected.ColumnTypes...)
			for index := range types {
				types[index] = strings.ToLower(types[index])
			}
			require.Equal(t, test.types, types)
			require.Len(t, selected.Records, test.rows)
			if strings.Contains(test.query, "AS matching") && test.rows > 0 {
				require.Equal(t, []string{"id", "matching"}, selected.Columns)
				require.True(t, selected.Records[0].GetValues()[1].IsNull(), "the first NULL scalar must remain NULL with its declared INT type")
			}
		})
	}
}
