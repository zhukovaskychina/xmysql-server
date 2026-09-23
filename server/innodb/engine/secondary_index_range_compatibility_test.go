package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/manager"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/metadata"
)

func TestSecondaryIndexPrefixRangePredicateRecognizesRangeAfterEqualityPrefix(t *testing.T) {
	equalities, column, operator, value, ok := secondaryIndexPrefixRangePredicates([]string{
		"tenant_id = 7 AND email >= 'm'",
	})

	require.True(t, ok)
	require.Equal(t, map[string]string{"tenant_id": "7"}, equalities)
	require.Equal(t, "email", column)
	require.Equal(t, ">=", operator)
	require.Equal(t, "m", value)
}

func TestSecondaryIndexPrefixRangePredicateRejectsNonPrefixOnlyRange(t *testing.T) {
	_, _, _, _, ok := secondaryIndexPrefixRangePredicates([]string{
		"email >= 'm'",
	})

	require.False(t, ok)
}

func TestSecondaryIndexRangeResidualUsesColumnTypeInsteadOfTextOrder(t *testing.T) {
	index := &manager.Index{
		Name:    "idx_score",
		Columns: []manager.Column{{Name: "score"}},
	}
	tableMeta := &metadata.TableMeta{Columns: []*metadata.ColumnMeta{{Name: "score", Type: metadata.TypeInt}}}
	key, err := manager.EncodeSecondaryIndexKey(11, metadata.IndexMeta{Name: "idx_score", Columns: []string{"score"}}, map[string]interface{}{"score": "10"}, []byte("pk-10"))
	require.NoError(t, err)

	require.True(t, secondaryIndexEntryMatchesPredicate(key, index, tableMeta, secondaryIndexPredicateBranch{
		column: "score", operator: ">", value: "2",
	}))
	require.False(t, secondaryIndexEntryMatchesPredicate(key, index, tableMeta, secondaryIndexPredicateBranch{
		column: "score", operator: "<", value: "2",
	}))
}

func TestSecondaryIndexRangeResidualUsesLexicalOrderForText(t *testing.T) {
	index := &manager.Index{
		Name:    "idx_email",
		Columns: []manager.Column{{Name: "email"}},
	}
	tableMeta := &metadata.TableMeta{Columns: []*metadata.ColumnMeta{{Name: "email", Type: metadata.TypeVarchar}}}
	key, err := manager.EncodeSecondaryIndexKey(11, metadata.IndexMeta{Name: "idx_email", Columns: []string{"email"}}, map[string]interface{}{"email": "m"}, []byte("pk-m"))
	require.NoError(t, err)

	require.True(t, secondaryIndexEntryMatchesPredicate(key, index, tableMeta, secondaryIndexPredicateBranch{
		column: "email", operator: ">=", value: "m",
	}))
	require.False(t, secondaryIndexEntryMatchesPredicate(key, index, tableMeta, secondaryIndexPredicateBranch{
		column: "email", operator: "<", value: "m",
	}))
}

func TestSplitIntersectExceptQuery(t *testing.T) {
	branches, operators, ok := splitIntersectExceptQuery("select 1 as n intersect select 1")
	require.True(t, ok)
	require.Equal(t, []string{"select 1 as n", "select 1"}, branches)
	require.Equal(t, []intersectExceptOperator{{kind: "INTERSECT"}}, operators)
}
