package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRewriteExtractDateFunctionsSkipsQuotedText(t *testing.T) {
	rewritten, err := rewriteExtractDateFunctions("select extract(year from created), 'extract(month from ignored)'")
	require.NoError(t, err)
	require.Equal(t, "select YEAR(created), 'extract(month from ignored)'", rewritten)
}

func TestRewriteExtractDateFunctionsRejectsUnknownUnits(t *testing.T) {
	_, err := rewriteExtractDateFunctions("select extract(unknown_unit from created)")
	require.EqualError(t, err, "unsupported EXTRACT unit UNKNOWN_UNIT")
}

func TestRewriteExtractDateFunctionsSupportsCompoundUnits(t *testing.T) {
	rewritten, err := rewriteExtractDateFunctions("select extract(day_microsecond from created), extract(year_month from created)")
	require.NoError(t, err)
	require.Equal(t, "select EXTRACT_DAY_MICROSECOND(created), EXTRACT_YEAR_MONTH(created)", rewritten)
}

func TestExtractCompoundUnitsExecuteThroughEngine(t *testing.T) {
	executor := newTestStorageIntegratedExecutor(t, t.TempDir())

	rows := mustQuerySQL(t, executor, "", "select extract(day_microsecond from '2024-01-02 03:04:05.123456'), extract(year_month from '2024-01-02 03:04:05.123456'), extract(second_microsecond from '2024-01-02 03:04:05.123456')")
	require.Len(t, rows, 1)
	require.Equal(t, "2030405.123456", rows[0][0])
	require.Equal(t, "202401", rows[0][1])
	require.Equal(t, "5.123456", rows[0][2])
}
