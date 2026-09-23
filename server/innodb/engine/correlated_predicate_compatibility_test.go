package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCorrelatedPredicateCompatibilityRejectsUnsafeORPrefilter(t *testing.T) {
	// An OR predicate surrounding the correlated subquery cannot be applied to
	// the outer candidate query after the subquery itself is removed.
	prefix, prefixConnector := splitCorrelatedConditionConnector("p.active = 1 OR", false)
	suffix, suffixConnector := splitCorrelatedConditionConnector("OR p.active = 1", true)
	require.Equal(t, "p.active = 1", prefix)
	require.Equal(t, "OR", prefixConnector)
	require.Equal(t, "p.active = 1", suffix)
	require.Equal(t, "OR", suffixConnector)

	andPrefix, andConnector := splitCorrelatedConditionConnector("p.active = 1 AND", false)
	require.Equal(t, "p.active = 1", andPrefix)
	require.Equal(t, "AND", andConnector)
}
