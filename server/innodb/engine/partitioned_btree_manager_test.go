package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionedBTreeManagerPrunesPhysicalFanoutForConstantRange(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "RANGE",
		expression:  "id",
		definitions: []interface{}{map[string]interface{}{"name": "p0", "values": "less than (100)"}, map[string]interface{}{"name": "p1", "values": "less than (200)"}, map[string]interface{}{"name": "pmax", "values": "maxvalue"}},
		partitions:  []partitionedBTree{{name: "p0"}, {name: "p1"}, {name: "pmax"}},
	}
	router.RestrictToWherePartitions([]string{"id = 150"})
	require.Len(t, router.partitions, 1)
	require.Equal(t, "p1", router.partitions[0].name)
}

func TestPartitionedBTreeManagerPrunesStringRangeValues(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "RANGE",
		expression:  "region",
		definitions: []interface{}{map[string]interface{}{"name": "p0", "values": "less than ('m')"}, map[string]interface{}{"name": "pmax", "values": "maxvalue"}},
		partitions:  []partitionedBTree{{name: "p0"}, {name: "pmax"}},
	}
	router.RestrictToWherePartitions([]string{"region = 'alpha'"})
	require.Len(t, router.partitions, 1)
	require.Equal(t, "p0", router.partitions[0].name)

	router = &partitionedBTreeManager{
		method:      "RANGE",
		expression:  "region",
		definitions: []interface{}{map[string]interface{}{"name": "p0", "values": "less than ('m')"}, map[string]interface{}{"name": "pmax", "values": "maxvalue"}},
		partitions:  []partitionedBTree{{name: "p0"}, {name: "pmax"}},
	}
	router.RestrictToWherePartitions([]string{"region >= 'zulu'"})
	require.Len(t, router.partitions, 1)
	require.Equal(t, "pmax", router.partitions[0].name)
}

func TestPartitionedBTreeManagerPrunesYearFunctionRange(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "RANGE",
		expression:  "year(event_date)",
		definitions: []interface{}{map[string]interface{}{"name": "p2024", "values": "less than (2025)"}, map[string]interface{}{"name": "pmax", "values": "maxvalue"}},
		partitions:  []partitionedBTree{{name: "p2024"}, {name: "pmax"}},
	}
	router.RestrictToWherePartitions([]string{"year(event_date) = 2024"})
	require.Len(t, router.partitions, 1)
	require.Equal(t, "p2024", router.partitions[0].name)
}

func TestPartitionedBTreeManagerPrunesArithmeticRange(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "RANGE",
		expression:  "id + 10",
		definitions: []interface{}{map[string]interface{}{"name": "p0", "values": "less than (20)"}, map[string]interface{}{"name": "p1", "values": "maxvalue"}},
		partitions:  []partitionedBTree{{name: "p0"}, {name: "p1"}},
	}
	router.RestrictToWherePartitions([]string{"id + 10 = 15"})
	require.Len(t, router.partitions, 1)
	require.Equal(t, "p0", router.partitions[0].name)
}

func TestPartitionedBTreeManagerIntersectsStringBetweenRange(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "RANGE",
		expression:  "region",
		definitions: []interface{}{map[string]interface{}{"name": "p0", "values": "less than ('m')"}, map[string]interface{}{"name": "pmax", "values": "maxvalue"}},
		partitions:  []partitionedBTree{{name: "p0"}, {name: "pmax"}},
	}
	router.RestrictToWherePartitions([]string{"region BETWEEN 'alpha' AND 'lemon'"})
	require.Len(t, router.partitions, 1)
	require.Equal(t, "p0", router.partitions[0].name)
}

func TestPartitionedBTreeManagerUnionsStringNotBetweenRange(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "RANGE",
		expression:  "region",
		definitions: []interface{}{map[string]interface{}{"name": "p0", "values": "less than ('m')"}, map[string]interface{}{"name": "p1", "values": "less than ('z')"}, map[string]interface{}{"name": "pmax", "values": "maxvalue"}},
		partitions:  []partitionedBTree{{name: "p0"}, {name: "p1"}, {name: "pmax"}},
	}
	router.RestrictToWherePartitions([]string{"region NOT BETWEEN 'm' AND 'z'"})
	require.Len(t, router.partitions, 2)
	require.Equal(t, []string{"p0", "pmax"}, []string{router.partitions[0].name, router.partitions[1].name})
}

func TestPartitionedBTreeManagerPrunesMultiColumnRangeValues(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "RANGE",
		expression:  "tenant_id, region",
		definitions: []interface{}{map[string]interface{}{"name": "p0", "values": "less than (10, 'm')"}, map[string]interface{}{"name": "pmax", "values": "maxvalue"}},
		partitions:  []partitionedBTree{{name: "p0"}, {name: "pmax"}},
	}
	router.RestrictToWherePartitions([]string{"(tenant_id, region) = (10, 'alpha')"})
	require.Len(t, router.partitions, 1)
	require.Equal(t, "p0", router.partitions[0].name)

	router = &partitionedBTreeManager{
		method:      "RANGE",
		expression:  "tenant_id, region",
		definitions: []interface{}{map[string]interface{}{"name": "p0", "values": "less than (10, 'm')"}, map[string]interface{}{"name": "pmax", "values": "maxvalue"}},
		partitions:  []partitionedBTree{{name: "p0"}, {name: "pmax"}},
	}
	router.RestrictToWherePartitions([]string{"(tenant_id, region) >= (10, 'zulu')"})
	require.Len(t, router.partitions, 1)
	require.Equal(t, "pmax", router.partitions[0].name)
}

func TestPartitionedBTreeManagerPrunesMultiColumnListValues(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "LIST",
		expression:  "tenant_id, region",
		definitions: []interface{}{map[string]interface{}{"name": "p_east", "values": "values in ((1, 'east'), (2, 'west'))"}, map[string]interface{}{"name": "p_south", "values": "values in ((3, 'south'))"}},
		partitions:  []partitionedBTree{{name: "p_east"}, {name: "p_south"}},
	}
	router.RestrictToWherePartitions([]string{"(tenant_id, region) IN ((3, 'south'))"})
	require.Len(t, router.partitions, 1)
	require.Equal(t, "p_south", router.partitions[0].name)
}

func TestPartitionedBTreeManagerPrunesMultiColumnNotInListValues(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "LIST",
		expression:  "tenant_id, region",
		definitions: []interface{}{map[string]interface{}{"name": "p_east", "values": "values in ((1, 'east'), (2, 'west'))"}, map[string]interface{}{"name": "p_south", "values": "values in ((3, 'south'))"}},
		partitions:  []partitionedBTree{{name: "p_east"}, {name: "p_south"}},
	}
	router.RestrictToWherePartitions([]string{"(tenant_id, region) NOT IN ((1, 'east'), (2, 'west'))"})
	require.Len(t, router.partitions, 1)
	require.Equal(t, "p_south", router.partitions[0].name)
}

func TestPartitionedBTreeManagerIntersectsMultiColumnBetweenRange(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "RANGE",
		expression:  "tenant_id, region",
		definitions: []interface{}{map[string]interface{}{"name": "p0", "values": "less than (10, 'm')"}, map[string]interface{}{"name": "p1", "values": "less than (20, 'm')"}, map[string]interface{}{"name": "pmax", "values": "maxvalue"}},
		partitions:  []partitionedBTree{{name: "p0"}, {name: "p1"}, {name: "pmax"}},
	}
	router.RestrictToWherePartitions([]string{"(tenant_id, region) BETWEEN (10, 'a') AND (10, 'l')"})
	require.Len(t, router.partitions, 1)
	require.Equal(t, "p0", router.partitions[0].name)
}

func TestPartitionedBTreeManagerUnionsMultiColumnNotBetweenRange(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "RANGE",
		expression:  "tenant_id, region",
		definitions: []interface{}{map[string]interface{}{"name": "p0", "values": "less than (10, 'm')"}, map[string]interface{}{"name": "p1", "values": "less than (20, 'm')"}, map[string]interface{}{"name": "pmax", "values": "maxvalue"}},
		partitions:  []partitionedBTree{{name: "p0"}, {name: "p1"}, {name: "pmax"}},
	}
	router.RestrictToWherePartitions([]string{"(tenant_id, region) NOT BETWEEN (10, 'm') AND (20, 'm')"})
	require.Len(t, router.partitions, 2)
	require.Equal(t, []string{"p0", "pmax"}, []string{router.partitions[0].name, router.partitions[1].name})
}

func TestPartitionedBTreeManagerIntersectsMultipleConstantRanges(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "RANGE",
		expression:  "id",
		definitions: []interface{}{map[string]interface{}{"name": "p0", "values": "less than (100)"}, map[string]interface{}{"name": "p1", "values": "less than (200)"}, map[string]interface{}{"name": "pmax", "values": "maxvalue"}},
		partitions:  []partitionedBTree{{name: "p0"}, {name: "p1"}, {name: "pmax"}},
	}
	router.RestrictToWherePartitions([]string{"id >= 100", "id < 200"})
	require.Len(t, router.partitions, 1)
	require.Equal(t, "p1", router.partitions[0].name)
}

func TestPartitionedBTreeManagerKeepsAllPhysicalPartitionsForUnsupportedPredicate(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "RANGE",
		expression:  "id",
		definitions: []interface{}{map[string]interface{}{"name": "p0", "values": "less than (100)"}, map[string]interface{}{"name": "pmax", "values": "maxvalue"}},
		partitions:  []partitionedBTree{{name: "p0"}, {name: "pmax"}},
	}
	router.RestrictToWherePartitions([]string{"id + 1 = 150"})
	require.Len(t, router.partitions, 2)
}

func TestPartitionedBTreeManagerUnionsTopLevelOrRanges(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "RANGE",
		expression:  "id",
		definitions: []interface{}{map[string]interface{}{"name": "p0", "values": "less than (100)"}, map[string]interface{}{"name": "p1", "values": "less than (200)"}, map[string]interface{}{"name": "pmax", "values": "maxvalue"}},
		partitions:  []partitionedBTree{{name: "p0"}, {name: "p1"}, {name: "pmax"}},
	}
	router.RestrictToWherePartitions([]string{"id < 100 OR id >= 200"})
	require.Len(t, router.partitions, 2)
	require.Equal(t, []string{"p0", "pmax"}, []string{router.partitions[0].name, router.partitions[1].name})
}

func TestPartitionedBTreeManagerPrunesConstantInValues(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "RANGE",
		expression:  "id",
		definitions: []interface{}{map[string]interface{}{"name": "p0", "values": "less than (100)"}, map[string]interface{}{"name": "p1", "values": "less than (200)"}, map[string]interface{}{"name": "pmax", "values": "maxvalue"}},
		partitions:  []partitionedBTree{{name: "p0"}, {name: "p1"}, {name: "pmax"}},
	}
	router.RestrictToWherePartitions([]string{"id IN (1, 150)"})
	require.Len(t, router.partitions, 2)
	require.Equal(t, []string{"p0", "p1"}, []string{router.partitions[0].name, router.partitions[1].name})
}

func TestPartitionedBTreeManagerPrunesStringListValues(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "LIST",
		expression:  "region",
		definitions: []interface{}{map[string]interface{}{"name": "p_east", "values": "values in ('east', 'west')"}, map[string]interface{}{"name": "p_south", "values": "values in ('south')"}},
		partitions:  []partitionedBTree{{name: "p_east"}, {name: "p_south"}},
	}
	router.RestrictToWherePartitions([]string{"region IN ('west', 'south')"})
	require.Len(t, router.partitions, 2)
	require.Equal(t, []string{"p_east", "p_south"}, []string{router.partitions[0].name, router.partitions[1].name})
}

func TestPartitionedBTreeManagerPrunesConstantNotInListValues(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "LIST",
		expression:  "region",
		definitions: []interface{}{map[string]interface{}{"name": "p_east", "values": "values in ('east', 'west')"}, map[string]interface{}{"name": "p_south", "values": "values in ('south')"}},
		partitions:  []partitionedBTree{{name: "p_east"}, {name: "p_south"}},
	}
	router.RestrictToWherePartitions([]string{"region NOT IN ('east', 'west')"})
	require.Len(t, router.partitions, 1)
	require.Equal(t, "p_south", router.partitions[0].name)
}

func TestPartitionedBTreeManagerIntersectsConstantBetweenRange(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "RANGE",
		expression:  "id",
		definitions: []interface{}{map[string]interface{}{"name": "p0", "values": "less than (100)"}, map[string]interface{}{"name": "p1", "values": "less than (200)"}, map[string]interface{}{"name": "pmax", "values": "maxvalue"}},
		partitions:  []partitionedBTree{{name: "p0"}, {name: "p1"}, {name: "pmax"}},
	}
	router.RestrictToWherePartitions([]string{"id BETWEEN 100 AND 199"})
	require.Len(t, router.partitions, 1)
	require.Equal(t, "p1", router.partitions[0].name)
}

func TestPartitionedBTreeManagerUnionsConstantNotBetweenRanges(t *testing.T) {
	router := &partitionedBTreeManager{
		method:      "RANGE",
		expression:  "id",
		definitions: []interface{}{map[string]interface{}{"name": "p0", "values": "less than (100)"}, map[string]interface{}{"name": "p1", "values": "less than (200)"}, map[string]interface{}{"name": "pmax", "values": "maxvalue"}},
		partitions:  []partitionedBTree{{name: "p0"}, {name: "p1"}, {name: "pmax"}},
	}
	router.RestrictToWherePartitions([]string{"id NOT BETWEEN 100 AND 200"})
	require.Len(t, router.partitions, 2)
	require.Equal(t, []string{"p0", "pmax"}, []string{router.partitions[0].name, router.partitions[1].name})
}
