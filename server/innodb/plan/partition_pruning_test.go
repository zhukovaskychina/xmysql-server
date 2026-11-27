package plan

import (
	"reflect"
	"testing"
)

func TestPartitionPruningConstantAndRangeMatrix(t *testing.T) {
	limit10, limit20 := int64(10), int64(20)
	rules := []PartitionRule{{Name: "p0", LessThan: &limit10}, {Name: "p1", LessThan: &limit20}, {Name: "p2", MaxValue: true}}
	cases := []struct {
		method, op string
		value      interface{}
		rules      []PartitionRule
		want       []int
	}{
		{"RANGE", "=", int64(7), rules, []int{0}},
		{"RANGE", "=", int64(15), rules, []int{1}},
		{"RANGE", "<", int64(15), rules, []int{0, 1}},
		{"RANGE", "<=", int64(10), rules, []int{0, 1}},
		{"RANGE", ">", int64(15), rules, []int{1, 2}},
		{"RANGE", ">=", int64(20), rules, []int{2}},
		{"LIST", "=", int64(3), []PartitionRule{{ListValue: []int64{1, 3}}, {ListValue: []int64{2, 4}}}, []int{0}},
		{"LIST", "=", "west", []PartitionRule{{ListText: []string{"east", "west"}}, {ListText: []string{"south"}}}, []int{0}},
		{"RANGE", "=", "alpha", []PartitionRule{{LessThanText: stringPointer("m")}, {MaxValue: true}}, []int{0}},
		{"RANGE", ">=", "zulu", []PartitionRule{{LessThanText: stringPointer("m")}, {MaxValue: true}}, []int{1}},
		{"HASH", "=", int64(-1), []PartitionRule{{}, {}, {}}, []int{2}},
		{"RANGE", "=", nil, rules, []int{0, 1, 2}},
	}
	for _, tc := range cases {
		got, err := PrunePartitions(tc.method, tc.op, tc.value, tc.rules)
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%s %s %v => %v, err=%v, want %v", tc.method, tc.op, tc.value, got, err, tc.want)
		}
	}
}

func TestPartitionPruningMultiColumnTupleRange(t *testing.T) {
	rules := []PartitionRule{{LessThanTuple: []interface{}{int64(10), "m"}}, {MaxValue: true}}
	allowed, err := PrunePartitions("RANGE", ">=", []interface{}{int64(10), "zulu"}, rules)
	if err != nil || !reflect.DeepEqual(allowed, []int{1}) {
		t.Fatalf("tuple >= pruning => %v, err=%v", allowed, err)
	}
}

func stringPointer(value string) *string {
	return &value
}
