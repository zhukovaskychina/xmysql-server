package basic

// BTreeStatistics represents statistics for a B+Tree.
type BTreeStatistics struct {
	LeafPages    uint32
	NonLeafPages uint32
	RecordCount  uint64
}
