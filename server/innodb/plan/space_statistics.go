package plan

// SpaceStatistics holds basic statistics for a tablespace.
type SpaceStatistics struct {
	SpaceID     uint32
	PageCount   uint32
	ExtentCount uint32
	UsedSpace   uint64
}
