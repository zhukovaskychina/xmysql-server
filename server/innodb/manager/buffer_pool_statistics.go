package manager

// BufferPoolStatistics represents basic statistics of the buffer pool.
type BufferPoolStatistics struct {
	Hits          uint64
	Misses        uint64
	Evictions     uint64
	Flushes       uint64
	PageReads     uint64
	PageWrites    uint64
	YoungHits     uint64
	OldHits       uint64
	DirtyPages    uint64
	TotalPages    uint64
	BackgroundOps uint64
	HitRate       float64


	CacheSize     uint32
}
