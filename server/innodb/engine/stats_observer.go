package engine

import "time"

type StatsEvent struct {
	StatsEventName string

	StatsEventType byte

	StatsEventContent []byte
}

//定义观察者接口
type StatsObserver interface {
	Update(event *StatsEvent)
}

type StatsInfoSelectExecutorWatcher struct {
	StatsObserver

	CpuCost float64

	IoCost float64

	StartTime time.Time

	EndTime time.Time

	Duration time.Duration

	AffectedRows int
}

func NewStatsInfoSelectExecutorWatcher(startTime time.Time) *StatsInfoSelectExecutorWatcher {
	stats := new(StatsInfoSelectExecutorWatcher)
	stats.StartTime = startTime
	return stats
}

func (st *StatsInfoSelectExecutorWatcher) Update(event *StatsEvent) {

}
