package replication

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type GTID struct {
	UUID string `json:"uuid"`
	Seq  uint64 `json:"seq"`
}

func (g GTID) String() string { return fmt.Sprintf("%s:%d", g.UUID, g.Seq) }

type GTIDSet map[string]map[uint64]struct{}

// GTIDInterval is an inclusive interval of executed transaction numbers.
// Wire-level GTID sets use half-open intervals; keeping the internal range
// inclusive avoids an overflow when the upper bound is MaxUint64.
type GTIDInterval struct {
	Start uint64
	End   uint64
}

// GTIDIntervals stores GTID history without expanding a large range into one
// map entry per transaction. It is used by native COM_BINLOG_DUMP_GTID
// handling, where interval semantics are part of the protocol contract.
type GTIDIntervals map[string][]GTIDInterval

const maxMaterializedGTIDSequences uint64 = 1_000_000

func cloneGTIDIntervals(source GTIDIntervals) GTIDIntervals {
	clone := GTIDIntervals{}
	for uuid, intervals := range source {
		clone[uuid] = append([]GTIDInterval(nil), intervals...)
	}
	return clone
}

func (set GTIDIntervals) Add(uuid string, sequence uint64) {
	if uuid == "" || sequence == 0 {
		return
	}
	set.AddRange(uuid, sequence, sequence)
}

func (set GTIDIntervals) AddRange(uuid string, start, end uint64) {
	if uuid == "" || start == 0 || end < start {
		return
	}
	intervals := append(set[uuid], GTIDInterval{Start: start, End: end})
	sort.Slice(intervals, func(i, j int) bool {
		if intervals[i].Start == intervals[j].Start {
			return intervals[i].End < intervals[j].End
		}
		return intervals[i].Start < intervals[j].Start
	})
	merged := make([]GTIDInterval, 0, len(intervals))
	for _, interval := range intervals {
		if len(merged) == 0 {
			merged = append(merged, interval)
			continue
		}
		last := &merged[len(merged)-1]
		adjacent := last.End != ^uint64(0) && interval.Start == last.End+1
		if interval.Start <= last.End || adjacent {
			if interval.End > last.End {
				last.End = interval.End
			}
			continue
		}
		merged = append(merged, interval)
	}
	set[uuid] = merged
}

// AddHalfOpen appends a protocol interval [start,end). The empty interval is
// rejected and MaxUint64 is handled without wrapping the inclusive end.
func (set GTIDIntervals) AddHalfOpen(uuid string, start, end uint64) error {
	if start == 0 || end <= start {
		return fmt.Errorf("invalid GTID interval %d-%d", start, end)
	}
	set.AddRange(uuid, start, end-1)
	return nil
}

func (set GTIDIntervals) Contains(gtid GTID) bool {
	for _, interval := range set[gtid.UUID] {
		if gtid.Seq < interval.Start {
			return false
		}
		if gtid.Seq <= interval.End {
			return true
		}
	}
	return false
}

func (set GTIDIntervals) Merge(other GTIDIntervals) {
	for uuid, intervals := range other {
		for _, interval := range intervals {
			set.AddRange(uuid, interval.Start, interval.End)
		}
	}
}

func (set GTIDIntervals) String() string {
	uuidList := make([]string, 0, len(set))
	for uuid := range set {
		uuidList = append(uuidList, uuid)
	}
	sort.Strings(uuidList)
	groups := make([]string, 0, len(uuidList))
	for _, uuid := range uuidList {
		parts := make([]string, 0, len(set[uuid]))
		for _, interval := range set[uuid] {
			if interval.Start == interval.End {
				parts = append(parts, strconv.FormatUint(interval.Start, 10))
			} else {
				parts = append(parts, fmt.Sprintf("%d-%d", interval.Start, interval.End))
			}
		}
		if len(parts) > 0 {
			groups = append(groups, uuid+":"+strings.Join(parts, ":"))
		}
	}
	return strings.Join(groups, ",")
}

func (set GTIDIntervals) ToGTIDSet() GTIDSet {
	var sequenceCount uint64
	for _, intervals := range set {
		for _, interval := range intervals {
			if interval.End < interval.Start {
				return nil
			}
			span := interval.End - interval.Start + 1
			if span > maxMaterializedGTIDSequences || sequenceCount > maxMaterializedGTIDSequences-span {
				// Keep the range-preserving API usable for large histories while
				// refusing an accidental map expansion through this legacy helper.
				return nil
			}
			sequenceCount += span
		}
	}
	result := GTIDSet{}
	for uuid, intervals := range set {
		for _, interval := range intervals {
			for sequence := interval.Start; sequence <= interval.End; sequence++ {
				result.Add(GTID{UUID: uuid, Seq: sequence})
				if sequence == ^uint64(0) {
					break
				}
			}
		}
	}
	return result
}

func GTIDIntervalsFromSet(source GTIDSet) GTIDIntervals {
	result := GTIDIntervals{}
	for uuid, sequences := range source {
		ordered := make([]uint64, 0, len(sequences))
		for sequence := range sequences {
			if sequence > 0 {
				ordered = append(ordered, sequence)
			}
		}
		sort.Slice(ordered, func(left, right int) bool { return ordered[left] < ordered[right] })
		for index := 0; index < len(ordered); {
			start, end := ordered[index], ordered[index]
			for index+1 < len(ordered) && ordered[index+1] == end+1 {
				index++
				end = ordered[index]
			}
			result.AddRange(uuid, start, end)
			index++
		}
	}
	return result
}

func ParseGTIDSet(raw string) (GTIDSet, error) {
	intervals, err := ParseGTIDIntervals(raw)
	if err != nil {
		return nil, err
	}
	set := intervals.ToGTIDSet()
	if set == nil && len(intervals) > 0 {
		return nil, fmt.Errorf("GTID set is too large to materialize; use interval form")
	}
	return set, nil
}

func ParseGTIDIntervals(raw string) (GTIDIntervals, error) {
	set := GTIDIntervals{}
	for _, group := range strings.Split(strings.TrimSpace(raw), ",") {
		group = strings.TrimSpace(group)
		if group == "" {
			continue
		}
		parts := strings.Split(group, ":")
		if len(parts) < 2 || strings.TrimSpace(parts[0]) == "" {
			return nil, fmt.Errorf("invalid GTID group %q", group)
		}
		uuid := strings.TrimSpace(parts[0])
		intervalStart := 1
		// MySQL 8.4 tagged GTIDs use uuid:tag:interval. Internally the tag
		// remains part of the map key so existing interval operations stay
		// compact and exactly-once checks remain backward compatible.
		if len(parts) >= 3 && !isGTIDIntervalToken(parts[1]) && isGTIDIntervalToken(parts[2]) {
			uuid += ":" + strings.TrimSpace(parts[1])
			intervalStart = 2
		}
		for _, interval := range parts[intervalStart:] {
			bounds := strings.SplitN(strings.TrimSpace(interval), "-", 2)
			start, err := strconv.ParseUint(bounds[0], 10, 64)
			if err != nil || start == 0 {
				return nil, fmt.Errorf("invalid GTID interval %q", interval)
			}
			end := start
			if len(bounds) == 2 {
				end, err = strconv.ParseUint(bounds[1], 10, 64)
				if err != nil || end < start {
					return nil, fmt.Errorf("invalid GTID interval %q", interval)
				}
			}
			set.AddRange(uuid, start, end)
		}
	}
	return set, nil
}

func isGTIDIntervalToken(token string) bool {
	bounds := strings.SplitN(strings.TrimSpace(token), "-", 2)
	if len(bounds) == 0 || bounds[0] == "" {
		return false
	}
	if _, err := strconv.ParseUint(bounds[0], 10, 64); err != nil {
		return false
	}
	if len(bounds) == 2 {
		if bounds[1] == "" {
			return false
		}
		if _, err := strconv.ParseUint(bounds[1], 10, 64); err != nil {
			return false
		}
	}
	return true
}

func (set GTIDSet) Add(gtid GTID) {
	if gtid.UUID == "" || gtid.Seq == 0 {
		return
	}
	if set[gtid.UUID] == nil {
		set[gtid.UUID] = map[uint64]struct{}{}
	}
	set[gtid.UUID][gtid.Seq] = struct{}{}
}

func (set GTIDSet) Contains(gtid GTID) bool {
	_, ok := set[gtid.UUID][gtid.Seq]
	return ok
}

func (set GTIDSet) Merge(other GTIDSet) {
	for uuid, sequences := range other {
		for seq := range sequences {
			set.Add(GTID{UUID: uuid, Seq: seq})
		}
	}
}

func (set GTIDSet) String() string {
	uuidList := make([]string, 0, len(set))
	for uuid := range set {
		uuidList = append(uuidList, uuid)
	}
	sort.Strings(uuidList)
	groups := make([]string, 0, len(uuidList))
	for _, uuid := range uuidList {
		sequences := make([]uint64, 0, len(set[uuid]))
		for seq := range set[uuid] {
			sequences = append(sequences, seq)
		}
		sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
		intervals := make([]string, 0)
		for i := 0; i < len(sequences); {
			start, end := sequences[i], sequences[i]
			for i+1 < len(sequences) && sequences[i+1] == end+1 {
				i++
				end = sequences[i]
			}
			if start == end {
				intervals = append(intervals, strconv.FormatUint(start, 10))
			} else {
				intervals = append(intervals, fmt.Sprintf("%d-%d", start, end))
			}
			i++
		}
		groups = append(groups, uuid+":"+strings.Join(intervals, ":"))
	}
	return strings.Join(groups, ",")
}
