package plan

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type compatibilityGTIDInterval struct {
	start uint64
	end   uint64
}

type compatibilityGTIDSet map[string][]compatibilityGTIDInterval

func evalGTIDFunction(name string, args []interface{}) (interface{}, error) {
	if len(args) != 2 || args[0] == nil || args[1] == nil {
		return nil, nil
	}
	left, err := parseCompatibilityGTIDSet(compatString(args[0]))
	if err != nil {
		return nil, err
	}
	right, err := parseCompatibilityGTIDSet(compatString(args[1]))
	if err != nil {
		return nil, err
	}
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "GTID_SUBSET":
		for uuid, intervals := range left {
			for _, interval := range intervals {
				if !compatibilityGTIDRangeCovered(right[uuid], interval) {
					return int64(0), nil
				}
			}
		}
		return int64(1), nil
	case "GTID_SUBTRACT":
		return formatCompatibilityGTIDSet(subtractCompatibilityGTIDSet(left, right)), nil
	default:
		return nil, fmt.Errorf("unknown GTID function: %s", name)
	}
}

func parseCompatibilityGTIDSet(raw string) (compatibilityGTIDSet, error) {
	result := compatibilityGTIDSet{}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return result, nil
	}
	for _, group := range strings.Split(raw, ",") {
		parts := strings.Split(strings.TrimSpace(group), ":")
		if len(parts) < 2 || strings.TrimSpace(parts[0]) == "" {
			return nil, fmt.Errorf("invalid GTID set group %q", group)
		}
		uuid := strings.ToLower(strings.TrimSpace(parts[0]))
		for _, rawInterval := range parts[1:] {
			bounds := strings.Split(strings.TrimSpace(rawInterval), "-")
			if len(bounds) > 2 || bounds[0] == "" {
				return nil, fmt.Errorf("invalid GTID interval %q", rawInterval)
			}
			start, err := strconv.ParseUint(bounds[0], 10, 64)
			if err != nil || start == 0 {
				return nil, fmt.Errorf("invalid GTID interval %q", rawInterval)
			}
			end := start
			if len(bounds) == 2 {
				end, err = strconv.ParseUint(bounds[1], 10, 64)
				if err != nil || end < start {
					return nil, fmt.Errorf("invalid GTID interval %q", rawInterval)
				}
			}
			result[uuid] = append(result[uuid], compatibilityGTIDInterval{start: start, end: end})
		}
	}
	for uuid, intervals := range result {
		result[uuid] = mergeCompatibilityGTIDIntervals(intervals)
	}
	return result, nil
}

func mergeCompatibilityGTIDIntervals(intervals []compatibilityGTIDInterval) []compatibilityGTIDInterval {
	sort.Slice(intervals, func(i, j int) bool {
		if intervals[i].start != intervals[j].start {
			return intervals[i].start < intervals[j].start
		}
		return intervals[i].end < intervals[j].end
	})
	merged := make([]compatibilityGTIDInterval, 0, len(intervals))
	for _, current := range intervals {
		if len(merged) == 0 || current.start > merged[len(merged)-1].end && current.start != merged[len(merged)-1].end+1 {
			merged = append(merged, current)
			continue
		}
		if current.end > merged[len(merged)-1].end {
			merged[len(merged)-1].end = current.end
		}
	}
	return merged
}

func compatibilityGTIDRangeCovered(intervals []compatibilityGTIDInterval, target compatibilityGTIDInterval) bool {
	for _, interval := range intervals {
		if interval.start <= target.start && interval.end >= target.end {
			return true
		}
		if interval.start > target.start {
			break
		}
	}
	return false
}

func subtractCompatibilityGTIDSet(left, right compatibilityGTIDSet) compatibilityGTIDSet {
	result := compatibilityGTIDSet{}
	for uuid, intervals := range left {
		for _, source := range intervals {
			remaining := []compatibilityGTIDInterval{source}
			for _, excluded := range right[uuid] {
				next := make([]compatibilityGTIDInterval, 0, len(remaining)+1)
				for _, current := range remaining {
					if excluded.end < current.start || excluded.start > current.end {
						next = append(next, current)
						continue
					}
					if current.start < excluded.start {
						next = append(next, compatibilityGTIDInterval{start: current.start, end: excluded.start - 1})
					}
					if excluded.end < current.end {
						next = append(next, compatibilityGTIDInterval{start: excluded.end + 1, end: current.end})
					}
				}
				remaining = next
			}
			result[uuid] = append(result[uuid], remaining...)
		}
	}
	for uuid, intervals := range result {
		result[uuid] = mergeCompatibilityGTIDIntervals(intervals)
		if len(result[uuid]) == 0 {
			delete(result, uuid)
		}
	}
	return result
}

func formatCompatibilityGTIDSet(set compatibilityGTIDSet) string {
	uuids := make([]string, 0, len(set))
	for uuid := range set {
		uuids = append(uuids, uuid)
	}
	sort.Strings(uuids)
	groups := make([]string, 0, len(uuids))
	for _, uuid := range uuids {
		parts := []string{uuid}
		for _, interval := range set[uuid] {
			if interval.start == interval.end {
				parts = append(parts, strconv.FormatUint(interval.start, 10))
			} else {
				parts = append(parts, strconv.FormatUint(interval.start, 10)+"-"+strconv.FormatUint(interval.end, 10))
			}
		}
		groups = append(groups, strings.Join(parts, ":"))
	}
	return strings.Join(groups, ",")
}
