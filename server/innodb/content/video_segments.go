package content

import (
	"context"
	"fmt"
	"strconv"
)

const (
	videoSegmentProcessorID      = "xmysql.iso-bmff-time-window"
	videoSegmentProcessorVersion = "1"
)

// ParseVideoSegmentsFromStore creates deterministic time windows over a
// validated ISO-BMFF source. It records exact source identity and time
// coordinates; the source container remains unchanged and no frame is decoded.
func ParseVideoSegmentsFromStore(ctx context.Context, store *SegmentStore, value Value, source SourceRef, segmentMS, overlapMS uint64) ([]Block, error) {
	if segmentMS < 1000 || segmentMS > 10*60*1000 || overlapMS >= segmentMS || overlapMS > 5*60*1000 {
		return nil, fmt.Errorf("video segment_ms must be 1000..600000 and overlap_ms must be less than segment_ms and at most 300000")
	}
	whole, err := ParseISOBaseMediaVideoFromStore(ctx, store, value, source)
	if err != nil {
		return nil, err
	}
	durationMS, err := strconv.ParseUint(whole.Attributes["duration_ms"], 10, 64)
	if err != nil || durationMS == 0 {
		return nil, fmt.Errorf("ISO-BMFF duration is invalid")
	}
	step := segmentMS - overlapMS
	count := uint64(1)
	if durationMS > segmentMS {
		remaining := durationMS - segmentMS
		count += (remaining-1)/step + 1
	}
	if count > 10000 {
		return nil, fmt.Errorf("video segmentation would produce %d blocks; maximum is 10000", count)
	}
	blocks := make([]Block, 0, int(count))
	for ordinal := uint64(0); ordinal < count; ordinal++ {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		startMS := ordinal * step
		endMS := startMS + segmentMS
		if endMS < startMS || endMS > durationMS {
			endMS = durationMS
		}
		start, end := startMS, endMS
		block, err := NewBlock(source, uint32(ordinal+1), BlockVideoSegment, "", Locator{StartMS: &start, EndMS: &end}, videoSegmentProcessorID, videoSegmentProcessorVersion, "", "")
		if err != nil {
			return nil, err
		}
		block.Attributes = map[string]string{
			"container": "iso-bmff", "duration_ms": strconv.FormatUint(endMS-startMS, 10),
			"decode_scope": "time_window_only", "frames_decoded": "false",
			"model_adapter_must_honor_locator": "true",
		}
		block.ID = block.Identity()
		if err := block.Validate(); err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}
