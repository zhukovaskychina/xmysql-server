package vector

import (
	"fmt"
	"sort"
)

// SearchApproximate searches every base segment in the current manifest and
// merges each segment's bounded candidate set. It does not inspect database
// deltas or tombstones; the caller must do that before presenting results.
func (c *IndexCatalog) SearchApproximate(indexID string, query Value, k, ef int, accept func(HNSWEntry) bool) (HNSWSearchResult, IndexManifest, error) {
	manifest, found, err := c.ReadLatest(indexID)
	if err != nil {
		return HNSWSearchResult{}, IndexManifest{}, err
	}
	if !found {
		return HNSWSearchResult{}, IndexManifest{}, ErrSegmentNotFound
	}
	if manifest.Freshness != IndexFreshnessReady {
		return HNSWSearchResult{}, manifest, ErrIndexNotReady
	}
	if err := validateManifestQuery(manifest, query); err != nil {
		return HNSWSearchResult{}, manifest, err
	}
	if k < 1 || k > hnswMaxSegmentNodes || ef < k || ef > hnswMaxEF {
		return HNSWSearchResult{}, manifest, fmt.Errorf("%w: k must be positive and ef must be k..%d", ErrInvalidSegment, hnswMaxEF)
	}
	latestMutations, err := c.currentDeltaMutations(manifest)
	if err != nil {
		return HNSWSearchResult{}, manifest, err
	}
	merged := HNSWSearchResult{Hits: make([]HNSWHit, 0, k), SearchBudget: ef, Approximate: true}
	for _, reference := range manifest.Segments {
		segment, err := c.segments.Read(reference)
		if err != nil {
			return HNSWSearchResult{}, manifest, err
		}
		baseAccept := func(entry HNSWEntry) bool {
			if _, changed := latestMutations[entry.ID]; changed {
				return false
			}
			return accept == nil || accept(entry)
		}
		result, err := segment.SearchApproximate(query, k, ef, baseAccept)
		if err != nil {
			return HNSWSearchResult{}, manifest, err
		}
		merged.CandidatesVisited += result.CandidatesVisited
		merged.MayHaveMore = merged.MayHaveMore || result.MayHaveMore
		merged.Hits = append(merged.Hits, result.Hits...)
		sortVectorHits(merged.Hits)
		if len(merged.Hits) > k {
			merged.Hits = merged.Hits[:k]
		}
	}
	for _, mutation := range latestMutations {
		if mutation.Delete || mutation.Value == nil {
			continue
		}
		entry := HNSWEntry{ID: mutation.EntryID, Value: mutation.Value.Clone()}
		if accept != nil && !accept(cloneHNSWEntry(entry)) {
			continue
		}
		distance, distanceErr := Distance(query, entry.Value, manifest.Metric)
		if distanceErr != nil {
			return HNSWSearchResult{}, manifest, distanceErr
		}
		merged.Hits = append(merged.Hits, HNSWHit{Entry: entry, Distance: distance})
		sortVectorHits(merged.Hits)
		if len(merged.Hits) > k {
			merged.Hits = merged.Hits[:k]
		}
	}
	return merged, manifest, nil
}

// SearchExact evaluates all rows in every immutable segment. It is the index
// layer's exact fallback; the engine must additionally merge exact deltas and
// recheck current row revisions, deletion visibility and authorization.
func (c *IndexCatalog) SearchExact(indexID string, query Value, k int, accept func(HNSWEntry) bool) ([]HNSWHit, IndexManifest, error) {
	manifest, found, err := c.ReadLatest(indexID)
	if err != nil {
		return nil, IndexManifest{}, err
	}
	if !found {
		return nil, IndexManifest{}, ErrSegmentNotFound
	}
	if manifest.Freshness != IndexFreshnessReady {
		return nil, manifest, ErrIndexNotReady
	}
	if err := validateManifestQuery(manifest, query); err != nil {
		return nil, manifest, err
	}
	if k < 1 || k > hnswMaxSegmentNodes {
		return nil, manifest, fmt.Errorf("%w: k must be positive", ErrInvalidSegment)
	}
	latestMutations, err := c.currentDeltaMutations(manifest)
	if err != nil {
		return nil, manifest, err
	}
	hits := make([]HNSWHit, 0, k)
	for _, reference := range manifest.Segments {
		segment, err := c.segments.Read(reference)
		if err != nil {
			return nil, manifest, err
		}
		baseAccept := func(entry HNSWEntry) bool {
			if _, changed := latestMutations[entry.ID]; changed {
				return false
			}
			return accept == nil || accept(entry)
		}
		segmentHits, err := segment.SearchExact(query, k, baseAccept)
		if err != nil {
			return nil, manifest, err
		}
		hits = append(hits, segmentHits...)
		sortVectorHits(hits)
		if len(hits) > k {
			hits = hits[:k]
		}
	}
	for _, mutation := range latestMutations {
		if mutation.Delete || mutation.Value == nil {
			continue
		}
		entry := HNSWEntry{ID: mutation.EntryID, Value: mutation.Value.Clone()}
		if accept != nil && !accept(cloneHNSWEntry(entry)) {
			continue
		}
		distance, distanceErr := Distance(query, entry.Value, manifest.Metric)
		if distanceErr != nil {
			return nil, manifest, distanceErr
		}
		hits = append(hits, HNSWHit{Entry: entry, Distance: distance})
		sortVectorHits(hits)
		if len(hits) > k {
			hits = hits[:k]
		}
	}
	return hits, manifest, nil
}

func (c *IndexCatalog) currentDeltaMutations(manifest IndexManifest) (map[string]DeltaMutation, error) {
	latest := make(map[string]DeltaMutation)
	for _, reference := range manifest.Deltas {
		batch, err := c.deltas.Read(reference)
		if err != nil {
			return nil, err
		}
		for _, mutation := range batch.Mutations {
			if mutation.Value != nil {
				value := mutation.Value.Clone()
				mutation.Value = &value
			}
			latest[mutation.EntryID] = mutation
		}
	}
	return latest, nil
}

func sortVectorHits(hits []HNSWHit) {
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Distance != hits[j].Distance {
			return hits[i].Distance < hits[j].Distance
		}
		return hits[i].Entry.ID < hits[j].Entry.ID
	})
}

func validateManifestQuery(manifest IndexManifest, query Value) error {
	if err := query.Validate(); err != nil {
		return err
	}
	if query.State == StateStale {
		return ErrStaleValue
	}
	if query.Space.ID != manifest.SpaceID || uint32(query.Dimension()) != manifest.Dimension {
		return ErrSpaceMismatch
	}
	return nil
}
