package vector

import (
	"bytes"
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"strings"
)

const (
	hnswSegmentVersion  = 1
	hnswMaxLinks        = 64
	hnswMaxEF           = 4096
	hnswMaxSegmentNodes = 1 << 20
	hnswMaxSegmentBytes = 1 << 30
)

var ErrInvalidSegment = errors.New("invalid HNSW vector segment")

// HNSWConfig defines the bounded graph-construction parameters for one
// immutable model-space segment. M applies to layer zero; upper layers use
// max(2, M/2). EFConstruction bounds candidate exploration while building.
type HNSWConfig struct {
	M              uint16
	EFConstruction uint16
}

// HNSWEntry associates a stable database-owned entry ID with its exact
// source-bound vector. The segment stores the vector and its provenance; the
// caller still resolves and authorizes the referenced row at result time.
type HNSWEntry struct {
	ID    string
	Value Value
}

// HNSWHit is a candidate from an immutable segment. ANN results are explicitly
// approximate and must be reconciled with the current row revision and ACL.
type HNSWHit struct {
	Entry    HNSWEntry
	Distance float64
}

type HNSWSearchResult struct {
	Hits              []HNSWHit
	CandidatesVisited int
	SearchBudget      int
	MayHaveMore       bool
	Approximate       bool
}

type hnswNode struct {
	entry HNSWEntry
	level int
	links [][]int
}

// HNSWSegment is an immutable, checksummed graph for a single vector space,
// dimension and metric. DML deltas and deletions belong to the engine's
// manifest layer; this type is only the immutable base-segment primitive.
type HNSWSegment struct {
	space      Space
	dimension  int
	metric     Metric
	config     HNSWConfig
	entryPoint int
	maxLevel   int
	nodes      []hnswNode
}

type hnswCandidate struct {
	index    int
	distance float64
}

type hnswMinHeap []hnswCandidate

func (h hnswMinHeap) Len() int           { return len(h) }
func (h hnswMinHeap) Less(i, j int) bool { return candidateLess(h[i], h[j]) }
func (h hnswMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *hnswMinHeap) Push(value interface{}) {
	*h = append(*h, value.(hnswCandidate))
}
func (h *hnswMinHeap) Pop() interface{} {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

type hnswMaxHeap []hnswCandidate

func (h hnswMaxHeap) Len() int           { return len(h) }
func (h hnswMaxHeap) Less(i, j int) bool { return candidateLess(h[j], h[i]) }
func (h hnswMaxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *hnswMaxHeap) Push(value interface{}) {
	*h = append(*h, value.(hnswCandidate))
}
func (h *hnswMaxHeap) Pop() interface{} {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

func candidateLess(left, right hnswCandidate) bool {
	if left.distance != right.distance {
		return left.distance < right.distance
	}
	return left.index < right.index
}

// BuildHNSWSegment constructs a deterministic graph from READY vectors. Input
// order does not affect the resulting node order or level assignment.
func BuildHNSWSegment(entries []HNSWEntry, metric Metric, config HNSWConfig) (*HNSWSegment, error) {
	metric = Metric(strings.ToUpper(strings.TrimSpace(string(metric))))
	if metric != MetricL2 && metric != MetricCosine && metric != MetricInnerProduct {
		return nil, ErrInvalidMetric
	}
	if config.M < 2 || config.M > hnswMaxLinks || config.EFConstruction < config.M || config.EFConstruction > hnswMaxEF {
		return nil, fmt.Errorf("%w: M must be 2..%d and EFConstruction must be M..%d", ErrInvalidSegment, hnswMaxLinks, hnswMaxEF)
	}
	if len(entries) > hnswMaxSegmentNodes {
		return nil, fmt.Errorf("%w: node count exceeds %d", ErrInvalidSegment, hnswMaxSegmentNodes)
	}
	segment := &HNSWSegment{metric: metric, config: config, entryPoint: -1, maxLevel: -1}
	ordered := append([]HNSWEntry(nil), entries...)
	for index := range ordered {
		ordered[index].ID = strings.TrimSpace(ordered[index].ID)
		ordered[index].Value = ordered[index].Value.Clone()
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	seen := make(map[string]struct{}, len(ordered))
	for index, entry := range ordered {
		if entry.ID == "" || len(entry.ID) > 65535 {
			return nil, fmt.Errorf("%w: entry %d has an invalid ID", ErrInvalidSegment, index)
		}
		if _, exists := seen[entry.ID]; exists {
			return nil, fmt.Errorf("%w: duplicate entry ID %q", ErrInvalidSegment, entry.ID)
		}
		seen[entry.ID] = struct{}{}
		if err := entry.Value.Validate(); err != nil {
			return nil, fmt.Errorf("%w: entry %q: %v", ErrInvalidSegment, entry.ID, err)
		}
		if entry.Value.State != StateReady || entry.Value.Source == nil {
			return nil, fmt.Errorf("%w: entry %q must have a READY source-bound vector", ErrInvalidSegment, entry.ID)
		}
		if index == 0 {
			segment.space = entry.Value.Space
			segment.dimension = entry.Value.Dimension()
		} else if entry.Value.Space.ID != segment.space.ID || entry.Value.Dimension() != segment.dimension {
			return nil, fmt.Errorf("%w: entry %q belongs to another vector space or dimension", ErrInvalidSegment, entry.ID)
		}
	}
	for _, entry := range ordered {
		segment.insert(entry)
	}
	return segment, nil
}

func (s *HNSWSegment) insert(entry HNSWEntry) {
	level := deterministicHNSWLevel(entry.ID, int(s.config.M))
	nodeIndex := len(s.nodes)
	node := hnswNode{entry: entry, level: level, links: make([][]int, level+1)}
	s.nodes = append(s.nodes, node)
	if s.entryPoint < 0 {
		s.entryPoint, s.maxLevel = nodeIndex, level
		return
	}
	current := s.entryPoint
	for layer := s.maxLevel; layer > level; layer-- {
		current = s.greedySearch(entry.Value, current, layer)
	}
	upper := level
	if s.maxLevel < upper {
		upper = s.maxLevel
	}
	for layer := upper; layer >= 0; layer-- {
		candidates, _ := s.searchLayer(entry.Value, []int{current}, int(s.config.EFConstruction), layer)
		limit := hnswMaxLinksAtLayer(s.config.M, layer)
		if len(candidates) > limit {
			candidates = candidates[:limit]
		}
		for _, candidate := range candidates {
			s.nodes[nodeIndex].links[layer] = append(s.nodes[nodeIndex].links[layer], candidate.index)
			s.nodes[candidate.index].links[layer] = append(s.nodes[candidate.index].links[layer], nodeIndex)
			s.prune(candidate.index, layer, limit)
		}
		if len(candidates) > 0 {
			current = candidates[0].index
		}
	}
	if level > s.maxLevel {
		s.entryPoint, s.maxLevel = nodeIndex, level
	}
}

func deterministicHNSWLevel(id string, m int) int {
	digest := sha256.Sum256([]byte(id))
	bitsPerLevel := bits.Len(uint(m - 1))
	level := bits.LeadingZeros64(binary.BigEndian.Uint64(digest[:8])) / bitsPerLevel
	if level > 32 {
		return 32
	}
	return level
}

func hnswMaxLinksAtLayer(m uint16, layer int) int {
	limit := int(m)
	if layer > 0 {
		limit /= 2
		if limit < 2 {
			limit = 2
		}
	}
	return limit
}

func (s *HNSWSegment) prune(index, layer, limit int) {
	links := s.nodes[index].links[layer]
	if len(links) <= limit {
		return
	}
	center := s.nodes[index].entry.Value
	candidates := make([]hnswCandidate, 0, len(links))
	seen := make(map[int]struct{}, len(links))
	for _, neighbor := range links {
		if neighbor < 0 || neighbor >= len(s.nodes) || neighbor == index {
			continue
		}
		if _, exists := seen[neighbor]; exists {
			continue
		}
		seen[neighbor] = struct{}{}
		distance, err := Distance(center, s.nodes[neighbor].entry.Value, s.metric)
		if err == nil {
			candidates = append(candidates, hnswCandidate{index: neighbor, distance: distance})
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidateLess(candidates[i], candidates[j]) })
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	s.nodes[index].links[layer] = make([]int, len(candidates))
	for i, candidate := range candidates {
		s.nodes[index].links[layer][i] = candidate.index
	}
}

func (s *HNSWSegment) greedySearch(query Value, entryPoint, layer int) int {
	current := entryPoint
	currentDistance, _ := Distance(query, s.nodes[current].entry.Value, s.metric)
	for changed := true; changed; {
		changed = false
		for _, neighbor := range s.nodes[current].links[layer] {
			distance, err := Distance(query, s.nodes[neighbor].entry.Value, s.metric)
			if err == nil && (distance < currentDistance || (distance == currentDistance && neighbor < current)) {
				current, currentDistance, changed = neighbor, distance, true
			}
		}
	}
	return current
}

func (s *HNSWSegment) searchLayer(query Value, entries []int, ef, layer int) ([]hnswCandidate, int) {
	if ef < 1 || len(entries) == 0 {
		return nil, 0
	}
	candidates := &hnswMinHeap{}
	best := &hnswMaxHeap{}
	heap.Init(candidates)
	heap.Init(best)
	visited := make(map[int]struct{}, ef*2)
	for _, index := range entries {
		if index < 0 || index >= len(s.nodes) || layer > s.nodes[index].level {
			continue
		}
		if _, exists := visited[index]; exists {
			continue
		}
		visited[index] = struct{}{}
		distance, err := Distance(query, s.nodes[index].entry.Value, s.metric)
		if err != nil {
			continue
		}
		candidate := hnswCandidate{index: index, distance: distance}
		heap.Push(candidates, candidate)
		heap.Push(best, candidate)
	}
	for candidates.Len() > 0 {
		current := heap.Pop(candidates).(hnswCandidate)
		if best.Len() >= ef && candidateLess((*best)[0], current) {
			break
		}
		for _, neighbor := range s.nodes[current.index].links[layer] {
			if _, exists := visited[neighbor]; exists {
				continue
			}
			visited[neighbor] = struct{}{}
			distance, err := Distance(query, s.nodes[neighbor].entry.Value, s.metric)
			if err != nil {
				continue
			}
			candidate := hnswCandidate{index: neighbor, distance: distance}
			if best.Len() < ef || candidateLess(candidate, (*best)[0]) {
				heap.Push(candidates, candidate)
				heap.Push(best, candidate)
				if best.Len() > ef {
					heap.Pop(best)
				}
			}
		}
	}
	result := append([]hnswCandidate(nil), (*best)...)
	sort.Slice(result, func(i, j int) bool { return candidateLess(result[i], result[j]) })
	return result, len(visited)
}

func (s *HNSWSegment) validateQuery(query Value) error {
	if s == nil || len(s.nodes) == 0 {
		return nil
	}
	if err := query.Validate(); err != nil {
		return err
	}
	if query.State == StateStale {
		return ErrStaleValue
	}
	if query.Space.ID != s.space.ID || query.Dimension() != s.dimension {
		return ErrSpaceMismatch
	}
	return nil
}

// SearchApproximate returns up to k nearby entries from this immutable base
// segment. A nil predicate accepts every entry. Callers must recheck source
// revision, deletion visibility and current authorization before exposing a
// hit. Under-filled filtered searches are reported through the returned hit
// count and CandidatesVisited; this method never claims exactness.
func (s *HNSWSegment) SearchApproximate(query Value, k, ef int, accept func(HNSWEntry) bool) (HNSWSearchResult, error) {
	if k < 1 || k > hnswMaxSegmentNodes || ef < k || ef > hnswMaxEF {
		return HNSWSearchResult{}, fmt.Errorf("%w: k must be positive and ef must be k..%d", ErrInvalidSegment, hnswMaxEF)
	}
	if len(s.nodes) == 0 {
		return HNSWSearchResult{Hits: []HNSWHit{}, SearchBudget: ef, Approximate: true}, nil
	}
	if err := s.validateQuery(query); err != nil {
		return HNSWSearchResult{}, err
	}
	maxEF := ef
	currentEF := min(max(k, min(16, maxEF)), len(s.nodes))
	if maxEF > len(s.nodes) {
		maxEF = len(s.nodes)
	}
	current := s.entryPoint
	for layer := s.maxLevel; layer > 0; layer-- {
		current = s.greedySearch(query, current, layer)
	}
	visited := 0
	var hits []HNSWHit
	for {
		candidates, visitedNow := s.searchLayer(query, []int{current}, currentEF, 0)
		visited = visitedNow
		hits = hits[:0]
		for _, candidate := range candidates {
			entry := s.nodes[candidate.index].entry
			if accept != nil && !accept(cloneHNSWEntry(entry)) {
				continue
			}
			hits = append(hits, HNSWHit{Entry: cloneHNSWEntry(entry), Distance: candidate.distance})
			if len(hits) == k {
				break
			}
		}
		if len(hits) >= k || currentEF >= maxEF {
			break
		}
		next := currentEF * 2
		if next <= currentEF || next > maxEF {
			next = maxEF
		}
		currentEF = next
	}
	return HNSWSearchResult{
		Hits: hits, CandidatesVisited: visited, SearchBudget: maxEF,
		MayHaveMore: len(hits) < k && visited < len(s.nodes), Approximate: true,
	}, nil
}

// SearchExact scans this immutable segment and serves as the correctness
// fallback for callers that require exact results or cannot accept ANN recall.
func (s *HNSWSegment) SearchExact(query Value, k int, accept func(HNSWEntry) bool) ([]HNSWHit, error) {
	if k < 1 || k > hnswMaxSegmentNodes {
		return nil, fmt.Errorf("%w: k must be positive", ErrInvalidSegment)
	}
	if len(s.nodes) == 0 {
		return []HNSWHit{}, nil
	}
	if err := s.validateQuery(query); err != nil {
		return nil, err
	}
	hits := make([]HNSWHit, 0, len(s.nodes))
	for _, node := range s.nodes {
		if accept != nil && !accept(cloneHNSWEntry(node.entry)) {
			continue
		}
		distance, err := Distance(query, node.entry.Value, s.metric)
		if err != nil {
			return nil, err
		}
		hits = append(hits, HNSWHit{Entry: cloneHNSWEntry(node.entry), Distance: distance})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Distance != hits[j].Distance {
			return hits[i].Distance < hits[j].Distance
		}
		return hits[i].Entry.ID < hits[j].Entry.ID
	})
	if len(hits) > k {
		hits = hits[:k]
	}
	return hits, nil
}

func cloneHNSWEntry(entry HNSWEntry) HNSWEntry {
	entry.Value = entry.Value.Clone()
	return entry
}

// MarshalBinary writes an immutable, versioned graph segment with an end-to-
// end SHA-256 checksum. The serialized format is independent of Go struct
// layout and stores each source-bound vector in the existing XMV encoding.
func (s *HNSWSegment) MarshalBinary() ([]byte, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	buffer.Write([]byte{'X', 'H', 'N', 'S', hnswSegmentVersion})
	writeString := func(value string) error {
		if len(value) > 65535 {
			return ErrInvalidSegment
		}
		_ = binary.Write(&buffer, binary.BigEndian, uint16(len(value)))
		buffer.WriteString(value)
		return nil
	}
	for _, field := range []string{s.space.ModelID, s.space.ModelVersion, s.space.ID, string(s.metric)} {
		if err := writeString(field); err != nil {
			return nil, err
		}
	}
	_ = binary.Write(&buffer, binary.BigEndian, uint32(s.dimension))
	_ = binary.Write(&buffer, binary.BigEndian, s.config.M)
	_ = binary.Write(&buffer, binary.BigEndian, s.config.EFConstruction)
	_ = binary.Write(&buffer, binary.BigEndian, uint32(len(s.nodes)))
	_ = binary.Write(&buffer, binary.BigEndian, int32(s.entryPoint))
	_ = binary.Write(&buffer, binary.BigEndian, int16(s.maxLevel))
	for _, node := range s.nodes {
		if err := writeString(node.entry.ID); err != nil {
			return nil, err
		}
		encoded, err := node.entry.Value.MarshalBinary()
		if err != nil {
			return nil, err
		}
		_ = binary.Write(&buffer, binary.BigEndian, uint32(len(encoded)))
		buffer.Write(encoded)
		_ = binary.Write(&buffer, binary.BigEndian, uint16(node.level))
		for _, layer := range node.links {
			if len(layer) > 65535 {
				return nil, ErrInvalidSegment
			}
			_ = binary.Write(&buffer, binary.BigEndian, uint16(len(layer)))
			for _, neighbor := range layer {
				_ = binary.Write(&buffer, binary.BigEndian, uint32(neighbor))
			}
		}
		if buffer.Len() > hnswMaxSegmentBytes {
			return nil, fmt.Errorf("%w: serialized segment exceeds %d bytes", ErrInvalidSegment, hnswMaxSegmentBytes)
		}
	}
	digest := sha256.Sum256(buffer.Bytes())
	buffer.Write(digest[:])
	return buffer.Bytes(), nil
}

// UnmarshalBinary validates the complete graph and all embedded source-bound
// vectors before exposing an immutable segment to search callers.
func (s *HNSWSegment) UnmarshalBinary(data []byte) error {
	if s == nil || len(data) < 5+sha256.Size || len(data) > hnswMaxSegmentBytes || string(data[:4]) != "XHNS" || data[4] != hnswSegmentVersion {
		return ErrInvalidSegment
	}
	contentEnd := len(data) - sha256.Size
	want := sha256.Sum256(data[:contentEnd])
	if !bytes.Equal(want[:], data[contentEnd:]) {
		return fmt.Errorf("%w: checksum mismatch", ErrInvalidSegment)
	}
	reader := bytes.NewReader(data[5:contentEnd])
	readString := func() (string, error) {
		var length uint16
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil || int(length) > reader.Len() {
			return "", ErrInvalidSegment
		}
		value := make([]byte, int(length))
		if _, err := reader.Read(value); err != nil {
			return "", ErrInvalidSegment
		}
		return string(value), nil
	}
	modelID, err := readString()
	if err != nil {
		return err
	}
	modelVersion, err := readString()
	if err != nil {
		return err
	}
	spaceID, err := readString()
	if err != nil {
		return err
	}
	metricName, err := readString()
	if err != nil {
		return err
	}
	var dimension uint32
	var config HNSWConfig
	var nodeCount uint32
	var entryPoint int32
	var maxLevel int16
	for _, target := range []interface{}{&dimension, &config.M, &config.EFConstruction, &nodeCount, &entryPoint, &maxLevel} {
		if err := binary.Read(reader, binary.BigEndian, target); err != nil {
			return ErrInvalidSegment
		}
	}
	if dimension > maxDimension || nodeCount > hnswMaxSegmentNodes {
		return ErrInvalidSegment
	}
	segment := HNSWSegment{
		space: Space{ModelID: modelID, ModelVersion: modelVersion, ID: spaceID}, dimension: int(dimension),
		metric: Metric(metricName), config: config, entryPoint: int(entryPoint), maxLevel: int(maxLevel),
		nodes: make([]hnswNode, int(nodeCount)),
	}
	for index := range segment.nodes {
		id, err := readString()
		if err != nil {
			return err
		}
		var valueLength uint32
		if err := binary.Read(reader, binary.BigEndian, &valueLength); err != nil || uint64(valueLength) > uint64(reader.Len()) {
			return ErrInvalidSegment
		}
		encoded := make([]byte, int(valueLength))
		if _, err := reader.Read(encoded); err != nil {
			return ErrInvalidSegment
		}
		var value Value
		if err := value.UnmarshalBinary(encoded); err != nil {
			return fmt.Errorf("%w: entry %q vector: %v", ErrInvalidSegment, id, err)
		}
		var level uint16
		if err := binary.Read(reader, binary.BigEndian, &level); err != nil || level > 32 {
			return ErrInvalidSegment
		}
		node := hnswNode{entry: HNSWEntry{ID: id, Value: value}, level: int(level), links: make([][]int, int(level)+1)}
		for layer := range node.links {
			var count uint16
			if err := binary.Read(reader, binary.BigEndian, &count); err != nil || count > hnswMaxLinks || int(count)*4 > reader.Len() {
				return ErrInvalidSegment
			}
			node.links[layer] = make([]int, int(count))
			for neighborIndex := range node.links[layer] {
				var neighbor uint32
				if err := binary.Read(reader, binary.BigEndian, &neighbor); err != nil || neighbor >= nodeCount || neighbor == uint32(index) {
					return ErrInvalidSegment
				}
				node.links[layer][neighborIndex] = int(neighbor)
			}
		}
		segment.nodes[index] = node
	}
	if reader.Len() != 0 {
		return ErrInvalidSegment
	}
	if err := segment.validate(); err != nil {
		return err
	}
	*s = segment
	return nil
}

func (s *HNSWSegment) validate() error {
	if s == nil || (s.metric != MetricL2 && s.metric != MetricCosine && s.metric != MetricInnerProduct) ||
		s.config.M < 2 || s.config.M > hnswMaxLinks || s.config.EFConstruction < s.config.M || s.config.EFConstruction > hnswMaxEF ||
		len(s.nodes) > hnswMaxSegmentNodes {
		return ErrInvalidSegment
	}
	if len(s.nodes) == 0 {
		if s.entryPoint != -1 || s.maxLevel != -1 {
			return ErrInvalidSegment
		}
		return nil
	}
	if s.entryPoint < 0 || s.entryPoint >= len(s.nodes) || s.maxLevel < 0 || s.maxLevel > 32 || s.nodes[s.entryPoint].level != s.maxLevel || s.dimension < 1 || s.dimension > maxDimension {
		return ErrInvalidSegment
	}
	seen := make(map[string]struct{}, len(s.nodes))
	for index, node := range s.nodes {
		if node.entry.ID == "" || len(node.entry.ID) > 65535 || node.level < 0 || node.level > s.maxLevel || len(node.links) != node.level+1 {
			return ErrInvalidSegment
		}
		if _, exists := seen[node.entry.ID]; exists {
			return ErrInvalidSegment
		}
		seen[node.entry.ID] = struct{}{}
		if err := node.entry.Value.Validate(); err != nil || node.entry.Value.State != StateReady || node.entry.Value.Source == nil ||
			node.entry.Value.Space.ID != s.space.ID || node.entry.Value.Dimension() != s.dimension {
			return ErrInvalidSegment
		}
		for layer, neighbors := range node.links {
			if len(neighbors) > hnswMaxLinksAtLayer(s.config.M, layer) {
				return ErrInvalidSegment
			}
			neighborsSeen := make(map[int]struct{}, len(neighbors))
			for _, neighbor := range neighbors {
				if neighbor < 0 || neighbor >= len(s.nodes) || neighbor == index || s.nodes[neighbor].level < layer {
					return ErrInvalidSegment
				}
				if _, exists := neighborsSeen[neighbor]; exists {
					return ErrInvalidSegment
				}
				neighborsSeen[neighbor] = struct{}{}
			}
		}
	}
	return nil
}
