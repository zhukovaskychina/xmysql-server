package manager

import (
	"fmt"
	"sort"
	"sync"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
)

// CompressedStorageProvider composes transparent page compression with an
// existing storage provider. The inner provider still receives exactly one
// fixed-size page per write; compressed bytes are stored in a zero-padded
// envelope and are expanded on reads.
type CompressedStorageProvider struct {
	basic.StorageProvider
	compression *CompressionManager
	pageSize    uint32

	mu               sync.RWMutex
	compressedSpaces map[uint32]struct{}
	excludedSpaces   map[uint32]struct{}
	defaultSettings  *CompressionSettings
}

func NewCompressedStorageProvider(inner basic.StorageProvider, compression *CompressionManager, pageSize uint32, compressedSpaces ...uint32) (*CompressedStorageProvider, error) {
	if inner == nil {
		return nil, fmt.Errorf("storage provider is required")
	}
	if compression == nil {
		return nil, fmt.Errorf("compression manager is required")
	}
	if pageSize == 0 {
		return nil, fmt.Errorf("page size must be positive")
	}
	spaces := make(map[uint32]struct{}, len(compressedSpaces))
	for _, spaceID := range compressedSpaces {
		spaces[spaceID] = struct{}{}
	}
	return &CompressedStorageProvider{
		StorageProvider:  inner,
		compression:      compression,
		pageSize:         pageSize,
		compressedSpaces: spaces,
		excludedSpaces:   make(map[uint32]struct{}),
	}, nil
}

func (p *CompressedStorageProvider) SetSpaceCompressed(spaceID uint32, compressed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if compressed {
		p.compressedSpaces[spaceID] = struct{}{}
		delete(p.excludedSpaces, spaceID)
	} else {
		delete(p.compressedSpaces, spaceID)
		if p.defaultSettings != nil {
			p.excludedSpaces[spaceID] = struct{}{}
		}
	}
}

// SetDefaultCompressionSettings enables compression for spaces that are not
// explicitly listed and supplies their settings lazily when a new space is
// created after startup.
func (p *CompressedStorageProvider) SetDefaultCompressionSettings(settings *CompressionSettings) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if settings == nil {
		p.defaultSettings = nil
		return
	}
	copySettings := *settings
	p.defaultSettings = &copySettings
}

type compressionPolicySnapshot struct {
	DefaultSettings  *CompressionSettings
	CompressedSpaces []uint32
	ExcludedSpaces   []uint32
}

func (p *CompressedStorageProvider) compressionPolicySnapshot() compressionPolicySnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()

	snapshot := compressionPolicySnapshot{
		CompressedSpaces: make([]uint32, 0, len(p.compressedSpaces)),
		ExcludedSpaces:   make([]uint32, 0, len(p.excludedSpaces)),
	}
	if p.defaultSettings != nil {
		settings := *p.defaultSettings
		snapshot.DefaultSettings = &settings
	}
	for spaceID := range p.compressedSpaces {
		snapshot.CompressedSpaces = append(snapshot.CompressedSpaces, spaceID)
	}
	for spaceID := range p.excludedSpaces {
		snapshot.ExcludedSpaces = append(snapshot.ExcludedSpaces, spaceID)
	}
	sort.Slice(snapshot.CompressedSpaces, func(i, j int) bool { return snapshot.CompressedSpaces[i] < snapshot.CompressedSpaces[j] })
	sort.Slice(snapshot.ExcludedSpaces, func(i, j int) bool { return snapshot.ExcludedSpaces[i] < snapshot.ExcludedSpaces[j] })
	return snapshot
}

func (p *CompressedStorageProvider) restoreCompressionPolicy(snapshot compressionPolicySnapshot) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.compressedSpaces = make(map[uint32]struct{}, len(snapshot.CompressedSpaces))
	for _, spaceID := range snapshot.CompressedSpaces {
		p.compressedSpaces[spaceID] = struct{}{}
	}
	p.excludedSpaces = make(map[uint32]struct{}, len(snapshot.ExcludedSpaces))
	for _, spaceID := range snapshot.ExcludedSpaces {
		p.excludedSpaces[spaceID] = struct{}{}
	}
	if snapshot.DefaultSettings == nil {
		p.defaultSettings = nil
	} else {
		settings := *snapshot.DefaultSettings
		p.defaultSettings = &settings
	}
	settingsTemplate := snapshot.DefaultSettings
	if settingsTemplate == nil {
		settingsTemplate = p.compression.GetCompressionSettings(0)
	}
	if settingsTemplate != nil {
		for spaceID := range p.compressedSpaces {
			if p.compression.GetCompressionSettings(spaceID) != nil {
				continue
			}
			settings := *settingsTemplate
			settings.SpaceID = spaceID
			p.compression.SetCompressionSettings(spaceID, &settings)
		}
	}
}

// IsSpaceCompressed reports the effective compression policy for a space.
func (p *CompressedStorageProvider) IsSpaceCompressed(spaceID uint32) bool {
	return p.isSpaceCompressed(spaceID)
}

func (p *CompressedStorageProvider) isSpaceCompressed(spaceID uint32) bool {
	p.mu.RLock()
	_, excluded := p.excludedSpaces[spaceID]
	_, ok := p.compressedSpaces[spaceID]
	if excluded {
		p.mu.RUnlock()
		return false
	}
	if !ok && p.defaultSettings != nil {
		ok = true
	}
	p.mu.RUnlock()
	return ok
}

func (p *CompressedStorageProvider) ensureSpaceSettings(spaceID uint32) {
	if p.compression.GetCompressionSettings(spaceID) != nil {
		return
	}
	p.mu.RLock()
	settings := p.defaultSettings
	if settings != nil {
		copySettings := *settings
		settings = &copySettings
	}
	p.mu.RUnlock()
	if settings == nil {
		if configured := p.compression.GetCompressionSettings(0); configured != nil {
			copySettings := *configured
			settings = &copySettings
		}
	}
	if settings != nil {
		settings.SpaceID = spaceID
		p.compression.SetCompressionSettings(spaceID, settings)
	}
}

func (p *CompressedStorageProvider) ReadPage(spaceID, pageNo uint32) ([]byte, error) {
	data, err := p.StorageProvider.ReadPage(spaceID, pageNo)
	if err != nil || !p.isSpaceCompressed(spaceID) {
		return data, err
	}
	p.ensureSpaceSettings(spaceID)
	decoded, err := p.compression.DecompressPage(spaceID, pageNo, data)
	if err != nil {
		return nil, fmt.Errorf("decompress page space=%d page=%d: %w", spaceID, pageNo, err)
	}
	if uint32(len(decoded)) != p.pageSize {
		return nil, fmt.Errorf("decompressed page space=%d page=%d has size %d, want %d", spaceID, pageNo, len(decoded), p.pageSize)
	}
	return decoded, nil
}

func (p *CompressedStorageProvider) WritePage(spaceID, pageNo uint32, data []byte) error {
	if !p.isSpaceCompressed(spaceID) {
		return p.StorageProvider.WritePage(spaceID, pageNo, data)
	}
	p.ensureSpaceSettings(spaceID)
	if uint32(len(data)) != p.pageSize {
		return fmt.Errorf("invalid page size for compression: %d, want %d", len(data), p.pageSize)
	}
	compressed, err := p.compression.CompressPage(spaceID, pageNo, data)
	if err != nil {
		return fmt.Errorf("compress page space=%d page=%d: %w", spaceID, pageNo, err)
	}
	if uint32(len(compressed)) > p.pageSize {
		return fmt.Errorf("compressed page space=%d page=%d has size %d, exceeds page size %d", spaceID, pageNo, len(compressed), p.pageSize)
	}
	envelope := make([]byte, p.pageSize)
	copy(envelope, compressed)
	return p.StorageProvider.WritePage(spaceID, pageNo, envelope)
}

func (p *CompressedStorageProvider) GetSpaceInfo(spaceID uint32) (*basic.SpaceInfo, error) {
	info, err := p.StorageProvider.GetSpaceInfo(spaceID)
	if err != nil || info == nil {
		return info, err
	}
	copyInfo := *info
	copyInfo.IsCompressed = p.isSpaceCompressed(spaceID)
	return &copyInfo, nil
}

func (p *CompressedStorageProvider) ListSpaces() ([]basic.SpaceInfo, error) {
	spaces, err := p.StorageProvider.ListSpaces()
	if err != nil {
		return nil, err
	}
	for i := range spaces {
		spaces[i].IsCompressed = p.isSpaceCompressed(spaces[i].SpaceID)
	}
	return spaces, nil
}
