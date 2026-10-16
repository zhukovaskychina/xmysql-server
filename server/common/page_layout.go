package common

// InnoDB file-header offsets. SpaceID is at byte 34; byte zero is a checksum.
const (
	InnoDBFileHeaderChecksumOffset  = 0
	InnoDBFileHeaderPageNoOffset    = 4
	InnoDBFileHeaderPrevOffset      = 8
	InnoDBFileHeaderNextOffset      = 12
	InnoDBFileHeaderLSNOffset       = 16
	InnoDBFileHeaderPageTypeOffset  = 24
	InnoDBFileHeaderFlushLSNOffset  = 26
	InnoDBFileHeaderSpaceIDOffset   = 34
	InnoDBFileTrailerChecksumOffset = 0
	InnoDBFileTrailerLSNLowOffset   = 4
)

// Legacy types.BasePage uses its own format: SpaceID occupies byte zero.
// Do not substitute the InnoDB layout even where individual offsets coincide.
const (
	LegacyBasePageHeaderSpaceIDOffset      = 0
	LegacyBasePageHeaderPageNoOffset       = 4
	LegacyBasePageHeaderPrevOffset         = 8
	LegacyBasePageHeaderNextOffset         = 12
	LegacyBasePageHeaderLSNOffset          = 16
	LegacyBasePageHeaderPageTypeOffset     = 24
	LegacyBasePageHeaderFileSequenceOffset = 26
	LegacyBasePageHeaderPageVersionOffset  = 28
	LegacyBasePageHeaderCompressTypeOffset = 30
	LegacyBasePageTrailerChecksumOffset    = 0
	LegacyBasePageTrailerLSNOffset         = 4
)

// Legacy page.BasePage has SpaceID at byte 34, but retains two-byte
// file-sequence and version fields rather than InnoDB's eight-byte FlushLSN.
const (
	LegacyPageBaseHeaderSpaceIDOffset      = 34
	LegacyPageBaseHeaderPageNoOffset       = 4
	LegacyPageBaseHeaderPrevOffset         = 8
	LegacyPageBaseHeaderNextOffset         = 12
	LegacyPageBaseHeaderLSNOffset          = 16
	LegacyPageBaseHeaderPageTypeOffset     = 24
	LegacyPageBaseHeaderFileSequenceOffset = 26
	LegacyPageBaseHeaderPageVersionOffset  = 28
	LegacyPageBaseTrailerChecksumOffset    = 0
	LegacyPageBaseTrailerLSNOffset         = 4
)

// File-header field widths, shared by InnoDB serializers.
const (
	InnoDBCheckSumSize      = 4
	InnoDBPageOffsetSize    = 4
	InnoDBPagePrevSize      = 4
	InnoDBPageNextSize      = 4
	InnoDBPageLSNSize       = 8
	InnoDBPageTypeSize      = 2
	InnoDBPageFileFlushSize = 8
	InnoDBPageArchSize      = 4
)
