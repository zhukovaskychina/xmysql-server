package replication

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var ErrChecksumMismatch = errors.New("binlog checksum mismatch")

type NativeBinlogFile struct {
	Name string
	Size uint64
}

// NativeBinlogEvent describes one physical event frame in a binlog.NNNNNN
// file. Position is the event start offset and EndPosition is the offset of
// the next event, matching the positions exposed by SHOW BINLOG EVENTS.
type NativeBinlogEvent struct {
	File        string
	Position    uint64
	EndPosition uint64
	Type        byte
	Raw         []byte
	// ChecksumLength is the physical footer width learned from the file's
	// FORMAT_DESCRIPTION_EVENT. A zero-width footer represents BINLOG_CHECKSUM_ALG_OFF.
	// Older in-memory callers leave it zero, so decoder helpers treat an
	// unknown value as the package's default CRC32 framing.
	ChecksumLength int
	ChecksumKnown  bool
}

// NativeGTIDIndexEntry identifies the physical GTID event for one executed
// transaction. SID is the 16-byte native SID in hexadecimal; keeping the SID
// alongside the logical sequence also supports non-UUID source names whose
// native encoding is deliberately hashed.
type NativeGTIDIndexEntry struct {
	SID      string `json:"sid"`
	Tag      string `json:"tag,omitempty"`
	Sequence uint64 `json:"sequence"`
	File     string `json:"file"`
	Position uint64 `json:"position"`
	End      uint64 `json:"end"`
}

type BinlogWriter struct {
	mu                  sync.Mutex
	path                string
	nativePath          string
	nativeIndexPath     string
	nativeGTIDIndexPath string
	nativeIndex         uint32
	serverID            uint32
	position            uint64
	previousGTIDs       GTIDSet
	executedGTIDs       GTIDSet
	nativeGTIDIndex     map[string]NativeGTIDIndexEntry
}

// Position returns the last logical stream position currently allocated by
// the writer. Call NextPosition when the next append position is required.
func (writer *BinlogWriter) Position() uint64 {
	if writer == nil {
		return 4
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.position
}

// NextPosition returns the position at which the next logical event will be
// allocated. Position 4 is the empty-binlog starting position.
func (writer *BinlogWriter) NextPosition() uint64 {
	if writer == nil {
		return 4
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.position <= 4 {
		return 4
	}
	return writer.position + 1
}

// ServerID returns the source server-id encoded in native binlog events.
func (writer *BinlogWriter) ServerID() uint32 {
	if writer == nil {
		return 0
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.serverID
}

// FileSize returns the durable JSONL byte size represented by the logical
// binlog. It is exposed for SHOW BINARY LOGS; the native wire adapter remains
// responsible for translating logical events into MySQL event frames.
func (writer *BinlogWriter) FileSize() uint64 {
	if writer == nil {
		return 0
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	info, err := os.Stat(writer.path)
	if err != nil {
		return 0
	}
	return uint64(info.Size())
}

// NativeFileSize returns the durable byte size of the MySQL-compatible
// binlog. The JSONL stream remains the internal recovery source, while this
// file is suitable for tools that consume binlog.000001 directly.
func (writer *BinlogWriter) NativeFileSize() uint64 {
	if writer == nil {
		return 0
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	info, err := os.Stat(writer.nativePath)
	if err != nil {
		return 0
	}
	return uint64(info.Size())
}

// NativeCurrentFilePosition returns the active native binlog filename and its
// durable end offset. The offset is the value replication clients expect from
// SHOW MASTER STATUS/SHOW SOURCE STATUS, unlike the internal JSONL position.
func (writer *BinlogWriter) NativeCurrentFilePosition() (string, uint64) {
	if writer == nil {
		return "", 4
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.nativePath == "" {
		return "", 4
	}
	info, err := os.Stat(writer.nativePath)
	if err != nil {
		return filepath.Base(writer.nativePath), 4
	}
	return filepath.Base(writer.nativePath), uint64(info.Size())
}

// NativeFiles returns the durable native binlog files in log order.
func (writer *BinlogWriter) NativeFiles() []NativeBinlogFile {
	if writer == nil {
		return nil
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.nativeFilesLocked()
}

// NativeGTIDIndex returns a stable snapshot of the durable GTID-to-physical
// position index used by native dump/PITR tooling.
func (writer *BinlogWriter) NativeGTIDIndex() []NativeGTIDIndexEntry {
	if writer == nil {
		return nil
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	entries := make([]NativeGTIDIndexEntry, 0, len(writer.nativeGTIDIndex))
	for _, entry := range writer.nativeGTIDIndex {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].File != entries[j].File {
			return entries[i].File < entries[j].File
		}
		if entries[i].Position != entries[j].Position {
			return entries[i].Position < entries[j].Position
		}
		return entries[i].SID < entries[j].SID
	})
	return entries
}

// NativeGTIDPosition resolves a logical UUID (or a 16-byte native SID in
// hexadecimal form) and sequence to its durable physical GTID event range.
// The lookup uses the same SID normalization as the native encoder, so it is
// safe for callers whose logical source UUID is not itself a UUID string.
func (writer *BinlogWriter) NativeGTIDPosition(uuidOrSID string, sequence uint64) (NativeGTIDIndexEntry, bool) {
	if writer == nil || strings.TrimSpace(uuidOrSID) == "" || sequence == 0 {
		return NativeGTIDIndexEntry{}, false
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.nativeGTIDIndex == nil {
		return NativeGTIDIndexEntry{}, false
	}
	sid := hex.EncodeToString(nativeGTIDSID(uuidOrSID))
	entry, ok := writer.nativeGTIDIndex[nativeGTIDIndexKey(sid, sequence)]
	return entry, ok
}

// NativeGTIDPositionTagged resolves a tagged GTID to its physical event range.
// Tags are part of the MySQL 8.4 transaction identity and therefore cannot be
// folded into the untagged SID/sequence key.
func (writer *BinlogWriter) NativeGTIDPositionTagged(uuidOrSID, tag string, sequence uint64) (NativeGTIDIndexEntry, bool) {
	if writer == nil || strings.TrimSpace(uuidOrSID) == "" || sequence == 0 || !validNativeGTIDTag(tag) {
		return NativeGTIDIndexEntry{}, false
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.nativeGTIDIndex == nil {
		return NativeGTIDIndexEntry{}, false
	}
	sid := hex.EncodeToString(nativeGTIDSID(uuidOrSID))
	entry, ok := writer.nativeGTIDIndex[nativeGTIDTaggedIndexKey(sid, tag, sequence)]
	return entry, ok
}

// NativeEvents reads and validates the physical event frames for one native
// binlog file. It is intentionally separate from ReadFrom: the latter is the
// durable logical recovery stream, while this method preserves native file
// offsets and wire payloads for administrative and replication consumers.
func (writer *BinlogWriter) NativeEvents(logName string) ([]NativeBinlogEvent, error) {
	if writer == nil {
		return nil, nil
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	index, err := nativeBinlogFileIndex(logName)
	if err != nil {
		return nil, err
	}
	path := writer.nativePathForIndex(uint32(index))
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) < 4 || string(raw[:4]) != string([]byte{nativeBinlogMagic, 'b', 'i', 'n'}) {
		return nil, errors.New("invalid native binlog magic")
	}
	result := make([]NativeBinlogEvent, 0)
	checksumLength := nativeChecksumLength
	for offset := 4; offset < len(raw); {
		if len(raw)-offset < nativeEventHeaderLength {
			return nil, fmt.Errorf("truncated native binlog event at position %d", offset)
		}
		eventSize := int(binary.LittleEndian.Uint32(raw[offset+9 : offset+13]))
		minimumSize := nativeEventHeaderLength + checksumLength
		if eventSize < minimumSize || eventSize > len(raw)-offset {
			return nil, fmt.Errorf("invalid native binlog event size %d at position %d", eventSize, offset)
		}
		frame := append([]byte(nil), raw[offset:offset+eventSize]...)
		if frame[4] == 15 && offset == 4 {
			if eventSize >= nativeEventHeaderLength+nativeChecksumLength && nativeFrameHasChecksum(frame, nativeChecksumLength) {
				checksumLength = nativeChecksumLength
			} else if eventSize >= nativeEventHeaderLength+1 && frame[eventSize-1] == 0 {
				checksumLength = 0
			} else {
				return nil, fmt.Errorf("FORMAT_DESCRIPTION_EVENT has unknown checksum mode")
			}
			minimumSize = nativeEventHeaderLength + checksumLength
			if eventSize < minimumSize {
				return nil, fmt.Errorf("invalid FORMAT_DESCRIPTION_EVENT size %d", eventSize)
			}
		}
		if checksumLength > 0 && !nativeFrameHasChecksum(frame, checksumLength) {
			return nil, ErrChecksumMismatch
		}
		result = append(result, NativeBinlogEvent{
			File:           logName,
			Position:       uint64(offset),
			EndPosition:    uint64(offset + eventSize),
			Type:           frame[4],
			Raw:            frame,
			ChecksumLength: checksumLength,
			ChecksumKnown:  true,
		})
		offset += eventSize
	}
	return result, nil
}

func nativeFrameHasChecksum(frame []byte, checksumLength int) bool {
	if checksumLength != nativeChecksumLength || len(frame) < nativeEventHeaderLength+checksumLength {
		return false
	}
	want := binary.LittleEndian.Uint32(frame[len(frame)-checksumLength:])
	return want == crc32.ChecksumIEEE(frame[:len(frame)-checksumLength])
}

func NewBinlogWriter(path string, serverID uint32) (*BinlogWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	nativeIndex := discoverNativeBinlogIndex(filepath.Dir(path))
	writer := &BinlogWriter{
		path:                path,
		nativeIndexPath:     filepath.Join(filepath.Dir(path), "binlog.index"),
		nativeGTIDIndexPath: filepath.Join(filepath.Dir(path), "binlog.gtid.index"),
		nativeIndex:         nativeIndex,
		serverID:            serverID,
		position:            4,
		previousGTIDs:       GTIDSet{},
		executedGTIDs:       GTIDSet{},
		nativeGTIDIndex:     make(map[string]NativeGTIDIndexEntry),
	}
	writer.nativePath = writer.nativePathForIndex(nativeIndex)
	if err := writer.ensureNativeFile(); err != nil {
		return nil, err
	}
	events, err := writer.ReadFrom(4)
	if err != nil {
		return nil, err
	}
	writer.reconcileGTIDs(events)
	for _, event := range events {
		if event.Position > writer.position {
			writer.position = event.Position
		}
	}
	nativeFiles := writer.NativeFiles()
	if !writer.nativeFilesMatchLogicalEvents(events, nativeFiles) {
		if err := writer.rebuildNativeFiles(events); err != nil {
			return nil, err
		}
	}
	if err := writer.ensureNativeIndex(); err != nil {
		return nil, err
	}
	if err := writer.rebuildNativeGTIDIndexLocked(); err != nil {
		return nil, err
	}
	return writer, nil
}

func (writer *BinlogWriter) nativeFilesMatchLogicalEvents(events []BinlogEvent, files []NativeBinlogFile) bool {
	expected := [][]byte{{15, 35}}
	for _, event := range events {
		if event.Type == EventRotate {
			expected[len(expected)-1] = append(expected[len(expected)-1], 4)
			expected = append(expected, []byte{15, 35})
			continue
		}
		switch event.Type {
		case EventBegin:
			expected[len(expected)-1] = append(expected[len(expected)-1], 33)
		case EventRow:
			if len(event.Changes) == 0 {
				expected[len(expected)-1] = append(expected[len(expected)-1], 2)
			} else {
				for index := 0; index < len(event.Changes); {
					change := event.Changes[index]
					expected[len(expected)-1] = append(expected[len(expected)-1], 19, nativeRowEventTypeForChange(change))
					index++
					for index < len(event.Changes) && nativeChangesCanShareRowEvent(change, event.Changes[index]) {
						index++
					}
				}
			}
		case EventCommit:
			expected[len(expected)-1] = append(expected[len(expected)-1], 16)
		default:
			return false
		}
	}
	if len(expected) != len(files) {
		return false
	}
	for index, file := range files {
		if file.Name != fmt.Sprintf("binlog.%06d", index+1) {
			return false
		}
		physical, err := writer.NativeEvents(file.Name)
		if err != nil {
			return false
		}
		actual := make([]byte, 0, len(physical))
		for _, event := range physical {
			actual = append(actual, event.Type)
		}
		if !bytes.Equal(actual, expected[index]) {
			return false
		}
	}
	return true
}

func (writer *BinlogWriter) Append(gtid GTID, changes []RowChange) ([]BinlogEvent, error) {
	return writer.AppendTransaction(gtid, changes, nil)
}

func (writer *BinlogWriter) AppendTransaction(gtid GTID, changes []RowChange, statements []Statement) ([]BinlogEvent, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	changes = nativeChangesWithDerivedPartialJSON(changes)
	file, err := os.OpenFile(writer.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	events := []BinlogEvent{
		{Timestamp: timeNow(), Type: EventBegin, ServerID: writer.serverID, GTID: gtid},
		{Timestamp: timeNow(), Type: EventRow, ServerID: writer.serverID, GTID: gtid, Changes: changes, Statements: statements},
		{Timestamp: timeNow(), Type: EventCommit, ServerID: writer.serverID, GTID: gtid},
	}
	for i := range events {
		writer.position++
		events[i].Position = writer.position
		payload, err := events[i].Encode()
		if err != nil {
			return nil, err
		}
		if err := json.NewEncoder(file).Encode(payload); err != nil {
			return nil, err
		}
	}
	if err := file.Sync(); err != nil {
		return nil, err
	}
	if err := writer.appendNativeTransaction(events); err != nil {
		return nil, err
	}
	writer.executedGTIDs.Add(gtid)
	return events, nil
}

func nativeChangesWithDerivedPartialJSON(changes []RowChange) []RowChange {
	if len(changes) == 0 {
		return nil
	}
	result := append([]RowChange(nil), changes...)
	for index := range result {
		change := result[index]
		if !strings.EqualFold(strings.TrimSpace(change.Action), "update") || len(change.PartialJSONUpdates) > 0 {
			continue
		}
		derived := make(map[string][]JSONPartialUpdate)
		for column := range change.After {
			if nativeColumnTypeForChange(change, column, change.After[column]) != 245 {
				continue
			}
			updates, ok := DeriveJSONPartialUpdates(change.Before[column], change.After[column])
			if len(updates) == 0 || !ok {
				continue
			}
			candidate := change
			candidate.PartialJSONUpdates = map[string][]JSONPartialUpdate{column: updates}
			if _, compact := nativePartialJSONValue(candidate, column); compact {
				derived[column] = updates
			}
		}
		if len(derived) > 0 {
			change.PartialJSONUpdates = derived
			result[index] = change
		}
	}
	return result
}

// Rotate appends a durable rotate marker and starts the next native binlog
// file. The JSONL stream keeps the logical position boundary used by the
// internal replica, while native consumers see the physical file transition.
func (writer *BinlogWriter) Rotate() (BinlogEvent, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	file, err := os.OpenFile(writer.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return BinlogEvent{}, err
	}
	defer file.Close()
	event := BinlogEvent{Timestamp: timeNow(), Type: EventRotate, ServerID: writer.serverID}
	writer.position++
	event.Position = writer.position
	payload, err := event.Encode()
	if err != nil {
		return BinlogEvent{}, err
	}
	if err := json.NewEncoder(file).Encode(payload); err != nil {
		return BinlogEvent{}, err
	}
	if err := file.Sync(); err != nil {
		return BinlogEvent{}, err
	}
	nextIndex := writer.nativeIndex + 1
	nextName := fmt.Sprintf("binlog.%06d", nextIndex)
	if err := writer.appendNativeRotate(event, nextName); err != nil {
		return BinlogEvent{}, err
	}
	writer.previousGTIDs = cloneGTIDSet(writer.executedGTIDs)
	nextPath := writer.nativePathForIndex(nextIndex)
	previousPath := writer.nativePath
	writer.nativeIndex = nextIndex
	writer.nativePath = nextPath
	if err := writer.ensureNativeFile(); err != nil {
		writer.nativeIndex = nextIndex - 1
		writer.nativePath = previousPath
		return BinlogEvent{}, err
	}
	if err := writer.ensureNativeIndex(); err != nil {
		return BinlogEvent{}, err
	}
	return event, nil
}

// Reset truncates the durable logical binlog and returns its position to the
// empty-stream origin used by RESET MASTER.
func (writer *BinlogWriter) Reset() error {
	if writer == nil {
		return nil
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	file, err := os.OpenFile(writer.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	writer.previousGTIDs = GTIDSet{}
	writer.executedGTIDs = GTIDSet{}
	writer.nativeGTIDIndex = make(map[string]NativeGTIDIndexEntry)
	if err := writer.resetNativeFile(); err != nil {
		return err
	}
	writer.position = 4
	if err := writer.ensureNativeIndex(); err != nil {
		return err
	}
	return writer.persistNativeGTIDIndexLocked()
}

var timeNow = func() time.Time { return time.Now().UTC() }

const (
	nativeBinlogMagic       byte = 0xfe
	nativeEventHeaderLength      = 19
	nativeChecksumLength         = 4
)

func (writer *BinlogWriter) ensureNativeFile() error {
	file, err := os.OpenFile(writer.nativePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		if _, err := file.Write([]byte{nativeBinlogMagic, 'b', 'i', 'n'}); err != nil {
			return err
		}
		format := buildNativeFormatDescriptionEvent(writer.serverID, 4)
		if _, err := file.Write(format); err != nil {
			return err
		}
		previous := buildNativePreviousGTIDsEvent(writer.serverID, uint64(4+len(format)), writer.previousGTIDs)
		if _, err := file.Write(previous); err != nil {
			return err
		}
		return file.Sync()
	}
	magic := make([]byte, 4)
	if _, err := file.ReadAt(magic, 0); err != nil {
		return err
	}
	if string(magic) != string([]byte{nativeBinlogMagic, 'b', 'i', 'n'}) {
		return errors.New("invalid native binlog magic")
	}
	return nil
}

func (writer *BinlogWriter) nativePathForIndex(index uint32) string {
	return filepath.Join(filepath.Dir(writer.path), "binlog."+fmt.Sprintf("%06d", index))
}

func (writer *BinlogWriter) nativeFilesLocked() []NativeBinlogFile {
	entries, err := os.ReadDir(filepath.Dir(writer.path))
	if err != nil {
		return nil
	}
	files := make([]NativeBinlogFile, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "binlog.") {
			continue
		}
		index, err := strconv.ParseUint(strings.TrimPrefix(entry.Name(), "binlog."), 10, 32)
		if err != nil || index == 0 {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		files = append(files, NativeBinlogFile{Name: entry.Name(), Size: uint64(info.Size())})
	}
	sort.Slice(files, func(left, right int) bool { return files[left].Name < files[right].Name })
	return files
}

func discoverNativeBinlogIndex(dir string) uint32 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 1
	}
	maxIndex := uint64(1)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "binlog.") {
			continue
		}
		index, err := strconv.ParseUint(strings.TrimPrefix(entry.Name(), "binlog."), 10, 32)
		if err == nil && index > maxIndex {
			maxIndex = index
		}
	}
	return uint32(maxIndex)
}

func (writer *BinlogWriter) resetNativeFile() error {
	for _, file := range writer.nativeFilesLocked() {
		if err := os.Remove(filepath.Join(filepath.Dir(writer.path), file.Name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	writer.nativeIndex = 1
	writer.nativePath = writer.nativePathForIndex(writer.nativeIndex)
	return writer.ensureNativeFile()
}

func (writer *BinlogWriter) reconcileGTIDs(events []BinlogEvent) {
	executed := GTIDSet{}
	previous := GTIDSet{}
	for _, event := range events {
		if event.Type == EventRotate {
			previous = cloneGTIDSet(executed)
			continue
		}
		if event.Type == EventCommit {
			executed.Add(event.GTID)
		}
	}
	writer.executedGTIDs = executed
	writer.previousGTIDs = previous
}

func (writer *BinlogWriter) ensureNativeIndex() error {
	if writer == nil || writer.nativeIndexPath == "" {
		return nil
	}
	files := writer.nativeFilesLocked()
	lines := make([]string, 0, len(files))
	for _, file := range files {
		lines = append(lines, filepath.Join(filepath.Dir(writer.path), file.Name))
	}
	raw := []byte(strings.Join(lines, "\n"))
	if len(raw) > 0 {
		raw = append(raw, '\n')
	}
	return writeReplicationFileAtomic(writer.nativeIndexPath, raw)
}

func (writer *BinlogWriter) rebuildNativeGTIDIndexLocked() error {
	if writer == nil {
		return nil
	}
	index := make(map[string]NativeGTIDIndexEntry)
	for _, file := range writer.nativeFilesLocked() {
		events, err := writer.NativeEvents(file.Name)
		if err != nil {
			return err
		}
		for _, event := range events {
			if event.Type == 33 && len(event.Raw) >= nativeEventHeaderLength+1+16+8 {
				sid := hex.EncodeToString(event.Raw[nativeEventHeaderLength+1 : nativeEventHeaderLength+1+16])
				sequence := binary.LittleEndian.Uint64(event.Raw[nativeEventHeaderLength+1+16 : nativeEventHeaderLength+1+16+8])
				index[nativeGTIDIndexKey(sid, sequence)] = NativeGTIDIndexEntry{SID: sid, Sequence: sequence, File: event.File, Position: event.Position, End: event.EndPosition}
				continue
			}
			if event.Type == 42 && len(event.Raw) >= nativeEventHeaderLength+nativeChecksumLength {
				fields, parseErr := decodeNativeTaggedGTIDIndexFields(nativeEventBody(event))
				if parseErr != nil {
					return parseErr
				}
				sid := hex.EncodeToString(fields.sid)
				index[nativeGTIDTaggedIndexKey(sid, fields.tag, fields.sequence)] = NativeGTIDIndexEntry{SID: sid, Tag: fields.tag, Sequence: fields.sequence, File: event.File, Position: event.Position, End: event.EndPosition}
			}
		}
	}
	writer.nativeGTIDIndex = index
	return writer.persistNativeGTIDIndexLocked()
}

func nativeGTIDIndexKey(sid string, sequence uint64) string {
	return strings.ToLower(strings.TrimSpace(sid)) + ":" + strconv.FormatUint(sequence, 10)
}

func nativeGTIDTaggedIndexKey(sid, tag string, sequence uint64) string {
	return strings.ToLower(strings.TrimSpace(sid)) + ":" + tag + ":" + strconv.FormatUint(sequence, 10)
}

func (writer *BinlogWriter) persistNativeGTIDIndexLocked() error {
	if writer == nil || writer.nativeGTIDIndexPath == "" {
		return nil
	}
	entries := make([]NativeGTIDIndexEntry, 0, len(writer.nativeGTIDIndex))
	for _, entry := range writer.nativeGTIDIndex {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].File != entries[j].File {
			return entries[i].File < entries[j].File
		}
		return entries[i].Position < entries[j].Position
	})
	raw, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return writeReplicationFileAtomic(writer.nativeGTIDIndexPath, raw)
}

func (writer *BinlogWriter) appendNativeTransaction(events []BinlogEvent) error {
	file, err := os.OpenFile(writer.nativePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	position := info.Size()
	for _, event := range events {
		payloads := buildNativeEventPayloadsForLogical(event, uint64(position))
		eventStart := uint64(position)
		for _, payload := range payloads {
			patchNativeEventLogPosition(payload, uint64(position)+uint64(len(payload)))
			if _, err := file.Write(payload); err != nil {
				return err
			}
			position += int64(len(payload))
		}
		if event.Type == EventBegin && len(payloads) > 0 && len(payloads[0]) >= nativeEventHeaderLength+1+16+8 {
			raw := payloads[0]
			sid := hex.EncodeToString(raw[nativeEventHeaderLength+1 : nativeEventHeaderLength+1+16])
			sequence := binary.LittleEndian.Uint64(raw[nativeEventHeaderLength+1+16 : nativeEventHeaderLength+1+16+8])
			if writer.nativeGTIDIndex == nil {
				writer.nativeGTIDIndex = make(map[string]NativeGTIDIndexEntry)
			}
			writer.nativeGTIDIndex[nativeGTIDIndexKey(sid, sequence)] = NativeGTIDIndexEntry{SID: sid, Sequence: sequence, File: filepath.Base(writer.nativePath), Position: eventStart, End: uint64(position)}
		}
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return writer.persistNativeGTIDIndexLocked()
}

func (writer *BinlogWriter) appendNativeEvent(event BinlogEvent) error {
	return writer.appendNativeTransaction([]BinlogEvent{event})
}

func (writer *BinlogWriter) appendNativeRotate(event BinlogEvent, nextName string) error {
	file, err := os.OpenFile(writer.nativePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	body := make([]byte, 8, 8+len(nextName))
	binary.LittleEndian.PutUint64(body, 4)
	body = append(body, []byte(nextName)...)
	payload := buildNativeEvent(4, body, eventWithNativePosition(event, uint64(info.Size())))
	if _, err := file.Write(payload); err != nil {
		return err
	}
	return file.Sync()
}

func (writer *BinlogWriter) rebuildNativeFiles(events []BinlogEvent) error {
	writer.previousGTIDs = GTIDSet{}
	writer.executedGTIDs = GTIDSet{}
	writer.nativeGTIDIndex = make(map[string]NativeGTIDIndexEntry)
	if err := writer.resetNativeFile(); err != nil {
		return err
	}
	pending := make([]BinlogEvent, 0, 3)
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		err := writer.appendNativeTransaction(pending)
		pending = pending[:0]
		return err
	}
	for _, event := range events {
		if event.Type != EventRotate {
			pending = append(pending, event)
			if event.Type == EventCommit {
				writer.executedGTIDs.Add(event.GTID)
			}
			continue
		}
		if err := flush(); err != nil {
			return err
		}
		nextIndex := writer.nativeIndex + 1
		nextName := fmt.Sprintf("binlog.%06d", nextIndex)
		if err := writer.appendNativeRotate(event, nextName); err != nil {
			return err
		}
		writer.previousGTIDs = cloneGTIDSet(writer.executedGTIDs)
		writer.nativeIndex = nextIndex
		writer.nativePath = writer.nativePathForIndex(nextIndex)
		if err := writer.ensureNativeFile(); err != nil {
			return err
		}
	}
	if err := flush(); err != nil {
		return err
	}
	return writer.ensureNativeIndex()
}

func (writer *BinlogWriter) nativeHeaderSize() uint64 {
	format := buildNativeFormatDescriptionEvent(writer.serverID, 4)
	previous := buildNativePreviousGTIDsEvent(writer.serverID, uint64(4+len(format)), writer.previousGTIDs)
	return uint64(4 + len(format) + len(previous))
}

func buildNativeFormatDescriptionEvent(serverID uint32, position uint64) []byte {
	const (
		formatDescriptionType = 15
		serverVersionLength   = 50
		eventTypeCount        = 40
	)
	body := make([]byte, 2+serverVersionLength+4+1+eventTypeCount)
	binary.LittleEndian.PutUint16(body[0:2], 4)
	copy(body[2:2+serverVersionLength], []byte("8.4.0-xmysql"))
	body[2+serverVersionLength+4] = nativeEventHeaderLength
	postHeaderLengths := body[2+serverVersionLength+5:]
	postHeaderLengths[2] = 13
	postHeaderLengths[19] = 8
	postHeaderLengths[30] = 6
	postHeaderLengths[31] = 6
	postHeaderLengths[32] = 6
	postHeaderLengths[33] = 42
	postHeaderLengths[35] = 8
	return buildNativeEvent(formatDescriptionType, body, BinlogEvent{
		Timestamp: timeNow(),
		ServerID:  serverID,
		Position:  position,
	})
}

func buildNativePreviousGTIDsEvent(serverID uint32, position uint64, set GTIDSet) []byte {
	const previousGTIDsEventType = 35
	uuidList := make([]string, 0, len(set))
	for uuid := range set {
		uuidList = append(uuidList, uuid)
	}
	sort.Strings(uuidList)
	body := make([]byte, 8)
	binary.LittleEndian.PutUint64(body, uint64(len(uuidList)))
	for _, uuid := range uuidList {
		body = append(body, nativeGTIDSID(uuid)...)
		sequences := make([]uint64, 0, len(set[uuid]))
		for sequence := range set[uuid] {
			if sequence > 0 {
				sequences = append(sequences, sequence)
			}
		}
		sort.Slice(sequences, func(left, right int) bool { return sequences[left] < sequences[right] })
		intervals := compactGTIDIntervals(sequences)
		count := make([]byte, 8)
		binary.LittleEndian.PutUint64(count, uint64(len(intervals)))
		body = append(body, count...)
		for _, interval := range intervals {
			bounds := make([]byte, 16)
			binary.LittleEndian.PutUint64(bounds[0:8], interval[0])
			binary.LittleEndian.PutUint64(bounds[8:16], interval[1])
			body = append(body, bounds...)
		}
	}
	return buildNativeEvent(previousGTIDsEventType, body, BinlogEvent{
		Timestamp: timeNow(),
		ServerID:  serverID,
		Position:  position,
	})
}

func compactGTIDIntervals(sequences []uint64) [][2]uint64 {
	if len(sequences) == 0 {
		return nil
	}
	intervals := make([][2]uint64, 0)
	start, end := sequences[0], sequences[0]
	for _, sequence := range sequences[1:] {
		if sequence == end+1 {
			end = sequence
			continue
		}
		intervals = append(intervals, [2]uint64{start, end + 1})
		start, end = sequence, sequence
	}
	intervals = append(intervals, [2]uint64{start, end + 1})
	return intervals
}

func buildNativeEventPayloadsForLogical(event BinlogEvent, position uint64) [][]byte {
	if event.Type != EventRow || len(event.Changes) == 0 {
		return [][]byte{buildNativeEventForLogical(event, position)}
	}
	payloads := make([][]byte, 0, len(event.Changes)*2)
	for index := 0; index < len(event.Changes); {
		first := event.Changes[index]
		end := index + 1
		for end < len(event.Changes) && nativeChangesCanShareRowEvent(first, event.Changes[end]) {
			end++
		}
		group := event.Changes[index:end]
		columns := nativeRowColumnsForChanges(group)
		tableID := nativeTableIDForChange(first.Table)
		payloads = append(payloads,
			buildNativeTableMapEventForChange(event, position, tableID, first, columns),
			buildNativeRowsEventForChanges(event, position, tableID, group, columns),
		)
		index = end
	}
	return payloads
}

// EncodeNativeBinlogEventPayloads exposes the same schema-aware native event
// builder used by the durable writer to protocol adapters in other packages.
// Keeping one row-image implementation prevents COM_BINLOG_DUMP helpers from
// silently losing negotiated column order, type metadata, or grouped rows.
func EncodeNativeBinlogEventPayloads(event BinlogEvent) [][]byte {
	payloads := buildNativeEventPayloadsForLogical(event, event.Position)
	position := event.Position
	for _, payload := range payloads {
		patchNativeEventLogPosition(payload, position+uint64(len(payload)))
		position += uint64(len(payload))
	}
	return payloads
}

func nativeChangesCanShareRowEvent(left, right RowChange) bool {
	if strings.ToLower(strings.TrimSpace(left.Table)) != strings.ToLower(strings.TrimSpace(right.Table)) || strings.ToLower(strings.TrimSpace(left.Action)) != strings.ToLower(strings.TrimSpace(right.Action)) {
		return false
	}
	if left.Flags != right.Flags {
		return false
	}
	if !bytes.Equal(left.ExtraRowInfo, right.ExtraRowInfo) {
		return false
	}
	leftPartial, _ := json.Marshal(left.PartialJSONUpdates)
	rightPartial, _ := json.Marshal(right.PartialJSONUpdates)
	if !bytes.Equal(leftPartial, rightPartial) {
		return false
	}
	leftColumns := nativeRowColumnsForChange(left)
	rightColumns := nativeRowColumnsForChange(right)
	if !bytes.Equal([]byte(strings.Join(leftColumns, "\x00")), []byte(strings.Join(rightColumns, "\x00"))) {
		return false
	}
	for _, column := range leftColumns {
		if left.ColumnTypes[column] != right.ColumnTypes[column] || !bytes.Equal(left.ColumnMetadata[column], right.ColumnMetadata[column]) {
			return false
		}
	}
	return true
}

func nativeRowColumnsForChanges(changes []RowChange) []string {
	if len(changes) == 0 {
		return nil
	}
	columns := nativeRowColumnsForChange(changes[0])
	seen := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		seen[column] = struct{}{}
	}
	for _, change := range changes[1:] {
		for _, column := range nativeRowColumnsForChange(change) {
			if _, ok := seen[column]; ok {
				continue
			}
			seen[column] = struct{}{}
			columns = append(columns, column)
		}
	}
	return columns
}

func nativeRowEventType(action string) byte {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "update":
		return 31 // UPDATE_ROWS_EVENTv2
	case "delete":
		return 32 // DELETE_ROWS_EVENTv2
	default:
		return 30 // WRITE_ROWS_EVENTv2
	}
}

func nativeRowEventTypeForChange(change RowChange) byte {
	if strings.EqualFold(strings.TrimSpace(change.Action), "update") && len(nativePartialJSONColumns(change)) > 0 {
		return 39 // PARTIAL_UPDATE_ROWS_EVENT
	}
	return nativeRowEventType(change.Action)
}

func nativePartialJSONColumns(change RowChange) []string {
	columns := nativeRowColumnsForChange(change)
	partial := make([]string, 0, len(change.PartialJSONUpdates))
	for _, column := range columns {
		if _, ok := nativePartialJSONValue(change, column); ok {
			partial = append(partial, column)
		}
	}
	return partial
}

func nativeTableIDForChange(table string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(strings.ToLower(strings.TrimSpace(table))))
	return hash.Sum64() & 0x0000FFFFFFFFFFFF
}

func nativeRowColumnsForChange(change RowChange) []string {
	if len(change.Columns) > 0 {
		columns := make([]string, 0, len(change.Columns))
		seen := make(map[string]struct{}, len(change.Columns))
		for _, column := range change.Columns {
			column = strings.TrimSpace(column)
			if column == "" {
				continue
			}
			if _, exists := seen[column]; exists {
				continue
			}
			seen[column] = struct{}{}
			columns = append(columns, column)
		}
		// Keep the writer tolerant of partial schema annotations: fields
		// present in the row image but absent from Columns are appended in a
		// deterministic order instead of being silently dropped.
		remaining := make([]string, 0, len(change.Before)+len(change.After))
		for column := range change.Before {
			if _, exists := seen[column]; !exists {
				seen[column] = struct{}{}
				remaining = append(remaining, column)
			}
		}
		for column := range change.After {
			if _, exists := seen[column]; !exists {
				seen[column] = struct{}{}
				remaining = append(remaining, column)
			}
		}
		sort.Strings(remaining)
		return append(columns, remaining...)
	}
	seen := make(map[string]struct{}, len(change.Before)+len(change.After))
	for column := range change.Before {
		seen[column] = struct{}{}
	}
	for column := range change.After {
		seen[column] = struct{}{}
	}
	columns := make([]string, 0, len(seen))
	for column := range seen {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

func appendNativeUint48(dst []byte, value uint64) []byte {
	for index := 0; index < 6; index++ {
		dst = append(dst, byte(value>>uint(index*8)))
	}
	return dst
}

func appendNativeLenencInt(dst []byte, value int) []byte {
	if value < 251 {
		return append(dst, byte(value))
	}
	if value <= 0xFFFF {
		dst = append(dst, 0xfc, 0, 0)
		binary.LittleEndian.PutUint16(dst[len(dst)-2:], uint16(value))
		return dst
	}
	dst = append(dst, 0xfd, 0, 0, 0)
	for index := 0; index < 3; index++ {
		dst[len(dst)-3+index] = byte(value >> uint(index*8))
	}
	return dst
}

func nativeValueForChange(change RowChange, column string) interface{} {
	if value, ok := change.After[column]; ok {
		return value
	}
	return change.Before[column]
}

func nativeColumnTypeForValue(value interface{}) byte {
	switch value.(type) {
	case bool, int8, uint8:
		return 1 // MYSQL_TYPE_TINY
	case int, int16, int32, uint, uint16, uint32:
		return 3 // MYSQL_TYPE_LONG
	case int64, uint64:
		return 8 // MYSQL_TYPE_LONGLONG
	case float32:
		return 4 // MYSQL_TYPE_FLOAT
	case float64:
		return 5 // MYSQL_TYPE_DOUBLE
	default:
		return 253 // MYSQL_TYPE_VAR_STRING
	}
}

func nativeColumnTypeForChange(change RowChange, column string, value interface{}) byte {
	if rawType, ok := change.ColumnTypes[column]; ok {
		if typeCode, recognized := nativeColumnTypeForName(rawType); recognized {
			return typeCode
		}
	}
	return nativeColumnTypeForValue(value)
}

func nativeColumnTypeForName(raw string) (byte, bool) {
	typeName := strings.ToUpper(strings.TrimSpace(raw))
	if strings.HasPrefix(typeName, "FLOAT(") {
		if close := strings.IndexByte(typeName, ')'); close > len("FLOAT(") {
			precision, err := strconv.ParseUint(strings.TrimSpace(typeName[len("FLOAT("):close]), 10, 8)
			if err == nil && precision > 24 {
				return 5, true // FLOAT(p>24) uses MYSQL_TYPE_DOUBLE.
			}
		}
	}
	if open := strings.IndexByte(typeName, '('); open >= 0 {
		typeName = typeName[:open]
	}
	if fields := strings.Fields(typeName); len(fields) > 0 {
		typeName = fields[0]
	}
	switch typeName {
	case "NULL":
		return 6, true // MYSQL_TYPE_NULL
	case "TINYINT", "BOOL", "BOOLEAN":
		return 1, true // MYSQL_TYPE_TINY
	case "SMALLINT":
		return 2, true // MYSQL_TYPE_SHORT
	case "MEDIUMINT":
		return 9, true // MYSQL_TYPE_INT24
	case "INT", "INTEGER":
		return 3, true // MYSQL_TYPE_LONG
	case "FLOAT":
		return 4, true // MYSQL_TYPE_FLOAT
	case "DOUBLE", "REAL":
		return 5, true // MYSQL_TYPE_DOUBLE
	case "TIMESTAMP":
		return 7, true // MYSQL_TYPE_TIMESTAMP
	case "TIME":
		return 11, true // MYSQL_TYPE_TIME
	case "YEAR":
		return 13, true // MYSQL_TYPE_YEAR
	case "NEWDATE":
		return 14, true // MYSQL_TYPE_NEWDATE
	case "TIME2":
		return 17, true // MYSQL_TYPE_TIME2
	case "DATETIME2":
		return 18, true // MYSQL_TYPE_DATETIME2
	case "TIMESTAMP2":
		return 19, true // MYSQL_TYPE_TIMESTAMP2
	case "BIGINT":
		return 8, true // MYSQL_TYPE_LONGLONG
	case "DATE":
		return 10, true // MYSQL_TYPE_DATE
	case "DATETIME":
		return 12, true // MYSQL_TYPE_DATETIME
	case "VARCHAR":
		return 15, true // MYSQL_TYPE_VARCHAR
	case "BIT":
		return 16, true // MYSQL_TYPE_BIT
	case "TYPED_ARRAY":
		return 20, true // MYSQL_TYPE_TYPED_ARRAY (replication-only)
	case "VECTOR":
		return 242, true // MYSQL_TYPE_VECTOR
	case "JSON":
		return 245, true // MYSQL_TYPE_JSON
	case "DECIMAL", "NUMERIC":
		return 246, true // MYSQL_TYPE_NEWDECIMAL
	case "ENUM", "SET":
		return 254, true // MYSQL_TYPE_STRING; real type is in metadata
	case "TINYBLOB", "TINYTEXT":
		return 249, true // MYSQL_TYPE_TINY_BLOB
	case "MEDIUMBLOB", "MEDIUMTEXT":
		return 250, true // MYSQL_TYPE_MEDIUM_BLOB
	case "LONGBLOB", "LONGTEXT":
		return 251, true // MYSQL_TYPE_LONG_BLOB
	case "BLOB", "TEXT":
		return 252, true // MYSQL_TYPE_BLOB
	case "VARBINARY":
		return 253, true // MYSQL_TYPE_VAR_STRING
	case "CHAR", "BINARY":
		return 254, true // MYSQL_TYPE_STRING
	case "GEOMETRY":
		return 255, true // MYSQL_TYPE_GEOMETRY
	default:
		return 0, false
	}
}

func nativeColumnMetadataForValue(typeCode byte, value interface{}) []byte {
	switch typeCode {
	case 20, 242:
		length := len(nativeColumnStringBytes(value))
		if length > 255 {
			return []byte{2}
		}
		return []byte{1}
	case 245, 255:
		// JSON and GEOMETRY are BLOB-derived row-event values. Their
		// metadata is the fixed-width length prefix used by the native
		// row decoder; without schema metadata, use the long-blob width.
		return []byte{4}
	case 4:
		// FLOAT metadata carries the native pack length.
		return []byte{4}
	case 5:
		// DOUBLE metadata carries the native pack length.
		return []byte{8}
	case 249:
		return []byte{1}
	case 250:
		return []byte{3}
	case 251:
		return []byte{4}
	case 252:
		return []byte{2}
	case 15, 253:
		length := len([]byte(fmt.Sprint(value)))
		if length > 0xFFFF {
			length = 0xFFFF
		}
		metadata := make([]byte, 2)
		binary.LittleEndian.PutUint16(metadata, uint16(length))
		return metadata
	case 254:
		length := len([]byte(fmt.Sprint(value)))
		if length > 1023 {
			length = 1023
		}
		return nativeStringFieldMetadata(length)
	default:
		return nil
	}
}

func nativeColumnMetadataForChange(change RowChange, column string, typeCode byte, value interface{}) []byte {
	if metadata, ok := change.ColumnMetadata[column]; ok {
		return append([]byte(nil), metadata...)
	}
	if rawType, ok := change.ColumnTypes[column]; ok {
		if metadata := nativeColumnMetadataForTypeName(rawType, typeCode); metadata != nil {
			return metadata
		}
	}
	return nativeColumnMetadataForValue(typeCode, value)
}

func nativeColumnMetadataForTypeName(raw string, typeCode byte) []byte {
	typeName := strings.ToUpper(strings.TrimSpace(raw))
	if typeCode == 247 || typeCode == 248 {
		return nativeEnumSetMetadata(typeName, typeCode)
	}
	typeBase := typeName
	if open := strings.IndexByte(typeBase, '('); open >= 0 {
		typeBase = typeBase[:open]
	}
	if fields := strings.Fields(typeBase); len(fields) > 0 {
		typeBase = fields[0]
	}
	if typeCode == 254 && typeBase == "ENUM" {
		return nativeEnumSetMetadata(typeName, 247)
	}
	if typeCode == 254 && typeBase == "SET" {
		return nativeEnumSetMetadata(typeName, 248)
	}
	open := strings.IndexByte(typeName, '(')
	if open < 0 {
		switch typeCode {
		case 4:
			return []byte{4}
		case 5:
			return []byte{8}
		case 17, 18, 19:
			// TIME2/DATETIME2/TIMESTAMP2 carry fractional-second
			// precision as one metadata byte. No suffix means fsp=0.
			return []byte{0}
		case 16:
			return nil
		case 20, 242:
			return []byte{1}
		case 245, 255:
			// JSON and GEOMETRY use a four-byte pack length by default.
			return []byte{4}
		case 249, 250, 251, 252:
			fields := strings.Fields(typeName)
			if len(fields) == 0 {
				return nil
			}
			switch fields[0] {
			case "TINYBLOB":
				return []byte{1}
			case "MEDIUMBLOB":
				return []byte{3}
			case "LONGBLOB":
				return []byte{4}
			default:
				width := byte(2)
				switch typeCode {
				case 249:
					width = 1
				case 250:
					width = 3
				case 251:
					width = 4
				}
				return []byte{width}
			}
		}
		return nil
	}
	close := strings.LastIndexByte(typeName, ')')
	if close <= open {
		return nil
	}
	arguments := strings.Split(typeName[open+1:close], ",")
	if (typeCode == 4 || typeCode == 5) && len(arguments) >= 1 {
		if precision, err := strconv.ParseUint(strings.TrimSpace(arguments[0]), 10, 8); err == nil {
			if typeCode == 4 && precision <= 24 {
				return []byte{4}
			}
			if typeCode == 5 && precision > 24 {
				return []byte{8}
			}
		}
	}
	if typeCode == 246 && len(arguments) == 2 {
		precision, precisionErr := strconv.ParseUint(strings.TrimSpace(arguments[0]), 10, 8)
		scale, scaleErr := strconv.ParseUint(strings.TrimSpace(arguments[1]), 10, 8)
		if precisionErr == nil && scaleErr == nil {
			return []byte{byte(precision), byte(scale)}
		}
	}
	if typeCode == 15 || typeCode == 253 {
		if length, err := strconv.ParseUint(strings.TrimSpace(arguments[0]), 10, 16); err == nil {
			metadata := make([]byte, 2)
			binary.LittleEndian.PutUint16(metadata, uint16(length))
			return metadata
		}
	}
	if typeCode == 254 && len(arguments) == 1 {
		if length, err := strconv.ParseUint(strings.TrimSpace(arguments[0]), 10, 16); err == nil && length <= 1023 {
			return nativeStringFieldMetadata(int(length))
		}
	}
	if typeCode == 16 && len(arguments) == 1 {
		bits, err := strconv.ParseUint(strings.TrimSpace(arguments[0]), 10, 8)
		if err == nil && bits > 0 && bits <= 64 {
			return []byte{byte(bits % 8), byte(bits / 8)}
		}
	}
	if (typeCode == 17 || typeCode == 18 || typeCode == 19) && len(arguments) == 1 {
		fsp, err := strconv.ParseUint(strings.TrimSpace(arguments[0]), 10, 8)
		if err == nil && fsp <= 6 {
			return []byte{byte(fsp)}
		}
	}
	if typeCode >= 249 && typeCode <= 252 && len(arguments) == 1 {
		length, err := strconv.ParseUint(strings.TrimSpace(arguments[0]), 10, 32)
		if err == nil {
			width := byte(4)
			switch {
			case length <= 0xff:
				width = 1
			case length <= 0xffff:
				width = 2
			case length <= 0xffffff:
				width = 3
			}
			return []byte{width}
		}
	}
	return nil
}

func nativeStringFieldMetadata(fieldLength int) []byte {
	if fieldLength < 0 || fieldLength > 1023 {
		return nil
	}
	return []byte{byte(254 ^ ((fieldLength & 0x300) >> 4)), byte(fieldLength)}
}

func nativeEnumSetMetadata(typeName string, typeCode byte) []byte {
	packLength := byte(1)
	if members, ok := nativeEnumSetMembers(typeName); ok {
		if typeCode == 247 && len(members) > 255 {
			packLength = 2
		}
		if typeCode == 248 {
			packLength = byte((len(members) + 7) / 8)
			if packLength == 0 || packLength > 8 {
				return nil
			}
		}
	}
	return []byte{typeCode, packLength}
}

func nativeEnumSetMembers(typeName string) ([]string, bool) {
	open := strings.IndexByte(typeName, '(')
	close := strings.LastIndexByte(typeName, ')')
	if open < 0 || close <= open {
		return nil, false
	}
	body := typeName[open+1 : close]
	parts := make([]string, 0, 8)
	start := 0
	quote := byte(0)
	escaped := false
	for index := 0; index < len(body); index++ {
		ch := body[index]
		if escaped {
			escaped = false
			continue
		}
		if quote != 0 {
			if ch == '\\' {
				escaped = true
			} else if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
		} else if ch == ',' {
			parts = append(parts, nativeEnumSetMemberValue(body[start:index]))
			start = index + 1
		}
	}
	if quote != 0 || escaped {
		return nil, false
	}
	parts = append(parts, nativeEnumSetMemberValue(body[start:]))
	if len(parts) == 1 && parts[0] == "" {
		return nil, false
	}
	return parts, true
}

func nativeEnumSetMemberValue(raw string) string {
	value := strings.TrimSpace(raw)
	if len(value) >= 2 && ((value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"')) {
		quote := value[0]
		value = value[1 : len(value)-1]
		value = strings.ReplaceAll(value, string([]byte{quote, quote}), string(quote))
		value = strings.ReplaceAll(value, "\\'", "'")
		value = strings.ReplaceAll(value, "\\\"", "\"")
		value = strings.ReplaceAll(value, "\\\\", "\\")
	}
	return value
}

func buildNativeTableMapEventForChange(event BinlogEvent, position, tableID uint64, change RowChange, columns []string) []byte {
	database, table := splitNativeTableForChange(change.Table)
	body := make([]byte, 0, 32+len(columns)*3)
	body = appendNativeUint48(body, tableID)
	body = append(body, 0, 0)
	body = append(body, byte(len(database)))
	body = append(body, database...)
	body = append(body, 0)
	body = append(body, byte(len(table)))
	body = append(body, table...)
	body = append(body, 0)
	body = appendNativeLenencInt(body, len(columns))
	typeCodes := make([]byte, 0, len(columns))
	metadata := make([]byte, 0, len(columns)*2)
	for _, column := range columns {
		value := nativeValueForChange(change, column)
		typeCode := nativeColumnTypeForChange(change, column, value)
		typeCodes = append(typeCodes, typeCode)
		body = append(body, typeCode)
		metadata = append(metadata, nativeColumnMetadataForChange(change, column, typeCode, value)...)
	}
	body = appendNativeLenencInt(body, len(metadata))
	body = append(body, metadata...)
	body = append(body, make([]byte, (len(columns)+7)/8)...)
	body = append(body, nativeTableMapOptionalMetadata(change, columns, typeCodes)...)
	return buildNativeEvent(19, body, eventWithNativePosition(event, position))
}

// nativeTableMapOptionalMetadata emits the optional signedness TLV when the
// row image carries an unsigned numeric column. MySQL stores one bit per
// numeric column (rather than per table column), with the first numeric
// column in the high bit of the first byte. Keeping this optional avoids
// claiming FULL row metadata for callers that only provide the default
// schema hints, while still preserving the information needed to decode
// unsigned values from a native TABLE_MAP_EVENT.
func nativeTableMapOptionalMetadata(change RowChange, columns []string, typeCodes []byte) []byte {
	numericCount := 0
	hasUnsigned := false
	for index, column := range columns {
		if index >= len(typeCodes) || !nativeNumericColumnType(typeCodes[index], change.ColumnTypes[column]) {
			continue
		}
		numericCount++
		if strings.Contains(strings.ToUpper(change.ColumnTypes[column]), "UNSIGNED") {
			hasUnsigned = true
		}
	}
	optional := make([]byte, 0)
	if numericCount > 0 && hasUnsigned {
		bitmap := make([]byte, (numericCount+7)/8)
		numericIndex := 0
		for index, column := range columns {
			if index >= len(typeCodes) || !nativeNumericColumnType(typeCodes[index], change.ColumnTypes[column]) {
				continue
			}
			if strings.Contains(strings.ToUpper(change.ColumnTypes[column]), "UNSIGNED") {
				bitmap[numericIndex/8] |= 0x80 >> uint(numericIndex%8)
			}
			numericIndex++
		}
		optional = append(optional, 1) // TABLE_MAP_OPT_META_SIGNEDNESS
		optional = append(optional, appendNativeLenencInt(nil, len(bitmap))...)
		optional = append(optional, bitmap...)
	}
	// Explicit column order is schema negotiation evidence. Encode the
	// COLUMN_NAME optional metadata only for callers that supplied it, keeping
	// legacy row-only callers on MySQL's default minimal metadata shape.
	if len(change.Columns) > 0 {
		value := make([]byte, 0, len(columns)*2)
		valid := true
		for _, column := range columns {
			if len(column) > 255 {
				valid = false
				break
			}
			value = append(value, byte(len(column)))
			value = append(value, column...)
		}
		if valid {
			optional = append(optional, 4) // TABLE_MAP_OPT_META_COLUMN_NAME
			optional = append(optional, appendNativeLenencInt(nil, len(value))...)
			optional = append(optional, value...)
		}
		setValues, enumValues := nativeEnumSetOptionalMetadata(change, columns)
		if len(setValues) > 0 {
			optional = append(optional, 5) // TABLE_MAP_OPT_META_SET_STR_VALUE
			optional = append(optional, appendNativeLenencInt(nil, len(setValues))...)
			optional = append(optional, setValues...)
		}
		if len(enumValues) > 0 {
			optional = append(optional, 6) // TABLE_MAP_OPT_META_ENUM_STR_VALUE
			optional = append(optional, appendNativeLenencInt(nil, len(enumValues))...)
			optional = append(optional, enumValues...)
		}
	}
	geometryTypes := make([]uint64, 0)
	geometryCount := 0
	for index, column := range columns {
		if index >= len(typeCodes) || typeCodes[index] != 255 { // MYSQL_TYPE_GEOMETRY
			continue
		}
		geometryCount++
		geometryType, ok := nativeGeometryTypeForName(change.ColumnTypes[column])
		if !ok {
			geometryType, ok = nativeGeometryTypeForValue(nativeValueForChange(change, column))
		}
		if !ok {
			geometryTypes = nil
			break
		}
		geometryTypes = append(geometryTypes, geometryType)
	}
	if geometryCount > 0 && len(geometryTypes) == geometryCount {
		value := make([]byte, 0, geometryCount)
		for _, geometryType := range geometryTypes {
			value = appendNativeLenencInt(value, int(geometryType))
		}
		optional = append(optional, 7) // TABLE_MAP_OPT_META_GEOMETRY_TYPE
		optional = append(optional, appendNativeLenencInt(nil, len(value))...)
		optional = append(optional, value...)
	}
	vectorDimensions := make([]uint64, 0)
	vectorCount := 0
	for index, column := range columns {
		if index >= len(typeCodes) || typeCodes[index] != 242 { // MYSQL_TYPE_VECTOR
			continue
		}
		vectorCount++
		dimension, ok := nativeVectorDimensionForName(change.ColumnTypes[column])
		if !ok {
			dimension, ok = nativeVectorDimensionForValue(nativeValueForChange(change, column))
		}
		if !ok {
			vectorDimensions = nil
			break
		}
		vectorDimensions = append(vectorDimensions, dimension)
	}
	if vectorCount > 0 && len(vectorDimensions) == vectorCount {
		value := make([]byte, 0, vectorCount)
		for _, dimension := range vectorDimensions {
			value = appendNativeLenencInt(value, int(dimension))
		}
		optional = append(optional, 13) // TABLE_MAP_OPT_META_VECTOR_DIMENSIONALITY
		optional = append(optional, appendNativeLenencInt(nil, len(value))...)
		optional = append(optional, value...)
	}
	return optional
}

func nativeVectorDimensionForName(raw string) (uint64, bool) {
	typeName := strings.ToUpper(strings.TrimSpace(raw))
	open := strings.IndexByte(typeName, '(')
	if open < 0 {
		return 0, false
	}
	close := strings.IndexByte(typeName[open+1:], ')')
	if close < 0 {
		return 0, false
	}
	dimension, err := strconv.ParseUint(strings.TrimSpace(typeName[open+1:open+1+close]), 10, 32)
	return dimension, err == nil && dimension > 0
}

func nativeVectorDimensionForValue(value interface{}) (uint64, bool) {
	switch typed := value.(type) {
	case []float32:
		return uint64(len(typed)), len(typed) > 0
	case []float64:
		return uint64(len(typed)), len(typed) > 0
	case []byte:
		return uint64(len(typed) / 4), len(typed) > 0 && len(typed)%4 == 0
	case *[]byte:
		if typed != nil {
			return uint64(len(*typed) / 4), len(*typed) > 0 && len(*typed)%4 == 0
		}
	}
	return 0, false
}

func nativeGeometryTypeForName(raw string) (uint64, bool) {
	typeName := strings.ToUpper(strings.TrimSpace(raw))
	if open := strings.IndexByte(typeName, '('); open >= 0 {
		typeName = typeName[:open]
	}
	switch typeName {
	case "POINT":
		return 1, true
	case "LINESTRING":
		return 2, true
	case "POLYGON":
		return 3, true
	case "MULTIPOINT":
		return 4, true
	case "MULTILINESTRING":
		return 5, true
	case "MULTIPOLYGON":
		return 6, true
	case "GEOMETRYCOLLECTION":
		return 7, true
	default:
		return 0, false
	}
}

func nativeGeometryTypeForValue(value interface{}) (uint64, bool) {
	var data []byte
	switch typed := value.(type) {
	case []byte:
		data = typed
	case *[]byte:
		if typed != nil {
			data = *typed
		}
	default:
		return 0, false
	}
	// MySQL's internal geometry value may carry a four-byte SRID before the
	// WKB. Accept both that form and a bare WKB payload, while leaving
	// extended/invalid geometry values on the conservative fallback path.
	for _, offset := range []int{0, 4} {
		if len(data) < offset+5 {
			continue
		}
		order := data[offset]
		if order != 0 && order != 1 {
			continue
		}
		var geometryType uint32
		if order == 0 {
			geometryType = binary.BigEndian.Uint32(data[offset+1 : offset+5])
		} else {
			geometryType = binary.LittleEndian.Uint32(data[offset+1 : offset+5])
		}
		if geometryType >= 1 && geometryType <= 7 {
			return uint64(geometryType), true
		}
	}
	return 0, false
}

func nativeEnumSetOptionalMetadata(change RowChange, columns []string) ([]byte, []byte) {
	setValue := make([]byte, 0)
	enumValue := make([]byte, 0)
	for _, column := range columns {
		rawType := strings.TrimSpace(change.ColumnTypes[column])
		upper := strings.ToUpper(rawType)
		if strings.HasPrefix(upper, "SET") {
			members, ok := nativeEnumSetMembers(rawType)
			if !ok {
				continue
			}
			setValue = append(setValue, appendNativeLenencInt(nil, len(members))...)
			for _, member := range members {
				setValue = append(setValue, appendNativeLenencInt(nil, len(member))...)
				setValue = append(setValue, member...)
			}
		}
		if strings.HasPrefix(upper, "ENUM") {
			members, ok := nativeEnumSetMembers(rawType)
			if !ok {
				continue
			}
			enumValue = append(enumValue, appendNativeLenencInt(nil, len(members))...)
			for _, member := range members {
				enumValue = append(enumValue, appendNativeLenencInt(nil, len(member))...)
				enumValue = append(enumValue, member...)
			}
		}
	}
	return setValue, enumValue
}

func nativeNumericColumnType(typeCode byte, _ string) bool {
	switch typeCode {
	case 0, 1, 2, 3, 4, 5, 8, 9, 13, 246:
		return true
	default:
		return false
	}
}

func splitNativeTableForChange(raw string) (string, string) {
	parts := strings.Split(strings.TrimSpace(raw), ".")
	if len(parts) >= 2 {
		return strings.Trim(parts[len(parts)-2], "` "), strings.Trim(parts[len(parts)-1], "` ")
	}
	return "", strings.Trim(raw, "` ")
}

func buildNativeRowsEventForChange(event BinlogEvent, position, tableID uint64, change RowChange, columns []string) []byte {
	return buildNativeRowsEventForChanges(event, position, tableID, []RowChange{change}, columns)
}

func buildNativeRowsEventForChanges(event BinlogEvent, position, tableID uint64, changes []RowChange, columns []string) []byte {
	body := make([]byte, 0, 48)
	body = appendNativeUint48(body, tableID)
	flags := uint16(0)
	extraRowInfo := []byte(nil)
	if len(changes) > 0 {
		flags = changes[0].Flags
		// The Rows_event post-header stores extra_data_len as a two-byte
		// little-endian value including the two length bytes themselves.
		// Preserve every payload that fits that envelope instead of using a
		// one-byte compatibility limit and silently dropping negotiated data.
		if len(changes[0].ExtraRowInfo) <= 0xffff-2 {
			extraRowInfo = append([]byte(nil), changes[0].ExtraRowInfo...)
		}
	}
	body = append(body, byte(flags), byte(flags>>8))
	varHeaderLength := 2 + len(extraRowInfo)
	body = append(body, byte(varHeaderLength), byte(varHeaderLength>>8))
	body = append(body, extraRowInfo...)
	body = appendNativeLenencInt(body, len(columns))
	for _, change := range changes {
		eventType := nativeRowEventTypeForChange(change)
		if eventType == 31 || eventType == 39 {
			beforeBitmap, beforeImage := nativeRowImageForChange(change, columns, change.Before)
			afterBitmap, afterImage := nativeRowImageForChangeMode(change, columns, change.After, eventType == 39)
			body = append(body, beforeBitmap...)
			body = append(body, afterBitmap...)
			body = append(body, beforeImage...)
			if eventType == 39 {
				body = append(body, nativePartialJSONSharedImage(change, columns)...)
			}
			body = append(body, afterImage...)
		} else {
			values := change.After
			if nativeRowEventType(change.Action) == 32 {
				values = change.Before
			}
			bitmap, image := nativeRowImageForChange(change, columns, values)
			body = append(body, bitmap...)
			body = append(body, image...)
		}
	}
	action := "insert"
	if len(changes) > 0 {
		action = changes[0].Action
	}
	return buildNativeEvent(nativeRowEventTypeForActionAndChanges(action, changes), body, eventWithNativePosition(event, position))
}

func nativeRowEventTypeForActionAndChanges(action string, changes []RowChange) byte {
	if len(changes) > 0 {
		return nativeRowEventTypeForChange(changes[0])
	}
	return nativeRowEventType(action)
}

func nativeRowImageForChange(change RowChange, columns []string, values map[string]interface{}) ([]byte, []byte) {
	return nativeRowImageForChangeMode(change, columns, values, false)
}

func nativeRowImageForChangeMode(change RowChange, columns []string, values map[string]interface{}, partialJSON bool) ([]byte, []byte) {
	columnsBitmap := make([]byte, (len(columns)+7)/8)
	presentCount := 0
	for index, column := range columns {
		if _, ok := values[column]; !ok {
			continue
		}
		columnsBitmap[index/8] |= 1 << uint(index%8)
		presentCount++
	}
	nullBitmap := make([]byte, (presentCount+7)/8)
	image := make([]byte, 0)
	presentIndex := 0
	for _, column := range columns {
		value, ok := values[column]
		if !ok {
			continue
		}
		if value == nil {
			nullBitmap[presentIndex/8] |= 1 << uint(presentIndex%8)
		} else {
			if partialJSON {
				if partial, ok := nativePartialJSONValue(change, column); ok {
					if encoded, fits := nativeBlobValueWithPackLength(change, column, 245, value, partial); fits {
						image = append(image, encoded...)
						continue
					}
				}
			}
			image = append(image, nativeValueBytesForColumn(change, column, value)...)
		}
		presentIndex++
	}
	return append(columnsBitmap, nullBitmap...), image
}

func nativePartialJSONSharedImage(change RowChange, columns []string) []byte {
	partialBitmap := make([]byte, (len(columns)+7)/8)
	for index, column := range columns {
		if _, ok := nativePartialJSONValue(change, column); ok {
			partialBitmap[index/8] |= 1 << uint(index%8)
		}
	}
	shared := []byte{1} // BINLOG_ROW_VALUE_OPTIONS_PARTIAL_JSON
	return append(shared, partialBitmap...)
}

func nativePartialJSONValue(change RowChange, column string) ([]byte, bool) {
	if nativeColumnTypeForChange(change, column, change.After[column]) != 245 {
		return nil, false
	}
	updates, ok := change.PartialJSONUpdates[column]
	if !ok || len(updates) == 0 {
		updates, ok = DeriveJSONPartialUpdates(change.Before[column], change.After[column])
	}
	if !ok || len(updates) == 0 {
		return nil, false
	}

	diffs := make([]byte, 0)
	for _, update := range updates {
		path := []byte(update.Path)
		if len(path) == 0 || !utf8.Valid(path) {
			return nil, false
		}
		if update.Operation != JSONPartialOperationReplace && update.Operation != JSONPartialOperationInsert && update.Operation != JSONPartialOperationRemove {
			return nil, false
		}
		data := []byte(nil)
		if update.Operation != JSONPartialOperationRemove {
			data = nativeJSONBinaryValue(update.Value)
		}
		diffs = append(diffs, update.Operation)
		diffs = appendNativePackedUint64(diffs, uint64(len(path)))
		diffs = append(diffs, path...)
		diffs = appendNativePackedUint64(diffs, uint64(len(data)))
		diffs = append(diffs, data...)
	}
	if len(diffs)+4 >= len(nativeJSONBinaryValue(change.After[column])) {
		return nil, false
	}
	result := make([]byte, 4, len(diffs)+4)
	binary.LittleEndian.PutUint32(result, uint32(len(diffs)))
	return append(result, diffs...), true
}

func appendNativePackedUint64(dst []byte, value uint64) []byte {
	switch {
	case value < 251:
		return append(dst, byte(value))
	case value <= 0xffff:
		dst = append(dst, 0xfc, 0, 0)
		binary.LittleEndian.PutUint16(dst[len(dst)-2:], uint16(value))
		return dst
	case value <= 0xffffff:
		dst = append(dst, 0xfd, 0, 0, 0)
		for index := 0; index < 3; index++ {
			dst[len(dst)-3+index] = byte(value >> uint(index*8))
		}
		return dst
	default:
		dst = append(dst, 0xfe, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.LittleEndian.PutUint64(dst[len(dst)-8:], value)
		return dst
	}
}

func nativeValueBytesForColumn(change RowChange, column string, value interface{}) []byte {
	typeCode := nativeColumnTypeForChange(change, column, value)
	if typeCode == 16 {
		if encoded, ok := nativeBitBinaryValue(change, column, value); ok {
			return encoded
		}
	}
	if typeCode == 9 {
		unsigned := false
		if rawType, exists := change.ColumnTypes[column]; exists {
			unsigned = strings.Contains(strings.ToUpper(rawType), "UNSIGNED")
		}
		if encoded, ok := nativeInt24BinaryValue(value, unsigned); ok {
			return encoded
		}
	}
	if typeCode == 247 || typeCode == 248 || typeCode == 254 {
		if encoded, ok := nativeEnumSetBinaryValue(change, column, typeCode, value); ok {
			return encoded
		}
	}
	if typeCode == 254 {
		if encoded, ok := nativeFixedStringValue(change, column, value); ok {
			return encoded
		}
	}
	if typeCode == 7 {
		if encoded, ok := nativeLegacyTimestampBinaryValue(value); ok {
			return encoded
		}
	}
	if typeCode == 13 {
		if encoded, ok := nativeYearBinaryValue(value); ok {
			return encoded
		}
	}
	if typeCode == 11 {
		if encoded, ok := nativeLegacyTimeBinaryValue(value); ok {
			return encoded
		}
	}
	if typeCode == 10 || typeCode == 12 || typeCode == 14 || typeCode == 17 || typeCode == 18 || typeCode == 19 {
		if encoded, ok := nativeTemporalBinaryValue(change, column, typeCode, value); ok {
			return encoded
		}
	}
	if typeCode == 245 {
		binaryJSON := nativeJSONBinaryValue(value)
		// The JSON field in a row event is a binary JSON document preceded
		// by the fixed-width pack length from the column metadata.
		if encoded, ok := nativeBlobValueWithPackLength(change, column, typeCode, value, binaryJSON); ok {
			return encoded
		}
		return append(appendNativeLenencInt(nil, len(binaryJSON)), binaryJSON...)
	}
	if typeCode == 246 {
		if rawType, ok := change.ColumnTypes[column]; ok {
			if encoded, ok := nativeDecimalBinaryValue(rawType, value); ok {
				return encoded
			}
		}
	}
	if typeCode == 255 {
		if data, ok := nativeBinaryValue(value); ok {
			if encoded, ok := nativeBlobValueWithPackLength(change, column, typeCode, value, data); ok {
				return encoded
			}
			return append(appendNativeLenencInt(nil, len(data)), data...)
		}
	}
	if (typeCode >= 249 && typeCode <= 254) || typeCode == 15 || typeCode == 20 || typeCode == 242 {
		if encoded, ok := nativeLengthEncodedColumnValue(change, column, typeCode, value); ok {
			return encoded
		}
	}
	return nativeValueBytesForType(typeCode, value)
}

func nativeFixedStringValue(change RowChange, column string, value interface{}) ([]byte, bool) {
	metadata := nativeColumnMetadataForChange(change, column, 254, value)
	if len(metadata) < 2 || metadata[0] == 247 || metadata[0] == 248 {
		return nil, false
	}
	fieldLength := int(metadata[1]) | ((int(metadata[0]^254) & 0x30) << 4)
	if fieldLength <= 0 {
		return nil, false
	}
	data := nativeColumnStringBytes(value)
	if len(data) > fieldLength {
		data = data[:fieldLength]
	}
	encoded := make([]byte, fieldLength)
	copy(encoded, data)
	padding := byte(' ')
	if rawType, ok := change.ColumnTypes[column]; ok {
		typeBase := strings.ToUpper(strings.TrimSpace(rawType))
		if open := strings.IndexByte(typeBase, '('); open >= 0 {
			typeBase = typeBase[:open]
		}
		if strings.TrimSpace(typeBase) == "BINARY" {
			padding = 0
		}
	}
	for index := len(data); index < len(encoded); index++ {
		encoded[index] = padding
	}
	return encoded, true
}

func nativeBlobValueWithPackLength(change RowChange, column string, typeCode byte, value interface{}, data []byte) ([]byte, bool) {
	metadata := nativeColumnMetadataForChange(change, column, typeCode, value)
	width := 4
	if len(metadata) > 0 {
		width = int(metadata[0])
	}
	if width < 1 || width > 4 {
		return nil, false
	}
	maxLength := uint64(1)<<uint(width*8) - 1
	if uint64(len(data)) > maxLength {
		return nil, false
	}
	encoded := make([]byte, width, width+len(data))
	for index := range encoded {
		encoded[index] = byte(uint64(len(data)) >> uint(index*8))
	}
	return append(encoded, data...), true
}

func nativeBinaryValue(value interface{}) ([]byte, bool) {
	switch typed := value.(type) {
	case []byte:
		return append([]byte(nil), typed...), true
	case *[]byte:
		if typed != nil {
			return append([]byte(nil), (*typed)...), true
		}
	}
	return nil, false
}

func nativeInt24BinaryValue(value interface{}, unsigned bool) ([]byte, bool) {
	if number, ok := nativeUnsignedInteger(value); ok {
		max := uint64(0x7fffff)
		if unsigned {
			max = 0xffffff
		}
		if number > max {
			return nil, false
		}
		return []byte{byte(number), byte(number >> 8), byte(number >> 16)}, true
	}
	number, err := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(value)), 10, 64)
	if err != nil || (unsigned && (number < 0 || number > 0xffffff)) || (!unsigned && (number < -8388608 || number > 8388607)) {
		return nil, false
	}
	encoded := uint32(number) & 0xffffff
	return []byte{byte(encoded), byte(encoded >> 8), byte(encoded >> 16)}, true
}

func nativeBitBinaryValue(change RowChange, column string, value interface{}) ([]byte, bool) {
	metadata, ok := change.ColumnMetadata[column]
	if !ok {
		if rawType, exists := change.ColumnTypes[column]; exists {
			metadata = nativeColumnMetadataForTypeName(rawType, 16)
		}
	}
	if len(metadata) < 2 {
		return nil, false
	}
	bitRemainder, fullBytes := int(metadata[0]), int(metadata[1])
	if bitRemainder > 7 || fullBytes > 8 || fullBytes*8+bitRemainder == 0 || fullBytes*8+bitRemainder > 64 {
		return nil, false
	}
	number, ok := nativeUnsignedInteger(value)
	if !ok {
		return nil, false
	}
	bitLength := fullBytes*8 + bitRemainder
	if bitLength < 64 && number >= uint64(1)<<uint(bitLength) {
		return nil, false
	}
	width := fullBytes
	if bitRemainder > 0 {
		width++
	}
	encoded := make([]byte, width)
	for index := 0; index < width; index++ {
		encoded[width-1-index] = byte(number >> uint(index*8))
	}
	return encoded, true
}

func nativeEnumSetBinaryValue(change RowChange, column string, typeCode byte, value interface{}) ([]byte, bool) {
	metadata := change.ColumnMetadata[column]
	if len(metadata) < 2 {
		if rawType, ok := change.ColumnTypes[column]; ok {
			metadata = nativeColumnMetadataForTypeName(rawType, typeCode)
		}
	}
	if len(metadata) < 2 {
		return nil, false
	}
	realType := metadata[0]
	if typeCode == 247 || typeCode == 248 {
		realType = typeCode
	}
	if realType != 247 && realType != 248 {
		return nil, false
	}
	width := int(metadata[1])
	if width < 1 || width > 8 {
		return nil, false
	}
	number, ok := nativeEnumSetNumber(change.ColumnTypes[column], realType, value)
	if !ok || (width < 8 && number >= uint64(1)<<uint(width*8)) {
		return nil, false
	}
	encoded := make([]byte, width)
	for index := 0; index < width; index++ {
		encoded[index] = byte(number >> uint(index*8))
	}
	return encoded, true
}

func nativeEnumSetNumber(rawType string, typeCode byte, value interface{}) (uint64, bool) {
	if number, ok := nativeUnsignedInteger(value); ok {
		return number, true
	}
	text, ok := value.(string)
	if !ok {
		return 0, false
	}
	members, ok := nativeEnumSetMembers(strings.TrimSpace(rawType))
	if !ok {
		return 0, false
	}
	if typeCode == 247 {
		for index, member := range members {
			if text == member {
				return uint64(index + 1), true
			}
		}
		return 0, false
	}
	var mask uint64
	for _, selected := range strings.Split(text, ",") {
		selected = strings.TrimSpace(selected)
		found := false
		for index, member := range members {
			if selected == member {
				mask |= uint64(1) << uint(index)
				found = true
				break
			}
		}
		if !found {
			return 0, false
		}
	}
	return mask, true
}

func nativeYearBinaryValue(value interface{}) ([]byte, bool) {
	text := strings.TrimSpace(fmt.Sprint(value))
	year, err := strconv.ParseInt(text, 10, 32)
	if err != nil || year < 0 || (year != 0 && (year < 1901 || year > 2155)) {
		return nil, false
	}
	if year == 0 {
		return []byte{0}, true
	}
	return []byte{byte(year - 1900)}, true
}

func nativeLegacyTimestampBinaryValue(value interface{}) ([]byte, bool) {
	parts, ok := nativeTemporalPartsForValue(7, value)
	if !ok {
		return nil, false
	}
	if parts.year == 0 && parts.month == 0 && parts.day == 0 && parts.hour == 0 && parts.minute == 0 && parts.second == 0 && parts.micro == 0 {
		return []byte{0, 0, 0, 0}, true
	}
	instant := time.Date(parts.year, time.Month(parts.month), parts.day, parts.hour, parts.minute, parts.second, 0, time.UTC)
	seconds := instant.Unix()
	if seconds < 0 || seconds > math.MaxUint32 {
		return nil, false
	}
	encoded := make([]byte, 4)
	binary.LittleEndian.PutUint32(encoded, uint32(seconds))
	return encoded, true
}

func nativeLegacyTimeBinaryValue(value interface{}) ([]byte, bool) {
	parts, ok := nativeTemporalPartsForValue(11, value)
	if !ok {
		if text, isText := value.(string); isText {
			parts, ok = nativeTemporalTimeParts(strings.TrimSpace(text))
		}
	}
	if !ok || parts.hour > 838 || parts.minute > 59 || parts.second > 59 {
		return nil, false
	}
	packed := int64(parts.hour*10000 + parts.minute*100 + parts.second)
	if parts.negative {
		packed = -packed
	}
	encoded := uint32(packed) & 0x00ffffff
	return []byte{byte(encoded), byte(encoded >> 8), byte(encoded >> 16)}, true
}

func nativeUnsignedInteger(value interface{}) (uint64, bool) {
	switch typed := value.(type) {
	case uint:
		return uint64(typed), true
	case uint8:
		return uint64(typed), true
	case uint16:
		return uint64(typed), true
	case uint32:
		return uint64(typed), true
	case uint64:
		return typed, true
	case int:
		if typed >= 0 {
			return uint64(typed), true
		}
	case int8:
		if typed >= 0 {
			return uint64(typed), true
		}
	case int16:
		if typed >= 0 {
			return uint64(typed), true
		}
	case int32:
		if typed >= 0 {
			return uint64(typed), true
		}
	case int64:
		if typed >= 0 {
			return uint64(typed), true
		}
	default:
		parsed, err := strconv.ParseUint(strings.TrimSpace(fmt.Sprint(value)), 10, 64)
		if err == nil {
			return parsed, true
		}
	}
	return 0, false
}

type nativeTemporalParts struct {
	year, month, day            int
	hour, minute, second, micro int
	negative                    bool
}

func nativeTemporalBinaryValue(change RowChange, column string, typeCode byte, value interface{}) ([]byte, bool) {
	parts, ok := nativeTemporalPartsForValue(typeCode, value)
	if !ok {
		return nil, false
	}
	fsp := 0
	if metadata := nativeColumnMetadataForChange(change, column, typeCode, value); len(metadata) > 0 && (typeCode == 17 || typeCode == 18 || typeCode == 19) {
		fsp = int(metadata[0])
		if fsp > 6 {
			return nil, false
		}
	}
	switch typeCode {
	case 10, 14:
		packed := uint32((parts.year << 9) | (parts.month << 5) | parts.day)
		return []byte{byte(packed), byte(packed >> 8), byte(packed >> 16)}, true
	case 12:
		// Legacy MYSQL_TYPE_DATETIME is the compact decimal form
		// YYYYMMDDHHMMSS, unlike DATETIME2's packed bit fields.
		packed := uint64(parts.year)
		packed = packed*100 + uint64(parts.month)
		packed = packed*100 + uint64(parts.day)
		packed = packed*100 + uint64(parts.hour)
		packed = packed*100 + uint64(parts.minute)
		packed = packed*100 + uint64(parts.second)
		return nativeLittleEndianWidth(packed, 8), true
	case 17:
		return nativePackedTimeBinaryValue(parts, fsp), true
	case 18:
		ymd := int64(((parts.year*13 + parts.month) << 5) | parts.day)
		hms := int64((parts.hour << 12) | (parts.minute << 6) | parts.second)
		packed := uint64((ymd << 17) | hms)
		encoded := nativeLittleEndianWidth(packed+0x8000000000, 5)
		return append(encoded, nativeTemporalFractionBytes(parts.micro, fsp)...), true
	case 19:
		instant := time.Date(parts.year, time.Month(parts.month), parts.day, parts.hour, parts.minute, parts.second, parts.micro*1000, time.UTC)
		encoded := make([]byte, 4)
		binary.LittleEndian.PutUint32(encoded, uint32(instant.Unix()))
		return append(encoded, nativeTemporalFractionBytes(parts.micro, fsp)...), true
	default:
		return nil, false
	}
}

func nativeTemporalFractionBytes(micro, fsp int) []byte {
	if fsp <= 0 {
		return nil
	}
	width := 1
	if fsp > 2 {
		width = 2
	}
	if fsp > 4 {
		width = 3
	}
	fraction := micro
	if fsp <= 2 {
		fraction /= 10000
	} else if fsp <= 4 {
		fraction /= 100
	}
	encoded := make([]byte, width)
	for index := range encoded {
		encoded[index] = byte(fraction >> uint(index*8))
	}
	return encoded
}

func nativePackedTimeBinaryValue(parts nativeTemporalParts, fsp int) []byte {
	hms := int64((parts.hour << 12) | (parts.minute << 6) | parts.second)
	packed := (hms << 24) | int64(parts.micro)
	if parts.negative {
		packed = -packed
	}
	if fsp > 4 {
		return nativeLittleEndianWidth(uint64(packed+0x800000000000), 6)
	}
	integerPart := packed >> 24
	encoded := nativeLittleEndianWidth(uint64(int64(0x800000)+integerPart), 3)
	fracPart := int32(packed & 0xffffff)
	if fracPart&0x800000 != 0 {
		fracPart -= 0x1000000
	}
	if fsp > 2 {
		encoded = append(encoded, byte(fracPart/100), byte((fracPart/100)>>8))
	} else if fsp > 0 {
		encoded = append(encoded, byte(fracPart/10000))
	}
	return encoded
}

func nativeLittleEndianWidth(value uint64, width int) []byte {
	encoded := make([]byte, width)
	for index := range encoded {
		encoded[index] = byte(value >> uint(index*8))
	}
	return encoded
}

func nativeTemporalPartsForValue(typeCode byte, value interface{}) (nativeTemporalParts, bool) {
	switch typed := value.(type) {
	case time.Time:
		return nativeTemporalParts{year: typed.Year(), month: int(typed.Month()), day: typed.Day(), hour: typed.Hour(), minute: typed.Minute(), second: typed.Second(), micro: typed.Nanosecond() / 1000}, true
	case *time.Time:
		if typed != nil {
			return nativeTemporalParts{year: typed.Year(), month: int(typed.Month()), day: typed.Day(), hour: typed.Hour(), minute: typed.Minute(), second: typed.Second(), micro: typed.Nanosecond() / 1000}, true
		}
	}
	text := strings.TrimSpace(fmt.Sprint(value))
	if typeCode == 17 {
		return nativeTemporalTimeParts(text)
	}
	if strings.HasPrefix(text, "0000-00-00") {
		if len(text) == len("0000-00-00") {
			return nativeTemporalParts{}, true
		}
		if len(text) > len("0000-00-00") && (text[10] == ' ' || text[10] == 'T') {
			parts, ok := nativeTemporalTimeParts(strings.TrimSpace(text[11:]))
			if ok {
				parts.year, parts.month, parts.day = 0, 0, 0
				return parts, true
			}
		}
	}
	for _, layout := range []string{"2006-01-02 15:04:05.999999", "2006-01-02 15:04:05", "2006-01-02T15:04:05.999999Z07:00", "2006-01-02T15:04:05Z07:00", "2006-01-02"} {
		if parsed, err := time.Parse(layout, text); err == nil {
			return nativeTemporalParts{year: parsed.Year(), month: int(parsed.Month()), day: parsed.Day(), hour: parsed.Hour(), minute: parsed.Minute(), second: parsed.Second(), micro: parsed.Nanosecond() / 1000}, true
		}
	}
	return nativeTemporalParts{}, false
}

func nativeTemporalTimeParts(text string) (nativeTemporalParts, bool) {
	negative := strings.HasPrefix(text, "-")
	if negative {
		text = text[1:]
	}
	if strings.HasPrefix(text, "+") {
		text = text[1:]
	}
	mainPart, fractionPart := text, ""
	if dot := strings.IndexByte(text, '.'); dot >= 0 {
		mainPart, fractionPart = text[:dot], text[dot+1:]
	}
	parts := strings.Split(mainPart, ":")
	if len(parts) != 3 {
		return nativeTemporalParts{}, false
	}
	hour, hourErr := strconv.Atoi(parts[0])
	minute, minuteErr := strconv.Atoi(parts[1])
	second, secondErr := strconv.Atoi(parts[2])
	if hourErr != nil || minuteErr != nil || secondErr != nil || hour < 0 || minute < 0 || minute > 59 || second < 0 || second > 59 {
		return nativeTemporalParts{}, false
	}
	if len(fractionPart) > 6 {
		fractionPart = fractionPart[:6]
	}
	for len(fractionPart) < 6 {
		fractionPart += "0"
	}
	micro := 0
	if fractionPart != "" {
		var err error
		micro, err = strconv.Atoi(fractionPart)
		if err != nil {
			return nativeTemporalParts{}, false
		}
	}
	return nativeTemporalParts{hour: hour, minute: minute, second: second, micro: micro, negative: negative}, true
}

func nativeLengthEncodedColumnValue(change RowChange, column string, typeCode byte, value interface{}) ([]byte, bool) {
	data := nativeColumnStringBytes(value)
	width := 1
	metadata, hasMetadata := change.ColumnMetadata[column]
	explicitMetadata := hasMetadata
	if !hasMetadata {
		if rawType, ok := change.ColumnTypes[column]; ok {
			metadata = nativeColumnMetadataForTypeName(rawType, typeCode)
			hasMetadata = len(metadata) > 0
		}
	}
	if (typeCode == 20 || typeCode == 242) && !explicitMetadata {
		metadata = nativeColumnMetadataForValue(typeCode, value)
		hasMetadata = len(metadata) > 0
	}
	if (typeCode >= 249 && typeCode <= 252) || typeCode == 20 || typeCode == 242 {
		if hasMetadata && len(metadata) > 0 {
			width = int(metadata[0])
		}
	} else if typeCode == 254 {
		if hasMetadata && len(metadata) >= 2 {
			fieldLength := int(metadata[1]) | ((int(metadata[0]^254) & 0x30) << 4)
			if fieldLength > 255 {
				width = 2
			}
		}
	} else if hasMetadata && len(metadata) >= 2 {
		maxLength := int(binary.LittleEndian.Uint16(metadata[:2]))
		if maxLength > 255 {
			width = 2
		}
	}
	if width < 1 || width > 4 {
		return nil, false
	}
	maxLength := uint64(1)<<(uint(width)*8) - 1
	if uint64(len(data)) > maxLength {
		return nil, false
	}
	encoded := make([]byte, width, width+len(data))
	for index := 0; index < width; index++ {
		encoded[index] = byte(uint64(len(data)) >> uint(index*8))
	}
	encoded = append(encoded, data...)
	return encoded, true
}

func nativeColumnStringBytes(value interface{}) []byte {
	switch typed := value.(type) {
	case []byte:
		return append([]byte(nil), typed...)
	case json.RawMessage:
		return append([]byte(nil), typed...)
	case string:
		return []byte(typed)
	default:
		return []byte(fmt.Sprint(value))
	}
}

type nativeJSONEncodedValue struct {
	typeCode byte
	payload  []byte
}

const (
	nativeJSONTypeSmallObject byte = 0x00
	nativeJSONTypeLargeObject byte = 0x01
	nativeJSONTypeSmallArray  byte = 0x02
	nativeJSONTypeLargeArray  byte = 0x03
	nativeJSONTypeLiteral     byte = 0x04
	nativeJSONTypeInt16       byte = 0x05
	nativeJSONTypeUint16      byte = 0x06
	nativeJSONTypeInt32       byte = 0x07
	nativeJSONTypeUint32      byte = 0x08
	nativeJSONTypeInt64       byte = 0x09
	nativeJSONTypeUint64      byte = 0x0a
	nativeJSONTypeDouble      byte = 0x0b
	nativeJSONTypeString      byte = 0x0c
)

func nativeJSONBinaryValue(value interface{}) []byte {
	if raw, ok := value.([]byte); ok && nativeJSONLooksBinary(raw) {
		return append([]byte(nil), raw...)
	}
	if raw, ok := value.(json.RawMessage); ok && nativeJSONLooksBinary(raw) {
		return append([]byte(nil), raw...)
	}

	decoded := value
	var raw []byte
	switch typed := value.(type) {
	case json.RawMessage:
		raw = []byte(typed)
	case []byte:
		raw = typed
	case string:
		raw = []byte(typed)
	}
	if len(raw) > 0 {
		var parsed interface{}
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if err := decoder.Decode(&parsed); err == nil {
			decoded = parsed
		}
	}
	encoded, ok := nativeJSONEncodeValue(decoded)
	if !ok {
		encoded = nativeJSONEncodedValue{typeCode: nativeJSONTypeString, payload: nativeJSONBinaryStringPayload(fmt.Sprint(value))}
	}
	return append([]byte{encoded.typeCode}, encoded.payload...)
}

func nativeJSONLooksBinary(raw []byte) bool {
	if len(raw) == 0 {
		return false
	}
	switch raw[0] {
	case nativeJSONTypeSmallObject, nativeJSONTypeLargeObject,
		nativeJSONTypeSmallArray, nativeJSONTypeLargeArray,
		nativeJSONTypeLiteral, nativeJSONTypeInt16, nativeJSONTypeUint16,
		nativeJSONTypeInt32, nativeJSONTypeUint32, nativeJSONTypeInt64,
		nativeJSONTypeUint64, nativeJSONTypeDouble, nativeJSONTypeString:
		return true
	default:
		return false
	}
}

func nativeJSONEncodeValue(value interface{}) (nativeJSONEncodedValue, bool) {
	switch typed := value.(type) {
	case nil:
		return nativeJSONEncodedValue{typeCode: nativeJSONTypeLiteral, payload: []byte{0}}, true
	case bool:
		if typed {
			return nativeJSONEncodedValue{typeCode: nativeJSONTypeLiteral, payload: []byte{1}}, true
		}
		return nativeJSONEncodedValue{typeCode: nativeJSONTypeLiteral, payload: []byte{2}}, true
	case json.Number:
		return nativeJSONEncodeJSONNumber(string(typed))
	case int:
		return nativeJSONEncodeSigned(int64(typed)), true
	case int8:
		return nativeJSONEncodeSigned(int64(typed)), true
	case int16:
		return nativeJSONEncodeSigned(int64(typed)), true
	case int32:
		return nativeJSONEncodeSigned(int64(typed)), true
	case int64:
		return nativeJSONEncodeSigned(typed), true
	case uint:
		return nativeJSONEncodeUnsigned(uint64(typed)), true
	case uint8:
		return nativeJSONEncodeUnsigned(uint64(typed)), true
	case uint16:
		return nativeJSONEncodeUnsigned(uint64(typed)), true
	case uint32:
		return nativeJSONEncodeUnsigned(uint64(typed)), true
	case uint64:
		return nativeJSONEncodeUnsigned(typed), true
	case float32:
		return nativeJSONEncodeFloat(float64(typed)), true
	case float64:
		return nativeJSONEncodeFloat(typed), true
	case string:
		return nativeJSONEncodedValue{typeCode: nativeJSONTypeString, payload: nativeJSONBinaryStringPayload(typed)}, true
	case []interface{}:
		return nativeJSONEncodeArray(typed)
	case map[string]interface{}:
		return nativeJSONEncodeObject(typed)
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nativeJSONEncodedValue{}, false
		}
		var parsed interface{}
		decoder := json.NewDecoder(strings.NewReader(string(encoded)))
		decoder.UseNumber()
		if err := decoder.Decode(&parsed); err != nil {
			return nativeJSONEncodedValue{}, false
		}
		return nativeJSONEncodeValue(parsed)
	}
}

func nativeJSONEncodeJSONNumber(raw string) (nativeJSONEncodedValue, bool) {
	if strings.ContainsAny(raw, ".eE") {
		number, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nativeJSONEncodedValue{}, false
		}
		return nativeJSONEncodeFloat(number), true
	}
	if number, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return nativeJSONEncodeSigned(number), true
	}
	number, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return nativeJSONEncodedValue{}, false
	}
	return nativeJSONEncodeUnsigned(number), true
}

func nativeJSONEncodeSigned(number int64) nativeJSONEncodedValue {
	if number >= math.MinInt16 && number <= math.MaxInt16 {
		payload := make([]byte, 2)
		binary.LittleEndian.PutUint16(payload, uint16(number))
		return nativeJSONEncodedValue{typeCode: nativeJSONTypeInt16, payload: payload}
	}
	if number >= math.MinInt32 && number <= math.MaxInt32 {
		payload := make([]byte, 4)
		binary.LittleEndian.PutUint32(payload, uint32(number))
		return nativeJSONEncodedValue{typeCode: nativeJSONTypeInt32, payload: payload}
	}
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, uint64(number))
	return nativeJSONEncodedValue{typeCode: nativeJSONTypeInt64, payload: payload}
}

func nativeJSONEncodeUnsigned(number uint64) nativeJSONEncodedValue {
	if number <= math.MaxUint16 {
		payload := make([]byte, 2)
		binary.LittleEndian.PutUint16(payload, uint16(number))
		return nativeJSONEncodedValue{typeCode: nativeJSONTypeUint16, payload: payload}
	}
	if number <= math.MaxUint32 {
		payload := make([]byte, 4)
		binary.LittleEndian.PutUint32(payload, uint32(number))
		return nativeJSONEncodedValue{typeCode: nativeJSONTypeUint32, payload: payload}
	}
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, number)
	return nativeJSONEncodedValue{typeCode: nativeJSONTypeUint64, payload: payload}
}

func nativeJSONEncodeFloat(number float64) nativeJSONEncodedValue {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, math.Float64bits(number))
	return nativeJSONEncodedValue{typeCode: nativeJSONTypeDouble, payload: payload}
}

func nativeJSONBinaryStringPayload(value string) []byte {
	payload := appendNativeJSONLength(nil, len([]byte(value)))
	return append(payload, []byte(value)...)
}

func appendNativeJSONLength(dst []byte, value int) []byte {
	for {
		part := value & 0x7f
		value >>= 7
		if value == 0 {
			return append(dst, byte(part))
		}
		dst = append(dst, byte(part)|0x80)
	}
}

func nativeJSONEncodeArray(values []interface{}) (nativeJSONEncodedValue, bool) {
	encoded := make([]nativeJSONEncodedValue, 0, len(values))
	for _, value := range values {
		item, ok := nativeJSONEncodeValue(value)
		if !ok {
			return nativeJSONEncodedValue{}, false
		}
		encoded = append(encoded, item)
	}
	return nativeJSONBuildContainer(nil, encoded, false)
}

func nativeJSONEncodeObject(values map[string]interface{}) (nativeJSONEncodedValue, bool) {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		leftBytes, rightBytes := []byte(keys[left]), []byte(keys[right])
		if len(leftBytes) != len(rightBytes) {
			return len(leftBytes) < len(rightBytes)
		}
		return keys[left] < keys[right]
	})
	encoded := make([]nativeJSONEncodedValue, 0, len(keys))
	for _, key := range keys {
		item, ok := nativeJSONEncodeValue(values[key])
		if !ok {
			return nativeJSONEncodedValue{}, false
		}
		encoded = append(encoded, item)
	}
	return nativeJSONBuildContainer(keys, encoded, true)
}

func nativeJSONBuildContainer(keys []string, values []nativeJSONEncodedValue, object bool) (nativeJSONEncodedValue, bool) {
	if len(values) > math.MaxUint16 {
		return nativeJSONBuildContainerSized(keys, values, object, true)
	}
	small, ok := nativeJSONBuildContainerSized(keys, values, object, false)
	if ok && len(small.payload) <= math.MaxUint16 {
		return small, true
	}
	return nativeJSONBuildContainerSized(keys, values, object, true)
}

func nativeJSONBuildContainerSized(keys []string, values []nativeJSONEncodedValue, object, large bool) (nativeJSONEncodedValue, bool) {
	if object && len(keys) != len(values) {
		return nativeJSONEncodedValue{}, false
	}
	if len(values) > math.MaxUint32 {
		return nativeJSONEncodedValue{}, false
	}
	keyEntrySize := 0
	if object {
		keyEntrySize = 4
		if large {
			keyEntrySize = 6
		}
	}
	valueEntrySize := 3
	if large {
		valueEntrySize = 5
	}
	headerSize := 4 + keyEntrySize*len(values) + valueEntrySize*len(values)
	if large {
		headerSize = 8 + keyEntrySize*len(values) + valueEntrySize*len(values)
	}
	keyData := make([]byte, 0)
	if object {
		for _, key := range keys {
			keyData = append(keyData, []byte(key)...)
		}
	}
	valueData := make([]byte, 0)
	valueEntries := make([][]byte, 0, len(values))
	for _, value := range values {
		entry := []byte{value.typeCode}
		inline := len(value.payload) <= valueEntrySize-1 && (value.typeCode == nativeJSONTypeLiteral || value.typeCode == nativeJSONTypeInt16 || value.typeCode == nativeJSONTypeUint16)
		if inline {
			entry = append(entry, value.payload...)
			entry = append(entry, make([]byte, valueEntrySize-1-len(value.payload))...)
		} else {
			offset := headerSize + len(keyData) + len(valueData)
			if large {
				encodedOffset := make([]byte, 4)
				binary.LittleEndian.PutUint32(encodedOffset, uint32(offset))
				entry = append(entry, encodedOffset...)
			} else {
				if offset > math.MaxUint16 {
					return nativeJSONEncodedValue{}, false
				}
				encodedOffset := make([]byte, 2)
				binary.LittleEndian.PutUint16(encodedOffset, uint16(offset))
				entry = append(entry, encodedOffset...)
			}
			valueData = append(valueData, value.payload...)
		}
		valueEntries = append(valueEntries, entry)
	}
	body := make([]byte, 0, headerSize+len(keyData)+len(valueData))
	if large {
		count := make([]byte, 4)
		binary.LittleEndian.PutUint32(count, uint32(len(values)))
		body = append(body, count...)
		size := make([]byte, 4)
		binary.LittleEndian.PutUint32(size, uint32(headerSize+len(keyData)+len(valueData)))
		body = append(body, size...)
	} else {
		count := make([]byte, 2)
		binary.LittleEndian.PutUint16(count, uint16(len(values)))
		body = append(body, count...)
		size := make([]byte, 2)
		binary.LittleEndian.PutUint16(size, uint16(headerSize+len(keyData)+len(valueData)))
		body = append(body, size...)
	}
	keyOffset := headerSize
	keyIndex := 0
	if object {
		for _, key := range keys {
			if large {
				offset := make([]byte, 4)
				binary.LittleEndian.PutUint32(offset, uint32(keyOffset))
				body = append(body, offset...)
			} else {
				offset := make([]byte, 2)
				binary.LittleEndian.PutUint16(offset, uint16(keyOffset))
				body = append(body, offset...)
			}
			keyLength := len([]byte(key))
			if keyLength > math.MaxUint16 {
				return nativeJSONEncodedValue{}, false
			}
			length := make([]byte, 2)
			binary.LittleEndian.PutUint16(length, uint16(keyLength))
			body = append(body, length...)
			keyOffset += keyLength
			keyIndex++
		}
	}
	for _, entry := range valueEntries {
		body = append(body, entry...)
	}
	body = append(body, keyData...)
	body = append(body, valueData...)
	typeCode := nativeJSONTypeSmallArray
	if object {
		typeCode = nativeJSONTypeSmallObject
	}
	if large {
		if object {
			typeCode = nativeJSONTypeLargeObject
		} else {
			typeCode = nativeJSONTypeLargeArray
		}
	}
	return nativeJSONEncodedValue{typeCode: typeCode, payload: body}, true
}

func nativeDecimalBinaryValue(rawType string, value interface{}) ([]byte, bool) {
	precision, scale, ok := nativeDecimalSpec(rawType)
	if !ok {
		return nil, false
	}
	text := strings.TrimSpace(fmt.Sprint(value))
	negative := strings.HasPrefix(text, "-")
	text = strings.TrimLeft(text, "+-")
	if strings.ContainsAny(text, "eE") {
		number, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return nil, false
		}
		text = strconv.FormatFloat(number, 'f', scale, 64)
	}
	parts := strings.SplitN(text, ".", 2)
	integerPart := strings.TrimLeft(parts[0], "0")
	if integerPart == "" {
		integerPart = "0"
	}
	fractionPart := ""
	if len(parts) == 2 {
		fractionPart = parts[1]
	}
	integerDigits := precision - scale
	if len(integerPart) > integerDigits || len(fractionPart) > scale {
		return nil, false
	}
	integerPart = strings.Repeat("0", integerDigits-len(integerPart)) + integerPart
	fractionPart += strings.Repeat("0", scale-len(fractionPart))
	encoded := make([]byte, 0, nativeDecimalBinarySize(integerDigits, scale))
	nativeDecimalAppendDigits(&encoded, integerPart)
	nativeDecimalAppendDigits(&encoded, fractionPart)
	if len(encoded) == 0 {
		return nil, false
	}
	encoded[0] |= 0x80
	if negative {
		for index := range encoded {
			encoded[index] = ^encoded[index]
		}
	}
	return encoded, true
}

func nativeDecimalSpec(rawType string) (int, int, bool) {
	typeName := strings.ToUpper(strings.TrimSpace(rawType))
	open := strings.IndexByte(typeName, '(')
	if open < 0 {
		fields := strings.Fields(typeName)
		if len(fields) == 0 || (fields[0] != "DECIMAL" && fields[0] != "NUMERIC") {
			return 0, 0, false
		}
		return 10, 0, true
	}
	close := strings.LastIndexByte(typeName, ')')
	if close <= open {
		return 0, 0, false
	}
	name := strings.TrimSpace(typeName[:open])
	if name != "DECIMAL" && name != "NUMERIC" {
		return 0, 0, false
	}
	arguments := strings.Split(typeName[open+1:close], ",")
	if len(arguments) == 0 || len(arguments) > 2 {
		return 0, 0, false
	}
	precision, precisionErr := strconv.Atoi(strings.TrimSpace(arguments[0]))
	scale := 0
	var scaleErr error
	if len(arguments) == 2 {
		scale, scaleErr = strconv.Atoi(strings.TrimSpace(arguments[1]))
	}
	if precisionErr != nil || scaleErr != nil || precision <= 0 || scale < 0 || scale > precision {
		return 0, 0, false
	}
	return precision, scale, true
}

func nativeDecimalBinarySize(integerDigits, fractionDigits int) int {
	return nativeDecimalGroupBytes(integerDigits%9) + (integerDigits/9)*4 + nativeDecimalGroupBytes(fractionDigits%9) + (fractionDigits/9)*4
}

func nativeDecimalGroupBytes(digits int) int {
	switch digits {
	case 0:
		return 0
	case 1, 2:
		return 1
	case 3, 4:
		return 2
	case 5, 6:
		return 3
	default:
		return 4
	}
}

func nativeDecimalAppendDigits(dst *[]byte, digits string) {
	if len(digits) == 0 {
		return
	}
	firstDigits := len(digits) % 9
	if firstDigits == 0 {
		firstDigits = 9
	}
	for offset := 0; offset < len(digits); {
		groupDigits := firstDigits
		if offset > 0 {
			groupDigits = 9
		}
		group, err := strconv.ParseUint(digits[offset:offset+groupDigits], 10, 32)
		if err != nil {
			return
		}
		width := nativeDecimalGroupBytes(groupDigits)
		for shift := width - 1; shift >= 0; shift-- {
			*dst = append(*dst, byte(group>>uint(shift*8)))
		}
		offset += groupDigits
	}
}

func nativeValueBytesForType(typeCode byte, value interface{}) []byte {
	if typeCode == 1 || typeCode == 2 || typeCode == 3 || typeCode == 8 {
		number, _ := strconv.ParseInt(fmt.Sprint(value), 10, 64)
		switch typeCode {
		case 1:
			return []byte{byte(number)}
		case 2:
			encoded := make([]byte, 2)
			binary.LittleEndian.PutUint16(encoded, uint16(number))
			return encoded
		case 3:
			encoded := make([]byte, 4)
			binary.LittleEndian.PutUint32(encoded, uint32(number))
			return encoded
		case 8:
			encoded := make([]byte, 8)
			binary.LittleEndian.PutUint64(encoded, uint64(number))
			return encoded
		}
	}
	if typeCode == 9 {
		if encoded, ok := nativeInt24BinaryValue(value, false); ok {
			return encoded
		}
	}
	if typeCode == 4 {
		number, _ := strconv.ParseFloat(fmt.Sprint(value), 32)
		encoded := make([]byte, 4)
		binary.LittleEndian.PutUint32(encoded, math.Float32bits(float32(number)))
		return encoded
	}
	if typeCode == 5 {
		number, _ := strconv.ParseFloat(fmt.Sprint(value), 64)
		encoded := make([]byte, 8)
		binary.LittleEndian.PutUint64(encoded, math.Float64bits(number))
		return encoded
	}
	return nativeValueBytesForChange(value)
}

func nativeValueBytesForChange(value interface{}) []byte {
	switch typed := value.(type) {
	case bool:
		if typed {
			return []byte{1}
		}
		return []byte{0}
	case int8:
		return []byte{byte(typed)}
	case uint8:
		return []byte{typed}
	case int, int16, int32, uint, uint16, uint32:
		encoded := make([]byte, 4)
		number, _ := strconv.ParseInt(fmt.Sprint(typed), 10, 64)
		binary.LittleEndian.PutUint32(encoded, uint32(number))
		return encoded
	case int64:
		encoded := make([]byte, 8)
		binary.LittleEndian.PutUint64(encoded, uint64(typed))
		return encoded
	case uint64:
		encoded := make([]byte, 8)
		binary.LittleEndian.PutUint64(encoded, typed)
		return encoded
	case float32:
		encoded := make([]byte, 4)
		binary.LittleEndian.PutUint32(encoded, math.Float32bits(typed))
		return encoded
	case float64:
		encoded := make([]byte, 8)
		binary.LittleEndian.PutUint64(encoded, math.Float64bits(typed))
		return encoded
	default:
		text := []byte(fmt.Sprint(value))
		encoded := appendNativeLenencInt(nil, len(text))
		return append(encoded, text...)
	}
}

func buildNativeEventForLogical(event BinlogEvent, position uint64) []byte {
	const (
		queryEventType  = 2
		rotateEventType = 4
		gtidEventType   = 33
		xidEventType    = 16
	)
	switch event.Type {
	case EventBegin:
		return buildNativeEvent(gtidEventType, nativeGTIDBody(event), eventWithNativePosition(event, position))
	case EventCommit:
		body := make([]byte, 8)
		binary.LittleEndian.PutUint64(body, event.GTID.Seq)
		return buildNativeEvent(xidEventType, body, eventWithNativePosition(event, position))
	case EventRotate:
		body := make([]byte, 8, 8+len("binlog.000001"))
		binary.LittleEndian.PutUint64(body, 4)
		body = append(body, []byte("binlog.000001")...)
		return buildNativeEvent(rotateEventType, body, eventWithNativePosition(event, position))
	default:
		database := ""
		queries := make([]string, 0, len(event.Statements))
		for _, statement := range event.Statements {
			if database == "" {
				database = statement.Database
			}
			if strings.TrimSpace(statement.SQL) != "" {
				queries = append(queries, strings.TrimSpace(statement.SQL))
			}
		}
		query := strings.Join(queries, "; ")
		if query == "" {
			raw, _ := json.Marshal(event.Changes)
			query = "/* XMYSQL ROW " + base64.RawStdEncoding.EncodeToString(raw) + " */"
		}
		databaseBytes := []byte(database)
		queryBytes := []byte(query)
		body := make([]byte, 13, 13+len(databaseBytes)+1+len(queryBytes))
		body[8] = byte(len(databaseBytes))
		body = append(body, databaseBytes...)
		body = append(body, 0)
		body = append(body, queryBytes...)
		return buildNativeEvent(queryEventType, body, eventWithNativePosition(event, position))
	}
}

func eventWithNativePosition(event BinlogEvent, position uint64) BinlogEvent {
	event.Position = position
	return event
}

func nativeGTIDBody(event BinlogEvent) []byte {
	body := make([]byte, 56)
	body[0] = 0
	copy(body[1:17], nativeGTIDSID(event.GTID.UUID))
	binary.LittleEndian.PutUint64(body[17:25], event.GTID.Seq)
	body[25] = 2
	lastCommitted := uint64(0)
	if event.GTID.Seq > 0 {
		lastCommitted = event.GTID.Seq - 1
	}
	binary.LittleEndian.PutUint64(body[26:34], lastCommitted)
	binary.LittleEndian.PutUint64(body[34:42], event.GTID.Seq)
	micros := uint64(event.Timestamp.UnixNano() / int64(time.Microsecond))
	for index := 0; index < 7; index++ {
		body[42+index] = byte(micros >> uint(8*index))
		body[49+index] = byte(micros >> uint(8*index))
	}
	return body
}

func nativeGTIDSID(raw string) []byte {
	compact := strings.ReplaceAll(strings.TrimSpace(raw), "-", "")
	if decoded, err := hex.DecodeString(compact); err == nil && len(decoded) == 16 {
		return decoded
	}
	hash := md5.Sum([]byte(raw))
	return hash[:]
}

func nativeGTIDUUID(raw string) string {
	sid := nativeGTIDSID(raw)
	if len(sid) != 16 {
		return ""
	}
	encoded := hex.EncodeToString(sid)
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

func buildNativeEvent(eventType byte, body []byte, event BinlogEvent) []byte {
	raw := make([]byte, nativeEventHeaderLength+len(body)+nativeChecksumLength)
	binary.LittleEndian.PutUint32(raw[0:4], uint32(event.Timestamp.Unix()))
	raw[4] = eventType
	binary.LittleEndian.PutUint32(raw[5:9], event.ServerID)
	binary.LittleEndian.PutUint32(raw[9:13], uint32(len(raw)))
	binary.LittleEndian.PutUint32(raw[13:17], uint32(event.Position)+uint32(len(raw)))
	binary.LittleEndian.PutUint16(raw[17:19], 0)
	copy(raw[19:], body)
	binary.LittleEndian.PutUint32(raw[len(raw)-nativeChecksumLength:], crc32.ChecksumIEEE(raw[:len(raw)-nativeChecksumLength]))
	return raw
}

func patchNativeEventLogPosition(raw []byte, endPosition uint64) {
	if len(raw) < nativeEventHeaderLength+nativeChecksumLength {
		return
	}
	binary.LittleEndian.PutUint32(raw[13:17], uint32(endPosition))
	binary.LittleEndian.PutUint32(raw[len(raw)-nativeChecksumLength:], crc32.ChecksumIEEE(raw[:len(raw)-nativeChecksumLength]))
}

func (writer *BinlogWriter) ReadFrom(position uint64) ([]BinlogEvent, error) {
	if writer == nil {
		return nil, nil
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	file, err := os.Open(writer.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	events := make([]BinlogEvent, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		var payload []byte
		if err := json.Unmarshal(scanner.Bytes(), &payload); err != nil {
			return nil, err
		}
		event, err := DecodeEvent(payload)
		if err != nil {
			return nil, err
		}
		if event.Position >= position {
			events = append(events, event)
		}
	}
	return events, scanner.Err()
}
