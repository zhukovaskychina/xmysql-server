package blob

import "testing"

func TestGenerateBlobIDDoesNotReuseExistingIDs(t *testing.T) {
	bm := NewBlobManager(nil, nil)
	bm.blobMeta[1] = &BlobMetadata{BlobID: 1}

	first := bm.generateBlobID()
	second := bm.generateBlobID()
	if first != 2 || second != 3 {
		t.Fatalf("generated blob IDs = %d, %d; want 2, 3", first, second)
	}
}

func TestReadBlobPartialAllowsEmptyRangeAtEnd(t *testing.T) {
	bm := NewBlobManager(nil, nil)
	bm.blobMeta[7] = &BlobMetadata{BlobID: 7, TotalSize: 12}

	data, err := bm.ReadBlobPartial(7, 12, 0)
	if err != nil {
		t.Fatalf("ReadBlobPartial empty end range: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("empty range returned %d bytes, want 0", len(data))
	}
}
