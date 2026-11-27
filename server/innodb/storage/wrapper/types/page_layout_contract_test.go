package types_test

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/common"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/basic"
	legacy "github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/page"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/storage/wrapper/types"
)

// These literal header images describe the pre-unification formats. They do
// not calculate expected offsets from the constants used by the serializers.
func originalPageImage(t *testing.T, header string) []byte {
	t.Helper()
	h, err := hex.DecodeString(header)
	if err != nil {
		t.Fatal(err)
	}
	if len(h) != 38 {
		t.Fatalf("fixture header length %d", len(h))
	}
	image := make([]byte, 16384)
	copy(image, h)
	return image
}

type pageImageStore struct {
	basic.StorageProvider
	data []byte
}

func (s *pageImageStore) ReadPage(uint32, uint32) ([]byte, error) {
	return append([]byte(nil), s.data...), nil
}
func (s *pageImageStore) WritePage(_ uint32, _ uint32, data []byte) error {
	s.data = append([]byte(nil), data...)
	return nil
}

func TestPageLayoutOriginalByteImages(t *testing.T) {
	const space = 0x11223344
	const pageNo = 0x55667788
	const lsn = 0x0102030405060708
	t.Run("types BasePage SpaceID zero", func(t *testing.T) {
		expected := originalPageImage(t, "112233445566778800000000000000000102030405060708000a000000000000000000000000")
		store := &pageImageStore{data: expected}
		p := types.NewBasePageWithStorage(space, pageNo, common.FIL_PAGE_TYPE_BLOB, store)
		p.SetLSN(lsn)
		if !bytes.Equal(p.GetFileHeader(), expected[:38]) {
			t.Fatalf("legacy constructor header = %x", p.GetFileHeader())
		}
		if err := p.Read(); err != nil {
			t.Fatal(err)
		}
		if p.GetSpaceID() != space || p.GetPageNo() != pageNo || p.GetLSN() != lsn {
			t.Fatal("literal legacy header decoded incorrectly")
		}
		if err := p.Write(); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(store.data, expected) {
			t.Fatal("legacy page changed original image")
		}
	})
	t.Run("page BasePage SpaceID thirty four", func(t *testing.T) {
		expected := originalPageImage(t, "000000005566778800000000000000000102030405060708000a000000000000000011223344")
		p := legacy.NewBasePage(space, pageNo, common.FIL_PAGE_TYPE_BLOB)
		p.SetLSN(lsn)
		actual, err := p.ToBytes()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(actual, expected) {
			t.Fatalf("legacy wrapper header = %x", actual[:38])
		}
		if err := p.ParseFromBytes(expected); err != nil {
			t.Fatal(err)
		}
		if p.GetSpaceID() != space || p.GetPageNo() != pageNo || p.GetLSN() != lsn {
			t.Fatal("literal wrapper header decoded incorrectly")
		}
	})
	t.Run("UnifiedPage InnoDB", func(t *testing.T) {
		expected := originalPageImage(t, "0000000055667788ffffffffffffffff0102030405060708000a000000000000000011223344")
		copy(expected[16380:], []byte{5, 6, 7, 8})
		p := types.NewUnifiedPage(space, pageNo, common.FIL_PAGE_TYPE_BLOB)
		p.SetLSN(lsn)
		actual, err := p.Serialize()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(actual, expected) {
			t.Fatalf("unified header = %x", actual[:38])
		}
		// Independently chosen nonzero body/trailer and FlushLSN bytes also survive.
		copy(expected[26:34], []byte{9, 10, 11, 12, 13, 14, 15, 16})
		expected[38], expected[16375] = 0xab, 0xcd
		copy(expected[16376:], []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88})
		if err := p.Deserialize(expected); err != nil {
			t.Fatal(err)
		}
		if p.GetSpaceID() != space || p.GetPageNo() != pageNo || p.GetLSN() != lsn || p.GetPageType() != common.FIL_PAGE_TYPE_BLOB {
			t.Fatal("literal InnoDB header decoded incorrectly")
		}
		actual, err = p.Serialize()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(actual, expected) {
			t.Fatal("InnoDB page changed original image")
		}
	})
}
