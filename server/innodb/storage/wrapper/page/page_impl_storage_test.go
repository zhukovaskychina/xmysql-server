package page

import (
	"testing"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

func TestLegacyPageRequiresStorageProvider(t *testing.T) {
	p := NewPage(1, 2, common.FIL_PAGE_INDEX)
	if err := p.Read(); err != ErrPageStorageUnavailable {
		t.Fatalf("Read() error = %v, want %v", err, ErrPageStorageUnavailable)
	}
	p.MarkDirty()
	if err := p.Write(); err != ErrPageStorageUnavailable {
		t.Fatalf("Write() error = %v, want %v", err, ErrPageStorageUnavailable)
	}
}
