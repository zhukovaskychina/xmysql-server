package page

import (
	"testing"
	"time"
)

func TestDataDictWrapperPlaceholder(t *testing.T) {
	t.Skip("需要完整数据字典和buffer_pool测试环境，当前保持占位以免误报")
}

func TestDataDictionaryPageWrapperToBytesDoesNotDeadlock(t *testing.T) {
	wrapper := NewDataDictionaryPageWrapper(1, 1, nil)

	done := make(chan error, 1)
	go func() {
		_, err := wrapper.ToBytes()
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ToBytes() returned error: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("ToBytes() deadlocked while updating checksum")
	}
}

func TestDataDictionaryPageWrapperAddTableDefDoesNotDeadlock(t *testing.T) {
	wrapper := NewDataDictionaryPageWrapper(1, 1, nil)

	done := make(chan error, 1)
	go func() {
		done <- wrapper.AddTableDef(&TableDef{ID: 1, Name: "users"})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("AddTableDef() returned error: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("AddTableDef() deadlocked while marking page dirty")
	}
}
