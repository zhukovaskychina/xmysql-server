package page

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDataDictionaryWrapperProviderRoundTrip(t *testing.T) {
	storage := newPageWrapperStorage()
	writer := NewDataDictionaryPageWrapperWithStorage(2, 9, nil, storage)
	require.NoError(t, writer.AddTableDef(&TableDef{ID: 11, Name: "users"}))
	require.NoError(t, writer.Write())

	reader := NewDataDictionaryPageWrapperWithStorage(2, 9, nil, storage)
	require.NoError(t, reader.Read())
	table, err := reader.GetTableDef(11)
	require.NoError(t, err)
	require.Equal(t, "users", table.Name)
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
