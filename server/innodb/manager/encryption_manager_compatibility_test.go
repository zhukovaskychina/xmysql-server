package manager

import (
	"bytes"
	"crypto/aes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func newTestEncryptionManager() (*EncryptionManager, error) {
	em := NewEncryptionManager([]byte("test-master-key"), EncryptionSettings{Method: ENCRYPTION_METHOD_AES})
	_, err := em.CreateKey(7)
	return em, err
}

func TestEncryptionManagerAESRoundTripsBinaryAndEmptyPages(t *testing.T) {
	em, err := newTestEncryptionManager()
	if err != nil {
		t.Fatalf("CreateKey() error = %v", err)
	}

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{name: "empty", data: nil},
		{name: "binary", data: []byte{0, 1, 2, 0xff, 0x10, 0x00}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ciphertext, err := em.EncryptPage(7, 42, tc.data)
			if err != nil {
				t.Fatalf("EncryptPage() error = %v", err)
			}
			plaintext, err := em.DecryptPage(7, 42, ciphertext)
			if err != nil {
				t.Fatalf("DecryptPage() error = %v", err)
			}
			if !bytes.Equal(plaintext, tc.data) {
				t.Fatalf("plaintext = %v, want %v", plaintext, tc.data)
			}
		})
	}
}

func TestEncryptionManagerRejectsMalformedAESCiphertext(t *testing.T) {
	em, err := newTestEncryptionManager()
	if err != nil {
		t.Fatalf("CreateKey() error = %v", err)
	}

	for _, ciphertext := range [][]byte{nil, []byte{1}, make([]byte, 15)} {
		_, err := em.DecryptPage(7, 42, ciphertext)
		if !errors.Is(err, ErrInvalidCiphertext) {
			t.Errorf("DecryptPage(%v) error = %v, want ErrInvalidCiphertext", ciphertext, err)
		}
	}
}

func TestEncryptionManagerRejectsInvalidPKCSPadding(t *testing.T) {
	em, err := newTestEncryptionManager()
	if err != nil {
		t.Fatalf("CreateKey() error = %v", err)
	}

	// Use two ciphertext blocks so changing the last byte of the first block
	// deterministically corrupts the second block's PKCS#7 padding.
	ciphertext, err := em.EncryptPage(7, 42, bytes.Repeat([]byte{'p'}, aes.BlockSize))
	if err != nil {
		t.Fatalf("EncryptPage() error = %v", err)
	}
	ciphertext[aes.BlockSize-1] ^= 0xff
	if _, err := em.DecryptPage(7, 42, ciphertext); !errors.Is(err, ErrInvalidCiphertext) {
		t.Fatalf("DecryptPage() error = %v, want ErrInvalidCiphertext", err)
	}
}

func TestEncryptionManagerDoesNotExposeMutableKeyMaterial(t *testing.T) {
	em, err := newTestEncryptionManager()
	if err != nil {
		t.Fatalf("CreateKey() error = %v", err)
	}

	key := em.GetKey(7)
	if key == nil {
		t.Fatal("GetKey() returned nil")
	}
	key.Key[0] ^= 0xff
	key.IV[0] ^= 0xff

	fresh := em.GetKey(7)
	if fresh == nil {
		t.Fatal("second GetKey() returned nil")
	}
	if bytes.Equal(key.Key, fresh.Key) || bytes.Equal(key.IV, fresh.IV) {
		t.Fatal("GetKey() exposed mutable key material")
	}
	if _, err := em.DecryptPage(7, 42, mustEncryptPage(t, em, 7, 42, []byte("protected"))); err != nil {
		t.Fatalf("key material was corrupted through GetKey(): %v", err)
	}
}

func TestEncryptionManagerPersistsEncryptedKeyring(t *testing.T) {
	masterKey := []byte("test-master-key")
	path := filepath.Join(t.TempDir(), "encryption", "keyring.bin")

	writer := NewEncryptionManager(masterKey, EncryptionSettings{Method: ENCRYPTION_METHOD_AES})
	if _, err := writer.CreateKey(7); err != nil {
		t.Fatalf("CreateKey() error = %v", err)
	}
	key := writer.GetKey(7)
	if key == nil {
		t.Fatal("GetKey() returned nil")
	}
	if err := writer.SaveKeyring(path); err != nil {
		t.Fatalf("SaveKeyring() error = %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if bytes.Contains(raw, key.Key) || bytes.Contains(raw, key.IV) {
		t.Fatal("keyring persisted raw key material")
	}

	reader := NewEncryptionManager(masterKey, EncryptionSettings{Method: ENCRYPTION_METHOD_AES})
	if err := reader.LoadKeyring(path); err != nil {
		t.Fatalf("LoadKeyring() error = %v", err)
	}
	ciphertext := mustEncryptPage(t, writer, 7, 42, []byte("persisted key"))
	plaintext, err := reader.DecryptPage(7, 42, ciphertext)
	if err != nil {
		t.Fatalf("DecryptPage() after load error = %v", err)
	}
	if string(plaintext) != "persisted key" {
		t.Fatalf("plaintext after load = %q", plaintext)
	}

	wrongMaster := NewEncryptionManager([]byte("wrong-master"), EncryptionSettings{Method: ENCRYPTION_METHOD_AES})
	if err := wrongMaster.LoadKeyring(path); err == nil {
		t.Fatal("LoadKeyring() with wrong master key unexpectedly succeeded")
	}
}

func mustEncryptPage(t *testing.T, em *EncryptionManager, spaceID, pageNo uint32, data []byte) []byte {
	t.Helper()
	ciphertext, err := em.EncryptPage(spaceID, pageNo, data)
	if err != nil {
		t.Fatalf("EncryptPage() error = %v", err)
	}
	return ciphertext
}
