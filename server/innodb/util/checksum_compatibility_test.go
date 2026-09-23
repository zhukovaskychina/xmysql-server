package util

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/OneOfOne/xxhash"
)

func TestChecksumCalculatorUsesDeclaredAlgorithms(t *testing.T) {
	data := []byte("xmysql checksum compatibility")

	xxExpected := xxhash.Checksum32(data)
	if got := NewChecksumCalculator(ChecksumXXHash).Calculate(data); got != xxExpected {
		t.Fatalf("xxhash checksum = %#x, want %#x", got, xxExpected)
	}

	shaExpectedBytes := sha256.Sum256(data)
	shaExpected := binary.BigEndian.Uint32(shaExpectedBytes[:4])
	if got := NewChecksumCalculator(ChecksumSHA256).Calculate(data); got != shaExpected {
		t.Fatalf("sha256 checksum = %#x, want %#x", got, shaExpected)
	}
}

func TestChecksumCalculatorParallelMatchesWholeInput(t *testing.T) {
	data := make([]byte, 8193)
	for i := range data {
		data[i] = byte((i * 31) % 251)
	}

	for _, algorithm := range []string{ChecksumCRC32, ChecksumCRC32C, ChecksumXXHash, ChecksumSHA256} {
		calculator := NewChecksumCalculator(algorithm)
		want := calculator.Calculate(data)
		if got := calculator.ParallelCalculate(data, 257); got != want {
			t.Errorf("ParallelCalculate(%s) = %#x, Calculate = %#x", algorithm, got, want)
		}
		if got := calculator.ParallelCalculate(data, 0); got != want {
			t.Errorf("ParallelCalculate(%s, chunkSize=0) = %#x, Calculate = %#x", algorithm, got, want)
		}
	}
}
