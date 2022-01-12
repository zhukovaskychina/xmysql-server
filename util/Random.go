package util

import "time"

const (
	multiplier = int64(0x5DEECE66D)

	integerMask int64 = (1 << 33) - 1

	seedUniquifier int64 = 8682522807148012

	mask int64 = (1 << 48) - 1

	addend int64 = 0xB
)

var (
	s = seedUniquifier + time.Now().UTC().UnixNano()

	seed = (s ^ multiplier) & mask

	seedBytes = []byte{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'q', 'w', 'e', 'r', 't',
		'y', 'u', 'i', 'o', 'p', 'a', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'z', 'x', 'c', 'v', 'b', 'n', 'm',
		'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P', 'A', 'S', 'D', 'F', 'G', 'H', 'J', 'K', 'L', 'Z', 'X',
		'C', 'V', 'B', 'N', 'M'}
)

func next() int64 {
	oldSeed := seed
	nextSeed := int64(0)

	for {

		nextSeed = int64((oldSeed*multiplier + addend) & mask)
		if oldSeed != nextSeed {
			break
		}
		//s = seedUniquifier + time.Now().UTC().UnixNano()
		//seed = (s ^ multiplier) & mask
		//oldSeed=seed
	}
	seed = nextSeed
	return nextSeed
}

func randomByte(bytes []byte) byte {
	ran := (int)((next() & integerMask) >> 16)
	return bytes[ran%len(bytes)]

}
func RandomBytes(size int) []byte {

	result := make([]byte, size)

	i := 0

	for i = 0; i < int(size); i++ {
		result[i] = randomByte(seedBytes)
	}
	return result
}

//生成UUID
func GetDBRowId() {

}

func GetTrxId() {

}
func GetRollPointer() {

}
