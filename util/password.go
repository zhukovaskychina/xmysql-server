package util

import (
	"crypto/sha1"
)

//stage1_hash = SHA1(password), using the password that the user has entered.
//token = SHA1(SHA1(stage1_hash), scramble) XOR stage1_hash
func GetPassword(pass []byte, seed []byte, restOfScrambleBuff []byte) []byte {
	salt := []byte{}
	for _, v := range seed {
		salt = append(salt, v)
	}
	for _, v := range restOfScrambleBuff {
		salt = append(salt, v)
	}

	sh := sha1.New()
	sh.Write(pass)
	stage1_hash := sh.Sum(nil)

	sh.Reset()
	sh.Write(stage1_hash)
	stage2_hash := sh.Sum(nil)

	sh.Reset()
	for _, v := range stage2_hash {
		salt = append(salt, v)
	}
	sh.Write(salt)

	stage3_hash := sh.Sum(nil)

	ret := []byte{}
	for k, _ := range stage3_hash {
		ret = append(ret, stage1_hash[k]^stage3_hash[k])
	}
	return ret
}
