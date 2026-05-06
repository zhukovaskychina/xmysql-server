/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package util

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	"reflect"
	"unsafe"
)

// BytesToString casts slice to string without copy
func BytesToString(b []byte) (s string) {
	if len(b) == 0 {
		return ""
	}

	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := reflect.StringHeader{Data: bh.Data, Len: bh.Len}

	return *(*string)(unsafe.Pointer(&sh))
}

// StringToBytes casts string to slice without copy
func StringToBytes(s string) []byte {
	if len(s) == 0 {
		return []byte{}
	}

	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{Data: sh.Data, Len: sh.Len, Cap: sh.Len}

	return *(*[]byte)(unsafe.Pointer(&bh))
}

// ToJSONString format v to the JSON encoding, return a string.
func ToJSONString(v interface{}, escapeHTML bool, prefix, indent string) (string, error) {
	// 生成紧凑的单行 JSON
	bf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(bf)
	enc.SetEscapeHTML(escapeHTML)
	if err := enc.Encode(v); err != nil {
		return "", err
	}
	s := bf.String()
	if len(s) > 0 && s[len(s)-1] == '\n' {
		s = s[:len(s)-1]
	}
	// 未指定缩进时直接返回紧凑格式
	if indent == "" {
		return s, nil
	}
	// 单行 JSON：在 { 之后、逗号之后插入缩进；在 } 之前插入缩进；在冒号后插入空格
	var out bytes.Buffer
	inString := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		// 处理字符串边界（考虑转义）
		if c == '"' {
			if i == 0 || s[i-1] != '\\' {
				inString = !inString
			}
			out.WriteByte(c)
			continue
		}
		if inString {
			out.WriteByte(c)
			continue
		}
		switch c {
		case '{':
			out.WriteByte(c)
			out.WriteString(indent)
		case ',':
			out.WriteByte(c)
			out.WriteString(indent)
		case '}':
			out.WriteByte(c)
		case ':':
			out.WriteString(": ")
		default:
			out.WriteByte(c)
		}
	}
	return out.String(), nil
}

// 生成md5
func MD5(str string) string {
	c := md5.New()
	c.Write([]byte(str))
	return hex.EncodeToString(c.Sum(nil))
}

//生成sha1
func SHA1(str string) string {
	c := sha1.New()
	c.Write([]byte(str))
	return hex.EncodeToString(c.Sum(nil))
}

func CRC32(str string) uint32 {
	return crc32.ChecksumIEEE([]byte(str))
}
