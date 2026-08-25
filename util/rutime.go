package util

import (
	"runtime"
	"strconv"
	"strings"
)

func Goid() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	fields := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))
	if len(fields) == 0 {
		return 0
	}
	id, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0
	}
	return id
}
