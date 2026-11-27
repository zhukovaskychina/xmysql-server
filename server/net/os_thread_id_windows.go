//go:build windows
// +build windows

package net

import "syscall"

var getCurrentThreadID = syscall.NewLazyDLL("kernel32.dll").NewProc("GetCurrentThreadId")

func currentOSThreadID() int64 {
	threadID, _, _ := getCurrentThreadID.Call()
	return int64(threadID)
}
