//go:build linux
// +build linux

package net

import "golang.org/x/sys/unix"

func currentOSThreadID() int64 {
	return int64(unix.Gettid())
}
