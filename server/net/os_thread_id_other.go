//go:build !windows && !linux
// +build !windows,!linux

package net

func currentOSThreadID() int64 {
	return 0
}
