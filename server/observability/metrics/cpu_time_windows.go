//go:build windows

package metrics

import (
	"syscall"
	"unsafe"
)

var (
	kernel32GetCurrentThread = syscall.NewLazyDLL("kernel32.dll").NewProc("GetCurrentThread")
	kernel32GetThreadTimes   = syscall.NewLazyDLL("kernel32.dll").NewProc("GetThreadTimes")
)

// CurrentThreadCPUTimeNanos returns user+kernel CPU time for the current OS
// thread. Windows exposes FILETIME values in 100-nanosecond units.
func CurrentThreadCPUTimeNanos() (int64, bool) {
	thread, _, _ := kernel32GetCurrentThread.Call()
	var creation, exit, kernel, user syscall.Filetime
	ret, _, _ := kernel32GetThreadTimes.Call(thread, uintptr(unsafe.Pointer(&creation)), uintptr(unsafe.Pointer(&exit)), uintptr(unsafe.Pointer(&kernel)), uintptr(unsafe.Pointer(&user)))
	if ret == 0 {
		return 0, false
	}
	return int64((filetimeToUint64(kernel) + filetimeToUint64(user)) * 100), true
}

func filetimeToUint64(value syscall.Filetime) uint64 {
	return uint64(value.HighDateTime)<<32 | uint64(value.LowDateTime)
}
