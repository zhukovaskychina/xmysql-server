//go:build linux

package metrics

import "syscall"

// CurrentThreadCPUTimeNanos returns user+system CPU time for the current Linux
// thread. Linux reports the values as timeval structures.
func CurrentThreadCPUTimeNanos() (int64, bool) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_THREAD, &usage); err != nil {
		return 0, false
	}
	return timevalToNanos(usage.Utime) + timevalToNanos(usage.Stime), true
}

func timevalToNanos(value syscall.Timeval) int64 {
	return value.Sec*1_000_000_000 + value.Usec*1_000
}
