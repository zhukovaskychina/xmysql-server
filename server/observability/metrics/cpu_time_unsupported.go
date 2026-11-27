//go:build !windows && !linux

package metrics

// CurrentThreadCPUTimeNanos reports that this platform has no implementation
// of the OS-thread CPU timer used by Performance Schema CPU_TIME.
func CurrentThreadCPUTimeNanos() (int64, bool) { return 0, false }
