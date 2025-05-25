package util

import "time"

// GetCurrentTimeMillis 获取当前时间的毫秒时间戳
func GetCurrentTimeMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// GetCurrentTimeNanos 获取当前时间的纳秒时间戳
func GetCurrentTimeNanos() int64 {
	return time.Now().UnixNano()
}

// GetCurrentTimestamp 获取当前时间戳（秒）
func GetCurrentTimestamp() int64 {
	return time.Now().Unix()
}
