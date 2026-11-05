package engine

import (
	"errors"
	"os"
	"strings"
	"syscall"
	"time"
)

// replaceDurableFile publishes a complete temporary file onto path. Windows
// rejects renaming over a file that is still open, which dropped event_log.frm
// while a B+ tree initialization still had the metadata open. Retry the rename,
// then publish by rewriting the existing file so the durable bytes are the
// temp file that was already synced.
func replaceDurableFile(tmpPath, path string) error {
	var last error
	for attempt := 0; attempt < 8; attempt++ {
		err := os.Rename(tmpPath, path)
		if err == nil {
			return nil
		}
		last = err
		if !replaceDenied(err) {
			return err
		}
		time.Sleep(time.Duration(5*(attempt+1)) * time.Millisecond)
	}
	data, err := os.ReadFile(tmpPath)
	if err != nil {
		return last
	}
	destination, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		return last
	}
	if _, err := destination.Write(data); err != nil {
		_ = destination.Close()
		return err
	}
	if err := destination.Truncate(int64(len(data))); err != nil {
		_ = destination.Close()
		return err
	}
	if err := destination.Sync(); err != nil {
		_ = destination.Close()
		return err
	}
	if err := destination.Close(); err != nil {
		return err
	}
	_ = os.Remove(tmpPath)
	return nil
}

func replaceDenied(err error) bool {
	if os.IsExist(err) || os.IsPermission(err) {
		return true
	}
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) && linkErr.Err != nil {
		err = linkErr.Err
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) && pathErr.Err != nil {
		err = pathErr.Err
	}
	errno, ok := err.(syscall.Errno)
	if !ok {
		return false
	}
	switch errno {
	case syscall.EACCES, syscall.EPERM, syscall.EBUSY:
		return true
	default:
		// Windows ERROR_ACCESS_DENIED is 5, ERROR_SHARING_VIOLATION is 32,
		// and ERROR_LOCK_VIOLATION is 33.
		return errno == 5 || errno == 32 || errno == 33
	}
}

func replaceRowFormatOption(raw, replacement string) (string, bool) {
	lower := strings.ToLower(raw)
	for start := 0; start < len(lower); {
		at := strings.Index(lower[start:], "row_format")
		if at < 0 {
			return raw, false
		}
		at += start
		if at > 0 && isSQLIdentByte(lower[at-1]) {
			start = at + len("row_format")
			continue
		}
		rest := strings.TrimLeft(raw[at+len("row_format"):], " \t\r\n")
		if !strings.HasPrefix(rest, "=") {
			start = at + len("row_format")
			continue
		}
		value := strings.TrimLeft(rest[1:], " \t\r\n")
		valueLower := strings.ToLower(value)
		matched := ""
		for _, candidate := range []string{"dynamic", "compact", "compressed", "redundant", "default"} {
			if strings.HasPrefix(valueLower, candidate) && (len(valueLower) == len(candidate) || !isSQLIdentByte(valueLower[len(candidate)])) {
				matched = candidate
				break
			}
		}
		if matched == "" {
			start = at + len("row_format")
			continue
		}
		valueStart := len(raw) - len(value)
		return raw[:at] + replacement + raw[valueStart+len(matched):], true
	}
	return raw, false
}
