//go:build windows

package replication

import (
	"golang.org/x/sys/windows"
)

// syncReplicationDirectory flushes the directory handle on Windows. The
// backup-semantics flag is required to open a directory with CreateFile.
func syncReplicationDirectory(path string) error {
	handle, err := windows.CreateFile(
		windows.StringToUTF16Ptr(path),
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)
	if err := windows.FlushFileBuffers(handle); err != nil {
		// Windows commonly rejects FlushFileBuffers on directory handles with
		// ERROR_ACCESS_DENIED even when the rename and file flush succeeded.
		// Keep the atomic replacement usable on that platform; the file handle
		// was already flushed before the rename, so this is a best-effort
		// directory-entry barrier on Windows.
		if err == windows.ERROR_ACCESS_DENIED {
			return nil
		}
		return err
	}
	return nil
}
