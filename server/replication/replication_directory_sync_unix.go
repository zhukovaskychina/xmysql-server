//go:build !windows

package replication

import "os"

// syncReplicationDirectory makes the rename durable on filesystems that
// expose directory handles. Without this step, a successful file fsync and
// rename can still lose the new directory entry after a power loss.
func syncReplicationDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
