package metadata

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

var publicationLocks sync.Map // canonical path -> *sync.RWMutex

func publicationLock(path string) *sync.RWMutex {
	key, err := filepath.Abs(path)
	if err != nil {
		key = filepath.Clean(path)
	}
	if runtime.GOOS == "windows" {
		key = strings.ToLower(key)
	}
	lock, _ := publicationLocks.LoadOrStore(key, &sync.RWMutex{})
	return lock.(*sync.RWMutex)
}

// ReadFile captures a complete metadata version while an in-process publisher
// may be replacing it, including Windows replacement's open-file fallback.
func ReadFile(path string) ([]byte, error) {
	lock := publicationLock(path)
	lock.RLock()
	defer lock.RUnlock()
	return os.ReadFile(path)
}

// LockFilePublication excludes coordinated readers until publication completes.
// The caller must always release the returned process-local lock.
func LockFilePublication(path string) func() {
	lock := publicationLock(path)
	lock.Lock()
	return lock.Unlock
}
