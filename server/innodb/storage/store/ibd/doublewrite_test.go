package ibd

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if os.Getenv("XMYSQL_DOUBLEWRITE_CHILD") == "1" {
		runDoublewriteKillChild()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestDoublewriteRestoresTornPageAfterProcessKill(t *testing.T) {
	dir := t.TempDir()
	t.Cleanup(func() {
		if buffer, err := EnableDoublewrite(dir); err == nil {
			ReleaseDoublewrite(dir, buffer)
		}
		path := filepath.Join(dir, "xb_doublewrite")
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			err := os.Remove(path)
			if err == nil || os.IsNotExist(err) {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
	cmd := exec.Command(os.Args[0], "-test.run=TestDoublewriteRestoresTornPageAfterProcessKill")
	cmd.Env = append(os.Environ(), "XMYSQL_DOUBLEWRITE_CHILD=1", "XMYSQL_DOUBLEWRITE_DIR="+dir)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := filepath.Join(dir, "ready")
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := os.Stat(ready); err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("child did not publish a torn page: %v", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_, _ = cmd.Process.Wait()

	if err := Restore(dir); err != nil {
		t.Fatal(err)
	}
	page, err := os.ReadFile(filepath.Join(dir, "kill.ibd"))
	if err != nil {
		t.Fatal(err)
	}
	offset := 2 * PageSize
	if len(page) < offset+PageSize {
		t.Fatalf("ibd length %d", len(page))
	}
	restored := page[offset : offset+PageSize]
	if !bytes.Equal(restored[:8], []byte("RESTORED")) {
		t.Fatalf("torn page was not restored, prefix %q", restored[:8])
	}
}

func runDoublewriteKillChild() {
	dir := os.Getenv("XMYSQL_DOUBLEWRITE_DIR")
	if _, err := EnableDoublewrite(dir); err != nil {
		panic(err)
	}
	file := NewIBDFile(dir, "kill", 7)
	if err := file.Create(); err != nil {
		panic(err)
	}
	page := bytes.Repeat([]byte{0x5A}, PageSize)
	copy(page[:8], []byte("RESTORED"))
	if err := file.WritePage(2, page); err != nil {
		panic(err)
	}
	raw, err := os.OpenFile(file.filePath, os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	if _, err := raw.WriteAt(make([]byte, 64), int64(2*PageSize)); err != nil {
		panic(err)
	}
	if err := raw.Sync(); err != nil {
		panic(err)
	}
	_ = raw.Close()
	if err := os.WriteFile(filepath.Join(dir, "ready"), []byte("torn"), 0644); err != nil {
		panic(err)
	}
	time.Sleep(2 * time.Minute)
}
