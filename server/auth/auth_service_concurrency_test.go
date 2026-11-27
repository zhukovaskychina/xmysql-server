package auth

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/zhukovaskychina/xmysql-server/server/common"
)

func TestAuthServiceConcurrentCacheAccess(t *testing.T) {
	service := NewAuthService(nil, &MockEngineAccessForTest{})
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			ctx := context.Background()
			for iteration := 0; iteration < 100; iteration++ {
				key := fmt.Sprintf("session-%d-%d", worker, iteration)
				if _, err := service.GenerateChallenge(key); err != nil {
					t.Error(err)
					return
				}
				service.GetChallenge(key)
				if _, err := service.GetUserInfo(ctx, "root", "localhost"); err != nil {
					t.Error(err)
					return
				}
				if err := service.ValidateDatabase(ctx, "mysql"); err != nil {
					t.Error(err)
					return
				}
				result, err := service.AuthenticateUser(ctx, "root", "", "localhost", "mysql")
				if err != nil || !result.Success {
					t.Errorf("authentication failed: %+v, %v", result, err)
					return
				}
				if err := service.FlushPrivileges(ctx); err != nil {
					t.Error(err)
					return
				}
			}
		}(worker)
	}
	workers.Wait()
}

type blockedPrivilegeAccess struct {
	MockEngineAccessForTest
	mu        sync.Mutex
	privilege common.PrivilegeType
	calls     int
	started   chan struct{}
	release   chan struct{}
}

func (a *blockedPrivilegeAccess) QueryUserPrivileges(context.Context, string, string) ([]common.PrivilegeType, error) {
	a.mu.Lock()
	privilege := a.privilege
	a.calls++
	first := a.calls == 1
	a.mu.Unlock()
	if first {
		close(a.started)
		<-a.release
	}
	return []common.PrivilegeType{privilege}, nil
}

func TestAuthServiceFlushDoesNotRestoreStaleInFlightLookup(t *testing.T) {
	testAuthServiceStaleInFlightLookup(t, func(service *AuthServiceImpl) {
		if err := service.FlushPrivileges(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAuthServiceInvalidationDoesNotRestoreStaleInFlightLookup(t *testing.T) {
	testAuthServiceStaleInFlightLookup(t, func(service *AuthServiceImpl) {
		service.invalidateCachedUser("root", "localhost")
	})
}

func testAuthServiceStaleInFlightLookup(t *testing.T, invalidate func(*AuthServiceImpl)) {
	t.Helper()
	access := &blockedPrivilegeAccess{privilege: common.SelectPriv, started: make(chan struct{}), release: make(chan struct{})}
	service := NewAuthService(nil, access).(*AuthServiceImpl)
	done := make(chan error, 1)
	go func() { _, err := service.GetUserInfo(context.Background(), "root", "localhost"); done <- err }()
	select {
	case <-access.started:
	case <-time.After(3 * time.Second):
		t.Fatal("lookup did not start")
	}
	access.mu.Lock()
	access.privilege = common.InsertPriv
	access.mu.Unlock()
	invalidate(service)
	close(access.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("lookup did not finish")
	}
	info, err := service.GetUserInfo(context.Background(), "root", "localhost")
	if err != nil {
		t.Fatal(err)
	}
	if len(info.GlobalPrivileges) != 1 || info.GlobalPrivileges[0] != common.InsertPriv {
		t.Fatalf("in-flight lookup restored stale privileges after flush: %v", info.GlobalPrivileges)
	}
}

type blockedDatabaseAccess struct {
	MockEngineAccessForTest
	mu      sync.Mutex
	exists  bool
	calls   int
	started chan struct{}
	release chan struct{}
}

func (a *blockedDatabaseAccess) QueryDatabase(_ context.Context, database string) (*DatabaseInfo, error) {
	a.mu.Lock()
	exists := a.exists
	a.calls++
	first := a.calls == 1
	a.mu.Unlock()
	if first {
		close(a.started)
		<-a.release
	}
	return &DatabaseInfo{Name: database, Exists: exists}, nil
}

func TestAuthServiceFlushDoesNotRestoreStaleDatabase(t *testing.T) {
	access := &blockedDatabaseAccess{exists: true, started: make(chan struct{}), release: make(chan struct{})}
	service := NewAuthService(nil, access)
	done := make(chan error, 1)
	go func() { done <- service.ValidateDatabase(context.Background(), "app") }()
	select {
	case <-access.started:
	case <-time.After(3 * time.Second):
		t.Fatal("database lookup did not start")
	}
	access.mu.Lock()
	access.exists = false
	access.mu.Unlock()
	if err := service.FlushPrivileges(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(access.release)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("in-flight query accepted a dropped database after flush")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("database lookup did not finish")
	}
	if err := service.ValidateDatabase(context.Background(), "app"); err == nil {
		t.Fatal("stale database lookup was cached after flush")
	}
}

type blockedPasswordValidator struct {
	PasswordValidator
	started chan struct{}
	release chan struct{}
}

func (v *blockedPasswordValidator) ValidatePassword(string, string, []byte) bool {
	close(v.started)
	<-v.release
	return true
}

func TestAuthServiceAuthenticationPreservesReplacedChallenge(t *testing.T) {
	service := NewAuthService(nil, &MockEngineAccessForTest{}).(*AuthServiceImpl)
	validator := &blockedPasswordValidator{PasswordValidator: NewMySQLNativePasswordValidator(), started: make(chan struct{}), release: make(chan struct{})}
	service.passwordValidator = validator
	if _, err := service.GenerateChallenge("root@localhost"); err != nil {
		t.Fatal(err)
	}
	done := make(chan *AuthResult, 1)
	go func() {
		result, _ := service.AuthenticateUser(context.Background(), "root", "", "localhost", "")
		done <- result
	}()
	select {
	case <-validator.started:
	case <-time.After(3 * time.Second):
		t.Fatal("password validation did not start")
	}
	replacement, err := service.GenerateChallenge("root@localhost")
	if err != nil {
		t.Fatal(err)
	}
	close(validator.release)
	select {
	case result := <-done:
		if result == nil || !result.Success {
			t.Fatalf("authentication failed: %+v", result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("password validation did not finish")
	}
	if !bytes.Equal(service.GetChallenge("root@localhost"), replacement) {
		t.Fatal("authentication deleted a newer challenge")
	}
}

func TestAuthServiceReturnedDataDoesNotMutateCache(t *testing.T) {
	service := NewAuthService(nil, &MockEngineAccessForTest{})
	info, err := service.GetUserInfo(context.Background(), "root", "localhost")
	if err != nil {
		t.Fatal(err)
	}
	info.GlobalPrivileges[0] = common.SelectPriv
	info.DatabasePrivileges["mysql"] = []common.PrivilegeType{common.InsertPriv}
	next, err := service.GetUserInfo(context.Background(), "root", "localhost")
	if err != nil {
		t.Fatal(err)
	}
	if next.GlobalPrivileges[0] != common.AllPriv || len(next.DatabasePrivileges["mysql"]) != 0 {
		t.Fatal("caller mutated cached user information")
	}
	challenge, err := service.GenerateChallenge("session")
	if err != nil {
		t.Fatal(err)
	}
	expected := append([]byte(nil), challenge...)
	challenge[0] ^= 0xff
	if !bytes.Equal(service.GetChallenge("session"), expected) {
		t.Fatal("generated challenge shares cache storage")
	}
	returned := service.GetChallenge("session")
	returned[0] ^= 0xff
	if !bytes.Equal(service.GetChallenge("session"), expected) {
		t.Fatal("returned challenge shares cache storage")
	}
}

func TestAuthServiceAuthenticationKeepsOtherSessionChallenges(t *testing.T) {
	service := NewAuthService(nil, &MockEngineAccessForTest{})
	for _, key := range []string{"session-a", "session-b"} {
		if _, err := service.GenerateChallenge(key); err != nil {
			t.Fatal(err)
		}
	}
	result, err := service.AuthenticateUser(context.Background(), "root", "", "localhost", "")
	if err != nil || !result.Success {
		t.Fatalf("authentication failed: %+v, %v", result, err)
	}
	remaining := 0
	for _, key := range []string{"session-a", "session-b"} {
		if len(service.GetChallenge(key)) > 0 {
			remaining++
		}
	}
	if remaining != 1 {
		t.Fatalf("one login removed challenges for other sessions: remaining=%d", remaining)
	}
}
