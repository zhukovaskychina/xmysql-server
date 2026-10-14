package net

import (
	"net"
	"sync"
	"testing"
	"time"
)

type acceptNotifyListener struct {
	net.Listener
	entered chan struct{}
	once    sync.Once
}

func (l *acceptNotifyListener) Accept() (net.Conn, error) {
	l.once.Do(func() { close(l.entered) })
	return l.Listener.Accept()
}

func TestTCPServerCloseUnblocksAccept(t *testing.T) {
	srv := NewTCPServer(WithLocalAddress("127.0.0.1:0")).(*serverimpl)
	if err := srv.listen(); err != nil {
		t.Fatal(err)
	}
	listener := &acceptNotifyListener{Listener: srv.streamListener, entered: make(chan struct{})}
	srv.streamListener = listener
	t.Cleanup(func() { _ = listener.Close() })
	srv.runTcpEventLoop(func(Session) error { t.Error("unexpected connection"); return nil })
	select {
	case <-listener.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("accept loop did not start")
	}
	closed := make(chan struct{})
	go func() { srv.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		_ = listener.Close() // Release the old implementation's blocked goroutine.
		<-closed
		t.Fatal("Close blocked with no incoming TCP connections")
	}
	if !srv.IsClosed() {
		t.Fatal("server was not marked closed")
	}
	rebound, err := net.Listen("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("listening address was not released: %v", err)
	}
	_ = rebound.Close()
	var closers sync.WaitGroup
	for i := 0; i < 8; i++ {
		closers.Add(1)
		go func() { defer closers.Done(); srv.Close() }()
	}
	closers.Wait()
}

func TestTCPServerAcceptLoopExitsWhenListenerCloses(t *testing.T) {
	srv := NewTCPServer(WithLocalAddress("127.0.0.1:0")).(*serverimpl)
	if err := srv.listen(); err != nil {
		t.Fatal(err)
	}
	listener := &acceptNotifyListener{Listener: srv.streamListener, entered: make(chan struct{})}
	srv.streamListener = listener
	srv.runTcpEventLoop(func(Session) error { return nil })
	<-listener.entered
	_ = listener.Close()
	finished := make(chan struct{})
	go func() { srv.wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(time.Second):
		srv.Close()
		<-finished
		t.Fatal("accept loop did not exit after listener was closed")
	}
	srv.Close()
}
