package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

type connectionPair struct {
	client net.Conn
	target net.Conn
}

func main() {
	listenAddr := flag.String("listen", "127.0.0.1:3323", "proxy listen address")
	targetAddr := flag.String("target", "127.0.0.1:3322", "server target address")
	triggerPath := flag.String("trigger", "", "file whose creation triggers a one-time fault")
	readyPath := flag.String("ready", "", "file written after active connections are closed")
	partitionPath := flag.String("partition", "", "file whose creation starts a persistent network partition")
	partitionReadyPath := flag.String("partition-ready", "", "file written after a persistent partition starts")
	healPath := flag.String("heal", "", "file whose creation heals a persistent network partition")
	healReadyPath := flag.String("heal-ready", "", "file written after a persistent partition heals")
	flag.Parse()
	if *triggerPath == "" || *readyPath == "" {
		fmt.Fprintln(os.Stderr, "--trigger and --ready are required")
		os.Exit(2)
	}
	if (*partitionPath == "") != (*partitionReadyPath == "") || (*healPath == "") != (*healReadyPath == "") {
		fmt.Fprintln(os.Stderr, "--partition/--partition-ready and --heal/--heal-ready must be supplied in pairs")
		os.Exit(2)
	}
	if (*partitionPath != "") != (*healPath != "") {
		fmt.Fprintln(os.Stderr, "--partition and --heal must be supplied together")
		os.Exit(2)
	}

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer listener.Close()

	var mu sync.Mutex
	active := make(map[*connectionPair]struct{})
	var faultOnce sync.Once
	partitioned := false
	closeActive := func() {
		mu.Lock()
		defer mu.Unlock()
		for pair := range active {
			_ = pair.client.Close()
			_ = pair.target.Close()
		}
	}
	go func() {
		for {
			if _, err := os.Stat(*triggerPath); err == nil {
				faultOnce.Do(func() {
					closeActive()
					_ = os.WriteFile(*readyPath, []byte("ready\n"), 0644)
				})
			}
			if *partitionPath != "" {
				if _, err := os.Stat(*partitionPath); err == nil {
					mu.Lock()
					starting := !partitioned
					partitioned = true
					mu.Unlock()
					if starting {
						closeActive()
						_ = os.WriteFile(*partitionReadyPath, []byte("partitioned\n"), 0644)
					}
				}
				if _, err := os.Stat(*healPath); err == nil {
					mu.Lock()
					healing := partitioned
					partitioned = false
					mu.Unlock()
					if healing {
						_ = os.WriteFile(*healReadyPath, []byte("healed\n"), 0644)
					}
				}
			}
			time.Sleep(25 * time.Millisecond)
		}
	}()

	for {
		client, err := listener.Accept()
		if err != nil {
			return
		}
		mu.Lock()
		blocked := partitioned
		mu.Unlock()
		if blocked {
			_ = client.Close()
			continue
		}
		target, err := net.Dial("tcp", *targetAddr)
		if err != nil {
			_ = client.Close()
			continue
		}
		pair := &connectionPair{client: client, target: target}
		mu.Lock()
		active[pair] = struct{}{}
		mu.Unlock()
		go func() {
			defer func() {
				_ = client.Close()
				_ = target.Close()
				mu.Lock()
				delete(active, pair)
				mu.Unlock()
			}()
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); _, _ = io.Copy(target, client) }()
			go func() { defer wg.Done(); _, _ = io.Copy(client, target) }()
			wg.Wait()
		}()
	}
}
