package main

import (
	"fmt"
	"testing"
	"time"

	"raftkv/client"
)

// BenchmarkPut / BenchmarkGet are plain Go benchmarks for single-client
// sequential latency (run with `go test -bench=. -benchtime=30s ./bench/...`).
// For concurrency-scaling and multi-node fault scenarios, use
// `go run ./bench` instead -- see bench.go.

func BenchmarkPut(b *testing.B) {
	c := startBenchCluster(3, "")
	defer c.shutdown()
	if li := c.waitForLeader(3 * time.Second); li == -1 {
		b.Fatal("no leader elected")
	}
	cl := client.New(c.httpAddrs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cl.Put(fmt.Sprintf("k%d", i%1000), "v"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	c := startBenchCluster(3, "")
	defer c.shutdown()
	if li := c.waitForLeader(3 * time.Second); li == -1 {
		b.Fatal("no leader elected")
	}
	cl := client.New(c.httpAddrs)
	if err := cl.Put("k", "v"); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := cl.Get("k"); err != nil {
			b.Fatal(err)
		}
	}
}
