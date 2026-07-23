package main

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/client"
)

type loadResult struct {
	Concurrency      int           `json:"concurrency"`
	Op               string        `json:"op"`
	Duration         time.Duration `json:"duration_ns"`
	Ops              int64         `json:"ops"`
	Errors           int64         `json:"errors"`
	ThroughputPerSec float64       `json:"throughput_per_sec"`
	P50              time.Duration `json:"p50_ns"`
	P95              time.Duration `json:"p95_ns"`
	P99              time.Duration `json:"p99_ns"`
}

// runLoad drives `concurrency` client goroutines against addrs for
// `duration`, performing op ("write", "read", or "mixed" -- 10% writes)
// against a rotating keyspace, and reports throughput + latency percentiles.
func runLoad(addrs []string, concurrency int, duration time.Duration, op string, keyspace int) loadResult {
	return runLoadWithClient(addrs, concurrency, duration, op, keyspace, false)
}

// runLoadWithClient is runLoad with control over whether the load
// generator's clients are allowed to follow leader-hint redirects outside
// addrs. Benchmarks simulating a network partition need restricted=true on
// the minority side -- otherwise a redirect to the (HTTP-reachable, since
// only raft RPC traffic is actually blocked) majority leader would let
// requests "escape" the simulated partition. See client.NewRestricted.
func runLoadWithClient(addrs []string, concurrency int, duration time.Duration, op string, keyspace int, restricted bool) loadResult {
	var successes, errs int64
	var mu sync.Mutex
	var latencies []time.Duration

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var c *client.Client
			if restricted {
				c = client.NewRestricted(addrs)
			} else {
				c = client.New(addrs)
			}
			counter := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("bench-%d", counter%keyspace)
				var err error
				start := time.Now()
				switch op {
				case "write":
					err = c.Put(key, fmt.Sprintf("v-%d-%d", worker, counter))
				case "read":
					_, _, err = c.Get(key)
				case "mixed":
					if counter%10 == 0 {
						err = c.Put(key, fmt.Sprintf("v-%d-%d", worker, counter))
					} else {
						_, _, err = c.Get(key)
					}
				default:
					panic("unknown op: " + op)
				}
				elapsed := time.Since(start)
				counter++
				if err != nil {
					atomic.AddInt64(&errs, 1)
					continue
				}
				atomic.AddInt64(&successes, 1)
				mu.Lock()
				latencies = append(latencies, elapsed)
				mu.Unlock()
			}
		}(w)
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		idx := int(p * float64(len(latencies)))
		if idx >= len(latencies) {
			idx = len(latencies) - 1
		}
		return latencies[idx]
	}

	return loadResult{
		Concurrency:      concurrency,
		Op:               op,
		Duration:         duration,
		Ops:              atomic.LoadInt64(&successes),
		Errors:           atomic.LoadInt64(&errs),
		ThroughputPerSec: float64(atomic.LoadInt64(&successes)) / duration.Seconds(),
		P50:              pct(0.50),
		P95:              pct(0.95),
		P99:              pct(0.99),
	}
}
