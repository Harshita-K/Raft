package main

import (
	"fmt"
	"sync"
	"time"

	"raftkv/client"
)

// runFailoverScenario kills the current leader under sustained write load
// and measures wall-clock time from the kill to the first write that
// succeeds again -- the "downtime" headline number.
func runFailoverScenario(c *cluster) (time.Duration, error) {
	addrs := append([]string(nil), c.httpAddrs...)
	stop := make(chan struct{})
	var wg sync.WaitGroup

	var mu sync.Mutex
	var killedAt, recoveredAt time.Time

	wg.Add(1)
	go func() {
		defer wg.Done()
		cl := client.New(addrs)
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			startedAt := time.Now()
			err := cl.Put(fmt.Sprintf("failover-%d", i%50), "x")
			completedAt := time.Now()
			i++
			if err == nil {
				mu.Lock()
				// Only count requests that STARTED after the kill --
				// otherwise a write already in flight at kill-time can
				// complete microseconds later and be mistaken for a fast
				// recovery, when really nothing had to fail over yet.
				if !killedAt.IsZero() && recoveredAt.IsZero() && startedAt.After(killedAt) {
					recoveredAt = completedAt
				}
				mu.Unlock()
			}
		}
	}()

	time.Sleep(500 * time.Millisecond) // let a leader settle under load
	leaderIdx := c.leaderIndex()
	if leaderIdx == -1 {
		close(stop)
		wg.Wait()
		return 0, fmt.Errorf("no leader before kill")
	}

	mu.Lock()
	killedAt = time.Now()
	mu.Unlock()
	c.nodes[leaderIdx].Kill()
	c.httpSrvs[leaderIdx].Close()

	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		done := !recoveredAt.IsZero()
		mu.Unlock()
		if done || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if recoveredAt.IsZero() {
		return 0, fmt.Errorf("cluster did not recover within timeout")
	}
	return recoveredAt.Sub(killedAt), nil
}

type partitionScenarioResult struct {
	MinorityThroughput float64
	MajorityThroughput float64
	HealRecoveryTime   time.Duration
}

// runPartitionScenario isolates a minority of nodes from the rest for
// `duration`, drives concurrent write load against each side separately
// (using clients that only know about that side's addresses, so they can
// never "cheat" and find the other side's leader), then heals the
// partition and measures time until the whole cluster is writable again.
func runPartitionScenario(c *cluster, duration time.Duration) partitionScenarioResult {
	minoritySize := c.n/3 + 1
	if minoritySize >= c.n {
		minoritySize = 1
	}
	minorityIdx := make([]int, minoritySize)
	for i := range minorityIdx {
		minorityIdx[i] = i
	}
	majorityIdx := make([]int, 0, c.n-minoritySize)
	for i := minoritySize; i < c.n; i++ {
		majorityIdx = append(majorityIdx, i)
	}

	inMinority := map[int]bool{}
	for _, id := range minorityIdx {
		inMinority[id] = true
	}
	for i, n := range c.nodes {
		blocked := map[int]bool{}
		for j := range c.nodes {
			if j == i {
				continue
			}
			if inMinority[i] != inMinority[j] {
				blocked[j] = true
			}
		}
		n.SetPartitioned(blocked)
	}

	addrsFor := func(idx []int) []string {
		out := make([]string, len(idx))
		for i, id := range idx {
			out[i] = c.httpAddrs[id]
		}
		return out
	}

	// restricted=true on both sides: SetPartitioned only blocks raft RPC
	// traffic between nodes, not arbitrary HTTP connections from these
	// benchmark clients, so without this a "minority" client would happily
	// follow a leader-hint redirect straight to the (HTTP-reachable)
	// majority leader and silently defeat the whole point of the scenario.
	var minorityResult, majorityResult loadResult
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		minorityResult = runLoadWithClient(addrsFor(minorityIdx), 10, duration, "write", 50, true)
	}()
	go func() {
		defer wg.Done()
		majorityResult = runLoadWithClient(addrsFor(majorityIdx), 10, duration, "write", 50, true)
	}()
	wg.Wait()

	healStart := time.Now()
	c.heal()

	cl := client.New(append([]string(nil), c.httpAddrs...))
	deadline := time.Now().Add(5 * time.Second)
	recovery := time.Duration(-1)
	for time.Now().Before(deadline) {
		if err := cl.Put("heal-check", "1"); err == nil {
			recovery = time.Since(healStart)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	return partitionScenarioResult{
		MinorityThroughput: minorityResult.ThroughputPerSec,
		MajorityThroughput: majorityResult.ThroughputPerSec,
		HealRecoveryTime:   recovery,
	}
}

// runSnapshotCatchupScenario isolates one node, writes nWrites keys to the
// rest of the cluster (enough to trigger log compaction when snapshotting
// is enabled), then reconnects the isolated node and times how long it
// takes to catch up -- with vs without snapshotting enabled, this shows
// whether InstallSnapshot is actually saving time over a full log replay.
func runSnapshotCatchupScenario(snapshotThreshold, nWrites int) (time.Duration, error) {
	c := startBenchClusterWithThreshold(5, "", snapshotThreshold)
	defer c.shutdown()

	leaderIdx := c.waitForLeader(3 * time.Second)
	if leaderIdx == -1 {
		return 0, fmt.Errorf("no leader elected")
	}

	// Pick a victim that is NOT the current leader -- isolating the leader
	// itself would force an extra, unplanned failover into the middle of
	// this scenario instead of a clean "one follower falls behind" case.
	victim := c.n - 1
	if victim == leaderIdx {
		victim = 0
	}
	c.isolate(victim)

	cl := client.New(c.addrsExcluding(victim))
	lastKey := fmt.Sprintf("k%d", nWrites-1)
	lastVal := fmt.Sprintf("v%d", nWrites-1)
	for i := 0; i < nWrites; i++ {
		if err := cl.Put(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
			return 0, fmt.Errorf("write %d failed: %w", i, err)
		}
	}

	start := time.Now()
	c.heal()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := c.nodes[victim].LocalGet(lastKey); ok && v == lastVal {
			return time.Since(start), nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return 0, fmt.Errorf("catch-up timed out")
}
