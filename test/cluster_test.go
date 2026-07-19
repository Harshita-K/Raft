package integration

import (
	"fmt"
	"testing"
	"time"
)

// TestClusterPutGetThroughClient covers the Week 2 checkpoint: PUT and GET
// through the client library against a real 3-node cluster over HTTP.
func TestClusterPutGetThroughClient(t *testing.T) {
	tc := startCluster(t, 3, "")
	defer tc.shutdown()

	if err := tc.client.Put("foo", "bar"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, ok, err := tc.client.Get("foo")
	if err != nil || !ok || v != "bar" {
		t.Fatalf("Get: v=%q ok=%v err=%v", v, ok, err)
	}

	if err := tc.client.Put("foo", "baz"); err != nil {
		t.Fatalf("Put overwrite: %v", err)
	}
	v, ok, err = tc.client.Get("foo")
	if err != nil || !ok || v != "baz" {
		t.Fatalf("Get after overwrite: v=%q ok=%v err=%v", v, ok, err)
	}

	if err := tc.client.Delete("foo"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, ok, err = tc.client.Get("foo")
	if err != nil || ok {
		t.Fatalf("expected key gone, ok=%v err=%v", ok, err)
	}
}

// TestClusterWriteReplicatesToMajority verifies a PUT is actually
// replicated (not just accepted by the leader) to a majority of nodes.
func TestClusterWriteReplicatesToMajority(t *testing.T) {
	tc := startCluster(t, 3, "")
	defer tc.shutdown()

	if err := tc.client.Put("k", "v"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	n := tc.waitReplicas("k", "v", 3, 2*time.Second)
	if n != 3 {
		t.Fatalf("expected all 3 replicas to have k=v, got %d", n)
	}
}

// TestClusterSurvivesAnyNodeKilled kills each node in turn (fresh cluster
// each time) and verifies the cluster still serves PUT/GET afterwards.
func TestClusterSurvivesAnyNodeKilled(t *testing.T) {
	for victim := 0; victim < 3; victim++ {
		t.Run(fmt.Sprintf("kill-node-%d", victim), func(t *testing.T) {
			tc := startCluster(t, 3, "")
			defer tc.shutdown()

			if err := tc.client.Put("warmup", "1"); err != nil {
				t.Fatalf("warmup put: %v", err)
			}
			tc.waitReplicas("warmup", "1", 3, 2*time.Second)

			tc.nodes[victim].Kill()
			tc.httpSrvs[victim].Close()

			if err := tc.client.Put("after-kill", "2"); err != nil {
				t.Fatalf("put after killing node %d: %v", victim, err)
			}
			n := tc.waitReplicas("after-kill", "2", 2, 2*time.Second) // majority w/o victim
			if n < 2 {
				t.Fatalf("expected at least 2 surviving replicas, got %d", n)
			}
			v, ok, err := tc.client.Get("after-kill")
			if err != nil || !ok || v != "2" {
				t.Fatalf("Get after kill: v=%q ok=%v err=%v", v, ok, err)
			}
		})
	}
}

// TestClusterLeaderKillMidWriteNoPartialCommit mirrors the raft-level test
// but drives everything through the real client/HTTP/Node stack: kill the
// leader process right after handing it a write, and confirm the write
// either ends up on a majority or nowhere -- never partially.
func TestClusterLeaderKillMidWriteNoPartialCommit(t *testing.T) {
	for trial := 0; trial < 5; trial++ {
		t.Run(fmt.Sprintf("trial%d", trial), func(t *testing.T) {
			tc := startCluster(t, 3, "")
			defer tc.shutdown()

			leaderIdx := -1
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) && leaderIdx == -1 {
				for i, n := range tc.nodes {
					if _, isLeader := n.State(); isLeader {
						leaderIdx = i
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
			if leaderIdx == -1 {
				t.Fatalf("no leader elected")
			}

			key := fmt.Sprintf("trial-%d", trial)
			done := make(chan error, 1)
			go func() { done <- tc.nodes[leaderIdx].Put(key, "value", time.Second) }()
			time.Sleep(2 * time.Millisecond) // let Start() land before we kill
			tc.nodes[leaderIdx].Kill()
			tc.httpSrvs[leaderIdx].Close()
			<-done // don't care whether it errored -- only what actually got applied matters

			time.Sleep(500 * time.Millisecond)
			n := tc.countReplicas(key, "value")
			if n == 1 {
				t.Fatalf("partial commit: key applied on exactly 1/3 nodes")
			}
			// n == 0 (never committed) or n >= 2 (committed to majority) are both fine.
		})
	}
}

// TestClusterRestartAllNodesPreservesData kills and restarts every node
// from disk and verifies previously-committed data survives, per the plan's
// "kill a node process entirely, restart it, verify it rejoins" checkpoint
// -- applied to the whole cluster.
func TestClusterRestartAllNodesPreservesData(t *testing.T) {
	dir := t.TempDir()
	tc := startCluster(t, 3, dir)
	defer tc.shutdown()

	if err := tc.client.Put("durable", "yes"); err != nil {
		t.Fatalf("put: %v", err)
	}
	tc.waitReplicas("durable", "yes", 3, 2*time.Second)

	for i := 0; i < 3; i++ {
		tc.restart(i)
	}

	// After all three restart (from disk, same addresses), the cluster
	// should re-elect a leader and still be able to read the old value once
	// it re-establishes commitment (via a fresh write in the current term,
	// see the raft-level persistence test for why that's required).
	deadline := time.Now().Add(3 * time.Second)
	ok := false
	for time.Now().Before(deadline) {
		if err := tc.client.Put("nudge", "1"); err == nil {
			ok = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("cluster never became writable again after full restart")
	}

	v, found, err := tc.client.Get("durable")
	if err != nil || !found || v != "yes" {
		t.Fatalf("durable data lost after restart: v=%q found=%v err=%v", v, found, err)
	}
}
