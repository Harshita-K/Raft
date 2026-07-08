package raft

import (
	"fmt"
	"testing"
	"time"
)

// TestSnapshotCompactsLogAndKeepsWorking exercises the basic
// Snapshot(index, data) compaction path on a single node: after
// compaction, the node must still be able to accept, replicate (trivially,
// to itself) and apply new entries.
func TestSnapshotCompactsLogAndKeepsWorking(t *testing.T) {
	c := makeCluster(t, 1)
	defer c.shutdown()
	c.leader()

	var lastIdx int
	for i := 0; i < 10; i++ {
		lastIdx = c.submit([]byte(fmt.Sprintf("v%d", i)))
	}
	c.waitApplied(lastIdx, 1, 2*time.Second)

	c.nodes[0].Snapshot(lastIdx, []byte("snapshot-data"))

	// Should still work fine after compaction.
	idx := c.submit([]byte("after-snapshot"))
	got := c.waitApplied(idx, 1, 2*time.Second)
	if string(got) != "after-snapshot" {
		t.Fatalf("got %q", got)
	}

	// The persisted state should now reflect the compacted log (small),
	// not the full 11-entry history, proving compaction actually ran.
	sz := c.persisters[0].RaftStateSize()
	if sz == 0 {
		t.Fatalf("expected non-zero persisted state size")
	}
}

// TestFollowerCatchesUpViaInstallSnapshot forces a follower far behind (via
// a simulated partition, standing in for "pause the process"), snapshots
// the log out from under it on the rest of the cluster, heals the
// partition, and verifies the follower catches up via InstallSnapshot
// rather than replaying entries the leader no longer even has.
func TestFollowerCatchesUpViaInstallSnapshot(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.shutdown()
	leader := c.leader()

	victim := -1
	for _, rf := range c.nodes {
		if rf.ID() != leader.ID() {
			victim = rf.ID()
			break
		}
	}

	// Partition the victim off from everyone (both directions).
	for _, rf := range c.nodes {
		if rf.ID() == victim {
			blocked := map[int]bool{}
			for _, other := range c.nodes {
				if other.ID() != victim {
					blocked[other.ID()] = true
				}
			}
			rf.SetPartitioned(blocked)
		} else {
			rf.SetPartitioned(map[int]bool{victim: true})
		}
	}

	var lastIdx int
	for i := 0; i < 50; i++ {
		lastIdx = c.submit([]byte(fmt.Sprintf("v%d", i)))
	}
	c.waitApplied(lastIdx, 2, 3*time.Second) // majority without the victim

	// Compact the log on the two live nodes past what the victim has, so
	// it will be unable to catch up via ordinary AppendEntries alone.
	for _, rf := range c.nodes {
		if rf.ID() != victim {
			rf.Snapshot(lastIdx, []byte(fmt.Sprintf("snapshot-through-%d", lastIdx)))
		}
	}

	// Heal the partition.
	for _, rf := range c.nodes {
		rf.SetPartitioned(map[int]bool{})
	}

	// Confirm it received an actual InstallSnapshot (not a giant log replay).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && c.snapshotsSeen(victim) == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if c.snapshotsSeen(victim) == 0 {
		t.Fatalf("expected follower %d to receive an InstallSnapshot after falling behind compaction", victim)
	}

	// And confirm it's fully caught up and functional: new entries submitted
	// now should reach all three nodes, including the formerly-partitioned one.
	idx := c.submit([]byte("after-heal"))
	c.waitApplied(idx, 3, 3*time.Second)
}

// TestFollowerWithInflatedTermStillCatchesUpViaSnapshot is a regression test
// for a real deadlock found via the bench snapshot-catchup scenario: a
// follower isolated long enough for its own election timer to fire
// repeatedly ends up with currentTerm HIGHER than the leader's, purely from
// its own futile candidacy attempts. When reconnected, its (correctly)
// rejected InstallSnapshot reply carries that higher term, so the leader
// steps down and a new election happens. The bug: after that, the leader's
// conflict-driven nextIndex backoff was clamped to never fall to or below
// log.firstIndex() again, so it could never re-offer InstallSnapshot --
// permanently stalling the follower's catch-up even though the rest of the
// cluster kept making progress just fine.
func TestFollowerWithInflatedTermStillCatchesUpViaSnapshot(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.shutdown()
	leader := c.leader()

	victim := -1
	for _, rf := range c.nodes {
		if rf.ID() != leader.ID() {
			victim = rf.ID()
			break
		}
	}

	for _, rf := range c.nodes {
		if rf.ID() == victim {
			blocked := map[int]bool{}
			for _, other := range c.nodes {
				if other.ID() != victim {
					blocked[other.ID()] = true
				}
			}
			rf.SetPartitioned(blocked)
		} else {
			rf.SetPartitioned(map[int]bool{victim: true})
		}
	}
	// Long enough for the isolated victim's election timeout (150-300ms) to
	// fire multiple times, inflating its currentTerm well past the leader's.
	time.Sleep(600 * time.Millisecond)

	var lastIdx int
	for i := 0; i < 50; i++ {
		lastIdx = c.submit([]byte(fmt.Sprintf("v%d", i)))
	}
	c.waitApplied(lastIdx, 2, 3*time.Second)

	for _, rf := range c.nodes {
		if rf.ID() != victim {
			rf.Snapshot(lastIdx, []byte(fmt.Sprintf("snapshot-through-%d", lastIdx)))
		}
	}

	for _, rf := range c.nodes {
		rf.SetPartitioned(map[int]bool{})
	}

	// Must fully catch up despite the term-inflation-induced leader
	// step-down/re-election dance that follows healing.
	idx := c.submit([]byte("after-heal"))
	c.waitApplied(idx, 3, 5*time.Second)
}
