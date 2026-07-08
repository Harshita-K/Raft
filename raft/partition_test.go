package raft

import (
	"testing"
	"time"
)

// partitionInto splits the cluster into two groups: each node in group A is
// blocked from every node in group B and vice versa; nodes within the same
// group can still talk to each other.
func (c *cluster) partitionInto(a, b []int) {
	inA := map[int]bool{}
	for _, id := range a {
		inA[id] = true
	}
	for _, rf := range c.nodes {
		blocked := map[int]bool{}
		for _, other := range c.nodes {
			if other.ID() == rf.ID() {
				continue
			}
			sameSide := inA[rf.ID()] == inA[other.ID()]
			if !sameSide {
				blocked[other.ID()] = true
			}
		}
		rf.SetPartitioned(blocked)
	}
}

func (c *cluster) heal() {
	for _, rf := range c.nodes {
		rf.SetPartitioned(map[int]bool{})
	}
}

// TestSplitBrainMinorityCannotCommit is the classic scenario from the plan:
// partition 1 leader + 1 follower away from the other 2 nodes in a 4-node
// cluster (so 2-vs-2 is avoided and the "majority side" is unambiguous).
// Actually per the plan's exact wording we use a 5-node cluster split
// 2-vs-3, which mirrors "1 leader + 1 follower" isolated from a
// still-majority remainder.
func TestSplitBrainMinorityCannotCommit(t *testing.T) {
	c := makeCluster(t, 5)
	defer c.shutdown()
	leader := c.leader()

	// Build the minority side out of the current leader plus one follower;
	// the other 3 form the majority side.
	minority := []int{leader.ID()}
	majority := []int{}
	for _, rf := range c.nodes {
		if rf.ID() == leader.ID() {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, rf.ID())
		} else {
			majority = append(majority, rf.ID())
		}
	}

	c.partitionInto(minority, majority)

	// Minority side (still has the old leader, who doesn't yet know it's
	// partitioned): writes submitted to it must NOT commit -- no quorum.
	var minorityRf *Raft
	for _, rf := range c.nodes {
		if rf.ID() == leader.ID() {
			minorityRf = rf
		}
	}
	idx, _, isLeader := minorityRf.Start([]byte("stuck"))
	if isLeader {
		time.Sleep(600 * time.Millisecond)
		// Only OUR "stuck" write landing anywhere would prove a
		// no-quorum commit; the same index legitimately ending up holding
		// some other entry (e.g. the majority side's new-leader no-op) is
		// unrelated and not a safety violation.
		if n, cmd := c.nApplied(idx); n > 0 && string(cmd) == "stuck" {
			t.Fatalf("minority side committed a write without quorum (applied on %d nodes)", n)
		}
	}

	// Majority side: must elect its own new leader and keep operating.
	deadline := time.Now().Add(3 * time.Second)
	var majorityLeader *Raft
	for time.Now().Before(deadline) {
		for _, id := range majority {
			for _, rf := range c.nodes {
				if rf.ID() != id {
					continue
				}
				if _, isLeader := rf.State(); isLeader {
					majorityLeader = rf
				}
			}
		}
		if majorityLeader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if majorityLeader == nil {
		t.Fatalf("majority side never elected a new leader")
	}

	widx, _, isLeader := majorityLeader.Start([]byte("progress"))
	if !isLeader {
		t.Fatalf("majority leader lost leadership unexpectedly")
	}
	c.waitApplied(widx, 3, 2*time.Second) // majority of the ORIGINAL 5 is 3

	// Heal: the old leader must step down (see higher term) and catch up
	// without divergence.
	c.heal()

	deadline = time.Now().Add(3 * time.Second)
	healed := false
	for time.Now().Before(deadline) {
		if n, _ := c.nApplied(widx); n == 5 {
			healed = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !healed {
		n, _ := c.nApplied(widx)
		t.Fatalf("cluster did not converge after healing partition: only %d/5 nodes caught up", n)
	}

	// The old leader should no longer believe it's leader of the current term.
	oldLeaderStillLeader := false
	maxTerm := 0
	for _, rf := range c.nodes {
		if term, _ := rf.State(); term > maxTerm {
			maxTerm = term
		}
	}
	if term, isLeader := minorityRf.State(); isLeader && term == maxTerm {
		oldLeaderStillLeader = true
	}
	if oldLeaderStillLeader {
		t.Fatalf("old (minority-side) leader did not step down after healing")
	}
}
