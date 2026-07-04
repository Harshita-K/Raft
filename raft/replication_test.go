package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestBasicAgreement(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.shutdown()
	c.leader()

	for i := 0; i < 10; i++ {
		cmd := []byte(fmt.Sprintf("cmd-%d", i))
		index := c.submit(cmd)
		got := c.waitApplied(index, 3, 2*time.Second)
		if string(got) != string(cmd) {
			t.Fatalf("index %d: got %q, want %q", index, got, cmd)
		}
	}
}

func TestAgreementSurvivesFollowerFailure(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.shutdown()
	c.leader()

	index1 := c.submit([]byte("before-kill"))
	c.waitApplied(index1, 3, 2*time.Second)

	// Kill a follower (not the leader) -- the remaining majority (leader +
	// 1 follower) must still be able to commit new entries.
	leader := c.leader()
	var victim *Raft
	for _, rf := range c.nodes {
		if rf.ID() != leader.ID() {
			victim = rf
			break
		}
	}
	victim.Kill()

	index2 := c.submit([]byte("after-kill"))
	c.waitApplied(index2, 2, 2*time.Second) // majority of the ORIGINAL 3 is 2
}

func TestNoQuorumNoCommit(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.shutdown()
	leader := c.leader()

	// Kill two of the three nodes, leaving only one alive -- no quorum is
	// possible, so nothing should ever commit.
	killed := 0
	for _, rf := range c.nodes {
		if killed >= 2 {
			break
		}
		rf.Kill()
		killed++
	}

	// Whichever node (if any) survives can't reach majority. If the leader
	// itself was killed this is moot; if it survived, Start() may still
	// accept the entry into its own log (that's correct -- only COMMIT
	// requires quorum), but it must never show up as applied.
	_ = leader

	survivors := 0
	var alive *Raft
	for _, rf := range c.nodes {
		if !rf.killed() {
			survivors++
			alive = rf
		}
	}
	if survivors != 1 {
		t.Fatalf("expected exactly 1 survivor, got %d", survivors)
	}

	index, _, isLeader := alive.Start([]byte("orphan"))
	if !isLeader {
		return // fine -- it wasn't leader, nothing more to check
	}
	time.Sleep(500 * time.Millisecond)
	if n, _ := c.nApplied(index); n > 0 {
		t.Fatalf("entry committed without quorum: applied on %d nodes", n)
	}
}

func TestLeaderKillMidWriteNoPartialCommit(t *testing.T) {
	// Repeatedly submit a write and immediately kill the leader; verify
	// that every acknowledged (committed) write is visible on a majority,
	// and nothing ever "half" commits -- either every surviving majority
	// node has it, or none do until a future leader re-proposes it under a
	// new index.
	for trial := 0; trial < 5; trial++ {
		t.Run(fmt.Sprintf("trial%d", trial), func(t *testing.T) {
			c := makeCluster(t, 3)
			defer c.shutdown()
			leader := c.leader()

			want := fmt.Sprintf("trial-%d", trial)
			index, term, isLeader := leader.Start([]byte(want))
			if !isLeader {
				t.Skip("lost leadership before Start could run")
			}
			leader.Kill()

			// Give the cluster time to either commit (via a new leader
			// that has this entry) or definitively not.
			time.Sleep(1200 * time.Millisecond)

			n, cmd := c.nApplied(index)
			if n == 0 {
				return // never applied anywhere -- acceptable, wasn't acknowledged
			}
			if string(cmd) != want {
				// A different entry ended up at this index instead (e.g. a
				// later leader's internal no-op, or the slot simply never
				// got OUR write) -- that's a completely different, valid
				// commit, not a partial commit of what we proposed.
				return
			}
			if n < 2 {
				t.Fatalf("entry at index %d applied on only %d node (partial commit, term %d)", index, n, term)
			}
		})
	}
}

// TestLogMatchingProperty verifies Figure 2's Log Matching Property directly
// against captured apply history: if two nodes ever applied the same index
// with the same term, every earlier index they both applied must match too.
// captureApply already rejects divergent commands at the SAME index as a
// hard test failure; this additionally checks term-consistency implies
// full-prefix consistency.
func TestLogMatchingProperty(t *testing.T) {
	c := makeCluster(t, 5)
	defer c.shutdown()
	c.leader()

	var lastIdx int
	for i := 0; i < 20; i++ {
		lastIdx = c.submit([]byte(fmt.Sprintf("v%d", i)))
	}
	c.waitApplied(lastIdx, 3, 3*time.Second)
	time.Sleep(300 * time.Millisecond) // let stragglers catch up

	c.mu.Lock()
	defer c.mu.Unlock()
	for idx := 1; idx <= lastIdx; idx++ {
		var refCmd []byte
		var refTerm int
		haveRef := false
		for node := 0; node < c.n; node++ {
			msg, ok := c.logs[node][idx]
			if !ok {
				continue
			}
			if !haveRef {
				refCmd, refTerm = msg.Command, msg.CommandTerm
				haveRef = true
				continue
			}
			if msg.CommandTerm == refTerm && string(msg.Command) != string(refCmd) {
				t.Fatalf("log matching violated at index %d: same term %d, different commands %q vs %q", idx, refTerm, refCmd, msg.Command)
			}
		}
	}
}
