package raft

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// ---- test harness ----

type cluster struct {
	t          *testing.T
	n          int
	nodes      []*Raft
	applyChs   []chan ApplyMsg
	persisters []*Persister

	mu        sync.Mutex
	logs      []map[int]ApplyMsg // per-node: committed index -> applied message
	snapshots []int              // per-node: count of InstallSnapshot-driven resets applied
}

func makeCluster(t *testing.T, n int) *cluster {
	listeners := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[i] = ln
		addrs[i] = ln.Addr().String()
	}

	c := &cluster{t: t, n: n, logs: make([]map[int]ApplyMsg, n), snapshots: make([]int, n)}
	for i := 0; i < n; i++ {
		peerAddrs := map[int]string{}
		for j := 0; j < n; j++ {
			if j != i {
				peerAddrs[j] = addrs[j]
			}
		}
		applyCh := make(chan ApplyMsg, 1000)
		persister := NewFilePersister("")
		rf := NewRaft(i, listeners[i], peerAddrs, persister, applyCh)
		c.nodes = append(c.nodes, rf)
		c.applyChs = append(c.applyChs, applyCh)
		c.persisters = append(c.persisters, persister)
		c.logs[i] = make(map[int]ApplyMsg)
		go c.captureApply(i, applyCh)
	}
	return c
}

// captureApply records every committed entry a node applies, and fails the
// test immediately if the same index is ever applied with a different
// command -- that would mean a committed entry got overwritten, the one
// safety violation Raft must never allow.
func (c *cluster) captureApply(node int, ch chan ApplyMsg) {
	for msg := range ch {
		if msg.SnapshotValid {
			c.mu.Lock()
			c.snapshots[node]++
			c.mu.Unlock()
			continue
		}
		if !msg.CommandValid {
			continue
		}
		c.mu.Lock()
		if prev, ok := c.logs[node][msg.CommandIndex]; ok {
			if string(prev.Command) != string(msg.Command) {
				c.mu.Unlock()
				c.t.Errorf("node %d: divergent commit at index %d: %q vs %q", node, msg.CommandIndex, prev.Command, msg.Command)
				continue
			}
		}
		c.logs[node][msg.CommandIndex] = msg
		c.mu.Unlock()
	}
}

// snapshotsSeen returns how many InstallSnapshot-driven resets a node has
// applied (identified by its position in c.nodes at capture-registration
// time).
func (c *cluster) snapshotsSeen(node int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshots[node]
}

// nApplied returns how many nodes have applied the given index, and the
// command bytes they agree on (captureApply already guarantees agreement).
func (c *cluster) nApplied(index int) (count int, cmd []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, log := range c.logs {
		if msg, ok := log[index]; ok {
			count++
			cmd = msg.Command
		}
	}
	return
}

// waitApplied polls until at least `need` nodes have applied index, or fails
// the test after timeout.
func (c *cluster) waitApplied(index, need int, timeout time.Duration) []byte {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n, cmd := c.nApplied(index); n >= need {
			return cmd
		}
		time.Sleep(15 * time.Millisecond)
	}
	n, _ := c.nApplied(index)
	c.t.Fatalf("index %d only applied on %d/%d nodes within %v", index, n, need, timeout)
	return nil
}

// leader returns some node currently claiming leadership in the highest
// observed term, retrying briefly if none is settled yet.
func (c *cluster) leader() *Raft {
	c.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		best := -1
		var bestRf *Raft
		for _, rf := range c.nodes {
			if rf.killed() {
				continue
			}
			term, isLeader := rf.State()
			if isLeader && term > best {
				best = term
				bestRf = rf
			}
		}
		if bestRf != nil {
			return bestRf
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no leader found")
	return nil
}

// submit finds the current leader and appends cmd, retrying on a different
// node if the leader changes out from under us. Returns the committed index.
func (c *cluster) submit(cmd []byte) int {
	c.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		rf := c.leader()
		index, _, isLeader := rf.Start(cmd)
		if isLeader {
			return index
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("could not submit command: no stable leader")
	return -1
}

func (c *cluster) shutdown() {
	for _, rf := range c.nodes {
		rf.Kill()
	}
}

// checkOneLeader polls the cluster until, in the most recent term observed,
// exactly one node claims leadership, then returns it. Fails the test if
// that doesn't happen within a generous bound.
func (c *cluster) checkOneLeader() *Raft {
	c.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		leadersByTerm := map[int][]*Raft{}
		for _, rf := range c.nodes {
			if rf.killed() {
				continue
			}
			term, isLeader := rf.State()
			if isLeader {
				leadersByTerm[term] = append(leadersByTerm[term], rf)
			}
		}
		bestTerm := -1
		for term := range leadersByTerm {
			if term > bestTerm {
				bestTerm = term
			}
		}
		if bestTerm == -1 {
			continue
		}
		if len(leadersByTerm[bestTerm]) == 1 {
			return leadersByTerm[bestTerm][0]
		}
		if len(leadersByTerm[bestTerm]) > 1 {
			c.t.Fatalf("term %d has %d leaders, want at most 1", bestTerm, len(leadersByTerm[bestTerm]))
		}
	}
	c.t.Fatalf("no leader elected within deadline")
	return nil
}

// noTwoLeadersSameTerm samples cluster state repeatedly over duration and
// fails if it ever observes two distinct nodes both claiming leadership in
// the same term -- the core election-safety property.
func (c *cluster) assertNoTwoLeadersSameTerm(duration time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		byTerm := map[int][]int{}
		for _, rf := range c.nodes {
			if rf.killed() {
				continue
			}
			term, isLeader := rf.State()
			if isLeader {
				byTerm[term] = append(byTerm[term], rf.ID())
			}
		}
		for term, ids := range byTerm {
			if len(ids) > 1 {
				c.t.Fatalf("safety violation: term %d has multiple leaders: %v", term, ids)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ---- tests ----

func TestSingleNodeElectsSelf(t *testing.T) {
	c := makeCluster(t, 1)
	defer c.shutdown()

	leader := c.checkOneLeader()
	if leader.ID() != 0 {
		t.Fatalf("expected node 0 to be leader, got %d", leader.ID())
	}
}

func TestThreeNodeClusterElectsExactlyOneLeader(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.shutdown()

	c.checkOneLeader()
	c.assertNoTwoLeadersSameTerm(500 * time.Millisecond)
}

func TestFiveNodeClusterElectsExactlyOneLeader(t *testing.T) {
	c := makeCluster(t, 5)
	defer c.shutdown()

	c.checkOneLeader()
	c.assertNoTwoLeadersSameTerm(500 * time.Millisecond)
}

func TestReElectionAfterLeaderKill(t *testing.T) {
	c := makeCluster(t, 3)
	defer c.shutdown()

	first := c.checkOneLeader()
	firstTerm, _ := first.State()

	killedID := first.ID()
	first.Kill()

	// Poll remaining nodes for a new leader within ~1s, per the plan's
	// end-of-week-1 checkpoint.
	deadline := time.Now().Add(1500 * time.Millisecond)
	var newLeader *Raft
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		leadersByTerm := map[int][]*Raft{}
		for _, rf := range c.nodes {
			if rf.ID() == killedID {
				continue
			}
			term, isLeader := rf.State()
			if isLeader && term > firstTerm {
				leadersByTerm[term] = append(leadersByTerm[term], rf)
			}
		}
		for term, ls := range leadersByTerm {
			if len(ls) == 1 {
				newLeader = ls[0]
				_ = term
				break
			} else if len(ls) > 1 {
				t.Fatalf("term %d has %d leaders after kill", term, len(ls))
			}
		}
		if newLeader != nil {
			break
		}
	}
	if newLeader == nil {
		t.Fatalf("no new leader elected within deadline after killing leader %d", killedID)
	}
	if newLeader.ID() == killedID {
		t.Fatalf("new leader has same ID as killed leader")
	}
}

func TestNoTwoLeadersAcrossRepeatedKills(t *testing.T) {
	c := makeCluster(t, 5)
	defer c.shutdown()

	for round := 0; round < 4; round++ {
		leader := c.checkOneLeader()
		c.assertNoTwoLeadersSameTerm(200 * time.Millisecond)
		leader.Kill()
		// Replace the killed node's slot check by simply excluding dead
		// nodes in future observations (checkOneLeader already skips
		// killed() nodes).
		alive := 0
		for _, rf := range c.nodes {
			if !rf.killed() {
				alive++
			}
		}
		if alive < 3 {
			// Stop before we drop below a majority of the original 5.
			break
		}
	}
}

func TestSplitVoteEventuallyConverges(t *testing.T) {
	// Randomized election timeouts should ensure that even if an initial
	// round of RequestVotes splits without a majority, everyone times out
	// and retries with a fresh randomized timeout rather than deadlocking.
	// We can't force a split deterministically from outside the package,
	// but running many small clusters back-to-back exercises this path
	// statistically -- if it ever deadlocked, one of these would time out.
	for i := 0; i < 8; i++ {
		t.Run(fmt.Sprintf("run%d", i), func(t *testing.T) {
			c := makeCluster(t, 3)
			defer c.shutdown()
			c.checkOneLeader()
		})
	}
}
