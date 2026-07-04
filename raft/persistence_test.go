package raft

import (
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// simpleApply collects committed entries from a single node into a
// thread-safe slice, for tests that don't need the full multi-node cluster
// harness.
type simpleApply struct {
	mu      sync.Mutex
	applied map[int][]byte
}

func newSimpleApply() *simpleApply { return &simpleApply{applied: map[int][]byte{}} }

func (s *simpleApply) run(ch chan ApplyMsg) {
	for msg := range ch {
		if msg.CommandValid {
			s.mu.Lock()
			s.applied[msg.CommandIndex] = msg.Command
			s.mu.Unlock()
		}
	}
}

func (s *simpleApply) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.applied)
}

func (s *simpleApply) get(index int) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.applied[index]
	return v, ok
}

// TestPersistenceSingleNodeRestart verifies that currentTerm/votedFor/log
// survive a full "process" restart (fresh Raft struct reloading the same
// on-disk persister), and that after re-electing itself the node can commit
// both its pre-crash and post-crash entries.
func TestPersistenceSingleNodeRestart(t *testing.T) {
	dir := t.TempDir()

	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln1.Addr().String()

	persister := NewFilePersister(filepath.Join(dir, "node0"))
	applyCh1 := make(chan ApplyMsg, 100)
	sa1 := newSimpleApply()
	go sa1.run(applyCh1)

	rf1 := NewRaft(0, ln1, map[int]string{}, persister, applyCh1)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, isLeader := rf1.State(); isLeader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, isLeader := rf1.State(); !isLeader {
		t.Fatalf("single node never elected itself leader")
	}

	// Note: becoming leader also appends one internal no-op entry (see
	// raft.becomeLeaderLocked), so indices here won't start at 1 -- track
	// the actual indices returned rather than assuming a count.
	preCrashIdx := make([]int, 3)
	for i := 0; i < 3; i++ {
		idx, _, isLeader := rf1.Start([]byte(fmt.Sprintf("pre-crash-%d", i)))
		if !isLeader {
			t.Fatalf("expected leader")
		}
		preCrashIdx[i] = idx
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			if _, ok := sa1.get(idx); ok {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	for i, idx := range preCrashIdx {
		want := fmt.Sprintf("pre-crash-%d", i)
		got, ok := sa1.get(idx)
		if !ok || string(got) != want {
			t.Fatalf("index %d: got %q, ok=%v; want %q", idx, got, ok, want)
		}
	}
	termBeforeCrash, _ := rf1.State()

	// "Crash": kill the node, then rebuild a fresh Raft struct pointed at
	// the same on-disk persister directory, simulating a process restart.
	rf1.Kill()
	close(applyCh1)

	persister2 := NewFilePersister(filepath.Join(dir, "node0"))
	ln2, err := net.Listen("tcp", addr) // rebind the exact same address
	if err != nil {
		t.Fatalf("failed to rebind original address after restart: %v", err)
	}
	applyCh2 := make(chan ApplyMsg, 100)
	sa2 := newSimpleApply()
	go sa2.run(applyCh2)

	rf2 := NewRaft(1000, ln2, map[int]string{}, persister2, applyCh2) // id irrelevant here, no peers
	defer rf2.Kill()

	termAfterRestart, _ := rf2.State()
	if termAfterRestart != termBeforeCrash {
		t.Fatalf("currentTerm not restored: before=%d after=%d", termBeforeCrash, termAfterRestart)
	}

	// Wait for it to re-elect itself (single node, no peers) and submit one
	// more entry -- this should commit it AND, transitively, the 3
	// pre-crash entries that were never marked committed on disk (Raft
	// deliberately does not persist commitIndex/lastApplied).
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, isLeader := rf2.State(); isLeader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	idx, _, isLeader := rf2.Start([]byte("post-crash"))
	if !isLeader {
		t.Fatalf("restarted node never became leader")
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := sa2.get(idx); ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, ok := sa2.get(idx)
	if !ok || string(got) != "post-crash" {
		t.Fatalf("post-crash entry missing or wrong: %q, ok=%v", got, ok)
	}
	// And, transitively, all 3 pre-crash entries (from the OLD term, never
	// marked committed before the crash) must now be applied too.
	for i, oldIdx := range preCrashIdx {
		want := fmt.Sprintf("pre-crash-%d", i)
		got, ok := sa2.get(oldIdx)
		if !ok || string(got) != want {
			t.Fatalf("pre-crash index %d not recovered after restart: got %q, ok=%v; want %q", oldIdx, got, ok, want)
		}
	}
}

// TestFollowerRestartCatchesUp kills a follower process entirely, lets the
// cluster keep committing writes without it, restarts it from disk on the
// same address, and verifies it rejoins and catches up.
func TestFollowerRestartCatchesUp(t *testing.T) {
	dir := t.TempDir()
	n := 3

	listeners := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = ln
		addrs[i] = ln.Addr().String()
	}

	c := &cluster{t: t, n: n, logs: make([]map[int]ApplyMsg, n)}
	persisters := make([]*Persister, n)
	for i := 0; i < n; i++ {
		peerAddrs := map[int]string{}
		for j := 0; j < n; j++ {
			if j != i {
				peerAddrs[j] = addrs[j]
			}
		}
		persisters[i] = NewFilePersister(filepath.Join(dir, fmt.Sprintf("node%d", i)))
		applyCh := make(chan ApplyMsg, 1000)
		rf := NewRaft(i, listeners[i], peerAddrs, persisters[i], applyCh)
		c.nodes = append(c.nodes, rf)
		c.applyChs = append(c.applyChs, applyCh)
		c.persisters = append(c.persisters, persisters[i])
		c.logs[i] = make(map[int]ApplyMsg)
		go c.captureApply(i, applyCh)
	}
	defer c.shutdown()

	c.leader()
	idx1 := c.submit([]byte("before-restart"))
	c.waitApplied(idx1, 3, 2*time.Second)

	// Kill a follower entirely (not the leader).
	leader := c.leader()
	var victimIdx int = -1
	for i, rf := range c.nodes {
		if rf.ID() != leader.ID() {
			victimIdx = i
			break
		}
	}
	victimAddr := addrs[victimIdx]
	c.nodes[victimIdx].Kill()

	// Keep writing while it's down.
	var lastIdx int
	for i := 0; i < 5; i++ {
		lastIdx = c.submit([]byte(fmt.Sprintf("while-down-%d", i)))
	}
	c.waitApplied(lastIdx, 2, 2*time.Second) // majority without the dead follower

	// Restart the follower from disk on the same address.
	ln, err := net.Listen("tcp", victimAddr)
	if err != nil {
		t.Fatalf("rebind: %v", err)
	}
	peerAddrs := map[int]string{}
	for j := 0; j < n; j++ {
		if j != victimIdx {
			peerAddrs[j] = addrs[j]
		}
	}
	newPersister := NewFilePersister(filepath.Join(dir, fmt.Sprintf("node%d", victimIdx)))
	applyCh := make(chan ApplyMsg, 1000)
	restarted := NewRaft(victimIdx, ln, peerAddrs, newPersister, applyCh)
	c.mu.Lock()
	c.nodes[victimIdx] = restarted
	c.applyChs[victimIdx] = applyCh
	c.mu.Unlock()
	go c.captureApply(victimIdx, applyCh)

	// It should catch up to the rest of the cluster's committed log.
	c.waitApplied(lastIdx, 3, 3*time.Second)
}
