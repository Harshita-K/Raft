package kvstore

import (
	"errors"
	"net"
	"sync"
	"time"

	"raftkv/raft"
)

var (
	ErrNotLeader     = errors.New("kvstore: not leader")
	ErrTimeout       = errors.New("kvstore: timed out waiting for commit")
	ErrLeaderChanged = errors.New("kvstore: leader changed before entry committed")
)

// Node ties one raft.Raft instance to a Store: it is the "apply loop" the
// plan calls the only bridge between raft/ and kvstore/. Every committed
// entry raft delivers gets applied here, in order; every client write goes
// through Propose, which calls raft.Start and blocks until that specific
// (index, term) either applies or is proven lost to a leader change.
type Node struct {
	ID    int
	rf    *raft.Raft
	store *Store

	persister         *raft.Persister
	snapshotThreshold int // trigger a snapshot once persisted raft state exceeds this many bytes; 0 disables

	mu      sync.Mutex
	waiters map[int]chan raft.ApplyMsg
}

// NewNode constructs the raft.Raft instance for id, restores the store from
// any existing on-disk snapshot, and starts the apply loop.
// snapshotThreshold is the log-compaction trigger in bytes; pass 0 to
// disable automatic snapshotting.
func NewNode(id int, ln net.Listener, peerAddrs map[int]string, persister *raft.Persister, snapshotThreshold int) *Node {
	n := &Node{
		ID:                id,
		store:             NewStore(),
		persister:         persister,
		snapshotThreshold: snapshotThreshold,
		waiters:           make(map[int]chan raft.ApplyMsg),
	}
	// raft only pushes a snapshot through the apply channel when it's
	// installed FROM a leader (InstallSnapshot RPC); on a plain local
	// restart from disk it never resends what's already on disk, so the
	// state machine has to independently reload it here.
	if snap := persister.ReadSnapshot(); len(snap) > 0 {
		n.store.Restore(snap)
	}

	applyCh := make(chan raft.ApplyMsg, 256)
	n.rf = raft.NewRaft(id, ln, peerAddrs, persister, applyCh)
	go n.applyLoop(applyCh)
	return n
}

func (n *Node) applyLoop(applyCh chan raft.ApplyMsg) {
	for msg := range applyCh {
		if msg.SnapshotValid {
			n.store.Restore(msg.Snapshot)
			continue
		}
		if !msg.CommandValid {
			continue
		}
		if len(msg.Command) > 0 {
			// Zero-length commands are raft-internal no-ops (appended by a
			// freshly-elected leader, see raft.becomeLeaderLocked) -- never
			// real client operations, so the state machine just skips them.
			n.store.Apply(msg.Command)
		}
		n.notify(msg.CommandIndex, msg)
		n.maybeSnapshot(msg.CommandIndex)
	}
}

func (n *Node) maybeSnapshot(appliedIndex int) {
	if n.snapshotThreshold <= 0 {
		return
	}
	if n.persister.RaftStateSize() < n.snapshotThreshold {
		return
	}
	n.rf.Snapshot(appliedIndex, n.store.Snapshot())
}

func (n *Node) notify(index int, msg raft.ApplyMsg) {
	n.mu.Lock()
	ch, ok := n.waiters[index]
	if ok {
		delete(n.waiters, index)
	}
	n.mu.Unlock()
	if ok {
		select {
		case ch <- msg:
		default:
		}
	}
}

// Propose appends cmd to the raft log (if this node is leader) and blocks
// until that specific log slot is either committed+applied or proven to
// have been overwritten by a leader change, or timeout elapses.
func (n *Node) Propose(cmd []byte, timeout time.Duration) error {
	index, term, isLeader := n.rf.Start(cmd)
	if !isLeader {
		return ErrNotLeader
	}

	ch := make(chan raft.ApplyMsg, 1)
	n.mu.Lock()
	n.waiters[index] = ch
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.waiters, index)
		n.mu.Unlock()
	}()

	select {
	case msg := <-ch:
		if msg.CommandTerm != term {
			// The slot at this index got applied, but from a different
			// term -- our proposal never made it, a later leader's entry
			// landed here instead.
			return ErrLeaderChanged
		}
		return nil
	case <-time.After(timeout):
		return ErrTimeout
	}
}

func (n *Node) Put(key, value string, timeout time.Duration) error {
	return n.Propose(encodeCommand(Command{Op: OpPut, Key: key, Value: value}), timeout)
}

func (n *Node) Delete(key string, timeout time.Duration) error {
	return n.Propose(encodeCommand(Command{Op: OpDelete, Key: key}), timeout)
}

// Get is served locally from the leader's applied state without going
// through the log -- a documented simplification (no read-index/lease
// mechanism), see README.
func (n *Node) Get(key string) (string, bool, error) {
	if _, isLeader := n.rf.State(); !isLeader {
		return "", false, ErrNotLeader
	}
	v, ok := n.store.Get(key)
	return v, ok, nil
}

// LocalGet reads this node's locally-applied state regardless of
// leadership. Intended for tests/observability (e.g. checking that a write
// actually replicated to a majority) -- NOT for client reads, since it
// carries no linearizability guarantee.
func (n *Node) LocalGet(key string) (string, bool) { return n.store.Get(key) }

func (n *Node) LeaderHint() int { return n.rf.LeaderHint() }

func (n *Node) State() (term int, isLeader bool) { return n.rf.State() }

func (n *Node) Kill() { n.rf.Kill() }

// SetPartitioned exposes the raft-level network partition simulator for
// chaos/integration tests built on top of kvstore.
func (n *Node) SetPartitioned(blocked map[int]bool) { n.rf.SetPartitioned(blocked) }
