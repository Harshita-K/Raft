// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm") from scratch. It has
// no knowledge of any particular state machine -- callers get committed
// commands delivered on an ApplyMsg channel and are responsible for
// interpreting them.
package raft

import (
	"math/rand"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	tickInterval       = 10 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
)

// Raft is a single participant in a Raft cluster.
type Raft struct {
	mu        sync.Mutex
	id        int
	peers     []int // IDs of every other node in the cluster (excludes self)
	transport *Transport
	persister *Persister
	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// --- Persistent state (Figure 2); must be written to disk before
	// replying to any RPC that depends on it. ---
	currentTerm int
	votedFor    int
	log         *Log

	// --- Volatile state on all servers ---
	role        Role
	commitIndex int
	lastApplied int
	leaderId    int // best known current leader; -1 if unknown

	// --- Volatile state on leaders (reinitialized after each election) ---
	nextIndex  map[int]int
	matchIndex map[int]int

	// pendingSnapshot is set by HandleInstallSnapshot and consumed solely by
	// applyLoop, so there is always exactly one goroutine sending on
	// applyCh -- avoids interleaving a snapshot reset with stale entry
	// applies racing on the same channel.
	pendingSnapshot *ApplyMsg

	electionResetAt time.Time
	electionTimeout time.Duration

	dead int32
}

// NewRaft constructs a Raft node, restores any persisted state, starts its
// RPC server on the given (already bound) listener, and launches its
// background goroutines. peerAddrs maps every OTHER node's ID to its
// raft-RPC address.
func NewRaft(id int, ln net.Listener, peerAddrs map[int]string, persister *Persister, applyCh chan ApplyMsg) *Raft {
	peers := make([]int, 0, len(peerAddrs))
	for p := range peerAddrs {
		peers = append(peers, p)
	}
	sort.Ints(peers)

	rf := &Raft{
		id:        id,
		peers:     peers,
		persister: persister,
		applyCh:   applyCh,
		log:       newLog(),
		votedFor:  -1,
		leaderId:  -1,
		role:      Follower,
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	if data := persister.ReadRaftState(); len(data) > 0 {
		if term, votedFor, entries, ok := decodeState(data); ok {
			rf.currentTerm = term
			rf.votedFor = votedFor
			rf.log.entries = entries
		}
	}
	rf.lastApplied = rf.log.firstIndex()
	rf.commitIndex = rf.log.firstIndex()
	rf.resetElectionTimerLocked()

	t, err := NewTransport(id, ln, peerAddrs, rf)
	if err != nil {
		panic(err)
	}
	rf.transport = t

	go rf.ticker()
	go rf.applyLoop()
	return rf
}

func (rf *Raft) ID() int      { return rf.id }
func (rf *Raft) Addr() string { return rf.transport.Addr() }

// State returns the node's current term and whether it believes it is leader.
func (rf *Raft) State() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// Role reports the raw role, mostly useful for logging/tests.
func (rf *Raft) Role() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

// LeaderHint returns the best-known current leader's ID, or -1 if unknown.
// Used by the KV server layer to redirect clients.
func (rf *Raft) LeaderHint() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderId
}

// SetPartitioned is exposed for chaos/partition tests -- see Transport.
func (rf *Raft) SetPartitioned(blocked map[int]bool) {
	rf.transport.SetPartitioned(blocked)
}

func (rf *Raft) killed() bool { return atomic.LoadInt32(&rf.dead) == 1 }

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.transport.Close()
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

// ---- persistence ----

func (rf *Raft) persistLocked() {
	rf.persister.SaveRaftState(encodeState(rf.currentTerm, rf.votedFor, rf.log))
}

// ---- election timer ----

func (rf *Raft) resetElectionTimerLocked() {
	rf.electionResetAt = time.Now()
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(tickInterval)
		rf.mu.Lock()
		if rf.role != Leader && time.Since(rf.electionResetAt) >= rf.electionTimeout {
			rf.mu.Unlock()
			rf.startElection()
			continue
		}
		rf.mu.Unlock()
	}
}

// becomeFollowerLocked steps down to Follower for a newly observed term.
// Caller holds rf.mu and is responsible for persisting afterwards.
func (rf *Raft) becomeFollowerLocked(term int) {
	rf.currentTerm = term
	rf.role = Follower
	rf.votedFor = -1
	rf.leaderId = -1
}

// ---- elections ----

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.id
	rf.leaderId = -1
	term := rf.currentTerm
	rf.persistLocked()
	rf.resetElectionTimerLocked()
	lastIdx := rf.log.lastIndex()
	lastTerm := rf.log.lastTerm()
	peers := append([]int(nil), rf.peers...)
	majority := len(rf.peers)/2 + 1 // votes needed, including the candidate's own vote for itself
	if len(peers) == 0 {
		// No peers to ask -- the self-vote already is a majority (single-node cluster).
		rf.becomeLeaderLocked()
	}
	rf.mu.Unlock()

	var mu sync.Mutex
	votes := 1

	for _, peer := range peers {
		go func(peer int) {
			args := &RequestVoteArgs{Term: term, CandidateId: rf.id, LastLogIndex: lastIdx, LastLogTerm: lastTerm}
			var reply RequestVoteReply
			if err := rf.transport.Call(peer, "Raft.RequestVote", args, &reply); err != nil {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				rf.persistLocked()
				return
			}
			if rf.role != Candidate || rf.currentTerm != term || !reply.VoteGranted {
				return
			}
			mu.Lock()
			votes++
			v := votes
			mu.Unlock()
			if v >= majority {
				rf.becomeLeaderLocked()
			}
		}(peer)
	}
}

// becomeLeaderLocked transitions a winning candidate to Leader. Caller holds rf.mu.
func (rf *Raft) becomeLeaderLocked() {
	if rf.role != Candidate {
		return
	}
	rf.role = Leader
	rf.leaderId = rf.id
	lastIdx := rf.log.lastIndex()
	rf.nextIndex = make(map[int]int, len(rf.peers))
	rf.matchIndex = make(map[int]int, len(rf.peers))
	for _, p := range rf.peers {
		rf.nextIndex[p] = lastIdx + 1
		rf.matchIndex[p] = 0
	}
	// Append a no-op entry in the new term right away. Without this, an
	// entry from an OLDER term can be replicated to a majority and still
	// sit uncommitted forever if no client ever submits another write --
	// §5.4.2's rule only lets a leader commit by counting replicas of its
	// OWN term's entries. This is the standard fix (also gives a fresh
	// leader a fast, cheap way to establish its commit point).
	noopIndex := lastIdx + 1
	rf.log.append(LogEntry{Term: rf.currentTerm, Index: noopIndex, Command: nil})
	rf.persistLocked()
	rf.maybeAdvanceCommitLocked() // handles the single-node (zero peers) case immediately
	term := rf.currentTerm
	go rf.heartbeatLoop(term)
}

func (rf *Raft) heartbeatLoop(term int) {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.role != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.broadcastAppendEntries(term)
		time.Sleep(heartbeatInterval)
	}
}

func (rf *Raft) broadcastAppendEntries(term int) {
	rf.mu.Lock()
	if rf.role != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	peers := append([]int(nil), rf.peers...)
	rf.mu.Unlock()
	for _, peer := range peers {
		go rf.replicateOnce(peer, term)
	}
}

// ---- RPC handlers (invoked from the transport layer on incoming RPCs) ----

func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persistLocked()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	reply.Term = rf.currentTerm

	lastIdx := rf.log.lastIndex()
	lastTerm := rf.log.lastTerm()
	// "at least as up-to-date" check (§5.4.1): strictly greater term wins
	// outright; equal term requires an index at least as long. Getting
	// this comparison backwards is the classic safety-breaking bug.
	logOk := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logOk {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimerLocked()
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persistLocked()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	} else if rf.role == Candidate {
		rf.role = Follower
	}
	// This is a legitimate leader for a term at least as current as ours --
	// reset the timer regardless of whether the consistency check below
	// passes, otherwise a lagging follower can spuriously start an
	// election while still hearing from a valid leader.
	rf.leaderId = args.LeaderId
	rf.resetElectionTimerLocked()
	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.log.lastIndex() {
		reply.ConflictIndex = rf.log.lastIndex() + 1
		reply.ConflictTerm = -1
		return
	}
	if args.PrevLogIndex < rf.log.firstIndex() {
		reply.ConflictIndex = rf.log.firstIndex() + 1
		reply.ConflictTerm = -1
		return
	}
	if prevTerm, _ := rf.log.termAt(args.PrevLogIndex); prevTerm != args.PrevLogTerm {
		conflictTerm := prevTerm
		idx := args.PrevLogIndex
		for idx > rf.log.firstIndex() {
			t, _ := rf.log.termAt(idx - 1)
			if t != conflictTerm {
				break
			}
			idx--
		}
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = idx
		return
	}

	insertAt := args.PrevLogIndex + 1
	i := 0
	for ; i < len(args.Entries); i++ {
		idx := insertAt + i
		if !rf.log.has(idx) {
			break
		}
		if rf.log.at(idx).Term != args.Entries[i].Term {
			rf.log.truncateAfter(idx - 1)
			break
		}
	}
	if i < len(args.Entries) {
		rf.log.append(args.Entries[i:]...)
	}

	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.LeaderCommit
		if last := rf.log.lastIndex(); newCommit > last {
			newCommit = last
		}
		rf.commitIndex = newCommit
		rf.applyCond.Broadcast()
	}

	reply.Success = true
}

func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	rf.leaderId = args.LeaderId
	rf.resetElectionTimerLocked()
	reply.Term = rf.currentTerm
	rf.persistLocked()

	if args.LastIncludedIndex <= rf.log.firstIndex() {
		return // stale snapshot, we're already at least this current
	}

	rf.log.compactThrough(args.LastIncludedIndex, args.LastIncludedTerm)
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	rf.persister.SaveStateAndSnapshot(encodeState(rf.currentTerm, rf.votedFor, rf.log), args.Data)

	rf.pendingSnapshot = &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyCond.Broadcast()
}

// ---- leader-side replication ----

// replicateOnce sends exactly one AppendEntries (or InstallSnapshot, if the
// follower needs entries we've already compacted away) round to peer.
func (rf *Raft) replicateOnce(peer int, term int) {
	rf.mu.Lock()
	if rf.killed() || rf.role != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	nextIdx := rf.nextIndex[peer]

	if nextIdx <= rf.log.firstIndex() {
		args := &InstallSnapshotArgs{
			Term:              term,
			LeaderId:          rf.id,
			LastIncludedIndex: rf.log.firstIndex(),
			LastIncludedTerm:  rf.log.entries[0].Term,
			Data:              append([]byte(nil), rf.persister.ReadSnapshot()...),
		}
		rf.mu.Unlock()

		var reply InstallSnapshotReply
		if err := rf.transport.Call(peer, "Raft.InstallSnapshot", args, &reply); err != nil {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			rf.persistLocked()
			return
		}
		if rf.role != Leader || rf.currentTerm != term {
			return
		}
		if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
			rf.nextIndex[peer] = args.LastIncludedIndex + 1
		}
		if args.LastIncludedIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = args.LastIncludedIndex
		}
		return
	}

	prevIdx := nextIdx - 1
	prevTerm, ok := rf.log.termAt(prevIdx)
	if !ok {
		rf.mu.Unlock()
		return // log moved under us (compaction race); next round will retry
	}
	entries := rf.log.slice(nextIdx)
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if err := rf.transport.Call(peer, "Raft.AppendEntries", args, &reply); err != nil {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		rf.persistLocked()
		return
	}
	if rf.role != Leader || rf.currentTerm != term {
		return
	}

	if reply.Success {
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatch
		}
		if newMatch+1 > rf.nextIndex[peer] {
			rf.nextIndex[peer] = newMatch + 1
		}
		rf.maybeAdvanceCommitLocked()
		return
	}

	// Rejected: back off nextIndex using the follower's conflict hint so we
	// don't have to decrement one entry at a time (§5.3 optimization).
	if reply.ConflictTerm == -1 {
		rf.nextIndex[peer] = reply.ConflictIndex
	} else {
		idx := args.PrevLogIndex
		found := -1
		for idx > rf.log.firstIndex() {
			t, _ := rf.log.termAt(idx)
			if t == reply.ConflictTerm {
				found = idx
				break
			}
			if t < reply.ConflictTerm {
				break
			}
			idx--
		}
		if found != -1 {
			rf.nextIndex[peer] = found + 1
		} else {
			rf.nextIndex[peer] = reply.ConflictIndex
		}
	}
	// Floor of 1 only -- deliberately NOT clamped to firstIndex()+1. If the
	// follower needs entries we've already compacted away, nextIndex
	// dropping to or below firstIndex() is exactly the signal the top of
	// this function uses to switch to InstallSnapshot next round. Clamping
	// it up here would permanently prevent that follower from ever being
	// offered a snapshot again, deadlocking its catch-up.
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
}

// maybeAdvanceCommitLocked implements the commit rule from §5.3/§5.4.2:
// a leader may only commit by counting replicas for entries from its OWN
// current term; earlier-term entries are committed transitively once a
// later entry commits.
func (rf *Raft) maybeAdvanceCommitLocked() {
	if rf.role != Leader {
		return
	}
	majority := len(rf.peers)/2 + 1
	for n := rf.log.lastIndex(); n > rf.commitIndex; n-- {
		t, ok := rf.log.termAt(n)
		if !ok || t != rf.currentTerm {
			continue
		}
		count := 1
		for _, p := range rf.peers {
			if rf.matchIndex[p] >= n {
				count++
			}
		}
		if count >= majority {
			rf.commitIndex = n
			rf.applyCond.Broadcast()
			return
		}
	}
}

// ---- client entry point ----

// Start appends command to the log if this node is currently leader. It
// returns immediately; the caller finds out the command committed by
// watching the apply channel for CommandIndex == the returned index (and
// CommandTerm still equal, in case of an intervening leader change).
func (rf *Raft) Start(command []byte) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	if rf.role != Leader {
		defer rf.mu.Unlock()
		return -1, rf.currentTerm, false
	}
	index = rf.log.lastIndex() + 1
	rf.log.append(LogEntry{Term: rf.currentTerm, Index: index, Command: command})
	rf.persistLocked()
	term = rf.currentTerm
	// With zero peers (single-node cluster) there's no RPC reply to ever
	// trigger maybeAdvanceCommitLocked, since that only runs from a peer's
	// reply handler -- so the leader must also check right here, where its
	// own vote is already trivially a majority.
	rf.maybeAdvanceCommitLocked()
	rf.mu.Unlock()

	go rf.broadcastAppendEntries(term) // don't make caller wait for the next heartbeat tick
	return index, term, true
}

// Snapshot tells Raft that the state machine has durably captured
// everything up to and including index, so the log can be compacted through
// that point.
func (rf *Raft) Snapshot(index int, snapshotData []byte) {
	rf.mu.Lock()
	if index <= rf.log.firstIndex() || index > rf.lastApplied {
		rf.mu.Unlock()
		return
	}
	term, ok := rf.log.termAt(index)
	if !ok {
		rf.mu.Unlock()
		return
	}
	rf.log.compactThrough(index, term)
	state := encodeState(rf.currentTerm, rf.votedFor, rf.log)
	rf.mu.Unlock()
	// The actual (fsync'd) disk write happens outside rf.mu: Snapshot is
	// only ever called sequentially by the state machine's single
	// apply-loop goroutine (one applied index at a time, monotonically
	// increasing), so there's no reordering risk in releasing the lock
	// first -- and holding rf.mu across a synchronous disk write here was
	// stalling heartbeat processing/election-timer resets on this node
	// long enough to trigger spurious elections under heavy write load
	// with a small snapshot threshold.
	rf.persister.SaveStateAndSnapshot(state, snapshotData)
}

// ---- apply loop: the only bridge to a state machine sitting on top ----

func (rf *Raft) applyLoop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		if rf.killed() {
			return
		}
		if rf.pendingSnapshot != nil {
			msg := *rf.pendingSnapshot
			rf.pendingSnapshot = nil
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			continue
		}
		if rf.lastApplied < rf.commitIndex && rf.log.has(rf.lastApplied+1) {
			rf.lastApplied++
			entry := rf.log.at(rf.lastApplied)
			msg := ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: entry.Index, CommandTerm: entry.Term}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			continue
		}
		rf.applyCond.Wait()
	}
}
