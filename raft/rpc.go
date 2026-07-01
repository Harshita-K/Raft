package raft

// RequestVoteArgs is sent by candidates to gather votes (Figure 2).
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs is sent by the leader both to replicate log entries and,
// with Entries empty, as a heartbeat (Figure 2).
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Fast backtracking hints (§5.3 optimization) so a leader can skip
	// straight past a conflicting term instead of decrementing nextIndex
	// one entry at a time.
	ConflictIndex int
	ConflictTerm  int
}

// InstallSnapshotArgs is sent by the leader when a follower has fallen so
// far behind that the required log entries have already been compacted away.
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// RPCService is the type registered with net/rpc; it just forwards to the
// underlying Raft instance's handlers, going through the network boundary.
type RPCService struct {
	rf *Raft
}

func (s *RPCService) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	s.rf.HandleRequestVote(args, reply)
	return nil
}

func (s *RPCService) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	s.rf.HandleAppendEntries(args, reply)
	return nil
}

func (s *RPCService) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	s.rf.HandleInstallSnapshot(args, reply)
	return nil
}
