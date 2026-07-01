package raft

// Role is the state a Raft node is currently in (Figure 2).
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry is a single entry in the replicated log.
type LogEntry struct {
	Term    int
	Index   int
	Command []byte
}

// ApplyMsg is sent on the apply channel once an entry is committed (or a
// snapshot needs to be installed into the state machine). This is the only
// interface between raft/ and any state machine built on top of it.
type ApplyMsg struct {
	// CommandValid is true for entries that should be applied to the state machine.
	CommandValid bool
	Command      []byte
	CommandIndex int
	CommandTerm  int

	// SnapshotValid is true when the state machine should reset itself
	// entirely from Snapshot instead of applying Command incrementally.
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
