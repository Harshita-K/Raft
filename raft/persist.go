package raft

import (
	"bytes"
	"encoding/gob"
	"os"
	"path/filepath"
	"sync"
)

// persistentState is exactly the state Figure 2 says must survive a crash.
type persistentState struct {
	CurrentTerm int
	VotedFor    int
	Entries     []LogEntry
}

// Persister durably stores Raft state and snapshots on disk. Writes go to a
// temp file, are fsync'd, then atomically renamed over the real file -- so a
// crash mid-write never leaves a corrupt/partial state file behind.
type Persister struct {
	mu       sync.Mutex
	stateFP  string
	snapFP   string
	stateBuf []byte
	snapBuf  []byte
}

// NewFilePersister creates/opens a persister rooted at dir (created if
// missing). If dir is "", state is kept in memory only (used by tests that
// don't care about crash recovery).
func NewFilePersister(dir string) *Persister {
	p := &Persister{}
	if dir == "" {
		return p
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		panic(err)
	}
	p.stateFP = filepath.Join(dir, "raftstate.bin")
	p.snapFP = filepath.Join(dir, "snapshot.bin")
	if b, err := os.ReadFile(p.stateFP); err == nil {
		p.stateBuf = b
	}
	if b, err := os.ReadFile(p.snapFP); err == nil {
		p.snapBuf = b
	}
	return p
}

func writeAtomic(path string, data []byte) {
	if path == "" {
		return
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		panic(err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		panic(err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		panic(err)
	}
}

func (p *Persister) SaveRaftState(state []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stateBuf = append([]byte(nil), state...)
	writeAtomic(p.stateFP, p.stateBuf)
}

func (p *Persister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.stateBuf...)
}

func (p *Persister) SaveStateAndSnapshot(state, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stateBuf = append([]byte(nil), state...)
	p.snapBuf = append([]byte(nil), snapshot...)
	writeAtomic(p.stateFP, p.stateBuf)
	writeAtomic(p.snapFP, p.snapBuf)
}

func (p *Persister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.snapBuf...)
}

func (p *Persister) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.stateBuf)
}

// encodeState serializes currentTerm/votedFor/log for persistence. Caller
// must hold rf.mu.
func encodeState(term, votedFor int, log *Log) []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(persistentState{CurrentTerm: term, VotedFor: votedFor, Entries: log.entries}); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func decodeState(data []byte) (term, votedFor int, entries []LogEntry, ok bool) {
	if len(data) == 0 {
		return 0, -1, nil, false
	}
	var ps persistentState
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&ps); err != nil {
		panic(err)
	}
	return ps.CurrentTerm, ps.VotedFor, ps.Entries, true
}
