// Package kvstore implements a simple replicated key-value state machine on
// top of raftkv/raft. It never touches raft internals directly -- the only
// interface between the two is committed log entries delivered as bytes
// (via raft.ApplyMsg) and the reverse Snapshot(index, data) call. That
// separation is deliberate: raft/ has no idea a key-value store exists.
package kvstore

import (
	"bytes"
	"encoding/gob"
	"sync"
)

// Store is the actual state machine: an in-memory map mutated only by
// applying committed commands, in order.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Apply applies one committed log entry to the state machine. This is the
// entire surface raft/ needs to know about a state machine: raw bytes in,
// nothing out. Applying the same LogEntry object twice is not itself
// harmful for this store (Put/Delete are naturally idempotent), which is
// what keeps this safe even though we don't do client-request
// deduplication -- see README "Design decisions & simplifications".
func (s *Store) Apply(entry []byte) {
	cmd, err := decodeCommand(entry)
	if err != nil {
		panic(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch cmd.Op {
	case OpPut:
		s.data[cmd.Key] = cmd.Value
	case OpDelete:
		delete(s.data, cmd.Key)
	}
}

// Snapshot serializes the entire current state, for raft log compaction.
func (s *Store) Snapshot() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(s.data); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// Restore replaces the entire map from a snapshot, either at startup (from
// disk) or when raft delivers an InstallSnapshot-driven reset.
func (s *Store) Restore(snapshot []byte) {
	data := make(map[string]string)
	if len(snapshot) > 0 {
		if err := gob.NewDecoder(bytes.NewReader(snapshot)).Decode(&data); err != nil {
			panic(err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = data
}
