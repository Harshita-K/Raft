package raft

// Log stores the replicated log with support for compaction via
// snapshots. entries[0] is always a sentinel representing the entry at
// (lastIncludedIndex, lastIncludedTerm) -- either (0, 0) for a fresh log, or
// the point a snapshot was taken up to. Real entries start at entries[1],
// i.e. slice position p holds log index (offset + p) where offset is
// entries[0].Index.
type Log struct {
	entries []LogEntry
}

func newLog() *Log {
	return &Log{entries: []LogEntry{{Term: 0, Index: 0}}}
}

// pos converts a log index into a position in the entries slice.
func (l *Log) pos(index int) int {
	return index - l.entries[0].Index
}

func (l *Log) firstIndex() int {
	return l.entries[0].Index
}

func (l *Log) lastIndex() int {
	return l.entries[0].Index + len(l.entries) - 1
}

func (l *Log) lastTerm() int {
	return l.entries[len(l.entries)-1].Term
}

// has reports whether index is currently addressable in this log (i.e. not
// already compacted away and not beyond the end).
func (l *Log) has(index int) bool {
	p := l.pos(index)
	return p >= 0 && p < len(l.entries)
}

// at returns the entry at index. Caller must ensure has(index) is true.
func (l *Log) at(index int) LogEntry {
	return l.entries[l.pos(index)]
}

// termAt returns the term of the entry at index, including the case where
// index == firstIndex() (the snapshot sentinel).
func (l *Log) termAt(index int) (int, bool) {
	p := l.pos(index)
	if p < 0 || p >= len(l.entries) {
		return 0, false
	}
	return l.entries[p].Term, true
}

// append adds new entries to the end of the log. Callers are responsible
// for truncating any conflicting suffix (via truncateAfter) first.
func (l *Log) append(entries ...LogEntry) {
	l.entries = append(l.entries, entries...)
}

// truncateAfter drops every entry after index (index itself is kept).
func (l *Log) truncateAfter(index int) {
	p := l.pos(index)
	if p+1 < len(l.entries) {
		l.entries = l.entries[:p+1]
	}
}

// slice returns a copy of entries in [from, lastIndex()].
func (l *Log) slice(from int) []LogEntry {
	p := l.pos(from)
	if p < 0 {
		p = 0
	}
	if p >= len(l.entries) {
		return nil
	}
	out := make([]LogEntry, len(l.entries)-p)
	copy(out, l.entries[p:])
	return out
}

// compactThrough discards all entries up to and including lastIncludedIndex,
// replacing them with a new sentinel. Used after taking a snapshot.
func (l *Log) compactThrough(lastIncludedIndex, lastIncludedTerm int) {
	if lastIncludedIndex <= l.entries[0].Index {
		return
	}
	if l.has(lastIncludedIndex) && l.at(lastIncludedIndex).Term == lastIncludedTerm {
		p := l.pos(lastIncludedIndex)
		rest := make([]LogEntry, len(l.entries)-p)
		copy(rest, l.entries[p:])
		rest[0] = LogEntry{Term: lastIncludedTerm, Index: lastIncludedIndex}
		l.entries = rest
		return
	}
	// Snapshot covers entries we don't have (e.g. installed from leader) --
	// discard everything.
	l.entries = []LogEntry{{Term: lastIncludedTerm, Index: lastIncludedIndex}}
}
