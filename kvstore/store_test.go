package kvstore

import "testing"

func TestStorePutGetDelete(t *testing.T) {
	s := NewStore()

	if _, ok := s.Get("missing"); ok {
		t.Fatalf("expected missing key to be absent")
	}

	s.Apply(encodeCommand(Command{Op: OpPut, Key: "foo", Value: "bar"}))
	v, ok := s.Get("foo")
	if !ok || v != "bar" {
		t.Fatalf("got %q, %v; want bar, true", v, ok)
	}

	s.Apply(encodeCommand(Command{Op: OpPut, Key: "foo", Value: "baz"}))
	v, ok = s.Get("foo")
	if !ok || v != "baz" {
		t.Fatalf("overwrite failed: got %q, %v", v, ok)
	}

	s.Apply(encodeCommand(Command{Op: OpDelete, Key: "foo"}))
	if _, ok := s.Get("foo"); ok {
		t.Fatalf("expected key to be deleted")
	}

	// Delete of a nonexistent key must be a harmless no-op.
	s.Apply(encodeCommand(Command{Op: OpDelete, Key: "never-existed"}))
}

func TestStoreSnapshotRoundTrip(t *testing.T) {
	s := NewStore()
	s.Apply(encodeCommand(Command{Op: OpPut, Key: "a", Value: "1"}))
	s.Apply(encodeCommand(Command{Op: OpPut, Key: "b", Value: "2"}))

	snap := s.Snapshot()

	s2 := NewStore()
	s2.Restore(snap)

	if v, ok := s2.Get("a"); !ok || v != "1" {
		t.Fatalf("restored store missing a=1, got %q, %v", v, ok)
	}
	if v, ok := s2.Get("b"); !ok || v != "2" {
		t.Fatalf("restored store missing b=2, got %q, %v", v, ok)
	}
}

func TestStoreRestoreEmptySnapshotClears(t *testing.T) {
	s := NewStore()
	s.Apply(encodeCommand(Command{Op: OpPut, Key: "a", Value: "1"}))
	s.Restore(nil)
	if _, ok := s.Get("a"); ok {
		t.Fatalf("expected restore(nil) to clear existing state")
	}
}
