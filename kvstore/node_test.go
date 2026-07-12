package kvstore

import (
	"fmt"
	"net"
	"testing"
	"time"

	"raftkv/raft"
)

func newSingleNode(t *testing.T) *Node {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	persister := raft.NewFilePersister("")
	n := NewNode(0, ln, map[int]string{}, persister, 0)
	t.Cleanup(n.Kill)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, isLeader := n.State(); isLeader {
			return n
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("single node never became leader")
	return nil
}

func TestNodePutGetDeleteAgainstRealRaft(t *testing.T) {
	n := newSingleNode(t)

	if err := n.Put("foo", "bar", time.Second); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, ok, err := n.Get("foo")
	if err != nil || !ok || v != "bar" {
		t.Fatalf("Get after Put: v=%q ok=%v err=%v", v, ok, err)
	}

	if err := n.Put("foo", "baz", time.Second); err != nil {
		t.Fatalf("Put overwrite: %v", err)
	}
	v, ok, err = n.Get("foo")
	if err != nil || !ok || v != "baz" {
		t.Fatalf("Get after overwrite: v=%q ok=%v err=%v", v, ok, err)
	}

	if err := n.Delete("foo", time.Second); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, ok, err = n.Get("foo")
	if err != nil || ok {
		t.Fatalf("expected key gone after Delete, ok=%v err=%v", ok, err)
	}
}

func TestNodeManySequentialWrites(t *testing.T) {
	n := newSingleNode(t)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("k%d", i)
		val := fmt.Sprintf("v%d", i)
		if err := n.Put(key, val, time.Second); err != nil {
			t.Fatalf("Put(%s): %v", key, err)
		}
	}
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("k%d", i)
		want := fmt.Sprintf("v%d", i)
		got, ok, err := n.Get(key)
		if err != nil || !ok || got != want {
			t.Fatalf("Get(%s) = %q, %v, %v; want %q", key, got, ok, err, want)
		}
	}
}

func TestNodeGetOnFollowerReturnsNotLeader(t *testing.T) {
	// Build a 3-node cluster; whichever node is NOT leader should reject
	// Get with ErrNotLeader (our documented "reads served from leader only"
	// simplification).
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
	nodes := make([]*Node, n)
	for i := 0; i < n; i++ {
		peerAddrs := map[int]string{}
		for j := 0; j < n; j++ {
			if j != i {
				peerAddrs[j] = addrs[j]
			}
		}
		nodes[i] = NewNode(i, listeners[i], peerAddrs, raft.NewFilePersister(""), 0)
	}
	defer func() {
		for _, node := range nodes {
			node.Kill()
		}
	}()

	deadline := time.Now().Add(3 * time.Second)
	var leaderIdx = -1
	for time.Now().Before(deadline) && leaderIdx == -1 {
		for i, node := range nodes {
			if _, isLeader := node.State(); isLeader {
				leaderIdx = i
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leaderIdx == -1 {
		t.Fatalf("no leader elected")
	}

	if err := nodes[leaderIdx].Put("k", "v", time.Second); err != nil {
		t.Fatalf("Put on leader: %v", err)
	}

	for i, node := range nodes {
		if i == leaderIdx {
			continue
		}
		if _, _, err := node.Get("k"); err != ErrNotLeader {
			t.Fatalf("node %d (follower): expected ErrNotLeader, got %v", i, err)
		}
	}
}
