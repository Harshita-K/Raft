// Package integration holds cluster-level and chaos tests that exercise
// raftkv end to end over real TCP + HTTP -- no in-process shortcuts -- via
// the same client library a real user would use.
package integration

import (
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"raftkv/client"
	"raftkv/kvstore"
	"raftkv/raft"
)

type testCluster struct {
	t          *testing.T
	n          int
	dataDir    string // "" => in-memory persisters
	nodes      []*kvstore.Node
	httpSrvs   []*http.Server
	httpAddrs  []string
	raftAddrs  []string
	persisters []*raft.Persister
	client     *client.Client
}

func startCluster(t *testing.T, n int, dataDir string) *testCluster {
	t.Helper()
	tc := &testCluster{t: t, n: n, dataDir: dataDir}

	raftListeners := make([]net.Listener, n)
	tc.raftAddrs = make([]string, n)
	httpListeners := make([]net.Listener, n)
	tc.httpAddrs = make([]string, n)
	for i := 0; i < n; i++ {
		rln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		raftListeners[i] = rln
		tc.raftAddrs[i] = rln.Addr().String()

		hln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		httpListeners[i] = hln
		tc.httpAddrs[i] = hln.Addr().String()
	}

	httpAddrMap := map[int]string{}
	for i, a := range tc.httpAddrs {
		httpAddrMap[i] = a
	}

	for i := 0; i < n; i++ {
		peerAddrs := map[int]string{}
		for j := 0; j < n; j++ {
			if j != i {
				peerAddrs[j] = tc.raftAddrs[j]
			}
		}
		persister := tc.newPersister(i)
		node := kvstore.NewNode(i, raftListeners[i], peerAddrs, persister, 0)
		srv := kvstore.NewServer(node, httpAddrMap, 2*time.Second)
		httpSrv := &http.Server{Handler: srv.Handler()}
		go httpSrv.Serve(httpListeners[i])

		tc.nodes = append(tc.nodes, node)
		tc.httpSrvs = append(tc.httpSrvs, httpSrv)
		tc.persisters = append(tc.persisters, persister)
	}

	tc.client = client.New(append([]string(nil), tc.httpAddrs...))
	return tc
}

func (tc *testCluster) newPersister(i int) *raft.Persister {
	if tc.dataDir == "" {
		return raft.NewFilePersister("")
	}
	return raft.NewFilePersister(filepath.Join(tc.dataDir, fmt.Sprintf("node%d", i)))
}

func (tc *testCluster) shutdown() {
	for _, n := range tc.nodes {
		n.Kill()
	}
	for _, s := range tc.httpSrvs {
		s.Close()
	}
}

// restart kills node i's raft+HTTP server and rebuilds it from the same
// persister directory on the same addresses -- simulating a process
// restart. Requires the cluster to have been started with a real dataDir.
func (tc *testCluster) restart(i int) {
	tc.t.Helper()
	if tc.dataDir == "" {
		tc.t.Fatalf("restart requires a file-backed cluster (dataDir != \"\")")
	}
	tc.nodes[i].Kill()
	tc.httpSrvs[i].Close()

	rln, err := net.Listen("tcp", tc.raftAddrs[i])
	if err != nil {
		tc.t.Fatalf("rebind raft addr: %v", err)
	}
	hln, err := net.Listen("tcp", tc.httpAddrs[i])
	if err != nil {
		tc.t.Fatalf("rebind http addr: %v", err)
	}
	peerAddrs := map[int]string{}
	for j := 0; j < tc.n; j++ {
		if j != i {
			peerAddrs[j] = tc.raftAddrs[j]
		}
	}
	httpAddrMap := map[int]string{}
	for j, a := range tc.httpAddrs {
		httpAddrMap[j] = a
	}
	persister := tc.newPersister(i)
	node := kvstore.NewNode(i, rln, peerAddrs, persister, 0)
	srv := kvstore.NewServer(node, httpAddrMap, 2*time.Second)
	httpSrv := &http.Server{Handler: srv.Handler()}
	go httpSrv.Serve(hln)

	tc.nodes[i] = node
	tc.httpSrvs[i] = httpSrv
	tc.persisters[i] = persister
}

// countReplicas returns how many nodes locally have key=value right now
// (bypassing the leader-only client read path).
func (tc *testCluster) countReplicas(key, value string) int {
	count := 0
	for _, n := range tc.nodes {
		if v, ok := n.LocalGet(key); ok && v == value {
			count++
		}
	}
	return count
}

func (tc *testCluster) waitReplicas(key, value string, need int, timeout time.Duration) int {
	tc.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c := tc.countReplicas(key, value); c >= need {
			return c
		}
		time.Sleep(20 * time.Millisecond)
	}
	return tc.countReplicas(key, value)
}
