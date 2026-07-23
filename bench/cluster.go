package main

import (
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"time"

	"raftkv/kvstore"
	"raftkv/raft"
)

// cluster is a self-contained in-process raftkv cluster used to drive
// benchmarks without requiring the operator to hand-launch N separate
// `cmd/server` processes first. It's still a faithful benchmark of the real
// stack: every node talks over real TCP (raft RPC) and real HTTP (client
// API), exactly as raftkv/kvstore.NewNode and kvstore.Server do in
// production -- only the OS-process boundary is collapsed for convenience.
type cluster struct {
	n                 int
	dataDir           string
	snapshotThreshold int
	nodes             []*kvstore.Node
	httpSrvs          []*http.Server
	raftAddrs         []string
	httpAddrs         []string
}

func startBenchCluster(n int, dataDir string) *cluster {
	return startBenchClusterWithThreshold(n, dataDir, 1<<20) // 1MB default
}

func startBenchClusterWithThreshold(n int, dataDir string, snapshotThreshold int) *cluster {
	c := &cluster{n: n, dataDir: dataDir, snapshotThreshold: snapshotThreshold}

	raftLns := make([]net.Listener, n)
	httpLns := make([]net.Listener, n)
	c.raftAddrs = make([]string, n)
	c.httpAddrs = make([]string, n)
	for i := 0; i < n; i++ {
		rln := mustListen()
		hln := mustListen()
		raftLns[i], httpLns[i] = rln, hln
		c.raftAddrs[i] = rln.Addr().String()
		c.httpAddrs[i] = hln.Addr().String()
	}

	httpAddrMap := map[int]string{}
	for i, a := range c.httpAddrs {
		httpAddrMap[i] = a
	}

	for i := 0; i < n; i++ {
		peerAddrs := map[int]string{}
		for j := 0; j < n; j++ {
			if j != i {
				peerAddrs[j] = c.raftAddrs[j]
			}
		}
		persister := c.newPersister(i)
		node := kvstore.NewNode(i, raftLns[i], peerAddrs, persister, c.snapshotThreshold)
		srv := kvstore.NewServer(node, httpAddrMap, 3*time.Second)
		httpSrv := &http.Server{Handler: srv.Handler()}
		go httpSrv.Serve(httpLns[i])

		c.nodes = append(c.nodes, node)
		c.httpSrvs = append(c.httpSrvs, httpSrv)
	}
	return c
}

func (c *cluster) newPersister(i int) *raft.Persister {
	if c.dataDir == "" {
		return raft.NewFilePersister("")
	}
	return raft.NewFilePersister(filepath.Join(c.dataDir, fmt.Sprintf("node%d", i)))
}

func mustListen() net.Listener {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	return ln
}

func (c *cluster) shutdown() {
	for _, n := range c.nodes {
		n.Kill()
	}
	for _, s := range c.httpSrvs {
		s.Close()
	}
}

func (c *cluster) leaderIndex() int {
	for i, n := range c.nodes {
		if _, isLeader := n.State(); isLeader {
			return i
		}
	}
	return -1
}

func (c *cluster) waitForLeader(timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if i := c.leaderIndex(); i != -1 {
			return i
		}
		time.Sleep(10 * time.Millisecond)
	}
	return -1
}

// restart kills node i and rebuilds it from the same persister directory on
// the same addresses, simulating a process restart. Requires a file-backed
// cluster (dataDir != "").
func (c *cluster) restart(i int) {
	c.nodes[i].Kill()
	c.httpSrvs[i].Close()

	rln, err := net.Listen("tcp", c.raftAddrs[i])
	if err != nil {
		panic(err)
	}
	hln, err := net.Listen("tcp", c.httpAddrs[i])
	if err != nil {
		panic(err)
	}
	peerAddrs := map[int]string{}
	for j := 0; j < c.n; j++ {
		if j != i {
			peerAddrs[j] = c.raftAddrs[j]
		}
	}
	httpAddrMap := map[int]string{}
	for j, a := range c.httpAddrs {
		httpAddrMap[j] = a
	}
	persister := c.newPersister(i)
	node := kvstore.NewNode(i, rln, peerAddrs, persister, c.snapshotThreshold)
	srv := kvstore.NewServer(node, httpAddrMap, 3*time.Second)
	httpSrv := &http.Server{Handler: srv.Handler()}
	go httpSrv.Serve(hln)

	c.nodes[i] = node
	c.httpSrvs[i] = httpSrv
}

// isolate blocks node victim from every other node, in both directions.
func (c *cluster) isolate(victim int) {
	for i, n := range c.nodes {
		if i == victim {
			blocked := map[int]bool{}
			for j := range c.nodes {
				if j != victim {
					blocked[j] = true
				}
			}
			n.SetPartitioned(blocked)
		} else {
			n.SetPartitioned(map[int]bool{victim: true})
		}
	}
}

func (c *cluster) heal() {
	for _, n := range c.nodes {
		n.SetPartitioned(map[int]bool{})
	}
}

// addrsExcluding returns every node's HTTP address except idx.
func (c *cluster) addrsExcluding(idx int) []string {
	out := make([]string, 0, c.n-1)
	for i, a := range c.httpAddrs {
		if i != idx {
			out = append(out, a)
		}
	}
	return out
}
