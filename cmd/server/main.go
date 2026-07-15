// Command server boots one raftkv node. Example, a 3-node local cluster:
//
//	go run cmd/server/main.go -id=1 -peers=localhost:8001,localhost:8002,localhost:8003
//	go run cmd/server/main.go -id=2 -peers=localhost:8001,localhost:8002,localhost:8003
//	go run cmd/server/main.go -id=3 -peers=localhost:8001,localhost:8002,localhost:8003
//
// -peers lists every node's raft RPC address in a fixed order; -id selects
// which position in that list this process is (1-based, matching the
// addresses above). The client-facing HTTP address for each node is
// derived by adding -http-port-offset to its raft port (default 1000, so
// localhost:8001's HTTP API is on localhost:9001).
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"raftkv/kvstore"
	"raftkv/raft"
)

func main() {
	id := flag.Int("id", 0, "this node's 1-based position in -peers (required)")
	peersFlag := flag.String("peers", "", "comma-separated raft addresses for the whole cluster, e.g. localhost:8001,localhost:8002,localhost:8003")
	dataDir := flag.String("data", "", "directory for persistent state (default ./data/nodeN)")
	httpPortOffset := flag.Int("http-port-offset", 1000, "client HTTP port = raft port + this offset")
	snapshotThreshold := flag.Int("snapshot-threshold-bytes", 1<<20, "compact the raft log once persisted state exceeds this many bytes (0 disables)")
	flag.Parse()

	if *id <= 0 || *peersFlag == "" {
		fmt.Fprintln(os.Stderr, "usage: server -id=N -peers=host:port,host:port,...")
		os.Exit(1)
	}

	raftAddrs := strings.Split(*peersFlag, ",")
	if *id > len(raftAddrs) {
		log.Fatalf("id %d out of range for %d peers", *id, len(raftAddrs))
	}
	nodeIdx := *id - 1 // 0-based internal raft ID

	peerRaftAddrs := map[int]string{}
	httpAddrs := map[int]string{}
	for i, addr := range raftAddrs {
		httpAddrs[i] = toHTTPAddr(addr, *httpPortOffset)
		if i != nodeIdx {
			peerRaftAddrs[i] = addr
		}
	}

	dir := *dataDir
	if dir == "" {
		dir = fmt.Sprintf("./data/node%d", *id)
	}
	persister := raft.NewFilePersister(dir)

	ln, err := net.Listen("tcp", raftAddrs[nodeIdx])
	if err != nil {
		log.Fatalf("listen on raft addr %s: %v", raftAddrs[nodeIdx], err)
	}

	node := kvstore.NewNode(nodeIdx, ln, peerRaftAddrs, persister, *snapshotThreshold)
	httpAddr := httpAddrs[nodeIdx]
	srv := kvstore.NewServer(node, httpAddrs, 2*time.Second)

	log.Printf("node %d: raft listening on %s, http listening on %s, data dir %s", *id, raftAddrs[nodeIdx], httpAddr, dir)
	go watchLeadership(node, *id)

	if err := http.ListenAndServe(httpAddr, srv.Handler()); err != nil {
		log.Fatalf("http server: %v", err)
	}
}

func toHTTPAddr(raftAddr string, offset int) string {
	host, portStr, err := net.SplitHostPort(raftAddr)
	if err != nil {
		log.Fatalf("bad peer address %q: %v", raftAddr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("bad port in address %q: %v", raftAddr, err)
	}
	return net.JoinHostPort(host, strconv.Itoa(port+offset))
}

func watchLeadership(node *kvstore.Node, id int) {
	wasLeader := false
	for {
		time.Sleep(200 * time.Millisecond)
		_, isLeader := node.State()
		if isLeader && !wasLeader {
			log.Printf("node %d: became leader", id)
		} else if !isLeader && wasLeader {
			log.Printf("node %d: stepped down / lost leadership", id)
		}
		wasLeader = isLeader
	}
}
