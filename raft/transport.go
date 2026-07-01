package raft

import (
	"errors"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
)

// Transport handles all node-to-node networking for one Raft instance: it
// serves incoming RPCs and dials out to peers. It also doubles as the test
// harness's network-partition simulator -- SetPartitioned blocks/unblocks
// calls to specific peers without touching any Raft logic, standing in for
// the "proxy layer that drops messages between node pairs" from the plan.
type Transport struct {
	id        int
	peerAddrs map[int]string

	listener net.Listener
	server   *rpc.Server

	connsMu sync.Mutex
	conns   map[net.Conn]struct{} // accepted inbound connections, so Close() can sever them

	mu      sync.Mutex
	clients map[int]*rpc.Client

	partMu    sync.RWMutex
	partition map[int]bool // peer IDs currently unreachable from us

	dead int32
}

var ErrPartitioned = errors.New("raft: peer unreachable (simulated partition)")

// NewTransport starts an RPC server for rf on the given (already bound)
// listener and returns a Transport that can call the given peers
// (peerAddrs must include every peer except id, but not id itself).
//
// Binding is a separate step from construction so callers -- especially
// test harnesses standing up a whole cluster -- can learn every node's
// actual address (e.g. after picking an ephemeral port with ":0") before
// any Raft node needs the full peer address map to start up.
func NewTransport(id int, ln net.Listener, peerAddrs map[int]string, rf *Raft) (*Transport, error) {
	t := &Transport{
		id:        id,
		peerAddrs: peerAddrs,
		clients:   make(map[int]*rpc.Client),
		partition: make(map[int]bool),
		listener:  ln,
		conns:     make(map[net.Conn]struct{}),
	}
	t.server = rpc.NewServer()
	if err := t.server.RegisterName("Raft", &RPCService{rf: rf}); err != nil {
		return nil, err
	}
	go t.acceptLoop()
	return t, nil
}

func (t *Transport) acceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			if atomic.LoadInt32(&t.dead) == 1 {
				return
			}
			continue
		}
		t.connsMu.Lock()
		t.conns[conn] = struct{}{}
		t.connsMu.Unlock()
		go func() {
			t.server.ServeConn(conn)
			t.connsMu.Lock()
			delete(t.conns, conn)
			t.connsMu.Unlock()
		}()
	}
}

func (t *Transport) Addr() string {
	return t.listener.Addr().String()
}

// SetPartitioned replaces the set of peer IDs that are currently
// unreachable from this node in both directions (outbound calls fail
// immediately; the RPC server itself keeps running, so tests typically
// partition both endpoints for a realistic split).
func (t *Transport) SetPartitioned(blocked map[int]bool) {
	cp := make(map[int]bool, len(blocked))
	for k, v := range blocked {
		if v {
			cp[k] = true
		}
	}
	t.partMu.Lock()
	t.partition = cp
	t.partMu.Unlock()
}

func (t *Transport) isPartitioned(peer int) bool {
	t.partMu.RLock()
	defer t.partMu.RUnlock()
	return t.partition[peer]
}

func (t *Transport) getClient(peer int) (*rpc.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[peer]; ok {
		return c, nil
	}
	addr, ok := t.peerAddrs[peer]
	if !ok {
		return nil, errors.New("raft: unknown peer")
	}
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	t.clients[peer] = c
	return c, nil
}

func (t *Transport) dropClient(peer int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[peer]; ok {
		c.Close()
		delete(t.clients, peer)
	}
}

// Call invokes serviceMethod ("Raft.RequestVote" etc.) on peer.
func (t *Transport) Call(peer int, serviceMethod string, args, reply interface{}) error {
	if t.isPartitioned(peer) {
		return ErrPartitioned
	}
	client, err := t.getClient(peer)
	if err != nil {
		return err
	}
	err = client.Call(serviceMethod, args, reply)
	if err != nil {
		// Connection likely broken; drop it so the next call redials.
		t.dropClient(peer)
	}
	return err
}

func (t *Transport) Close() {
	atomic.StoreInt32(&t.dead, 1)
	t.listener.Close()
	t.mu.Lock()
	for _, c := range t.clients {
		c.Close()
	}
	t.mu.Unlock()
	// Sever every inbound connection too -- otherwise a peer holding an
	// already-established connection to us could keep getting real RPC
	// replies from our still-running rpc.Server after "kill", which would
	// let a supposedly-dead node keep voting/acking as if alive.
	t.connsMu.Lock()
	for c := range t.conns {
		c.Close()
	}
	t.connsMu.Unlock()
}
