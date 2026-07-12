package kvstore

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

// Server is the client-facing HTTP layer. It never contains any Raft logic
// itself -- it just translates HTTP verbs into Node.Get/Put/Delete calls and
// turns "not leader" into a redirect hint, per the standard Raft client
// interaction pattern.
type Server struct {
	node      *Node
	httpAddrs map[int]string // raft node ID -> client-facing HTTP address, for redirects
	timeout   time.Duration
}

func NewServer(node *Node, httpAddrs map[int]string, timeout time.Duration) *Server {
	return &Server{node: node, httpAddrs: httpAddrs, timeout: timeout}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/status", s.handleStatus)
	return mux
}

type errorResponse struct {
	Error      string `json:"error"`
	LeaderHint string `json:"leader_hint,omitempty"`
}

func (s *Server) writeNotLeader(w http.ResponseWriter) {
	hint := s.node.LeaderHint()
	addr := ""
	if hint >= 0 {
		addr = s.httpAddrs[hint]
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusMisdirectedRequest) // 421: "you hit the wrong node"
	json.NewEncoder(w).Encode(errorResponse{Error: "not leader", LeaderHint: addr})
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		val, ok, err := s.node.Get(key)
		if err == ErrNotLeader {
			s.writeNotLeader(w)
			return
		}
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(val))

	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		s.writeProposeResult(w, s.node.Put(key, string(body), s.timeout))

	case http.MethodDelete:
		s.writeProposeResult(w, s.node.Delete(key, s.timeout))

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) writeProposeResult(w http.ResponseWriter, err error) {
	switch err {
	case nil:
		w.WriteHeader(http.StatusOK)
	case ErrNotLeader:
		s.writeNotLeader(w)
	case ErrTimeout, ErrLeaderChanged:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(errorResponse{Error: err.Error()})
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	term, isLeader := s.node.State()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"id":          s.node.ID,
		"term":        term,
		"is_leader":   isLeader,
		"leader_hint": s.node.LeaderHint(),
	})
}
