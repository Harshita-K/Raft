// Package client is a small HTTP client for talking to a raftkv cluster. It
// knows nothing about Raft internals -- it just retries against the node
// list, follows "not leader, try X" redirect hints, and gives up after a
// bounded number of rounds.
package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

const (
	defaultPerRequestTimeout = 2 * time.Second
	defaultMaxRounds         = 20
	defaultRoundBackoff      = 150 * time.Millisecond
)

type Client struct {
	mu     sync.Mutex
	addrs  []string
	leader string // best-guess current leader address; "" if unknown

	// restrictToAddrs, if set, makes the client ignore any leader-hint
	// redirect pointing outside its configured addrs. Real clients should
	// never set this -- following a hint to a node you didn't start with
	// is exactly how you discover the rest of the cluster. It exists only
	// for test/benchmark harnesses simulating a client that is genuinely
	// unable to route to certain nodes (e.g. one side of a network
	// partition), where blindly following a redirect would let it "cheat"
	// past a simulation that only isolates raft-to-raft RPC traffic, not
	// arbitrary HTTP connections.
	restrictToAddrs bool

	http *http.Client

	maxRounds    int
	roundBackoff time.Duration
}

// New creates a client that knows about every node's client-facing HTTP
// address. It does not need to know which one is leader -- it'll find out.
func New(addrs []string) *Client {
	return &Client{
		addrs:        addrs,
		http:         &http.Client{Timeout: defaultPerRequestTimeout},
		maxRounds:    defaultMaxRounds,
		roundBackoff: defaultRoundBackoff,
	}
}

// NewRestricted is like New, but the client will never follow a
// leader-hint redirect to an address outside addrs. See restrictToAddrs.
func NewRestricted(addrs []string) *Client {
	c := New(addrs)
	c.restrictToAddrs = true
	return c
}

type errorResponse struct {
	Error      string `json:"error"`
	LeaderHint string `json:"leader_hint,omitempty"`
}

func (c *Client) candidates() []string {
	c.mu.Lock()
	leader := c.leader
	c.mu.Unlock()
	if leader == "" {
		return append([]string(nil), c.addrs...)
	}
	out := make([]string, 0, len(c.addrs))
	out = append(out, leader)
	for _, a := range c.addrs {
		if a != leader {
			out = append(out, a)
		}
	}
	return out
}

func (c *Client) setLeader(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.restrictToAddrs {
		known := false
		for _, a := range c.addrs {
			if a == addr {
				known = true
				break
			}
		}
		if !known {
			return
		}
	}
	c.leader = addr
}

func (c *Client) forgetLeader() {
	c.mu.Lock()
	c.leader = ""
	c.mu.Unlock()
}

// request tries every known node, preferring the last-known leader, for up
// to maxRounds passes (sleeping roundBackoff between passes so a fresh
// election has time to settle), following leader-hint redirects as it goes.
func (c *Client) request(method, key string, body []byte) (respBody []byte, status int, err error) {
	var lastErr error
	for round := 0; round < c.maxRounds; round++ {
		for _, addr := range c.candidates() {
			url := fmt.Sprintf("http://%s/kv/%s", addr, key)
			var reqBody io.Reader
			if body != nil {
				reqBody = bytes.NewReader(body)
			}
			req, err := http.NewRequest(method, url, reqBody)
			if err != nil {
				return nil, 0, err
			}
			resp, err := c.http.Do(req)
			if err != nil {
				lastErr = err
				c.forgetLeader() // that node might be down; stop preferring it
				continue
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			if resp.StatusCode == http.StatusMisdirectedRequest {
				var er errorResponse
				_ = json.Unmarshal(b, &er)
				if er.LeaderHint != "" {
					c.setLeader(er.LeaderHint)
				} else {
					c.forgetLeader()
				}
				lastErr = errors.New("not leader")
				continue
			}
			if resp.StatusCode == http.StatusServiceUnavailable {
				var er errorResponse
				_ = json.Unmarshal(b, &er)
				lastErr = fmt.Errorf("server: %s", er.Error)
				continue
			}
			return b, resp.StatusCode, nil
		}
		time.Sleep(c.roundBackoff)
	}
	return nil, 0, fmt.Errorf("client: exhausted retries against %v: %w", c.addrs, lastErr)
}

func (c *Client) Put(key, value string) error {
	_, status, err := c.request(http.MethodPut, key, []byte(value))
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("client: put returned status %d", status)
	}
	return nil
}

func (c *Client) Delete(key string) error {
	_, status, err := c.request(http.MethodDelete, key, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("client: delete returned status %d", status)
	}
	return nil
}

func (c *Client) Get(key string) (value string, found bool, err error) {
	body, status, err := c.request(http.MethodGet, key, nil)
	if err != nil {
		return "", false, err
	}
	switch status {
	case http.StatusOK:
		return string(body), true, nil
	case http.StatusNotFound:
		return "", false, nil
	default:
		return "", false, fmt.Errorf("client: get returned status %d", status)
	}
}
