// Command bench is a standalone load generator / scenario runner for
// raftkv, per the plan's benchmarking checklist. By default it spins up its
// own self-contained in-process cluster (still real TCP+HTTP underneath,
// see cluster.go) so it works with zero setup:
//
//	go run ./bench -mode=throughput -clients=1,10,50,100 -duration=10s -op=write
//	go run ./bench -mode=throughput -clients=50 -duration=10s -op=read
//	go run ./bench -mode=failover
//	go run ./bench -mode=partition
//	go run ./bench -mode=snapshot
//	go run ./bench -mode=all   # everything, writes results/bench-results.json
//
// (Note: `go run bench/bench.go` alone won't compile -- this package spans
// multiple files. Use `go run ./bench ...` instead.)
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {
	mode := flag.String("mode", "all", "throughput | failover | partition | snapshot | all")
	nodes := flag.Int("nodes", 5, "cluster size for self-contained runs")
	clientsFlag := flag.String("clients", "1,10,50,100", "comma-separated concurrency levels for throughput mode")
	duration := flag.Duration("duration", 10*time.Second, "load duration per concurrency level")
	op := flag.String("op", "write", "write | read | mixed (throughput mode)")
	keyspace := flag.Int("keyspace", 1000, "number of distinct keys to spread load over")
	out := flag.String("out", "results", "directory to write JSON results into")
	failoverTrials := flag.Int("failover-trials", 10, "number of leader-kill trials for failover mode")
	snapshotWrites := flag.Int("snapshot-writes", 2000, "number of writes an isolated node must catch up on for snapshot mode")
	flag.Parse()

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "mkdir results dir:", err)
		os.Exit(1)
	}

	results := map[string]any{}

	if *mode == "throughput" || *mode == "all" {
		results["throughput"] = runThroughputSuite(*nodes, parseInts(*clientsFlag), *duration, *op, *keyspace)
	}
	if *mode == "failover" || *mode == "all" {
		results["failover"] = runFailoverSuite(*nodes, *failoverTrials)
	}
	if *mode == "partition" || *mode == "all" {
		results["partition"] = runPartitionSuite(*nodes, *duration)
	}
	if *mode == "snapshot" || *mode == "all" {
		results["snapshot_catchup"] = runSnapshotSuite(*snapshotWrites)
	}

	path := filepath.Join(*out, fmt.Sprintf("bench-%s-%d.json", *mode, time.Now().Unix()))
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "write results:", err)
		os.Exit(1)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(results); err != nil {
		fmt.Fprintln(os.Stderr, "encode results:", err)
	}
	f.Close()
	fmt.Println("\nresults written to", path)
}

func parseInts(csv string) []int {
	parts := strings.Split(csv, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad concurrency value %q: %v\n", p, err)
			os.Exit(1)
		}
		out = append(out, n)
	}
	return out
}

func runThroughputSuite(nodes int, concurrencies []int, duration time.Duration, op string, keyspace int) []loadResult {
	fmt.Printf("=== throughput (%s), %d-node cluster ===\n", op, nodes)
	c := startBenchCluster(nodes, "")
	defer c.shutdown()
	if li := c.waitForLeader(3 * time.Second); li == -1 {
		fmt.Fprintln(os.Stderr, "no leader elected, aborting throughput suite")
		return nil
	}

	var results []loadResult
	for _, n := range concurrencies {
		r := runLoad(c.httpAddrs, n, duration, op, keyspace)
		fmt.Printf("concurrency=%-4d ops=%-8d errs=%-6d throughput=%.1f/s p50=%v p95=%v p99=%v\n",
			r.Concurrency, r.Ops, r.Errors, r.ThroughputPerSec, r.P50, r.P95, r.P99)
		results = append(results, r)
	}
	return results
}

type failoverSummary struct {
	Trials        int             `json:"trials"`
	Successes     int             `json:"successes"`
	DowntimesNs   []time.Duration `json:"downtimes_ns"`
	AvgDowntimeNs time.Duration   `json:"avg_downtime_ns"`
	MaxDowntimeNs time.Duration   `json:"max_downtime_ns"`
}

func runFailoverSuite(nodes, trials int) failoverSummary {
	fmt.Printf("\n=== leader failover, %d trials, %d-node cluster ===\n", trials, nodes)
	summary := failoverSummary{Trials: trials}
	var total time.Duration
	for i := 0; i < trials; i++ {
		c := startBenchCluster(nodes, "")
		if li := c.waitForLeader(3 * time.Second); li == -1 {
			c.shutdown()
			continue
		}
		d, err := runFailoverScenario(c)
		c.shutdown()
		if err != nil {
			fmt.Printf("trial %d: failed: %v\n", i, err)
			continue
		}
		fmt.Printf("trial %d: downtime=%v\n", i, d)
		summary.Successes++
		summary.DowntimesNs = append(summary.DowntimesNs, d)
		total += d
		if d > summary.MaxDowntimeNs {
			summary.MaxDowntimeNs = d
		}
	}
	if summary.Successes > 0 {
		summary.AvgDowntimeNs = total / time.Duration(summary.Successes)
	}
	fmt.Printf("avg downtime=%v, max=%v (%d/%d trials recovered)\n", summary.AvgDowntimeNs, summary.MaxDowntimeNs, summary.Successes, trials)
	return summary
}

func runPartitionSuite(nodes int, duration time.Duration) partitionScenarioResult {
	fmt.Printf("\n=== network partition, %d-node cluster ===\n", nodes)
	c := startBenchCluster(nodes, "")
	defer c.shutdown()
	if li := c.waitForLeader(3 * time.Second); li == -1 {
		fmt.Fprintln(os.Stderr, "no leader elected, aborting partition suite")
		return partitionScenarioResult{}
	}
	r := runPartitionScenario(c, duration)
	fmt.Printf("minority throughput=%.2f/s, majority throughput=%.2f/s, heal recovery=%v\n",
		r.MinorityThroughput, r.MajorityThroughput, r.HealRecoveryTime)
	return r
}

type snapshotSummary struct {
	Writes              int           `json:"writes"`
	CatchupWithSnapshot time.Duration `json:"catchup_with_snapshot_ns"`
	CatchupWithoutSnap  time.Duration `json:"catchup_without_snapshot_ns"`
	SnapshotErr         string        `json:"snapshot_err,omitempty"`
	NoSnapshotErr       string        `json:"no_snapshot_err,omitempty"`
}

func runSnapshotSuite(nWrites int) snapshotSummary {
	fmt.Printf("\n=== snapshot catch-up, %d writes ===\n", nWrites)
	summary := snapshotSummary{Writes: nWrites}

	// Small threshold so snapshotting definitely kicks in during nWrites.
	withSnap, err := runSnapshotCatchupScenario(16*1024, nWrites)
	if err != nil {
		summary.SnapshotErr = err.Error()
		fmt.Println("with snapshotting: FAILED:", err)
	} else {
		summary.CatchupWithSnapshot = withSnap
		fmt.Println("with snapshotting:    catch-up took", withSnap)
	}

	withoutSnap, err := runSnapshotCatchupScenario(0, nWrites)
	if err != nil {
		summary.NoSnapshotErr = err.Error()
		fmt.Println("without snapshotting: FAILED:", err)
	} else {
		summary.CatchupWithoutSnap = withoutSnap
		fmt.Println("without snapshotting: catch-up took", withoutSnap)
	}
	return summary
}
