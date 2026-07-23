#!/usr/bin/env python3
"""Generate throughput/latency plots from bench-throughput-*.json results.

Usage: python3 results/plot.py
Reads the most recent write- and read-throughput JSON files in results/ and
writes throughput_vs_concurrency.png + latency_percentiles.png alongside them.
"""
import glob
import json
import os

import matplotlib.pyplot as plt

RESULTS_DIR = os.path.dirname(os.path.abspath(__file__))


def load_throughput_runs():
    write_run, read_run = None, None
    for path in sorted(glob.glob(os.path.join(RESULTS_DIR, "bench-throughput-*.json"))):
        with open(path) as f:
            data = json.load(f)["throughput"]
        if not data:
            continue
        op = data[0]["op"]
        if op == "write":
            write_run = data
        elif op == "read":
            read_run = data
    return write_run, read_run


def main():
    write_run, read_run = load_throughput_runs()
    if not write_run or not read_run:
        print("Need both a write and a read bench-throughput-*.json in results/")
        return

    concurrency = [r["concurrency"] for r in write_run]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.5))

    ax1.plot(concurrency, [r["throughput_per_sec"] for r in write_run], marker="o", label="write", color="#d9534f")
    ax1.plot(concurrency, [r["throughput_per_sec"] for r in read_run], marker="o", label="read", color="#0275d8")
    ax1.set_xlabel("Concurrent clients")
    ax1.set_ylabel("Throughput (ops/sec, log scale)")
    ax1.set_title("Throughput vs. concurrency")
    ax1.set_xscale("log")
    ax1.set_yscale("log")
    ax1.legend()
    ax1.grid(alpha=0.3, which="both")

    ax2.plot(concurrency, [r["p50_ns"] / 1e6 for r in write_run], marker="o", label="write p50", color="#d9534f")
    ax2.plot(concurrency, [r["p99_ns"] / 1e6 for r in write_run], marker="^", linestyle="--", label="write p99", color="#d9534f")
    ax2.plot(concurrency, [r["p50_ns"] / 1e6 for r in read_run], marker="o", label="read p50", color="#0275d8")
    ax2.plot(concurrency, [r["p99_ns"] / 1e6 for r in read_run], marker="^", linestyle="--", label="read p99", color="#0275d8")
    ax2.set_xlabel("Concurrent clients")
    ax2.set_ylabel("Latency (ms)")
    ax2.set_title("Latency vs. concurrency")
    ax2.set_xscale("log")
    ax2.set_yscale("log")
    ax2.legend(fontsize=8)
    ax2.grid(alpha=0.3)

    fig.suptitle("RaftKV: 5-node cluster, single leader serving all writes")
    fig.tight_layout()
    out = os.path.join(RESULTS_DIR, "throughput_and_latency.png")
    fig.savefig(out, dpi=150)
    print("wrote", out)


if __name__ == "__main__":
    main()
