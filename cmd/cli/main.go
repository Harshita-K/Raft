// Command cli is a small interactive/one-shot client for a raftkv cluster.
//
//	go run cmd/cli/main.go put foo bar
//	go run cmd/cli/main.go get foo
//	go run cmd/cli/main.go delete foo
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"raftkv/client"
)

func main() {
	addrsFlag := flag.String("addrs", "localhost:9001,localhost:9002,localhost:9003", "comma-separated client HTTP addresses of the cluster")
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(1)
	}

	c := client.New(strings.Split(*addrsFlag, ","))

	switch args[0] {
	case "put":
		if len(args) != 3 {
			fmt.Fprintln(os.Stderr, "usage: put <key> <value>")
			os.Exit(1)
		}
		if err := c.Put(args[1], args[2]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		fmt.Println("OK")

	case "get":
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, "usage: get <key>")
			os.Exit(1)
		}
		v, ok, err := c.Get(args[1])
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		if !ok {
			fmt.Println("(not found)")
			os.Exit(1)
		}
		fmt.Println(v)

	case "delete":
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, "usage: delete <key>")
			os.Exit(1)
		}
		if err := c.Delete(args[1]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		fmt.Println("OK")

	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: cli [-addrs=host:port,...] put <key> <value> | get <key> | delete <key>")
}
