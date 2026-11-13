package main

import (
	"flag"
	"fmt"
	"os"
	"replicated-kv/kvraft"
	"strings"
)

func main() {
	serversFlag := flag.String("servers", "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003", "Comma-separated cluster server addresses")
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		fmt.Println("Usage: client -servers <s1,s2,s3> <get|put|append> <key> [value]")
		fmt.Println("Examples:")
		fmt.Println("  client put course CS6824")
		fmt.Println("  client append course \" Distributed Systems\"")
		fmt.Println("  client get course")
		os.Exit(1)
	}

	servers := strings.Split(*serversFlag, ",")
	ck := kvraft.MakeClerk(servers)

	cmd := strings.ToLower(args[0])
	key := args[1]

	switch cmd {
	case "put":
		if len(args) < 3 {
			fmt.Println("Error: put requires a value")
			os.Exit(1)
		}
		val := args[2]
		ck.Put(key, val)
		fmt.Printf("[Client] Successfully PUT: key='%s', val='%s'\n", key, val)

	case "append":
		if len(args) < 3 {
			fmt.Println("Error: append requires a value")
			os.Exit(1)
		}
		val := args[2]
		ck.Append(key, val)
		fmt.Printf("[Client] Successfully APPENDED: key='%s', val='%s'\n", key, val)

	case "get":
		val := ck.Get(key)
		fmt.Printf("[Client] GET result: key='%s' => val='%s'\n", key, val)

	default:
		fmt.Printf("Unknown command: %s. Use get, put, or append.\n", cmd)
		os.Exit(1)
	}
}
