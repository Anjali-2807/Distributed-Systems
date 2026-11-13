package main

import (
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"replicated-kv/kvraft"
	"strings"
	"syscall"
)

func main() {
	id := flag.Int("id", 0, "Server node ID (0, 1, 2...)")
	listenAddr := flag.String("listen", "127.0.0.1:8001", "TCP address to bind server")
	peersFlag := flag.String("peers", "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003", "Comma-separated peer addresses")
	flag.Parse()

	peers := strings.Split(*peersFlag, ",")

	server := rpc.NewServer()
	kvServer := kvraft.StartKVServer(peers, *id)

	// Register RPC services
	err := server.RegisterName("KVServer", kvServer)
	if err != nil {
		fmt.Printf("Failed to register KVServer RPC: %v\n", err)
		os.Exit(1)
	}

	err = server.RegisterName("Raft", kvServer.GetRaft())
	if err != nil {
		fmt.Printf("Failed to register Raft RPC: %v\n", err)
		os.Exit(1)
	}
	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		fmt.Printf("Failed to listen on %s: %v\n", *listenAddr, err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Printf("=================================================================\n")
	fmt.Printf("[Cluster Node %d] Replicated KV Server running on %s\n", *id, *listenAddr)
	fmt.Printf("[Cluster Node %d] Peers: %v\n", *id, peers)
	fmt.Printf("=================================================================\n")

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Printf("\n[Cluster Node %d] Shutting down...\n", *id)
		listener.Close()
		os.Exit(0)
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		go server.ServeConn(conn)
	}
}
