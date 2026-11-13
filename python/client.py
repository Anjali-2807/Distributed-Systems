#!/usr/bin/env python3
"""
Distributed Key-Value Client with Leader Discovery and Automatic Retries
Author: Anjali Tiwari
Course: Parallel and Distributed Computing
Instructor: Dr. Sonal Yadav
"""

import argparse
import json
import random
import socket
import sys
import time

class Clerk:
    def __init__(self, servers):
        # servers is a list of (host, port)
        self.servers = servers
        self.leader_idx = 0
        self.client_id = random.randint(1000000, 9999999)
        self.seq_num = 0

    def _rpc(self, host, port, payload, timeout=1.5):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect((host, port))
            s.sendall(json.dumps(payload).encode('utf-8'))
            resp = s.recv(4096).decode('utf-8')
            s.close()
            return json.loads(resp)
        except Exception:
            return None

    def put(self, key, value):
        return self._send_kv("Put", key, value)

    def append(self, key, value):
        return self._send_kv("Append", key, value)

    def get(self, key):
        self.seq_num += 1
        payload = {
            "action": "Get",
            "key": key,
            "client_id": self.client_id,
            "seq_num": self.seq_num
        }

        attempts = 0
        while attempts < len(self.servers) * 3:
            host, port = self.servers[self.leader_idx]
            resp = self._rpc(host, port, payload)

            if resp and resp.get("status") == "OK":
                return resp.get("value", "")

            # If wrong leader or node offline, try next server
            self.leader_idx = (self.leader_idx + 1) % len(self.servers)
            attempts += 1
            time.sleep(0.3)

        return None

    def _send_kv(self, action, key, value):
        self.seq_num += 1
        payload = {
            "action": action,
            "key": key,
            "value": value,
            "client_id": self.client_id,
            "seq_num": self.seq_num
        }

        attempts = 0
        while attempts < len(self.servers) * 3:
            host, port = self.servers[self.leader_idx]
            resp = self._rpc(host, port, payload)

            if resp and resp.get("status") == "OK":
                return True

            # If wrong leader or node offline, try next server
            self.leader_idx = (self.leader_idx + 1) % len(self.servers)
            attempts += 1
            time.sleep(0.3)

        return False

    def get_cluster_status(self):
        statuses = []
        for i, (host, port) in enumerate(self.servers):
            resp = self._rpc(host, port, {"action": "Status"}, timeout=0.8)
            if resp:
                statuses.append(resp)
            else:
                statuses.append({"node_id": i, "state": "OFFLINE", "term": "-", "commit_index": "-", "kv_count": 0})
        return statuses


def main():
    parser = argparse.ArgumentParser(description="Distributed Replicated KV Client")
    parser.add_argument("--servers", type=str, default="127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003",
                        help="Comma-separated server host:port list")
    parser.add_argument("command", choices=["get", "put", "append", "status"], help="Operation to perform")
    parser.add_argument("key", nargs="?", default="", help="Key for get/put/append")
    parser.add_argument("value", nargs="?", default="", help="Value for put/append")
    args = parser.parse_args()

    server_list = []
    for s in args.servers.split(","):
        host, port = s.split(":")
        server_list.append((host, int(port)))

    client = Clerk(server_list)

    if args.command == "put":
        if not args.key or not args.value:
            print("Error: put requires key and value.")
            sys.exit(1)
        ok = client.put(args.key, args.value)
        if ok:
            print(f"[Client] PUT Success: key='{args.key}', val='{args.value}'")
        else:
            print(f"[Client] PUT Failed: Cluster could not reach quorum consensus.")

    elif args.command == "append":
        if not args.key or not args.value:
            print("Error: append requires key and value.")
            sys.exit(1)
        ok = client.append(args.key, args.value)
        if ok:
            print(f"[Client] APPEND Success: key='{args.key}', val='{args.value}'")
        else:
            print(f"[Client] APPEND Failed: Cluster could not reach quorum consensus.")

    elif args.command == "get":
        if not args.key:
            print("Error: get requires a key.")
            sys.exit(1)
        val = client.get(args.key)
        if val is not None:
            print(f"[Client] GET Success: key='{args.key}' => val='{val}'")
        else:
            print(f"[Client] GET Failed: Key not found or cluster offline.")

    elif args.command == "status":
        statuses = client.get_cluster_status()
        print("\n=================================================================")
        print("                  CLUSTER REPLICATION STATUS                     ")
        print("=================================================================")
        print(f" {'Node ID':<10} | {'State':<12} | {'Term':<6} | {'Commit Index':<14} | {'Keys Count':<10}")
        print("-----------------------------------------------------------------")
        for s in statuses:
            print(f" Node {s['node_id']:<5} | {s['state']:<12} | {str(s['term']):<6} | {str(s['commit_index']):<14} | {str(s['kv_count']):<10}")
        print("=================================================================\n")

if __name__ == "__main__":
    main()
