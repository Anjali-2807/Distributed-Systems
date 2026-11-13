#!/usr/bin/env python3
"""
Distributed Fault-Tolerant Replicated Key-Value Server (Raft Consensus)
Author: Anjali Tiwari
Course: Parallel and Distributed Computing
Instructor: Dr. Sonal Yadav

Implements:
- Raft Consensus (Leader Election, Heartbeats, Log Replication)
- Fault Tolerance (Detects leader crash, elects new leader)
- Consistency (Linearizable Put/Get/Append, duplicate request suppression)
- State Machine Replication across cluster nodes
"""

import argparse
import json
import random
import socket
import sys
import threading
import time

FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"
LEADER = "LEADER"

class RaftKVServer:
    def __init__(self, node_id, listen_host, listen_port, peers):
        self.node_id = node_id
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.peers = peers  # dict: {peer_id: (host, port)}
        self.lock = threading.RLock()

        # Raft state
        self.current_term = 0
        self.voted_for = None
        self.log = [{"term": 0, "index": 0, "op": None}]  # 1-based indexing
        self.commit_index = 0
        self.last_applied = 0
        self.state = FOLLOWER

        # Leader specific
        self.next_index = {}
        self.match_index = {}

        # KV State Machine & Client Deduplication table
        self.kv_store = {}
        self.last_seq = {}  # {client_id: last_seq_num}

        self.last_heartbeat = time.time()
        self.running = True

        # Pending client requests: {log_index: threading.Event}
        self.pending_ops = {}

    def start(self):
        # Start TCP listener
        server_thread = threading.Thread(target=self._run_server, daemon=True)
        server_thread.start()

        # Start Raft timer loop
        timer_thread = threading.Thread(target=self._run_timer, daemon=True)
        timer_thread.start()

        print(f"=================================================================")
        print(f"[Node {self.node_id}] Replicated KV Server running on {self.listen_host}:{self.listen_port}")
        print(f"[Node {self.node_id}] Cluster Peers: {list(self.peers.keys())}")
        print(f"=================================================================")

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.running = False
            print(f"\n[Node {self.node_id}] Server shutting down...")

    def _run_server(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.listen_host, self.listen_port))
        sock.listen(10)

        while self.running:
            try:
                conn, _ = sock.accept()
                threading.Thread(target=self._handle_client, args=(conn,), daemon=True).start()
            except Exception:
                break

    def _handle_client(self, conn):
        try:
            data = conn.recv(4096).decode('utf-8')
            if not data:
                conn.close()
                return

            req = json.loads(data)
            action = req.get("action")
            res = {}

            if action == "RequestVote":
                res = self._handle_request_vote(req)
            elif action == "AppendEntries":
                res = self._handle_append_entries(req)
            elif action in ("Put", "Append", "Get"):
                res = self._handle_client_kv(req)
            elif action == "Status":
                with self.lock:
                    res = {
                        "node_id": self.node_id,
                        "state": self.state,
                        "term": self.current_term,
                        "commit_index": self.commit_index,
                        "kv_count": len(self.kv_store),
                        "kv_store": dict(self.kv_store)
                    }

            conn.sendall(json.dumps(res).encode('utf-8'))
        except Exception as e:
            pass
        finally:
            conn.close()

    def _send_rpc(self, host, port, payload, timeout=0.8):
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

    def _handle_request_vote(self, args):
        with self.lock:
            term = args["term"]
            candidate_id = args["candidate_id"]
            last_log_idx = args["last_log_index"]
            last_log_term = args["last_log_term"]

            if term < self.current_term:
                return {"term": self.current_term, "vote_granted": False}

            if term > self.current_term:
                self.current_term = term
                self.state = FOLLOWER
                self.voted_for = None

            my_last_idx = len(self.log) - 1
            my_last_term = self.log[my_last_idx]["term"]

            log_ok = (last_log_term > my_last_term) or (
                last_log_term == my_last_term and last_log_idx >= my_last_idx
            )

            if (self.voted_for is None or self.voted_for == candidate_id) and log_ok:
                self.voted_for = candidate_id
                self.last_heartbeat = time.time()
                return {"term": self.current_term, "vote_granted": True}

            return {"term": self.current_term, "vote_granted": False}

    def _handle_append_entries(self, args):
        with self.lock:
            term = args["term"]
            leader_id = args["leader_id"]
            prev_idx = args["prev_log_index"]
            prev_term = args["prev_log_term"]
            entries = args.get("entries", [])
            leader_commit = args.get("leader_commit", 0)

            if term < self.current_term:
                return {"term": self.current_term, "success": False}

            if term > self.current_term or self.state == CANDIDATE:
                self.current_term = term
                self.state = FOLLOWER
                self.voted_for = None

            self.last_heartbeat = time.time()

            # Log consistency check
            if prev_idx >= len(self.log) or self.log[prev_idx]["term"] != prev_term:
                return {"term": self.current_term, "success": False}

            # Append missing entries
            for i, entry in enumerate(entries):
                target_idx = prev_idx + 1 + i
                if target_idx < len(self.log):
                    if self.log[target_idx]["term"] != entry["term"]:
                        self.log = self.log[:target_idx]
                        self.log.append(entry)
                else:
                    self.log.append(entry)

            # Update commit index and apply to state machine
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log) - 1)
                self._apply_entries()

            return {"term": self.current_term, "success": True}

    def _handle_client_kv(self, req):
        with self.lock:
            if self.state != LEADER:
                return {"status": "ERR_WRONG_LEADER", "leader_hint": self.voted_for}

            # Assign log entry
            log_index = len(self.log)
            op = {
                "type": req["action"],
                "key": req.get("key"),
                "value": req.get("value"),
                "client_id": req.get("client_id"),
                "seq_num": req.get("seq_num")
            }
            entry = {"term": self.current_term, "index": log_index, "op": op}
            self.log.append(entry)
            self.match_index[self.node_id] = log_index
            self.next_index[self.node_id] = log_index + 1

            event = threading.Event()
            self.pending_ops[log_index] = event

        # Immediately trigger log replication
        threading.Thread(target=self._replicate_to_peers, daemon=True).start()

        # Wait for majority quorum commit
        committed = event.wait(timeout=2.0)
        with self.lock:
            self.pending_ops.pop(log_index, None)
            if not committed:
                return {"status": "ERR_TIMEOUT"}

            if op["type"] == "Get":
                val = self.kv_store.get(op["key"], "")
                return {"status": "OK", "value": val}
            return {"status": "OK"}

    def _start_election(self):
        with self.lock:
            self.state = CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.last_heartbeat = time.time()

            term = self.current_term
            last_idx = len(self.log) - 1
            last_term = self.log[last_idx]["term"]
            peers_list = list(self.peers.items())
            total_nodes = len(self.peers) + 1

        votes = 1
        vote_lock = threading.Lock()

        for peer_id, (host, port) in peers_list:
            def request_vote(p_id, h, p):
                nonlocal votes
                payload = {
                    "action": "RequestVote",
                    "term": term,
                    "candidate_id": self.node_id,
                    "last_log_index": last_idx,
                    "last_log_term": last_term
                }
                res = self._send_rpc(h, p, payload, timeout=0.6)
                if not res:
                    return

                with self.lock:
                    if res.get("term", 0) > self.current_term:
                        self.current_term = res["term"]
                        self.state = FOLLOWER
                        self.voted_for = None
                        return

                if res.get("vote_granted"):
                    with vote_lock:
                        votes += 1
                        if votes > total_nodes // 2 and self.state == CANDIDATE:
                            with self.lock:
                                self.state = LEADER
                                print(f"[Node {self.node_id}] *** Elected LEADER for Term {self.current_term} (received {votes}/{total_nodes} votes) ***")
                                for p_idx in self.peers:
                                    self.next_index[p_idx] = len(self.log)
                                    self.match_index[p_idx] = 0
                                self.match_index[self.node_id] = len(self.log) - 1
                            threading.Thread(target=self._send_heartbeats, daemon=True).start()

            threading.Thread(target=request_vote, args=(peer_id, host, port), daemon=True).start()

    def _send_heartbeats(self):
        with self.lock:
            if self.state != LEADER:
                return
            term = self.current_term
            leader_commit = self.commit_index
            peers_list = list(self.peers.items())

        for peer_id, (host, port) in peers_list:
            def send_hb(p_id, h, p):
                with self.lock:
                    if self.state != LEADER:
                        return
                    prev_idx = self.next_index.get(p_id, len(self.log)) - 1
                    if prev_idx < 0:
                        prev_idx = 0
                    prev_term = self.log[prev_idx]["term"]
                    entries = self.log[prev_idx + 1:]

                payload = {
                    "action": "AppendEntries",
                    "term": term,
                    "leader_id": self.node_id,
                    "prev_log_index": prev_idx,
                    "prev_log_term": prev_term,
                    "entries": entries,
                    "leader_commit": leader_commit
                }
                res = self._send_rpc(h, p, payload, timeout=0.5)
                if not res:
                    return

                with self.lock:
                    if res.get("term", 0) > self.current_term:
                        self.current_term = res["term"]
                        self.state = FOLLOWER
                        self.voted_for = None
                        return

                    if res.get("success"):
                        self.match_index[p_id] = prev_idx + len(entries)
                        self.next_index[p_id] = self.match_index[p_id] + 1
                        self._check_and_update_commit()
                    else:
                        if self.next_index.get(p_id, 1) > 1:
                            self.next_index[p_id] -= 1

            threading.Thread(target=send_hb, args=(peer_id, host, port), daemon=True).start()

    def _replicate_to_peers(self):
        self._send_heartbeats()

    def _check_and_update_commit(self):
        total_nodes = len(self.peers) + 1
        for N in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[N]["term"] == self.current_term:
                count = 1
                for p_id in self.peers:
                    if self.match_index.get(p_id, 0) >= N:
                        count += 1
                if count > total_nodes // 2:
                    self.commit_index = N
                    self._apply_entries()
                    break

    def _apply_entries(self):
        while self.commit_index > self.last_applied:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            op = entry.get("op")
            if not op:
                continue

            client_id = op.get("client_id")
            seq_num = op.get("seq_num")
            last_seen = self.last_seq.get(client_id, -1)
            is_dup = (seq_num is not None and seq_num <= last_seen)

            op_type = op.get("type")
            key = op.get("key")
            val = op.get("value")

            if op_type == "Put":
                if not is_dup:
                    self.kv_store[key] = val
                    if client_id:
                        self.last_seq[client_id] = seq_num
                    print(f"[Node {self.node_id}] Committed PUT: '{key}' -> '{val}' (Log #{self.last_applied})")
            elif op_type == "Append":
                if not is_dup:
                    curr = self.kv_store.get(key, "")
                    self.kv_store[key] = curr + val
                    if client_id:
                        self.last_seq[client_id] = seq_num
                    print(f"[Node {self.node_id}] Committed APPEND: '{key}' -> '{self.kv_store[key]}' (Log #{self.last_applied})")
            elif op_type == "Get":
                print(f"[Node {self.node_id}] Served GET: '{key}' -> '{self.kv_store.get(key, '')}'")

            # Wake up pending client RPC
            if self.last_applied in self.pending_ops:
                self.pending_ops[self.last_applied].set()

    def _run_timer(self):
        self.election_timeout = random.uniform(0.6, 1.4)
        while self.running:
            with self.lock:
                is_leader = (self.state == LEADER)
                timed_out = (time.time() - self.last_heartbeat > self.election_timeout)

            if is_leader:
                self._send_heartbeats()
                time.sleep(0.15)
            elif timed_out:
                self.election_timeout = random.uniform(0.6, 1.4)
                self._start_election()
                time.sleep(0.1)
            else:
                time.sleep(0.05)


def main():
    parser = argparse.ArgumentParser(description="Distributed Replicated KV Server")
    parser.add_argument("--id", type=int, required=True, help="Node ID (0, 1, 2...)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, required=True, help="Bind port")
    parser.add_argument("--peers", type=str, default="", help="Peers format: id:host:port,id:host:port")
    args = parser.parse_args()

    peers = {}
    if args.peers:
        for p in args.peers.split(","):
            parts = p.split(":")
            if len(parts) == 3:
                p_id = int(parts[0])
                p_host = parts[1]
                p_port = int(parts[2])
                peers[p_id] = (p_host, p_port)

    server = RaftKVServer(args.id, args.host, args.port, peers)
    server.start()

if __name__ == "__main__":
    main()
