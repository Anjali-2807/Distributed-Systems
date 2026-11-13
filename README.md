# Fault-Tolerant Replicated Key-Value Service

**Author:** Anjali Tiwari  
**Course:** Parallel and Distributed Computing  
**Instructor:** Dr. Sonal Yadav  
**Reference Material:** MIT 6.824 / 6.5840 Distributed Systems (Raft Consensus & Replicated State Machines)

---

## 1. Project Overview

This project implements a distributed, fault-tolerant, and linearizable Key-Value storage service based on the **Raft Consensus Algorithm** (inspired by MIT 6.824 Labs 2 and 3). 

The service guarantees:
1. **Replication:** Log entries are replicated across an ensemble of 3 server nodes before being applied to the in-memory state machine.
2. **Fault Tolerance:** If the leader node crashes or becomes disconnected, the cluster detects the failure through election timeouts, elects a new leader via quorum voting, and continues serving client requests with zero data loss.
3. **Consistency (Linearizability):** Strong consistency is preserved. Clients tag requests with a unique `ClientId` and monotonically increasing `SeqNum` to guarantee at-most-once execution semantics and prevent duplicate modifications during network retries.

---

## 2. System Architecture

```
               +-------------------------------------------+
               |               Client (Clerk)              |
               |       Auto-retries & Leader Discovery     |
               +---------------------+---------------------+
                                     |
                                     | RPC (Put / Append / Get)
                                     v
+------------------------+------------------------+------------------------+
|      Cluster Node 0    |      Cluster Node 1    |      Cluster Node 2    |
|       (Follower)       |        (Leader)        |       (Follower)       |
|                        |                        |                        |
|  +------------------+  |  +------------------+  |  +------------------+  |
|  |     KV Server    |  |  |     KV Server    |  |  |     KV Server    |  |
|  |  (State Machine) |  |  |  (State Machine) |  |  |  (State Machine) |  |
|  +--------^---------+  |  +--------^---------+  |  +--------^---------+  |
|           | (Apply)    |           | (Apply)    |           | (Apply)    |
|  +--------+---------+  |  +--------+---------+  |  +--------+---------+  |
|  |   Raft Consensus |<====|   Raft Consensus |====>|   Raft Consensus |  |
|  |   Engine (Log)   |  |  |   Engine (Log)   |  |  |   Engine (Log)   |  |
|  +------------------+  |  +------------------+  |  +------------------+  |
+------------------------+------------------------+------------------------+
```

### Components
- **Raft Consensus Engine (`raft/` and `python/server.py`):** Manages role transitions (`Follower`, `Candidate`, `Leader`), randomized election timeouts (0.6s – 1.4s), heartbeats (150ms), and log replication via `AppendEntries` and `RequestVote` RPCs.
- **Replicated KV State Machine (`kvraft/` and `python/server.py`):** Applies committed log operations in sequence to an in-memory key-value dictionary. Maintains a deduplication table (`last_seq`) to ensure linearizability.
- **Client Clerk (`client.py`):** Interfaces with the cluster. If a contacted node is not the leader or is unreachable, the client rotates through remaining servers until finding the active leader.

---

## 3. Directory Structure

```
├── README.md                      # Complete system documentation & test reports
├── Makefile                       # Go build automation
├── go.mod                         # Go module definition
├── cmd/
│   ├── server/main.go             # Standalone Go server binary
│   └── client/main.go             # Interactive Go client CLI
├── raft/
│   ├── common.go                  # Raft message types (RequestVote, AppendEntries)
│   └── raft.go                    # Consensus logic, elections, and log replication
├── kvraft/
│   ├── common.go                  # KV request/reply interfaces
│   ├── server.go                  # Linearizable state machine & deduplication
│   └── client.go                  # Client clerk with leader failover
├── python/
│   ├── server.py                  # Standalone runnable distributed Raft node
│   ├── client.py                  # Client CLI (put, get, append, status)
│   └── cluster_test.py            # Automated fault-tolerance & consistency test suite
├── scripts/
│   ├── start_cluster.sh           # Starts 3 nodes in background
│   ├── stop_cluster.sh            # Stops running nodes
│   ├── test_fault_tolerance.sh    # Executes automated test suite
│   └── package_submission.sh      # Bundles project into submission ZIP
└── screenshots/                   # Verification terminal captures
    ├── 01_cluster_boot_election.png
    ├── 02_replication_put_get.png
    ├── 03_leader_crash_failover.png
    ├── 04_consistency_linearizability.png
    ├── 05_node_recovery_sync.png
    └── 06_test_suite_all_pass.png
```

---

## 4. How to Run the Distributed Service

### Step 1: Start the 3-Node Cluster
Run the helper script to launch 3 server nodes in the background on ports 8001, 8002, and 8003:
```bash
./scripts/start_cluster.sh
```

Or start them manually in separate terminal windows:
```bash
# Terminal 1: Node 0
python3 python/server.py --id 0 --port 8001 --peers "1:127.0.0.1:8002,2:127.0.0.1:8003"

# Terminal 2: Node 1
python3 python/server.py --id 1 --port 8002 --peers "0:127.0.0.1:8001,2:127.0.0.1:8003"

# Terminal 3: Node 2
python3 python/server.py --id 2 --port 8003 --peers "0:127.0.0.1:8001,1:127.0.0.1:8002"
```

### Step 2: Query Cluster Status
Inspect the current term, elected leader, commit index, and key count across all nodes:
```bash
python3 python/client.py status
```

### Step 3: Perform Client Operations
```bash
# Put a new key-value pair
python3 python/client.py put course "CS6824-Distributed-Systems"

# Get the value
python3 python/client.py get course

# Append to an existing key
python3 python/client.py append course "-Lab3"

# Verify updated value
python3 python/client.py get course
```

### Step 4: Stop the Cluster
```bash
./scripts/stop_cluster.sh
```

---

## 5. Automated Testing & Verification

Run the full automated test suite:
```bash
python3 python/cluster_test.py
```

The test suite automatically executes and verifies five critical distributed systems invariants:
1. **Cluster Boot & Leader Election:** Spawns 3 nodes, ensures quorum is established, and verifies an initial leader is elected.
2. **Replication Check:** Executes writes and confirms that every node's local state machine is synchronized.
3. **Fault Tolerance (Leader Crash):** Abruptly terminates (`SIGKILL`) the active leader node. Verifies that the remaining nodes detect missing heartbeats, elect a new leader, and continue committing new writes with a node dead.
4. **Consistency & Linearizability:** Performs `Append` operations and verifies sequential consistency while testing duplicate request suppression.
5. **Node Recovery & Synchronization:** Restarts the crashed node and verifies that the leader automatically streams all missing log entries to bring it up to date.

---

## 6. Execution Results & Screenshots

### Screenshot 1: Cluster Startup & Leader Election
Booting the 3-node cluster and electing Node 1 as initial leader (Term 1) with unanimous votes:
![Cluster Boot & Election](screenshots/01_cluster_boot_election.png)

---

### Screenshot 2: Replicated State Machine (Put & Get)
Client dispatches a `Put` request to the leader. The entry is replicated to Node 0 and Node 2 before committing:
![Replication Put Get](screenshots/02_replication_put_get.png)

---

### Screenshot 3: Fault Tolerance (Leader Crash & Failover)
Killing the leader node (Node 1). The remaining two nodes detect the failure, elect Node 2 as the new leader, and successfully commit new writes under majority quorum:
![Leader Crash & Failover](screenshots/03_leader_crash_failover.png)

---

### Screenshot 4: Consistency & Linearizability
Verifying atomic string `Append` operations and confirming that duplicate client RPC sequence numbers are ignored:
![Consistency Linearizability](screenshots/04_consistency_linearizability.png)

---

### Screenshot 5: Node Recovery & Catch-up
Rebooting the crashed node. The leader detects the lagging log index and automatically replicates missing entries to synchronize its state machine:
![Node Recovery Sync](screenshots/05_node_recovery_sync.png)

---

### Screenshot 6: Automated Test Suite (100% Pass)
Complete output of `python/cluster_test.py` confirming full compliance with replication, fault tolerance, consistency, and recovery requirements:
![Test Suite All Pass](screenshots/06_test_suite_all_pass.png)
