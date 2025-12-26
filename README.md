# Raft KV Store (Learning Project)

A small distributed **Key/Value store** implemented on top of the **Raft consensus algorithm**.

- **Node↔Node communication:** gRPC (Raft RPCs)
- **External client API:** HTTP + JSON (simple PUT/GET)
- Goal: learn Raft concepts step-by-step (correctness first), not performance.

---

## What this project implements

### Core Raft concepts
- **Leader election** (terms, voting, election timeouts)
- **Log replication** (AppendEntries heartbeats + entries)
- **Commit & apply pipeline**
  - `commit_index`: highest **committed** (cluster-agreed) log index
  - `last_applied`: highest log index **applied locally** to the state machine (KV)
- **Linearizable writes**
  - `PUT` returns success only after the entry is committed (majority replication)
- **Linearizable reads (leader-based)**
  - Reads are served by the leader; optionally confirm leadership via quorum before serving
- **Durability**
  - Persist `current_term`, `voted_for`, and `log` (Raft’s durable state)
  - Rebuild KV by replaying committed log entries (and/or loading snapshot)
- **Snapshots + log compaction**
  - Persist a snapshot of KV + `(snap_last_idx, snap_last_term)`
  - Truncate committed prefix of the log to prevent unbounded growth
- **InstallSnapshot RPC**
  - If a follower falls behind the leader’s snapshot boundary, the leader sends the snapshot so the follower can catch up.

---

## Step-by-step development roadmap (how we built it)

### Step 1 — Minimal cluster harness
- Create multiple nodes (A/B/C)
- Fake “network” first (in-memory bus) to validate algorithm flow

### Step 2 — Election timer + roles
- Follower → Candidate on timeout
- Candidate requests votes
- Candidate becomes Leader on majority votes
- Leader sends heartbeats periodically

### Step 3 — RequestVote RPC
- Vote once per term (`voted_for`)
- Only vote if candidate log is **up-to-date**
  - `up_to_date = (cand_last_term, cand_last_idx) >= (my_last_term, my_last_idx)`

### Step 4 — AppendEntries RPC (heartbeats + replication)
- Heartbeat: AppendEntries with no entries
- Replication: AppendEntries includes log entries
- Followers reject if `prev_idx/prev_term` doesn’t match

### Step 5 — Commit & apply
- Leader tracks `match_index` and `next_index` per peer
- Leader advances `commit_index` when a majority has an index
- Each node applies committed entries in order:
  - while `last_applied < commit_index`: apply next entry to KV

### Step 6 — Linearizability
- **Linearizable write:** ack only after commit
- **Linearizable read:** serve reads from leader (optionally prove leadership via quorum heartbeat/read-index style check)

### Step 7 — Durability
Persist before “acting”:
- `current_term`
- `voted_for`
- `log`
Recover by loading persisted state on startup.

### Step 8 — Snapshot + compaction
- Create snapshot from **applied** KV and snapshot metadata:
  - `snap_last_idx`, `snap_last_term`
- Truncate log prefix covered by snapshot

### Step 9 — InstallSnapshot
- When follower is behind snapshot:
  - leader sends snapshot
  - follower replaces KV + snapshot metadata
  - follower trims its log accordingly
  - follower advances `commit_index`/`last_applied` to snapshot index

### Step 10 — Real networking
- Replace in-memory bus with gRPC:
  - `RequestVote`
  - `AppendEntries`
  - `InstallSnapshot`
- Add error handling: peer-down errors must NOT crash the node (treat as failed RPC)

### Step 11 — External JSON API
- Add HTTP endpoints for clients:
  - `PUT /kv/{key}` body: `{"value":"..."}`
  - `GET /kv/{key}`
- Followers return `409 not_leader` + leader hint so clients can redirect.

---

## Running the cluster (3 processes)

Each node runs:
- Raft core + background tasks
- gRPC server for node↔node RPC
- HTTP server for external client API
- its own data directory (durable state + snapshot)

Example ports:
- Node A: gRPC `50051`, HTTP `8001`
- Node B: gRPC `50052`, HTTP `8002`
- Node C: gRPC `50053`, HTTP `8003`

Start each node in its own terminal:

```bash
python run_node.py \
  --node-id A --grpc-port 50051 --http-port 8001 --host 127.0.0.1 \
  --grpc-peers "B=127.0.0.1:50052,C=127.0.0.1:50053" \
  --http-peers "A=http://127.0.0.1:8001,B=http://127.0.0.1:8002,C=http://127.0.0.1:8003" \
  --data-dir data/A
