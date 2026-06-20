An HTTP-based distributed key-value store written in Go. It supports consistent hashing, leader-follower replication, write-ahead logging, snapshots, health tracking, configurable write consistency, structured logs, and metrics.

## Architecture

```text
Client
  |
  v
Any node HTTP API
  |
  +-- /kv/{key}
  |     |
  |     +-- consistent hash ring chooses key replicas
  |     +-- first healthy replica is the leader
  |     +-- non-leader nodes forward to leader
  |
  v
Leader node
  |
  +-- append PUT/DELETE to durable WAL
  +-- apply mutation to in-memory store
  +-- replicate concurrently to healthy followers
  +-- wait for one/quorum/all acknowledgments
  |
  v
Followers
  |
  +-- append replicated mutation to local WAL
  +-- apply mutation to in-memory store

Background tasks:
  - heartbeat checks update node health
  - snapshots persist full in-memory state
```

Data is held in memory for serving requests. Durability comes from a node-local WAL and periodic snapshot files under `data/{node_id}/`.

## API

### Put

```bash
curl -X PUT 'http://127.0.0.1:8081/kv/user:1?consistency=quorum' \
  -H 'Content-Type: application/json' \
  -d '{"value":"Ada"}'
```

### Get

```bash
curl http://127.0.0.1:8082/kv/user:1
```

### Delete

```bash
curl -X DELETE 'http://127.0.0.1:8083/kv/user:1?consistency=all'
```

### Cluster Nodes

```bash
curl http://127.0.0.1:8081/cluster/nodes
```

### Metrics

```bash
curl http://127.0.0.1:8081/metrics
```

Metrics include request counts, WAL appends, snapshots, replication results, forwarded requests, quorum failures, and node health.

## Consistency Model

Writes are routed to the key leader. The leader commits locally first by appending to the WAL and applying the in-memory mutation, then replicates to followers.

Supported write consistency levels:

- `one`: success after the leader commits locally.
- `quorum`: success after a majority of replicas acknowledge. With 3 replicas, this requires 2 acknowledgments.
- `all`: success only when every configured replica acknowledges.

The default is configured in each node config:

```json
"replication": {
  "virtual_nodes": 64,
  "factor": 3,
  "consistency": "quorum"
}
```

Per-request override:

```bash
curl -X PUT 'http://127.0.0.1:8081/kv/order:9?consistency=one' \
  -H 'Content-Type: application/json' \
  -d '{"value":"created"}'
```

Unhealthy nodes are skipped for routing and replication attempts. Skipped or failed replicas do not count as acknowledgments, so `quorum` and `all` can fail when too many replicas are unavailable.

## WAL And Snapshot Recovery

Every `PUT` and `DELETE` is appended to `data/{node_id}/wal.log` before being applied. WAL entries are newline-delimited JSON and include a monotonically increasing local sequence number.

Snapshots are written atomically to `data/{node_id}/snapshot.json` and contain:

- latest WAL sequence number included in the snapshot
- creation timestamp
- full key-value map

Startup recovery:

1. Load the latest snapshot, if present.
2. Restore the in-memory map from the snapshot.
3. Replay WAL entries with sequence numbers greater than the snapshot sequence.
4. Resume serving requests.

Snapshot interval can be set in config:

```json
"snapshot_interval": "30s"
```

Or overridden at launch:

```bash
go run ./cmd/kvstore -config configs/node1.json -snapshot-interval 10s
```

Use `-snapshot-interval 0` to disable periodic snapshots.

## Run 3 Local Nodes

From this directory:

```bash
go run ./cmd/kvstore -config configs/node1.json
```

Second terminal:

```bash
go run ./cmd/kvstore -config configs/node2.json
```

Third terminal:

```bash
go run ./cmd/kvstore -config configs/node3.json
```

Write and read through any node:

```bash
curl -X PUT 'http://127.0.0.1:8081/kv/account:42?consistency=quorum' \
  -H 'Content-Type: application/json' \
  -d '{"value":"active"}'

curl http://127.0.0.1:8082/kv/account:42
curl -X DELETE 'http://127.0.0.1:8083/kv/account:42?consistency=quorum'
```

## Failure Simulation

Start all three nodes, then stop one follower with `Ctrl-C`.

A quorum write should still succeed if two replicas are available:

```bash
curl -X PUT 'http://127.0.0.1:8081/kv/failure:test?consistency=quorum' \
  -H 'Content-Type: application/json' \
  -d '{"value":"survives-one-node-down"}'
```

An `all` write should fail while any replica is down:

```bash
curl -X PUT 'http://127.0.0.1:8081/kv/failure:all?consistency=all' \
  -H 'Content-Type: application/json' \
  -d '{"value":"requires-every-node"}'
```

Inspect health and quorum failures:

```bash
curl http://127.0.0.1:8081/cluster/nodes
curl http://127.0.0.1:8081/metrics
```

Restart the stopped node. Heartbeats will mark it healthy again after the next interval.

## Configuration

Each node loads one JSON config. The three sample configs use the same cluster membership and differ by `node_id`.

```json
{
  "node_id": "node1",
  "nodes": [
    { "id": "node1", "address": "127.0.0.1:8081" },
    { "id": "node2", "address": "127.0.0.1:8082" },
    { "id": "node3", "address": "127.0.0.1:8083" }
  ],
  "replication": {
    "virtual_nodes": 64,
    "factor": 3,
    "consistency": "quorum"
  },
  "request_timeout": "2s",
  "retry_count": 2,
  "data_dir": "data",
  "snapshot_interval": "30s",
  "heartbeat_interval": "2s"
}
```

## Tests

```bash
go test ./...
```

The tests cover hashing, routing, CRUD behavior, replication, WAL replay, snapshot recovery, quorum behavior, and unhealthy-node handling.
