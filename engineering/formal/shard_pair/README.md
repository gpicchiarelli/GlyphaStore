# ShardPair TLA+ model (Phase B1)

Reduced formal model of one paired Reader–Writer shard (publish/adopt/shutdown drain).

Linked requirement: `GS-CONCUR-TLA-001`.

## Run TLC

```bash
export TLA2TOOLS_JAR=/path/to/tla2tools.jar
./engineering/formal/shard_pair/run-tlc.sh
```

CI: `.github/workflows/formal-shard-pair.yml` (best-effort, bounded timeout).

## Residual gaps

Durable I/O, compaction leases, multi-shard routing, and full Store API linearizability
are out of model scope (see the C++ history checker for the latter).
