# ShardPair TLA+ model (Phase B1)

Reduced formal model of one paired Reader–Writer shard (publish/adopt/shutdown drain).

Linked requirement: `GS-CONCUR-TLA-001`.

## Run TLC

```bash
export TLA2TOOLS_JAR=/path/to/tla2tools.jar
./engineering/formal/shard_pair/run-tlc.sh
```

CI: `.github/workflows/formal-models.yml` (required bounded run; TLC errors and timeout fail closed).

## Residual gaps

Durable I/O, compaction leases, multi-shard routing, and full Store API linearizability
are out of this model's scope (see the separate persistence model and the C++ history checker).
