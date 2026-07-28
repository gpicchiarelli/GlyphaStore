# Paired production GET — audit A/B del 2026-07-28

Confronto locale macOS arm64, AppleClang 21, Release, fra `94aae2d` e il worktree della migrazione
paired. Un warmup e tre campioni; mediana; chiavi da 16 byte, valori da 64 byte. Il raw summary è in
`benchmark-results/paired-shards/94aae2d-dirty/macos-arm64/summary.csv` (directory locale ignorata da
Git per policy). Pipeline 32 misura il completamento progressivo del batch, non il lookup isolato.

| Workload | Topologia | Δ throughput | Δ p50 GET | Δ p95 GET | Δ p99 GET | Δ p99.9 GET | Δ RSS |
|---|---:|---:|---:|---:|---:|---:|---:|
| GET-only | 4 pair / 8 client | +3,65% | -2,90% | -1,84% | -1,23% | -7,35% | +36,6% |
| GET-only | 1 pair / 8 client | -1,90% | -0,74% | +14,81% | +12,34% | +37,18% | +36,8% |
| 99% GET / 1% PUT | 1 pair / 8 client | -13,90% | -7,77% | +24,14% | -4,66% | -0,92% | +36,5% |
| PUT→GET, pipeline 1 | 1 pair / 1 client | -51,80% | +88,76% | +97,34% | +71,05% | +59,86% | +29,6% |

Giudizio: il GET owner-bound è ora realmente privo di mutex e scala positivamente con quattro pair,
ma il gate complessivo non è chiuso. Il doppio handoff penalizza fortemente le mutazioni sincrone; il
doppio livello penalizza alcuni casi single-pair; gli handle e le chiavi duplicate aumentano troppo
la memoria.

Durante l'A/B è stata scartata una prima publication che ricostruiva il delta a ogni ACK: nel 99/1
produceva p99.9 GET nell'ordine di 14 ms. La versione misurata nella tabella usa pagine Swiss
copy-on-write, directory gerarchica e QSBR bounded; ha riportato il p99.9 99/1 a ~320 µs, in parità
col baseline.

P0 successivi:

1. merge delta→base incrementale e non monolitico;
2. `ImmutableReadIndex` compatto senza `shared_ptr` per entry né doppia copia delle chiavi;
3. mutation slot pool preallocato;
4. telemetry di publication/reclaim e benchmark Reader/Writer su connessioni separate;
5. scatter/gather/get-into;
6. generation durevole con pin di file e bootstrap recovery.
