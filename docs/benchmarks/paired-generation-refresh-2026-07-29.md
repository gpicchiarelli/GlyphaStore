# Paired durable generation refresh — audit del 2026-07-29

## Obiettivo

Rotation e compaction sostituiscono i pin del catalogo runtime. La `ReadGeneration` paired precedente
era corretta perché possedeva i descriptor esatti, ma li tratteneva fino al restart. Questo blocco
aggiunge refresh e reclamation senza spostare I/O o costruzione dell'indice sul Reader.

## Protocollo verificato

- revisione monotona **per shard**, incrementata dopo l'installazione del nuovo catalogo runtime;
- snapshot coerente `Index + exact generation pins + revision` sotto Worker/catalog lock, senza I/O;
- costruzione della base immutabile sul Writer e publication release del singolo generation pointer;
- `visible_through` monotono anche quando la sequenza più alta appartiene a una chiave cancellata;
- vecchia generation in retire list bounded, liberata dopo adozione acquire e quiescenza Reader;
- wakeup di reclamation anche senza nuove mutazioni;
- retry non distruttivo su `resource_exhausted`, fail-closed sulle incoerenze;
- nessun refresh cross-shard: una compaction della pair 0 lascia invariata la pair 1.

Le metriche `read_catalog_revision`, `read_refresh_attempts/successes/failures/deferrals`,
`generations_retired` e `retired_generation_count` sono disponibili per lane tramite `STATS`.

## A/B steady-state

macOS arm64, AppleClang 21, Release, durable-periodic, 64 B, 1 pair, 8 client, pipeline 32,
50.000 operazioni, 2 warmup e 5 campioni. Il binario “prima” è stato eseguito immediatamente prima
del rebuild dello stesso worktree; macOS non applica hard affinity.

| Variante | Throughput | p50 | p95 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| prima del revision poll | 180.710 ops/s | 900 µs | 1,636 ms | 2,009 ms | 2,384 ms |
| refresh per-shard | 179.476 ops/s | 917 µs | 1,653 ms | 2,020 ms | 2,431 ms |
| delta | -0,68% | +1,89% | +1,05% | +0,55% | +1,94% |

La differenza è entro il rumore operativo atteso e non costituisce una regressione dimostrata. Il
run aggiuntivo 4 pair / 32 client ha prodotto 170.849 ops/s; conferma che la scalabilità del durable
cold materialization resta il collo di bottiglia già identificato, non prova un effetto del refresh
perché manca un A/B alternato con affinity.

## Test e limiti

- Debug: 453/453;
- ASan/UBSan: 453/453;
- TSan: 453/453;
- test end-to-end: rotazione → compaction → cambio `RecordRef` → refresh → adozione → retirement;
- test multi-pair: revision e rebuild confinati allo shard compattato.

Il refresh ricostruisce oggi l'intera base sul Writer. Non blocca GET, ma può aumentare la latenza
PUT dello shard con dataset grandi. Il prossimo lavoro architetturale sulla publication è quindi il
merge incrementale/compacted immutable index; non va spostato sul Reader e non va risolto con una
catena non bounded di delta.
