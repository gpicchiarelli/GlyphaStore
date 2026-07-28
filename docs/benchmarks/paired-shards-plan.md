# Piano benchmark: shard a coppie Reader–Writer

Status: primo gate micro A/B eseguito; integrazione daemon non ancora autorizzata

ADR: [paired Reader–Writer shards](../adr/paired-reader-writer-shards.md)

Baseline iniziale: runtime corrente su `main`, stessa persistence v1 e wire v2

Risultati del 2026-07-28: `benchmark-results/paired-shards/fc9463d-dirty/macos-m4/2026-07-28/`.
Questo run di gate è versionato esplicitamente; gli artifact benchmark locali ordinari restano ignorati.

## Regole del confronto

Ogni confronto usa due binari dalla stessa base sorgente: runtime corrente e runtime paired. Build,
dataset, seed, client, filesystem, durability policy e limiti devono coincidere. L'ordine è
interleaved `old/new/new/old`; almeno due cicli, una warmup e sette sample misurati per cella. Si
conservano output raw, configurazione effettiva e commit in:

```text
benchmark-results/paired-shards/<commit>/<platform>/<run-id>/
```

Un run è invalido se perde risposte, request id, payload, mutation outcome o verifica finale. Client
e server sullo stesso host devono avere CPU disgiunte oppure il risultato è marcato combined-load.

## Matrice minima

| Asse | Valori |
|---|---|
| coppie | 1, 2, 4, 8 entro i core fisici |
| mix | GET 100%; GET/PUT 99/1, 95/5, 90/10 |
| distribuzione | uniform, Zipf, hot key; stesso shard e shard distribuiti |
| working set | L2, LLC, 4× LLC |
| value | 64 B, 1 KiB, 64 KiB, 256 KiB |
| pipeline | 1, 8, 32, 128 |
| connessioni | 1 e 4 per coppia; poi saturation sweep |
| durability | volatile, sync, group, periodic |
| writer batch | 1, 4, 16, 32, 128 |
| overlap | burst, queue full, rotation, compaction incrementale |

Read-after-write ha una cella dedicata: per la stessa connessione e coppia, ogni ACK di PUT è seguito
da GET e deve restituire esattamente la versione appena acknowledged.

## Metriche obbligatorie

- throughput e latenza p50/p95/p99/p99.9/max;
- admission→dequeue, dequeue→publication, publication→durability e durability→completion;
- profondità/high-water/full delle due SPSC;
- record e byte per batch/publication;
- CPU per Reader/Writer, IPC, branch miss, cache/LLC/dTLB miss;
- context switch, CPU migration, syscall e wakeup;
- byte copiati, allocazioni nel timed path e RSS componentizzato;
- generation vive/ritirate, retire backlog e delay;
- tempo e numero di socket con read interest disarmato per backpressure.

Su Linux si raccolgono `perf stat` e `perf record` separando PID/TID Reader e Writer. Su macOS si
usano Instruments Time Profiler/Counters e affinity/QoS dichiarati come advisory. Nessun contatore
non disponibile viene stimato da throughput.

## Gate per fase

### Prototipo volatile, una coppia

- zero mutex/queue/allocation sul GET verificato con instrumentation;
- due lookup massimi;
- read-after-write e queue wraparound corretti;
- memoria invariata dopo saturation/drain;
- p99 GET sotto 95/5 non peggiore della baseline oltre il rumore interleaved.

### Multi-pair

- efficienza GET almeno 80% da 1→numero di coppie fisiche disponibili;
- nessun traffico cross-pair dopo bind;
- nessuna CPU migration nella baseline Linux hard-pinned;
- pair lento non altera materialmente p99 degli altri pair.

### Durabilità

- outcome e recovery equivalenti alla suite v1;
- nessun ACK sync/group prima del durable boundary;
- queueing, publication e commit delay riportati separatamente;
- crash/fault matrix completa per append→publish→durable→completion.

### Compaction e QSBR

- nessuna attesa Reader per copy/commit;
- retire backlog e memoria restano bounded durante output socket lento;
- p99/p99.9 con overlap confrontati, non solo throughput;
- nessuna reclamation prima della quiescenza dimostrata da TSAN e fault gate deterministici.

## Esperimenti da non confondere con la baseline

- Reader/Writer su sibling SMT;
- oversubscription;
- PGO/native CPU solo sul paired;
- client e server concorrenti sugli stessi core;
- tmpfs contro filesystem reale;
- GET interno contro TCP;
- durability policy o batch differenti.

Questi run sono utili come sensitivity analysis, ma non sostengono un claim architetturale.

## Risultati richiesti

Il report finale deve mostrare valori assoluti, rapporto paired/current, min/median/max, dispersione e
ambiente completo. Un esito è `pass`, `fail` o `inconclusive`; valori dentro lo spread combinato sono
`inconclusive`. Il successo richiede insieme correttezza, boundedness e tail latency, non una media
ops/s più alta.

## Esito del primo gate e backlog

Il data path GET isolato passa il gate preliminare: 2,75× a 64 B e 6,11× a 1 KiB, con p99 inferiore.
Il prototipo nel suo insieme fallisce invece il throughput 95/5: 0,30× e 0,25× rispetto al current.
La causa primaria è la copia cumulativa del delta e dei payload durante ogni publication.

Ordine vincolante prima dell'integrazione Reactor:

1. P0 — storage payload stabile e `RecordView` compatto generation-pinned;
2. P0 — delta cut/double-buffer senza copia cumulativa e merge base incrementale;
3. P0 — instrumentation allocator/copy esatta e nuovo A/B 100/0 + 95/5;
4. P1 — Reactor sperimentale con ownership socket e output lease QSBR;
5. P1 — multi-pair 1/2/4/8, affinity/topologia e isolamento fra pair;
6. P1 — TCP pipeline 1/8/32/128 e valori 64 B–256 KiB a semantica equivalente;
7. P2 — Segment v1, durability sync/group/periodic, crash/fault/recovery;
8. P2 — compaction incrementale e reclamation con socket lento.

Non si procede al punto 4 finché il 95/5 non è almeno non-regressivo oltre lo spread combinato.
