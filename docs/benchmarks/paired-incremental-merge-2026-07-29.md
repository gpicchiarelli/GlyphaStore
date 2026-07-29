# GlyphaStore — incremental delta→base merge (2026-07-29)

## Esito

Il merge incrementale elimina il principale head-of-line blocking del Writer quando il delta
raggiunge la soglia. Nel carico volatile PUT→GET con 50.000 chiavi uniche, quindi sei attraversamenti
della soglia da 8.192 entry, il p99 scende da 89,5 ms a 2,77 ms e il massimo tempo di servizio da
187,8 ms a 1,98 ms. Il throughput cresce del 164%. Il costo osservato è circa +6,6% di RSS mediana
durante il carico di merge.

Il gate non autorizza ancora una dichiarazione production-ready: la latenza centrale PUT→GET sale di
circa il 5%, il GET durable-periodic mostra un p99.9 rumoroso e la memoria conserva handle/chiavi
duplicati che devono essere compattati nel prossimo P0.

## Ambiente e metodo

- Apple arm64, macOS, Apple LLVM 21.0.0;
- build Release, nessuna affinity hard disponibile (`cpu_pin_applied=0`);
- protocollo TCP raw, una shard pair, 8 client owner-bound, pipeline 32;
- 2 warmup e 5 campioni; i valori riportati sono mediane, salvo i massimi espliciti;
- baseline: worktree detached del commit `a65e454`, con merge monolitico;
- candidato: working tree dello stesso commit con merge incrementale e i blocchi durable precedenti;
- i run old/new non sono interleaved; variazioni piccole vanno quindi trattate come indicative.

I raw output completi sono in `benchmarks/results/paired-incremental-merge-2026-07-29/`.

## A/B — PUT→GET volatile attraverso il merge

50.000 operazioni logiche generano 100.000 frame misurati. Le chiavi sono uniche: il delta attraversa
la soglia sei volte e non misura soltanto update di entry già presenti.

| Metrica | Monolitico | Incrementale | Delta |
|---|---:|---:|---:|
| throughput | 110.467 frame/s | 291.718 frame/s | +164,1% |
| tempo/frame | 9.052,5 ns | 3.428,0 ns | −62,1% |
| p50 | 897,3 µs | 945,0 µs | +5,3% |
| p95 | 1,852 ms | 1,948 ms | +5,2% |
| p99 | 89,48 ms | 2,775 ms | −96,9% (32,3×) |
| p99.9 | 187,49 ms | 4,249 ms | −97,7% (44,1×) |
| queue wait mediano | 88,91 µs | 8,32 µs | −90,6% |
| queue wait massimo | 187,78 ms | 1,835 ms | −99,0% (102,4×) |
| servizio mediano | 18,90 µs | 5,12 µs | −72,9% |
| servizio massimo | 187,78 ms | 1,984 ms | −98,9% (94,6×) |
| RSS mediana | 58,90 MB | 62,80 MB | +6,6% |
| RSS picco | 63,47 MB | 67,35 MB | +6,1% |

Il p50/p95 leggermente peggiore non viene nascosto: il doppio aggiornamento del delta durante il
post-cut e il layout ancora basato su `shared_ptr` hanno un costo centrale. Il risultato importante è
la rimozione della pausa di coda di circa 188 ms; il prossimo layout compatto deve recuperare anche il
costo centrale senza riaprire il tail.

## Gate GET steady-state

| Modalità | Metrica | Prima | Dopo | Delta |
|---|---|---:|---:|---:|
| volatile | throughput | 1,952 Mops/s | 2,248 Mops/s | +15,2% |
| volatile | p50 | 121,5 µs | 109,3 µs | −10,0% |
| volatile | p95 | 145,0 µs | 125,0 µs | −13,8% |
| volatile | p99 | 183,0 µs | 178,8 µs | −2,3% |
| volatile | p99.9 | 232,0 µs | 271,8 µs | +17,2% |
| durable-periodic | throughput | 179.534 ops/s | 179.528 ops/s | neutro |
| durable-periodic | p50 | 948,7 µs | 926,5 µs | −2,3% |
| durable-periodic | p95 | 1,731 ms | 1,754 ms | +1,3% |
| durable-periodic | p99 | 2,152 ms | 2,192 ms | +1,9% |
| durable-periodic | p99.9 | 2,799 ms | 3,429 ms | +22,5% |

Il campione volatile chiude il gate di non regressione del normale GET a p99, ma non quello p99.9.
Il durable-periodic è neutro in throughput e misto nelle code; la dispersione fra campioni e
l'assenza di affinity impediscono di attribuire il p99.9 al layout con sufficiente confidenza.

## Invarianti verificate

- Reader: massimo due lookup (`D` poi `B`), nessun mutex e nessuna coda nel GET owner-bound;
- Writer: un solo mutatore, builder e cursori di merge interamente thread-local;
- publication: un singolo puntatore release/acquire a una generation immutabile;
- post-cut: ogni mutazione aggiorna il delta cumulativo visibile e il solo `Dpost` privato;
- lavoro: un quantum conta al massimo 4.096 slot, compresi quelli vuoti;
- memoria: delta hard-cap 40.960 entry; post-cut default 32.736; nessuna crescita unbounded;
- backpressure: capacità verificata prima della mutazione Store;
- refresh: rotation/compaction può sostituire la generation e annullare il merge mantenendo i pin;
- recovery: nessun byte persistente o autorità v1 è stato modificato.

## Validazione

- Debug: 458 test, 0 failure;
- ASan + UBSan: 458 test, 0 failure;
- TSan: 458 test, 0 failure;
- allocation fault injection: pass;
- crash matrix sync/periodic/group e daemon: 8/8 pass;
- diff whitespace: pulito.

## Decisione

Il merge incrementale è promosso nel runtime paired production. Il vecchio merge monolitico resta
solo nell'API test/oracle e non viene chiamato dal `PairWriterPool`. Il prossimo P0 è il layout
read-only compatto: rimuovere un `shared_ptr` per entry e le chiavi duplicate, misurando separatamente
lookup base/delta e memoria durante la doppia generation.
