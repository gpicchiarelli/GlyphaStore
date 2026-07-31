# Paired Delta directory-chunk COW — 2026-07-31

## Esito

**Accettato come ottimizzazione strutturale bounded; magnitude mixed non chiusa su macOS.**
Il follow-up P1 al gate
[`paired-generational-delta-arena-2026-07-29.md`](paired-generational-delta-arena-2026-07-29.md)
riduce il costo di pubblicazione senza indebolire lifetime QSBR né la capacità per **versioni**.

La spine del directory Delta a capacità massima (~40 960 entry) esponeva ~256
`shared_ptr<DeltaDirectoryBlock>` copiati a ogni `DeltaBuilder`. Ogni PUT→GET a pipeline 1
pagava quel traffico di refcount anche per un solo record. Il layout a chunk introduce un
secondo livello COW (`DeltaDirectoryChunk`, 16 block per chunk): la publication copia ~16
puntatori di chunk e fa COW solo del chunk/blocco/pagina toccati.

Micro-opt collaterali, stesso lifetime:

- contatori O(1) per byte chiave arena (niente scan dei key block a ogni update stats);
- costruzione in-place della cella `DeltaRecord` da 64 B;
- cache last-page sul builder per batch multi-mutation.

## Invarianti preservati

- celle arena 64 B, blocchi da 64 record, chiavi lunghe in blocchi 4 KiB;
- capacità contata in versioni; merge/backpressure invariati;
- publication release / Reader acquire come unico edge cross-thread;
- reclamation solo dopo quiescenza QSBR;
- nessuna catena Delta aggiuntiva, nessun handle `shared_ptr<ReadRecord>` per versione.

## Metodo A/B (advisory macOS)

- stesso host arm64, Apple LLVM 21, Release;
- baseline = tree `fddb355` senza questo patch; candidate = patch;
- ordine interleaved `old/new/new/old` (+ ciclo di conferma);
- PUT→GET volatile p1: key 16 B, value 64 B, 1 pair, 8 client, warmup 2, repeats 5, 100 000 ops;
- GET volatile p32 di controllo;
- affinity Mach advisory; **non** hard-pinned.

Raw: `benchmark-results/paired-shards/fddb355-dirty/macos-arm64/2026-07-31-p1-delta-directory-chunks/`.

## Risultati

Lo spread termico/load sullo stesso host è ampio (campioni PUT→GET da ~60 k a ~208 k ops/s).
Non si dichiara il recupero completo del −1,8% storico contro il pre-arena.

| sottoinsieme | old median | new median | nota |
|---|---:|---:|---|
| tutti i campioni PUT→GET interleaved+confirm | ~122 k | ~149 k | spread dominante; non claim |
| campioni ≥150 k ops/s (meno throttled) | ~179 k | ~205 k | direzionale, ancora advisory |
| GET volatile p32 | 1,998 M | 1,967 M | ~−1,6%, entro rumore locale |

Nessun claim production-ready su throughput medio. Nessun `e3_certified=yes`.

## Verifica

- Debug `glyphastore_tests`: 471/471 sul tree candidate;
- test arena overwrite/version capacity invariati (`pair_read_generation_tests`);
- lifetime e merge path non ristrutturati oltre la spine directory.

## Decisione e seguito

Si promuove il chunked directory COW perché elimina una tassa strutturale sulla publication
single-record senza toccare capacità o QSBR. Il residual P1 di **magnitude** (quanto del −1,8%
misto storico è recuperato) resta aperto fino all'A/B Linux hard-pinned
([`paired-shards-linux-p1.md`](paired-shards-linux-p1.md)).
