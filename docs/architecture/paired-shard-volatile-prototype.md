# Prototipo volatile a coppia Reader–Writer

Status: sperimentale, confinato al target `glyphastore_tests`

Data: 2026-07-28
ADR: [shard a coppie Reader–Writer](../adr/paired-reader-writer-shards.md)

## Scopo implementato

Il prototipo esegue una sola coppia con:

- un caller Reader-owner e un `std::jthread` Writer persistente;
- mutation e completion ring SPSC da 256 elementi;
- indici producer/consumer separati su allineamento 128 byte;
- pool di 256 mutation slot e value arena contigua allocati allo startup;
- completion credit implicito nello slot: lo slot torna libero solo dopo il completion pop;
- micro-batch Writer fino a 32 mutazioni senza attesa artificiale;
- `MutableDeltaIndex` Writer-private e publication `base + frozen delta`;
- merge del delta alla soglia configurata, senza catene oltre due livelli;
- publication tramite un solo descriptor immutabile release/acquire;
- `ImmutableReadIndex` read-only Swiss-style con control group da otto slot, load massimo 0,75,
  matching SIMD/scalar già condiviso con l'Index di produzione e nessun tombstone;
- `StableRecord` allocato una sola volta per mutazione; base, delta e generation condividono soltanto
  handle compatti e il GET non modifica il refcount;
- delta persistente a pagine immutable da 16 slot: i delta piccoli restano flat; quelli grandi
  separano una directory di ownership persistente a due livelli da una vista piatta di puntatori
  non-owning. La `ReadGeneration` pinna l'ownership e il GET conserva un accesso diretto alla pagina;
- micro-batch configurabile per record e deadline: il profilo bilanciato usa 32 record/2 us, mentre
  il benchmark espone entrambe le soglie per non confondere throughput e latenza di visibilità;
- pool di 258 generation slot e QSBR basato sui turn del Reader, senza refcount sul GET o
  sull'adozione;
- PUT, ERASE, TTL, read-after-write e drain di shutdown;
- metriche di queue, batch, publication latency/storage, copia ingress e generation retirement.

Il GET usa soltanto il descriptor locale adottato dal Reader. I test verificano che 1.000 GET non
incrementino push/pop di nessuna ring.

## Confinamento

`paired_shard.cpp` viene compilato direttamente dentro `glyphastore_tests`. Non appartiene a
`glyphastore_core`, non è raggiungibile dal daemon, non apre Segment e non cambia Store, wire v2 o
persistence v1. Questa separazione impedisce un'attivazione accidentale del runtime incompleto.

## Reclamation implementata

Il Reader incrementa un contatore di turn quiescenti prima di adottare con acquire il descriptor
corrente. Il Writer pubblica con release un solo puntatore e ritira la generation precedente solo
dopo due ulteriori confini quiescenti. Il margine di due turn copre l'adozione concorrente che può
ancora avere osservato il descriptor precedente. Lo span restituito da GET resta valido fino alla
successiva `adopt_publication()` dello stesso Reader.

Il pool contiene `queue_capacity + 2` slot: è sufficiente per tutte le mutazioni già accettabili
anche se il Reader tarda ad adottare. Pool pieno sospende la publication Writer e quindi applica
backpressure bounded; non libera mai una generation anticipatamente. Durante shutdown due turn
forzati liberano il backlog preesistente, mentre la capacità residua copre tutto il drain ammesso.

## Limiti intenzionali

- L'arena delle richieste e il pool generation sono bounded. Ogni nuova versione alloca ancora uno
  `StableRecord`, le pagine delta toccate e le strutture di publication. Un PMR generalista e uno
  slab `StableRecord` Writer-only sono stati misurati e respinti: hanno peggiorato il 95/5 e lo slab
  tratteneva capacità payload fino all'high-watermark. Un allocator production richiede classi di
  size e policy di retention, non un pool unico applicato indiscriminatamente.
- Il Writer usa yield polling; wakeup, affinity e profili non sono ancora implementati.
- Non esistono ancora SegmentView, durability, rotation, recovery, compaction o Reactor socket.
- Il prototipo implementa una sola coppia e non certifica scaling.
- Il benchmark A/B è un microbenchmark del motore: la baseline corrente restituisce `OwnedValue`
  copiato, il paired restituisce uno span. Non è un confronto wire-to-wire.

Di conseguenza i primi risultati A/B qualificano soltanto il potenziale del data path. Un successo
architetturale richiede ancora lo stesso protocol path, Segment immutabili, durability e multi-pair.

## Evidenza attuale

- suite Debug completa: pass;
- suite ASan+UBSan completa: pass;
- suite TSAN completa sul primo prototipo: pass;
- suite Debug, ASan+UBSan e TSAN completa sulla directory persistente/vista pinzata: pass (441 test);
- test concorrente SPSC con 100.000 elementi e wraparound;
- saturation di tutti i 256 completion credit senza perdita;
- riuso slot dopo drain;
- publication/ACK/read-after-write;
- merge ripetuti, tombstone, TTL e shutdown drain.
- 512 publication consecutive con reclamation QSBR bounded e nessuna backpressure;
- benchmark A/B interleaved GET 100% e GET/PUT 95/5 nel target dedicato
  `glyphastore_paired_benchmark`.
- secondo gate P0: delta paged e record stabili portano il 95/5 a 11,30 Mops/s (64 B) e
  10,90 Mops/s (1 KiB), superando rispettivamente la baseline di 2,15× e 4,57×.
- directory ownership persistente + vista Reader piatta riducono del 65% circa le copie di handle
  owning nel profilo 4.096 chiavi; deadline 2 us mantiene il gate bilanciato, mentre 8 us raggiunge
  11,59/11,55 Mops/s mixed a 64 B/1 KiB con un diverso contratto di visibilità.

## Prossimo gate

1. collegare una coppia a un Reactor sperimentale dietro flag non-default;
2. aggiungere multi-pair e affinity, poi ripetere la matrice di scalabilità;
3. progettare allocator per classi di size con retention bounded, dopo profiling del percorso TCP;
4. integrare Segment v1 e durability solo dopo i gate precedenti.
