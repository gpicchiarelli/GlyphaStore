# Prototipo volatile a coppia Reader–Writer

Status: sperimentale, lab-only (test e benchmark dedicati; non raggiungibile da `glyphastored`)
Applies to: prototipo volatile Reader–Writer e Reactor TCP a coppia singola sotto `src/experimental/`
Owner: storage, networking e performance maintainers
Last reviewed: 2026-07-31

ADR: [shard a coppie Reader–Writer](../adr/paired-reader-writer-shards.md)

## Scopo implementato

Il prototipo esegue una sola coppia con:

- un caller Reader-owner e un `std::thread` Writer persistente con stop/join espliciti;
- mutation e completion ring SPSC da 256 elementi;
- indici producer/consumer separati su allineamento 128 byte;
- pool di 256 mutation slot e value arena contigua allocati allo startup;
- completion credit implicito nello slot: lo slot torna libero solo dopo il completion pop;
- micro-batch Writer fino a 32 mutazioni, con deadline configurabile e default bilanciato di 2 us;
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

Il secondo stadio collega la coppia a un Reactor TCP cleartext sperimentale:

- protocollo v2 compatibile con il client C++ pubblico per `INIT`, `BIND_WORKER`, `PING`, `GET`,
  `PUT`, `ERASE`, `HEALTH` e `READY` su una sola coppia;
- parser, input/output watermark e connection slot bounded;
- pipeline ordinata con al massimo una mutazione in attesa di completion per connessione;
- risposta GET con `sendmsg` scatter/gather: header e valore non vengono concatenati;
- generation pin solo quando l'output del socket è parziale, rilasciato dopo l'ultimo byte;
- snapshot delle metriche pubblicata a fine event-loop turn: la lettura cross-thread non corre sui
  contatori Reader-locali e non aggiunge atomiche o mutex al normale GET;
- wakeup nativo Writer→Reader una volta per batch di completion;
- telemetria per richieste, response byte, `sendmsg`, write parziali, pin e backpressure.

Il GET usa soltanto il descriptor locale adottato dal Reader. I test verificano che 1.000 GET non
incrementino push/pop di nessuna ring.

## Confinamento

`paired_shard.cpp` e `paired_reactor.cpp` vengono compilati direttamente in `glyphastore_tests` e
nei target `glyphastore_paired_benchmark` / `glyphastore_paired_reactor_benchmark`. Non appartengono
a `glyphastore_core` o `glyphastore_server_core`, non sono installati e non sono raggiungibili da
`glyphastored`. Il prototipo non apre Segment persistenti e non cambia Store, wire v2 o persistence
v1. Questa separazione impedisce un'attivazione accidentale del runtime incompleto.

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
- Il Writer usa ancora yield polling per l'admission idle; il ritorno completion usa già il wakeup
  nativo del Poller. Affinity, adaptive spin/park e profili non sono ancora implementati.
- Il Reactor è cleartext, volatile e a coppia singola: non implementa TLS, handoff, durable cold
  read, Segment v1, durability, rotation, recovery o compaction.
- Il prototipo implementa una sola coppia e non certifica scaling.
- Il benchmark `glyphastore_paired_benchmark` resta un microbenchmark del motore; il nuovo
  `glyphastore_paired_reactor_benchmark` è invece wire-to-wire, usa lo stesso client pubblico e
  interlaccia baseline corrente e prototipo. Le latenze TCP attuali sono di completamento batch,
  non ancora istogrammi separati per singolo GET e PUT.

Di conseguenza i primi risultati A/B qualificano soltanto il potenziale del data path. Un successo
architetturale richiede ancora lo stesso protocol path, Segment immutabili, durability e multi-pair.

## Evidenza attuale

- suite Debug completa: 446 test, pass;
- suite ASan+UBSan completa: 446 test, pass;
- suite TSAN completa: 446 test, pass. Il primo run TCP ha individuato una race nella lettura delle
  metriche; la snapshot a confine di turn l'ha rimossa e il rerun completo è pulito;
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
- test TCP con il client C++ pubblico per CRUD, read-after-write e pipeline ordinata;
- test deterministico di output lento: write parziale, generation pin osservato e rilascio a drain;
- benchmark A/B TCP interleaved tra Reactor corrente e paired Reactor, con throughput e
  p50/p99/p99.9 di batch oltre alla telemetria di publication e output.
- GET TCP 64 B, pipeline 32: da +0,6% a +4,8% per 1–8 connessioni; il p99 batch non migliora in
  modo uniforme. A pipeline 128 il vantaggio throughput è +11,4%, mentre pipeline 1/8 resta entro
  una regressione del 2,5%.
- GET TCP 1 KiB/64 KiB/256 KiB: +21%, +233% e +221%; per i valori grandi scatter/gather riduce
  anche il p99 batch del 64–80% e il test osserva i pin soltanto sulle write parziali.
- mixed TCP 64 B, quattro connessioni, pipeline 32: -18% a 99/1, -54% a 95/5 e -73% a 90/10.
  Il batch publication medio è soltanto 1,07/1,25/1,43 record: handoff, publication e barriera di
  ordinamento per connessione dominano il piccolo valore.

## Prossimo gate

Il prototipo sotto `src/experimental/` resta **lab-only**: non è un secondo runtime selezionabile e
non verrà promosso in `glyphastored`. Il daemon di produzione è già il modello paired
([ADR 0031](../adr/paired-reader-writer-shards.md), [server model](server-model.md)).

I residuali di adozione e performance sono i gate P1 del piano di produzione, non un cutover del
prototipo volatile ([paired-shards-plan](../benchmarks/paired-shards-plan.md)):

1. P1 — Delta mixed magnitude: directory-chunk COW landed; conferma −1,8% solo su Linux
   hard-pinned ([paired-delta-directory-chunks-2026-07-31](../benchmarks/paired-delta-directory-chunks-2026-07-31.md));
2. P1 — `get-into` / scatter multi-extent: **rejected** pending bounded+win proof
   ([paired-get-into-multi-extent-reject-2026-07-31](../benchmarks/paired-get-into-multi-extent-reject-2026-07-31.md));
3. P1 — A/B 1/2/4/8 pair Linux hard-pinned: harness ready, waiting on `glyphastore-linux-perf`
   ([paired-shards-linux-p1](../benchmarks/paired-shards-linux-p1.md));
4. P1 — backend I/O Linux opzionale: **deferred** for 0.1.0 (no proven win without ordering risk).

Il lavoro lab sul prototipo TCP volatile (istogrammi GET/PUT distinti, affinity sperimentale,
allocator a classi di size) può continuare nei target dedicati, ma non è un gate di integrazione
nel daemon.
