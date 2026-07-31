# Paired generational Delta arena gate — 2026-07-29

## Esito

**Accettato con follow-up prestazionale P1.** Il Delta non possiede più un control block
`shared_ptr<ReadRecord>` per ogni versione pubblicata. I record immutabili sono celle da 64 B in
blocchi append-only Writer-owned; pagine Swiss e generazioni copiano solo puntatori stabili. Chiavi
lunghe e pin di Segment/file generation vivono nell'arena, che resta posseduta dallo stato Delta
fino alla quiescenza QSBR.

Il gate è positivo per safety, boundedness, memoria e GET volatile. Non è una vittoria universale:
PUT→GET perde circa 1,8% di throughput e il GET durable-periodic circa 1,5% su questo host macOS non
pinned. Questo costo residuo va profilato sul publication path e verificato su Linux hard-pinned;
non va attribuito al GET owner-bound, che migliora.

## Layout e invarianti

- `DeltaRecord` è trivially-copyable, allineato e grande esattamente una cache line (64 B);
- blocchi record da 64 celle e blocchi chiave da 4 KiB non spostano mai memoria pubblicata;
- il Writer costruisce record, chiave e pin prima di rendere il puntatore raggiungibile dalla nuova
  generation; publication release e adozione acquire sono il solo edge cross-thread;
- il Reader non consulta mai vettori, cursori o contatori mutabili dell'arena;
- durante il merge uno stato possiede al massimo arena cut e post-cut; la stessa cella post-cut viene
  referenziata dal delta cumulativo e da quello post-cut, senza doppia materializzazione;
- la capacità conta le **versioni**, non soltanto le chiavi logiche. Churn ripetuto sulla stessa
  chiave avvia il merge alla soglia e applica backpressure prima di esaurire l'arena;
- pin volatile e durevoli sono deduplicati per generazione; il fast path confronta prima l'ultimo
  pin, che è il caso normale dell'active Segment;
- reclamation di celle, chiavi e pin coincide con il retirement della generation dopo quiescenza.

Le metriche per pair espongono `delta_entries`, `delta_record_versions`, byte record allocati, byte
chiave usati e capacità dei blocchi chiave. Questo rende visibili overwrite amplification e slack.

## Metodo

- macOS arm64, Apple LLVM 21, Release, una shard pair e 8 client owner-bound;
- key 16 B, value 64 B;
- GET pipeline 32: 4 warm-up e 7 campioni da 50.000 operazioni;
- PUT→GET pipeline 1: 2 warm-up e 5 campioni da 100.000 coppie;
- stesso wire protocol, routing, batch e durability policy;
- RSS dell'intero processo; affinity macOS advisory, nessun hard pin.

I raw result pre/post e i run di conferma sono in
`benchmarks/results/paired-generational-delta-arena-2026-07-29/`.

## Risultati A/B

| scenario | throughput pre | post | delta | RSS pre | post | delta |
|---|---:|---:|---:|---:|---:|---:|
| GET volatile p32 | 2,100 Mops/s | 2,171 Mops/s | **+3,4%** | 47,56 MB | 45,58 MB | **−4,2%** |
| PUT→GET volatile p1 | 214,2 kops/s | 210,3 kops/s | **−1,8%** | 138,23 MB | 134,63 MB | **−2,6%** |
| GET durable-periodic p32 | 187,2 kops/s | 184,4 kops/s | **−1,5%** | 76,53 MB | 74,84 MB | **−2,2%** |

| scenario | p50 pre/post | p95 pre/post | p99 pre/post | p99.9 pre/post |
|---|---:|---:|---:|---:|
| GET volatile p32 | 121,1 / 116,2 µs | 141,4 / 123,9 µs | 162,5 / 133,7 µs | 191,2 / 161,3 µs |
| PUT→GET volatile p1 | 71,8 / 72,7 µs | 101,2 / 102,8 µs | 123,3 / 126,1 µs | 425,5 / 167,0 µs |
| GET durable-periodic p32 | 875,8 / 919,0 µs | 1,593 / 1,611 ms | 1,960 / 1,986 ms | 2,284 / 2,296 ms |

Il p99.9 baseline del PUT→GET contiene un outlier e non sostiene da solo un miglioramento. Due run
di conferma post hanno prodotto 209,8–210,3 kops/s: la piccola regressione mixed è ripetibile. Anche
il durable post precedente (185,4 kops/s) conferma una fascia da −1% a −1,5%, non un crollo.

## Verifica

- test unitario di 65 overwrite sulla stessa chiave più chiave esterna, con snapshot antico ancora
  leggibile e contabilità esatta dei blocchi;
- test integrazione del Writer: merge per entry distinte e merge attivato dalle versioni con una
  sola chiave logica, ritorno delle metriche arena a zero e valore finale corretto;
- suite Release: 467/467;
- Debug, ASan+UBSan, TSan e strict sono parte del gate finale del blocco.

## Decisione e seguito

Il terzo layout differisce dai due candidati respinti: non inserisce record inline nelle pagine COW
e non crea un blocco per publication. Ammortizza i metadati su 64 versioni, mantiene indirizzi
stabili e limita esplicitamente lo churn di overwrite. Per questo viene promosso nonostante il costo
mixed misurato: chiude ownership e amplificazione per-entry, riduce RSS e migliora il read path.

Il follow-up non deve cambiare lifetime o aggiungere una catena Delta. Va misurato con profilo
Writer separando `prepare`, pin retention, arena store, page COW, publication e completion. Il gate
successivo è Linux hard-pinned con `perf`; un'ottimizzazione è accettabile solo se conserva capacità
per versioni, publication atomica e QSBR.

## Follow-up (2026-07-31)

Candidate strutturale: directory-chunk COW
([`paired-delta-directory-chunks-2026-07-31.md`](paired-delta-directory-chunks-2026-07-31.md)).
La magnitude del recupero mixed resta da confermare su Linux hard-pinned.

