# Paired borrowed cold-read lease — 2026-07-29

## Esito

**Accettato come sottoblocco P0 di safety e ownership.** Il cold GET del daemon paired non copia più
`PublishedReadRecord` e non incrementa più il `shared_ptr` del file pin per richiesta. Il task porta
una vista trivially-copyable di chiave, `RecordRef` e generation, valida esclusivamente entro una
lease di epoch Reader-private. Il percorso pubblico/legacy di `Store::get` conserva invece il token
owning: il borrowed lifetime non è diventato un contratto API.

Il risultato prestazionale è positivo a pipeline 1 e sostanzialmente neutro a pipeline 32. Le code
alte del run macOS non pinned sono miste: non viene attribuito al cambiamento un miglioramento della
tail latency. Il blocco è promosso perché elimina refcount e duplicazione della chiave mantenendo una
prova di lifetime esplicita e bounded; non perché dimostri da solo un salto di throughput.

## Protocollo di lifetime

- il Reader adotta una sola `PairReadGeneration` per event-loop turn;
- prima del trasferimento del task cold registra l'epoch in un tracker locale senza atomiche o
  allocazioni;
- la publication Writer osserva `min(epoch adottato, epoch cold ancora in volo)` come
  `reader_safe_epoch`;
- la retire list libera soltanto generation con `retired_epoch < reader_safe_epoch`;
- il task può quindi prendere in prestito key storage, pin descriptor e file generation dalla
  generation immutabile, senza un owner per richiesta;
- l'I/O helper materializza un `OwnedValue`; solo dopo la completion il Reader rilascia la lease;
- una connection chiusa cancella il task ma non anticipa il rilascio della lease;
- shutdown drena tutte le completion cold prima di fermare Writer e Store.

Il tracker ha 65 celle (`64` generation ritirate massime più la corrente). Saturazione o overflow
producono overload, mai crescita dinamica o reclamation forzata. Il test deterministico conserva un
task borrowed attraverso compaction, refresh, retirement del source e completion; verifica poi che
la generation venga reclamata solo dopo l'avanzamento esplicito dell'epoch sicuro.

## Benchmark

macOS arm64, Apple LLVM 21, Release, una pair, 8 client, key 16 B, value 64 B,
durable-periodic 1000 ms. Baseline: pool di slot cold con pin owning. Candidato: stessa struttura con
lease QSBR e pin borrowed. I raw result sono in
`benchmarks/results/paired-borrowed-cold-read-2026-07-29/`. I run sono sequenziali e
`cpu_pin_applied=0`; differenze piccole e variazioni RSS temporali sono indicative.

| workload | metrica | owning slot | borrowed lease | delta |
|---|---|---:|---:|---:|
| GET p32 | throughput | 185.892/s | 186.012/s | +0,06% |
| GET p32 | p50 | 909,3 µs | 870,3 µs | −4,28% |
| GET p32 | p95 | 1,628 ms | 1,634 ms | +0,36% |
| GET p32 | p99 | 1,978 ms | 2,018 ms | +2,04% |
| GET p32 | p99.9 | 2,268 ms | 2,453 ms | +8,14% |
| GET p32 | RSS mediana | 75,64 MB | 74,65 MB | −1,32% |
| GET p1 | throughput | 106.058/s | 112.984/s | +6,53% |
| GET p1 | p50 / p99 | 73,3 / 112,1 µs | 69,9 / 109,3 µs | −4,60% / −2,49% |
| GET p1 | p99.9 | 134,8 µs | 133,0 µs | −1,30% |
| GET p1 | RSS mediana | 74,37 MB | 80,13 MB | +7,75% |
| 99/1 p32 | throughput | 170.859/s | 173.874/s | +1,76% |
| 99/1 p32 | p50 / p99 | 787,0 µs / 3,791 ms | 752,8 µs / 3,809 ms | −4,34% / +0,49% |
| 99/1 p32 | p99.9 | 5,325 ms | 5,439 ms | +2,14% |
| 99/1 p32 | RSS mediana | 73,45 MB | 78,95 MB | +7,49% |

Il profilo p32 è il confronto più stabile: throughput e RSS restano neutrali, mentre il p99.9
peggiora in un run con spread non controllato. Pipeline 1 mostra il beneficio atteso dall'eliminazione
del refcount/copia record (+6,5%), ma l'RSS fra processi non è ripetibile. Il gate definitivo delle
code richiede A/B interleaved Linux hard-pinned; questi dati non autorizzano claim sul p99.9.

## Safety gate

- suite Debug: 459/459;
- allocation fault injection: pass;
- ASan + UBSan: 459/459;
- TSan: 459/459;
- build strict ISO C++23/hardening: pass;
- nessuna modifica a Manifest, Segment, Record, routing o wire format.

## Gate successivo

Il seguito è documentato in `paired-scatter-output-2026-07-29.md`: il cleartext grande non pipelined
usa una lease bounded e non copia più `OwnedValue` nel buffer di connessione. TLS e pipeline restano
owning/contigui per contratto e per risultato A/B. Resta P0 il pool preallocato delle mutation
key/value; `get-into` è una possibile ottimizzazione successiva, non una scorciatoia sui lifetime.
