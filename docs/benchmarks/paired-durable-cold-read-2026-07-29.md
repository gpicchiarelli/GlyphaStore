# Paired durable cold-read — audit del 2026-07-29

## Perimetro

Audit su macOS arm64, AppleClang 21, Release, protocollo raw-wire, routing owner-bound, chiavi 16 B e
valori 64 B. Il confronto parte dal percorso durable-periodic precedente al commit SPSC cold-read e
arriva al worktree `a65e454` con due ulteriori ottimizzazioni:

- un I/O helper e una SPSC privata per `ShardPair`, senza mutex/condition variable globale;
- scratch di record verificato riusabile e consumer-private per lane;
- cancellazione tramite epoch preallocata per connection slot, senza `shared_ptr`/allocazione per GET.

I raw summary locali sono in
`benchmark-results/paired-shards/a65e454-dirty/macos-arm64/2026-07-29-durable-cold-read/`.
Non essendoci hard affinity su macOS e non essendo i run alternati, variazioni inferiori a circa il
5% sono indicative, non conclusive.

## Risultati

| Workload | Topologia | Prima | Dopo | Δ throughput | p50 prima/dopo | p99 prima/dopo | p99.9 prima/dopo |
|---|---:|---:|---:|---:|---:|---:|---:|
| GET-only, pipeline 32 | 1 pair / 8 client | 162.351 | 179.973 ops/s | +10,9% | 961 / 924 µs | 2,30 / 2,06 ms | 3,07 / 2,41 ms |
| GET-only, pipeline 32 | 4 pair / 8 client | 159.791 | 159.108 ops/s | -0,4% | 885 / 864 µs | 2,19 / 2,06 ms | 2,93 / 3,49 ms |
| GET-only, pipeline 1 | 1 pair / 8 client | 99.209 | 94.677 ops/s | -4,6% | 82 / 78 µs | 185 / 156 µs | 261 / 212 µs |
| 99% GET / 1% PUT | 1 pair / 8 client | 143.796 | 167.718 ops/s | +16,6% | 797 / 809 µs | 4,08 / 3,89 ms | 5,49 / 5,37 ms |

Lo scratch riusabile, isolato prima della cancellazione a epoch, ha portato il GET-only 1-pair da
168.320 a 178.890 ops/s (+6,3%) e ha ridotto p95/p99/p99.9. L'epoch elimina un'altra allocazione e
il relativo refcount, con un guadagno più piccolo ma con ownership più semplice e bounded.

## Giudizio

Il blocco chiude la regressione locale del cold GET single-pair e migliora il mixed workload senza
spostare I/O sotto lock. Non chiude il gate di scalabilità durable: a 32 client e pipeline 32, una
coppia produce 180.484 ops/s e quattro coppie 176.309 ops/s. Lo stesso harness volatile raggiunge
3,76 Mops/s a quattro coppie, quindi il limite è specifico del materialization path durevole, non del
solo reactor TCP.

Il profilo corrente compie ancora, per ogni cold GET:

1. copia del `PublishedReadRecord` con incremento del pin `shared_ptr`;
2. allocazione del pImpl `PreparedColdRead`;
3. `pread` e verifica completa Record/CRC;
4. allocazione e copia del valore in `OwnedValue`;
5. wakeup I/O→Reader e copia nella coda di output del socket.

La singola lane I/O per coppia paga handoff/wakeup per ogni record. Aggiungere thread alla stessa
coppia violerebbe SPSC/ownership e maschererebbe il problema; la direzione corretta è ridurre
materializzazioni, copie e syscall, poi misurare batching/io_uring su Linux mantenendo l'ordering.

## Safety gate

- CTest completo: 37/37, incluse matrici crash e allocation fault;
- suite Debug: 453/453;
- ASan/UBSan: 453/453;
- TSan: 453/453;
- late completion/connection-slot reuse e shutdown con pin in-flight restano coperti;
- il nuovo scratch ha un test esplicito di riuso della capacità e validazione del valore.

## Priorità successive

1. **P0 — cold-read lease:** eliminare pImpl e refcount per richiesta con slot preallocati e QSBR che
   copra I/O e risposta, senza far uscire `RecordRef` o file handle non pinnati.
2. **P0 — get-into/scatter-gather:** una sola materializzazione bounded del valore e response lifetime
   esplicito; TLS richiede un percorso separato perché non preserva automaticamente iovec esterni.
3. **P1 — platform backend:** misurare `io_uring` su Linux e batching di completion; macOS mantiene il
   backend portabile finché un backend nativo dimostra una coda migliore.
4. **P1 — osservabilità:** contatori per lane (submit, wake, batch, service, scratch growth, bytes copied)
   e benchmark alternato 1/2/4/8 pair con CPU/NUMA affinity reale su Linux.

Il P0 refresh è chiuso dal protocollo per-shard documentato nell'ADR e dal relativo audit A/B in
`paired-generation-refresh-2026-07-29.md`.

Aggiornamento 2026-07-29: i punti 1 e 2 sono stati chiusi rispettivamente dai gate
`paired-borrowed-cold-read-2026-07-29.md` e `paired-scatter-output-2026-07-29.md`; scatter è adattivo,
mentre TLS e connessioni pipelined restano contigui per contratto e benchmark.
