# Paired inline cold-read slots — 2026-07-29

## Esito

**Accettato come sottoblocco P0.** `PreparedColdRead` non alloca più un pImpl per richiesta. Lo stato
move-only con il pin esatto della generazione file vive inline in un pool bounded per lane; le due
SPSC trasportano soltanto indici di slot. Il refcount del pin e le copie del valore restano aperti:
questo blocco non dichiara completato il protocollo cold-read lease.

## Ownership e sincronizzazione

- il Reader è l'unico producer di `submitted` e consumer di `recycled`;
- l'I/O helper è l'unico consumer di `submitted` e producer di `recycled`;
- ogni indice identifica uno slot stabile preallocato;
- il release/acquire di `submitted` trasferisce al consumer task, `RecordRef` e pin owning;
- il consumer pubblica la completion, distrugge il task, restituisce lo slot con release e solo dopo
  risveglia il Reader;
- shutdown drena la lane e distrugge ogni pin prima di `Store::close`.

Non viene esposto alcun `RecordRef`, file handle o Segment generation raw senza un owner. Lo storage
inline usa costruzione/distruzione esplicita, con limite e allineamento verificati a compile time.

## Benchmark

macOS arm64, Apple LLVM 21, Release, una pair, 8 client, key 16 B, value 64 B,
durable-periodic 1000 ms. I raw result sono in
`benchmarks/results/paired-inline-cold-read-2026-07-29/`.

Il confronto più vicino e omogeneo usa 4 warm-up e 7 campioni, pipeline 32:

| metrica | pImpl baseline | inline + slot pool | delta |
|---|---:|---:|---:|
| throughput | 186.431 ops/s | 185.892 ops/s | −0,3% |
| p50 | 876,8 µs | 909,3 µs | +3,7% |
| p95 | 1,605 ms | 1,628 ms | +1,4% |
| p99 | 1,969 ms | 1,978 ms | +0,4% |
| p99.9 | 2,291 ms | 2,268 ms | −1,0% |
| RSS mediano | 74,29 MB | 75,64 MB | +1,8% |

Tutte le differenze temporali sono entro il rumore atteso del run macOS senza hard affinity. Il
primo candidato inline che spostava l'intero payload nella ring aveva p99 +9,5%, p99.9 +21,6% e RSS
+7,5%; è stato sostituito dal pool di slot, non accettato in quella forma.

Profili aggiuntivi correnti:

- pipeline 1: 106.058 ops/s, p50 73,3 µs, p99 112,1 µs, p99.9 134,8 µs;
- 99/1 GET/PUT pipeline 32: 170.859 ops/s, p50 787,0 µs, p99 3,79 ms, p99.9 5,33 ms.

I confronti storici di questi due profili includono anche i blocchi Base/merge intermedi e sono
quindi conservati solo come controllo di non-regressione, non come attribuzione causale.

## Safety gate

- CTest 37/37, incluse 8 matrici crash e allocation fault;
- suite Debug 459/459;
- ASan/UBSan 459/459;
- TSan 459/459;
- build `unix-strict` pulita;
- coperti saturazione cold-read, completion tardiva su connection slot riusato, source retirement
  concorrente e shutdown con pin in-flight.

## Prossimo gate

Estendere QSBR con una lease esplicita per I/O e output, poi rendere borrowed key/pin soltanto entro
quella lease. Solo dopo si può eliminare il refcount `PublishedReadPin`. La materializzazione deve
passare a get-into/scatter-gather con percorso TLS separato; un raw pointer senza epoch pin non è una
scorciatoia ammessa.
