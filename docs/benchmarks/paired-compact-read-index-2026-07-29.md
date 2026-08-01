# GlyphaStore — Base Index compatto (2026-07-29)

## Esito

Il Base Index del Reader non conserva più un `shared_ptr`, una `std::string` e un pin owning per
entry. Ogni record read-only occupa al massimo 64 byte, conserva inline le chiavi fino a 16 byte e
usa un'arena a blocchi per le chiavi lunghe. I pin di Segment e file generation sono deduplicati per
generation. Control byte, hash e slot sono piatti nel layout pubblicato; il Writer inizializza i
control byte a quanta prima di copiare Base e Delta, quindi non reintroduce una pausa monolitica.

Il risultato finale migliora throughput e memoria nei carichi durable e PUT→GET. Sul GET volatile
puro il throughput mediano resta circa il 3% sotto la baseline, mentre p95–p99.9 e RSS migliorano
fortemente. Nel PUT→GET p95–p99.9 sono circa il 3–4% peggiori nel campione finale, nonostante
throughput e RSS migliori. Il blocco è quindi promosso come fondazione bounded e memory-safe, ma non
chiude ancora il gate prestazionale Reader: servono A/B interleaved con affinity reale e perf/CPU
counters su Linux prima di dichiarare il Base definitivo.

## Layout e invarianti

- `CompactReadRecord <= 64 B`, verificato da `static_assert`;
- chiavi `<= 16 B` inline; chiavi maggiori in blocchi stabili da almeno 64 KiB;
- un solo pin owning per Segment/generazione file distinto, mai uno per record;
- puntatori delle entry stabili: `records_` riserva la dimensione massima prima della costruzione;
- tre array piatti per control byte, hash e puntatore record nel Base pubblicato;
- memoria riservata prima del merge e control byte inizializzati in quantum bounded;
- Reader vede gli array soltanto dopo il publish release della generation completa;
- Delta ancora paginato COW e massimo due lookup per GET (`Delta`, poi `Base`);
- nessuna modifica a Record, Segment, Manifest, routing o wire v1/v2.

## Ambiente e metodo

- macOS arm64, Apple LLVM 21, Release;
- una shard pair, 8 client owner-bound, pipeline 32, protocollo raw-wire;
- 50.000 chiavi da 16 B e valori da 64 B;
- baseline: 2 warmup e 5 campioni immediatamente prima del layout compatto;
- candidato finale: 4 warmup e 7 campioni per GET, 2 warmup e 5 campioni per PUT→GET;
- `cpu_pin_applied=0`: su macOS i run non sono hard-pinned e differenze piccole restano indicative.

I raw output, compresi i layout intermedi rifiutati, sono in
`benchmarks/results/paired-compact-read-index-2026-07-29/`.

## A/B finale

| Workload | Metrica | Prima | Dopo | Delta |
|---|---|---:|---:|---:|
| GET volatile | throughput | 2,194 M/s | 2,128 M/s | −3,02% |
| GET volatile | RSS mediana | 48,43 MB | 45,84 MB | −5,35% |
| GET volatile | p50 | 111,9 µs | 118,0 µs | +5,44% |
| GET volatile | p95 | 209,8 µs | 127,0 µs | −39,48% |
| GET volatile | p99 | 290,2 µs | 135,1 µs | −53,45% |
| GET volatile | p99.9 | 425,4 µs | 163,3 µs | −61,61% |
| GET durable-periodic | throughput | 181.471/s | 186.431/s | +2,73% |
| GET durable-periodic | RSS mediana | 75,17 MB | 74,29 MB | −1,18% |
| GET durable-periodic | p50 | 903,9 µs | 876,8 µs | −3,00% |
| GET durable-periodic | p95 | 1,658 ms | 1,605 ms | −3,21% |
| GET durable-periodic | p99 | 2,042 ms | 1,969 ms | −3,58% |
| GET durable-periodic | p99.9 | 2,437 ms | 2,291 ms | −5,97% |
| PUT→GET volatile | throughput | 341.578 frame/s | 362.867 frame/s | +6,23% |
| PUT→GET volatile | RSS mediana | 62,31 MB | 61,29 MB | −1,63% |
| PUT→GET volatile | p50 | 866,8 µs | 866,5 µs | −0,03% |
| PUT→GET volatile | p95 | 1,760 ms | 1,832 ms | +4,05% |
| PUT→GET volatile | p99 | 2,407 ms | 2,505 ms | +4,06% |
| PUT→GET volatile | p99.9 | 3,557 ms | 3,673 ms | +3,23% |

## Design rifiutati durante l'A/B

1. **`ReadRecord` completo contiguo nel Base.** Duplicava i record tra generazioni durante il merge:
   RSS GET volatile 48,4→68,6 MB, durable 75,2→99,1 MB e PUT→GET 62,3→91,2 MB. Rifiutato.
2. **Arena esterna per tutte le chiavi.** Riduceva la memoria ma aggiungeva un'indirezione alle chiavi
   da 16 B, con regressioni di throughput fino a circa il 10%. Rifiutato.
3. **Inline key e puntatore esterno come campi separati.** Recuperava il lookup, ma portava il record
   oltre la cache line e annullava parte del risparmio RSS. Sostituito da una union a 16 B.
4. **Directory paginata a due livelli.** Manteneva il merge lazy ma pagava due carichi dipendenti per
   hit. Una directory piatta recuperava solo parte del divario. Il layout finale usa array piatti con
   inizializzazione incrementale dei control byte.
5. **Puntatore raw al Segment nel record compatto.** Migliorava meno dell'1% il GET e richiedeva una
   risoluzione ownership per record durante il merge. Rifiutato a favore dell'indice del pin.

## Safety e qualità

- Debug: 459 test, 0 failure;
- allocation fault injection: pass;
- crash matrix sync/periodic/group e daemon: 8/8 pass;
- ASan + UBSan: 459 test, 0 failure;
- TSan: 459 test, 0 failure;
- strict ISO C++23/hardening build: pass;
- test specifico: chiave inline da 16 B, confine esterno da 17 B e chiave oltre 64 KiB;
- diff whitespace: pulito.

## Decisione e prossimo P0

Il Base compatto resta nel runtime paired: elimina ownership per-entry, riduce l'amplificazione e
mantiene il merge bounded senza riaprire le pause Writer. Non viene però etichettato “performance
complete”. Il prossimo lavoro sullo stesso asse è eliminare dal **Delta** gli `shared_ptr<ReadRecord>`
per entry con un'arena Writer bounded, quindi misurare separatamente Base-hit, Delta-hit e miss con
`perf stat/record` (IPC, branch, LLC, dTLB e migration) su Linux hard-pinned. Solo quell'A/B può
chiudere il −3% GET residuo o motivare un ulteriore layout.
