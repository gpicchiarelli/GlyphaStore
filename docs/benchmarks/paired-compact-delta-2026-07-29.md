# Paired Delta layout gate — 2026-07-29

## Esito

**Respinto (gate storico).** Il Delta copy-on-write conservava gli handle
`shared_ptr<const ReadRecord>` per entry.
Due layout compatti hanno mantenuto la correttezza, ma nessuno ha migliorato simultaneamente
throughput, tail latency e memoria rispetto al layout corrente. Il codice sperimentale non è rimasto
nel motore.

## Metodo

- macOS arm64, Apple LLVM 21, build Release;
- una `ShardPair`, 8 client owner-bound, pipeline 32;
- key 16 B, value 64 B;
- GET volatile e durable-periodic: 4 warm-up + 7 campioni da 50.000 operazioni;
- PUT→GET volatile: 2 warm-up + 5 campioni da 100.000 operazioni;
- confronto con il Base Index compatto già accettato;
- RSS riferito all'intero processo benchmark.

I raw result sono in `benchmarks/results/paired-compact-delta-2026-07-29/`.

## Candidati

1. **Record inline nella pagina COW.** Elimina il control block per entry, ma ogni copia di pagina
   duplica 16 record completi. Rispetto alla baseline, RSS cresce di circa 24% nel GET volatile,
   25% nel durable e 21% nel mixed. Respinto per amplificazione strutturale.
2. **Blocco immutabile per publication batch.** Le pagine contengono puntatori raw e lo stato Delta
   possiede i blocchi. La prima variante riservava 32 record anche quando la publication ne conteneva
   uno: RSS +112% volatile e +66% durable. Una seconda variante ha usato capacità esatta e GC
   adattivo, eliminando tale spreco, ma non ha superato il gate prestazionale.

## Variante migliore contro baseline

| scenario | throughput baseline | candidato | delta | RSS baseline | candidato | delta |
|---|---:|---:|---:|---:|---:|---:|
| GET volatile | 2,128 Mops/s | 1,824 Mops/s | **−14,3%** | 45,84 MB | 48,09 MB | **+4,9%** |
| GET durable-periodic | 186,4 kops/s | 189,7 kops/s | +1,8% | 74,29 MB | 73,35 MB | −1,3% |
| PUT→GET volatile | 362,9 kops/s | 342,9 kops/s | **−5,5%** | 61,29 MB | 64,06 MB | **+4,5%** |

Nel GET volatile il candidato peggiora p50/p95/p99/p99.9 rispettivamente di circa
13,7%/33,5%/37,3%/28,5%. Nel mixed migliora le latenze, ma perde throughput e memoria. Nel durable
il throughput è quasi neutro mentre p99.9 peggiora di circa 12,8%. Il trade-off non è accettabile
per un'ottimizzazione del percorso Reader.

## Conclusione tecnica

Il workload reale pubblica spesso batch piccoli. Un'arena per batch introduce lifetime metadata,
ritenzione dei blocchi e un'indirezione senza garantire densità; il record inline nella pagina rende
troppo costosa la granularità COW. L'handle per entry resta, per ora, il compromesso misurato migliore.

La rimozione è stata poi chiusa con un terzo layout sostanzialmente differente: arena append-only per
generation, celle compatte separate dai pin e capacità bounded sulle versioni. Decisione e A/B sono
in [`paired-generational-delta-arena-2026-07-29.md`](paired-generational-delta-arena-2026-07-29.md).
