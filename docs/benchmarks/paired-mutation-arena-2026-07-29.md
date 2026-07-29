# Gate mutation slot/key/value arena paired — 2026-07-29

## Esito

**Pass per il blocco P0 mutation task storage.** Il Reader non costruisce più `std::string` o
`std::vector` owning per `PUT`/`ERASE`: copia una sola volta i byte parsati in un'arena bounded
preallocata della propria coppia e trasferisce al Writer un descrittore fisso tramite SPSC. Lo slot
torna riutilizzabile soltanto dopo l'acquire della completion corrispondente. Il percorso GET non è
stato modificato.

Il gate prestazionale macOS arm64 non pinned è positivo sui carichi mutation-bound e sostanzialmente
neutro sul controllo read-heavy. Non costituisce ancora un claim Linux/NUMA production.

## Protocollo implementato

Ogni lane possiede:

- `capacity` slot fissi, uguale alla capacità della mutation/completion ring;
- un'arena logica di `--durable-mutation-queue-bytes`;
- una guardia fisica grande al massimo quanto un frame, per mantenere contiguo un payload che
  attraversa il wrap senza eseguire una seconda copia;
- cursori FIFO monotoni e contatori separati per payload e admission bytes;
- tre cause osservabili di rifiuto: slot esauriti, byte esauriti e singolo payload troppo grande.

La linearizzazione è:

```text
Reader copia payload nello slot
→ mutation queue release
→ Writer dequeue acquire e usa view borrowed
→ completion queue release
→ Reader completion acquire
→ rilascio FIFO e possibile riuso dello slot
```

Nessun `RecordRef`, span, file handle o altro riferimento borrowed oltrepassa il completion edge. Un
rilascio fuori ordine, un completion senza slot valido o un payload non più osservabile rende il
runtime fail-closed. Il fallimento del push immediatamente successivo all'acquisizione esegue solo il
rollback dell'ultimo slot non pubblicato.

Con i default correnti una coppia espone 256 slot, 16 MiB di budget logico e circa 18 MiB di storage
virtuale massimo (budget più guardia frame). La memoria è quindi esplicita e bounded per pair; il
profilo memory-constrained deve ridurre il byte budget, non introdurre crescita dinamica.

## Benchmark A/B

Binario pre-arena conservato prima della ricompilazione Release contro working tree post-arena.
Apple Silicon arm64, macOS, client e server sullo stesso host, nessun hard affinity; 2 warmup e 7
sample. I raw result completi sono in
[`benchmarks/results/paired-mutation-arena-2026-07-29/`](../../benchmarks/results/paired-mutation-arena-2026-07-29/).

| Workload | Metrica | Pre | Post | Delta |
|---|---:|---:|---:|---:|
| read-after-write, value 64 B, 8 client, p1 | throughput | 178.278 ops/s | 212.638 ops/s | **+19,27%** |
| | p50 | 80,7 us | 72,3 us | **−10,38%** |
| | p95 | 128,6 us | 106,1 us | **−17,52%** |
| | p99 | 179,0 us | 138,8 us | **−22,47%** |
| | p99.9 | 435,0 us | 433,4 us | −0,36% |
| read-after-write, value 1 KiB, 8 client, p1 | throughput | 203.662 ops/s | 207.187 ops/s | +1,73% |
| | p50 / p95 | 74,3 / 104,7 us | 73,7 / 103,5 us | −0,90% / −1,12% |
| | p99 / p99.9 | 129,1 / 428,0 us | 130,3 / 439,5 us | +0,90% / +2,69% |
| 99/1, value 64 B, 8 client, p32 | throughput | 1,629 Mops/s | 1,589 Mops/s | −2,47% |
| | p99 / p99.9 | 234,1 / 323,4 us | 242,0 / 345,4 us | +3,40% / +6,82% |
| 99/1 replica post | throughput | — | 1,601 Mops/s | −1,70% vs pre |
| | p99 / p99.9 | — | 237,4 / 333,8 us | +1,42% / +3,21% vs pre |

Il read-after-write da 64 B migliora anche nel peggior sample di throughput (202.351 contro 170.758
ops/s), quindi il guadagno non dipende dalla sola mediana. A 1 KiB gli intervalli sono stretti e il
vantaggio è piccolo ma ripetibile. Nel 99/1 lo spread pre/post si sovrappone e la seconda replica
riduce la regressione: l'esito è **inconclusive/neutral**, da rieseguire interleaved su Linux
hard-pinned. Non viene dichiarato un miglioramento del GET da questo blocco.

L'RSS mediano del processo cresce del 2,5–6,2% nei run post. Il valore comprende dataset, processi
client/server nello stesso benchmark e l'arena per ogni istanza ripetuta. È coerente con la scelta
di convertire un limite di coda prima solo contabile in storage fisicamente riservabile; non è
accettabile moltiplicarlo ciecamente per decine di pair. Il prossimo gate Linux deve riportare RSS
per componente e working-set resident dopo churn, oltre alla capacità virtuale già esposta da
STATS.

## Correttezza e robustezza

- Debug: 466/466 test;
- ASan + UBSan: 466/466 test;
- TSan: 466/466 test, inclusi i trasferimenti cross-thread dello slot;
- CTest ASan integrale: 37/37, compresi allocation-fault e crash matrix;
- build `unix-strict`: completata senza nuovi warning;
- unit test dedicati: copia stabile, wrap contiguo, rollback, release fuori ordine, slot/byte/payload
  exhaustion distinti;
- integrazione Reactor: saturation della capacità slot e saturation indipendente del byte budget,
  high-water e ritorno a zero dopo completion.

## Decisione e seguito

Il task storage owning precedente è rimosso dal normale percorso paired. Il P0 chiuso riguarda
soltanto payload e descrittore della mutazione; non chiude automaticamente il costo della
publication. Il prossimo blocco resta il Delta Writer-owned bounded: un nuovo candidato viene
promosso solo se supera i due layout già respinti senza aumentare copie, RSS o p99. In parallelo il
gate production richiede ancora 1/2/4/8 pair Linux hard-pinned, contatori `perf` e memoria per NUMA
node.
