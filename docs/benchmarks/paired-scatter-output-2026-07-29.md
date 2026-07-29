# Paired cold-read scatter output — 2026-07-29

## Esito

**Accettato in forma adattiva e confinata.** Il cold GET cleartext non copia più
`OwnedValue` nel buffer della connessione quando il payload è almeno 4 KiB e la connessione non ha
dimostrato uso della pipeline. Header e valore restano due extent owning stabili fino al completo
drain tramite `sendmsg`. TLS conserva il frame contiguo perché `SSL_write` non offre lo stesso
contratto scatter/gather.

Non è uno zero-copy end-to-end: il helper cold legge e verifica ancora il Record nel proprio scratch
e materializza un `OwnedValue`. Il blocco elimina esattamente la copia successiva
`OwnedValue -> Connection::output` nel caso ammesso.

Il candidato scatter indiscriminato è stato respinto. Su valori da 64 B ha perso il 14,9% di
throughput a pipeline 1 e il 3,1% a pipeline 32; sui valori da 64 KiB una singola lease ha inoltre
serializzato la pipeline 8. La versione accettata applica quindi due decisioni misurate:

- sotto 4 KiB usa il percorso contiguo, perché il costo fisso di `sendmsg` supera la copia;
- dopo aver osservato più Store request nello stesso input turn, la connessione resta contigua per
  preservare l'overlap fra output e cold read successivo;
- il percorso scatter conserva una sola lease bounded, disarma il read interest durante una write
  parziale e non introduce una coda multi-extent o allocazioni di framing per risposta.

## Invarianti di ownership e backpressure

- socket e lease appartengono esclusivamente al Reader/Reactor;
- la lease possiede l'`OwnedValue`; nessun `RecordRef`, descriptor o pin di generation viene usato
  dopo il rilascio della lease QSBR del task I/O;
- una response contigua già accodata precede sempre header e valore scatter nello stesso `sendmsg`;
- offset di buffer, header e valore avanzano separatamente attraverso short write ed `EAGAIN`;
- il watermark `maximum_output_bytes` include tutti gli extent pending;
- con lease attiva il socket non viene letto: la pressione resta nel receive buffer del kernel e la
  memoria utente non cresce senza limite;
- close, timeout, hangup, shutdown drain e slot reuse considerano e distruggono la lease;
- il Writer non scrive mai sul socket e non acquisisce lock di catalogo per il drain.

Metriche aggregate e per Reactor: `output_scatter_responses`, `output_scatter_bytes`,
`output_scatter_partial_writes`, `output_scatter_completions`.

## A/B macOS arm64

Release Apple LLVM 21, una pair, quattro client, durable-periodic 1000 ms, key 16 B. I raw result
sono in `benchmarks/results/paired-scatter-output-2026-07-29/`. Sono run sequenziali non pinned e il
campione da 64 KiB ha tre ripetizioni: le code sono evidenza locale, non un claim cross-platform.

### Perché il candidato indiscriminato è stato respinto

| workload 64 B | baseline borrowed | scatter sempre | delta throughput |
|---|---:|---:|---:|
| GET p1 | 112.984/s | 96.176/s | -14,9% |
| GET p32 | 186.012/s | 180.333/s | -3,1% |
| GET/PUT 99/1 p32 | 173.874/s | 159.199/s | -8,4% |

Il run adattivo da 64 B, che usa il frame contiguo, torna a 109.199/s a p1 e 185.355/s a p32.
Lo spread non pinned non permette di distinguere il residuo dal rumore; soprattutto, elimina la
regressione strutturale di `sendmsg` sui payload piccoli.

### Gate grande, pipeline 1

| metrica, 64 KiB | contiguo | scatter accettato | delta |
|---|---:|---:|---:|
| throughput | 27.602/s | 27.487/s | -0,4% |
| p50 | 95,1 us | 95,2 us | +0,04% |
| p95 | 128,9 us | 125,3 us | -2,8% |
| p99 | 173,0 us | 149,8 us | -13,4% |
| p99.9 | 319,2 us | 223,5 us | -30,0% |

Il throughput è neutrale nel campione e le code alte migliorano. L'RSS di processo non è usato per
un claim: include dataset durevole, page cache e ordine sequenziale dei run.

A pipeline 8 la versione finale classifica la connessione come pipelined e usa il percorso contiguo.
Il run locale ha spread e deriva termica troppo elevati (33,1-40,4k/s) per un confronto numerico;
il test deterministico verifica invece che due cold GET grandi conservino ordine e zero lease
scatter su quella connessione. Un futuro multi-extent scatter richiederà una struttura preallocata,
un watermark per byte e un A/B separato prima di sostituire questa scelta.

## Safety gate

- suite Debug: 461/461;
- ASan + UBSan: 461/461; preset completo 37/37 incluse matrici crash e allocation fault;
- TSan: 461/461;
- build strict ISO C++23/hardening: pass;
- short write reale con receive buffer ristretto e payload da 768 KiB;
- pipeline cold grande, ordine delle response e classificazione contigua;
- TLS resta sul percorso owning contiguo;
- nessuna modifica a wire v2, Record, Segment, Manifest o persistence v1.

## Prossimo gate

Il P0 output è chiuso per il percorso cleartext diretto; `get-into` può ancora eliminare la
materializzazione nel helper, ma richiede buffer ownership e checksum verification senza prestito
oltre la generation. Il prossimo blocco con impatto più generale è la mutation arena preallocata per
eliminare `string`/`vector` per PUT/ERASE mantenendo completion credit e byte budget invariati.
