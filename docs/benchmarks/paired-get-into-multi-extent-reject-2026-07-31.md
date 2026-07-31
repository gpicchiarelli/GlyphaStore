# Paired get-into / multi-extent scatter — reject 2026-07-31

## Esito

**Respinto (promozione P1).** Non esiste un'implementazione candidata di `get-into` né di coda
multi-extent sul daemon di produzione, e non c'è un A/B che batta il percorso adattivo già accettato
su pipeline **e** memoria bounded. Promuovere ora riprirebbe lifetime e backpressure senza prova.

Il cold GET cleartext resta sul modello di
[`paired-scatter-output-2026-07-29.md`](paired-scatter-output-2026-07-29.md): sotto 4 KiB, su TLS, o
dopo pipelining osservato usa il frame contiguo; altrimenti una sola lease owning a due extent
(`sendmsg`) con watermark e disarm del read interest.

## Perché non promuovere

1. **Nessuna prova.** Non c'è branch, flag o harness che confronti get-into / multi-extent contro
   l'adattivo su 64 B–256 KiB e pipeline 1/8/32/128 con memoria e short-write controllati.
2. **Lifetime.** `read_record_into` riempie scratch riusabile del helper cold; CRC/key/TTL sono
   validi solo a lettura completa. Puntare `sendmsg` su quello scratch senza slot owning o pin QSBR
   fino al drain finale viola il contratto già chiuso dal borrowed cold-read gate.
3. **Backpressure.** Una coda multi-extent deve preservare ordine wire, short write, close/timeout,
   handoff e watermark aggregato. La lease singola evita di serializzare cold GET pipelined dietro
   un drain lento — esattamente il fallimento del candidato scatter indiscriminato (−14,9% a 64 B
   p1; serializzazione a 64 KiB p8).
4. **TLS.** `SSL_write` non conserva iovec del caller; serve comunque materializzazione contigua.
5. **QSBR.** Un get-into che espone iovec generation-borrowed deve trattenere la generation fino
   all'ultima short write (ADR 0031). La lease owning attuale rilascia il pin I/O a completion e non
   accoppia client lenti alla reclamation.

## Criteri di riapertura (tutti richiesti)

- descriptor di extent a capacità fissa per connessione, con crediti byte **e** extent;
- destinazione owning per response (arena slot) **oppure** pin QSBR esplicito fino al drain finale;
- comparator TLS contiguo separato;
- test deterministici: short-write, client lento, ordine pipeline, close/reuse, shutdown,
  pressione retire/compaction;
- A/B interleaved Linux hard-pinned contro l'adattivo su 64 B–256 KiB e pipeline 1/8/32/128;
- nessun peggioramento di p99 batch sul mixed/pipelined oltre lo spread combinato.

Finché manca anche uno solo di questi punti, lo stato resta **reject pending proof**, non “deferred
optimization implied by scatter accept”.

## Relazione con i gate precedenti

| Gate | Decisione rilevante |
|---|---|
| inline cold-read | slot bounded; niente raw pointer senza epoch pin |
| borrowed cold-read | lease I/O fino a completion; valore ancora `OwnedValue` |
| scatter output | accept solo adattivo 4 KiB+ cleartext non pipelined; reject scatter sempre |
| questo documento | reject promozione get-into / multi-extent senza prova bounded+win |

## Verifica di questo reject

- nessun cambio al data path di produzione;
- evidenza numerica del reject scatter resta in
  `benchmarks/results/paired-scatter-output-2026-07-29/`.
