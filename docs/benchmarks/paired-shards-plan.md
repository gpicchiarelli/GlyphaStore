# Piano benchmark: shard a coppie Reader–Writer

Status: runtime paired unico per 0.1.0; output lease, mutation arena e Delta arena chiuse

ADR: [paired Reader–Writer shards](../adr/paired-reader-writer-shards.md)

Baseline iniziale: runtime corrente su `main`, stessa persistence v1 e wire v2

Risultati del 2026-07-28: `benchmark-results/paired-shards/fc9463d-dirty/macos-m4/2026-07-28/`.
Questo run di gate è versionato esplicitamente; gli artifact benchmark locali ordinari restano ignorati.

Secondo gate P0: `benchmark-results/paired-shards/573f4f1-dirty/macos-m4/2026-07-28-p0-paged-delta/`.

Gate directory/batching:
`benchmark-results/paired-shards/55acedd-dirty/macos-m4/2026-07-28-persistent-directory-batching/`.

Gate Reactor TCP, una coppia:
`benchmark-results/paired-reactor/d2077cf-dirty/macos-m4/2026-07-28-phase2-tcp/`.

## Stato corrente — 2026-07-31

Le sezioni storiche sotto documentano i gate che hanno preceduto la migrazione. Il daemon non offre
due modelli concorrenti: Reader/Reactor e Writer seriale per shard sono il **solo** runtime di
`glyphastored` 0.1.0. Il prototipo volatile sotto `src/experimental/` resta lab-only e non è un
secondo modello selezionabile. Sono chiusi e coperti da test il routing multi-pair, mutation/completion
SPSC, publication immutabile, refresh per rotation/compaction, Base Index compatto, merge
incrementale, cold-read lane per pair, task slot preallocati, lease QSBR del cold I/O, output lease
cleartext adattiva con short-write backpressure, mutation slot/key/value arena bounded e Delta
generazionale Writer-owned bounded sulle versioni.

Ordine operativo residuo (P1 — non riapre un dual-runtime):

1. P1 — Delta mixed: candidate hierarchical directory-chunk COW landed; macOS advisory evidence in
   [`paired-delta-directory-chunks-2026-07-31.md`](paired-delta-directory-chunks-2026-07-31.md);
   magnitude vs the historical −1,8% still needs Linux hard-pinned confirmation;
2. P1 — `get-into` / scatter multi-extent: **rejected pending proof**
   ([`paired-get-into-multi-extent-reject-2026-07-31.md`](paired-get-into-multi-extent-reject-2026-07-31.md));
3. P1 — A/B 1/2/4/8 pair Linux hard-pinned: harness ready, gate **not closed on macOS**
   ([`paired-shards-linux-p1.md`](paired-shards-linux-p1.md));
4. P1 — backend I/O Linux opzionale soltanto se riduce coda e syscall senza cambiare ordering.

Il gate cold-read più recente è
[`paired-scatter-output-2026-07-29.md`](paired-scatter-output-2026-07-29.md). I risultati macOS non
pinned restano evidenza orientativa, non claim production.

Il gate mutation task storage è
[`paired-mutation-arena-2026-07-29.md`](paired-mutation-arena-2026-07-29.md): elimina ownership e
allocazioni per task, rende reale il byte budget e passa Debug, sanitizer, fault/crash e A/B
mutation-bound. Il controllo 99/1 resta neutro/inconcludente su macOS non pinned.

Il gate Delta finale è
[`paired-generational-delta-arena-2026-07-29.md`](paired-generational-delta-arena-2026-07-29.md):
elimina gli handle per versione con celle da 64 B e lifetime QSBR, rende bounded anche lo churn sulla
stessa chiave, riduce RSS del 2–4% e migliora il GET volatile. Il costo mixed di circa 1,8% resta P1
esplicito e richiede profilo Linux hard-pinned.

## Regole del confronto

Ogni confronto usa due binari dalla stessa base sorgente: runtime corrente e runtime paired. Build,
dataset, seed, client, filesystem, durability policy e limiti devono coincidere. L'ordine è
interleaved `old/new/new/old`; almeno due cicli, una warmup e sette sample misurati per cella. Si
conservano output raw, configurazione effettiva e commit in:

```text
benchmark-results/paired-shards/<commit>/<platform>/<run-id>/
```

Un run è invalido se perde risposte, request id, payload, mutation outcome o verifica finale. Client
e server sullo stesso host devono avere CPU disgiunte oppure il risultato è marcato combined-load.

## Matrice minima

| Asse | Valori |
|---|---|
| coppie | 1, 2, 4, 8 entro i core fisici |
| mix | GET 100%; GET/PUT 99/1, 95/5, 90/10 |
| distribuzione | uniform, Zipf, hot key; stesso shard e shard distribuiti |
| working set | L2, LLC, 4× LLC |
| value | 64 B, 1 KiB, 64 KiB, 256 KiB |
| pipeline | 1, 8, 32, 128 |
| connessioni | 1 e 4 per coppia; poi saturation sweep |
| durability | volatile, sync, group, periodic |
| writer batch | 1, 4, 16, 32, 128 |
| overlap | burst, queue full, rotation, compaction incrementale |

Read-after-write ha una cella dedicata: per la stessa connessione e coppia, ogni ACK di PUT è seguito
da GET e deve restituire esattamente la versione appena acknowledged.

## Metriche obbligatorie

- throughput e latenza p50/p95/p99/p99.9/max;
- admission→dequeue, dequeue→publication, publication→durability e durability→completion;
- profondità/high-water/full delle due SPSC;
- record e byte per batch/publication;
- CPU per Reader/Writer, IPC, branch miss, cache/LLC/dTLB miss;
- context switch, CPU migration, syscall e wakeup;
- byte copiati, allocazioni nel timed path e RSS componentizzato;
- generation vive/ritirate, retire backlog e delay;
- tempo e numero di socket con read interest disarmato per backpressure.

Su Linux si raccolgono `perf stat` e `perf record` separando PID/TID Reader e Writer. Su macOS si
usano Instruments Time Profiler/Counters e affinity/QoS dichiarati come advisory. Nessun contatore
non disponibile viene stimato da throughput.

## Gate per fase

### Prototipo volatile, una coppia

- zero mutex/queue/allocation sul GET verificato con instrumentation;
- due lookup massimi;
- read-after-write e queue wraparound corretti;
- memoria invariata dopo saturation/drain;
- p99 GET sotto 95/5 non peggiore della baseline oltre il rumore interleaved.

### Multi-pair

- efficienza GET almeno 80% da 1→numero di coppie fisiche disponibili;
- nessun traffico cross-pair dopo bind;
- nessuna CPU migration nella baseline Linux hard-pinned;
- pair lento non altera materialmente p99 degli altri pair.

### Durabilità

- outcome e recovery equivalenti alla suite v1;
- nessun ACK sync/group prima del durable boundary;
- queueing, publication e commit delay riportati separatamente;
- crash/fault matrix completa per append→publish→durable→completion.

### Compaction e QSBR

- nessuna attesa Reader per copy/commit;
- retire backlog e memoria restano bounded durante output socket lento;
- p99/p99.9 con overlap confrontati, non solo throughput;
- nessuna reclamation prima della quiescenza dimostrata da TSAN e fault gate deterministici.

## Esperimenti da non confondere con la baseline

- Reader/Writer su sibling SMT;
- oversubscription;
- PGO/native CPU solo sul paired;
- client e server concorrenti sugli stessi core;
- tmpfs contro filesystem reale;
- GET interno contro TCP;
- durability policy o batch differenti.

Questi run sono utili come sensitivity analysis, ma non sostengono un claim architetturale.

## Risultati richiesti

Il report finale deve mostrare valori assoluti, rapporto paired/current, min/median/max, dispersione e
ambiente completo. Un esito è `pass`, `fail` o `inconclusive`; valori dentro lo spread combinato sono
`inconclusive`. Il successo richiede insieme correttezza, boundedness e tail latency, non una media
ops/s più alta.

## Esito del primo gate e backlog

Il data path GET isolato passa il gate preliminare: 2,75× a 64 B e 6,11× a 1 KiB, con p99 inferiore.
Il prototipo nel suo insieme fallisce invece il throughput 95/5: 0,30× e 0,25× rispetto al current.
La causa primaria è la copia cumulativa del delta e dei payload durante ogni publication.

Ordine vincolante prima dell'integrazione Reactor:

1. P0 — storage payload stabile e `RecordView` compatto generation-pinned — completato nel
   prototipo con `StableRecord` condiviso fuori dal GET;
2. P0 — delta senza copia cumulativa — completato con pagine immutable copy-on-write;
3. P0 — instrumentation allocator/copy esatta — copie ingress, ownership directory, viste raw,
   pagine e record logici sono misurati. PMR generalista e slab unico sono stati respinti dai gate;
   gli hook allocator production vanno disegnati per classi di size e retention bounded;
4. P1 — Reactor sperimentale con ownership socket e output lease QSBR;
5. P1 — multi-pair 1/2/4/8, affinity/topologia e isolamento fra pair;
6. P1 — TCP pipeline 1/8/32/128 e valori 64 B–256 KiB a semantica equivalente;
7. P2 — Segment v1, durability sync/group/periodic, crash/fault/recovery;
8. P2 — compaction incrementale e reclamation con socket lento.

Il secondo gate supera la condizione per il punto 4: 95/5 pari a 2,15× della baseline a 64 B e
4,57× a 1 KiB, con p99 GET rispettivamente 208 e 208 ns contro 292 e 500 ns. Il risultato resta
micro/in-process e non autorizza ancora l'attivazione default nel daemon.

## Esito gate Reactor TCP a coppia singola

Il target wire-to-wire usa il client C++ pubblico, una sola coppia, 1–8 connessioni, workload
deterministico e ordine A/B interlacciato. Debug, ASan+UBSan e TSan passano tutti i 446 test. Il
percorso di output lento è coperto forzando una write parziale e verificando pin e rilascio della
generation.

Esito per area:

| Area | Esito | Evidenza principale |
|---|---|---|
| GET 64 B, pipeline 32 | pass throughput preliminare | +2,0%/+4,8%/+0,6%/+1,6% con 1/2/4/8 connessioni; p99 non uniforme |
| GET 64 B, pipeline 1/8 | inconclusive | -1,8%/-2,5%, entro una fascia da confermare su host isolato |
| GET 64 B, pipeline 128 | pass preliminare | +11,4%, p99 batch inferiore |
| GET 1 KiB–256 KiB | pass preliminare | +21% fino a +233%; scatter/gather domina sui valori grandi |
| mixed 64 B 99/1 | fail | -18%, p99 batch +38%, publication batch medio 1,07 |
| mixed 64 B 95/5 | fail critico | -54%, p99 batch +131%, publication batch medio 1,25 |
| mixed 64 B 90/10 | fail critico | -73%, p99 batch +262%, publication batch medio 1,43 |
| bounded output/reclamation | pass | pin soltanto su write parziale, high-water bounded, zero backlog finale |

Le latenze riportate dal target TCP sono p50/p99/p99.9 del completamento dell'intero batch client.
Non possono essere presentate come p99 del solo GET quando il batch contiene una mutazione. Il
prossimo benchmark deve separare istogrammi read/write e connessioni read-only/write-only.

Il gate vieta per ora multi-pair e integrazione nel daemon. Il nuovo P0 è il costo della mutazione
isolata: ogni connessione ammette correttamente una sola mutazione prima della barriera read-after-
write, quindi con quattro client il Writer forma batch medi troppo piccoli per ripagare handoff,
publication immutabile e completion wakeup. Aumentare soltanto `max_wait` porta il batch 95/5 fino
a 2,52 record a 32 us, ma riduce il throughput; non è una soluzione.

Ordine P0:

1. profilare separatamente submit→dequeue, build delta, publication e completion→ACK;
2. adaptive spin/park del Reader dopo submit e wakeup Reader→Writer, distinti per profilo;
3. ridurre allocazioni e cache-line traffic della publication isolata con retention bounded;
4. consentire coalescing di PUT consecutivi della stessa connessione fino alla prima barriera GET,
   senza ACK o visibilità anticipati;
5. ripetere 99/1 e 95/5 a 64 B; autorizzare multi-pair solo se 99/1 rientra nel rumore e p99 GET
   isolato migliora sotto Writer concorrente.
