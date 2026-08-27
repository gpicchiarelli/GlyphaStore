# Candidato generation slot-pool production-congruent

Status: sperimentale; non selezionabile da `ShardPairRuntime` o `glyphastored`
ADR: [0036](../adr/0036-generation-slot-pool-publish.md)
Last reviewed: 2026-08-27

## Scopo

`src/experimental/generation_slot_pool.hpp` verifica il protocollo di lifetime proposto da ADR
0036 sul vero grafo immutabile `PairReadGeneration`. A differenza del prototipo volatile completo,
non reimplementa Index, delta o Segment: lo slot possiede direttamente la generation prodotta dal
codice di produzione, inclusi i pin delle generazioni durevoli.

Il pool e la banca di storage sono compilati soltanto in `glyphastore_tests`. Il bridge minimo che
costruisce il grafo privato nello storage riservato è compilato nel core, perché `DeltaState` è
intenzionalmente opaco, ma il suo header di accesso resta sotto `src/experimental/`, non viene
installato e nessun percorso ufficiale lo invoca. Non esiste un flag runtime: ACK, recovery,
persistence v1 e wire v2 restano invariati.

## Publication

Ogni pool ha capacità compile-time fissa e un solo Writer. Lo slot contiene:

- ownership del grafo `shared_ptr<const Generation>`;
- `epoch` e `visible_through` immutabili;
- stato Writer-only `free`, `building`, `published` o `retired`;
- contatore di reincarnazione Writer-only.

Nel candidato completo lo stesso indice seleziona anche uno storage shell preallocato. Lo slot
continua a possedere il grafo tramite `shared_ptr`, ma `allocate_shared` colloca nello storage fisso
il control block, `PairReadGeneration` e il suo `DeltaState` embedded.

## Storage fisso della generation

`PairReadGenerationShellBank<Capacity>` prepara una banca bounded insieme al pool. Dopo
`try_reserve()`, il Writer usa esclusivamente lo storage con lo stesso `slot_index()`:

```text
reserve slot N
→ build PairReadGeneration in shell_bank[N]
→ Store linearization
→ commit slot N
→ publish token {epoch, N}
```

Ogni storage accetta una sola co-allocazione viva. Un secondo tentativo fallisce con
`resource_exhausted`; non cade sullo heap. La memoria torna disponibile soltanto quando sono stati
distrutti sia l'ultimo `shared_ptr` sia l'ultimo `weak_ptr` del control block. L'allocator trattiene
il backing storage, perciò la distruzione del proprietario esterno non può produrre use-after-free.

Il blocco candidato è 512 byte allineato a 64 byte. Per la capacità normativa di 65 slot il budget
shell è 33.280 byte per shard, allocato una volta. Size o alignment del control block superiori al
budget falliscono esplicitamente: prima di un'integrazione multipiattaforma servono prove su ogni
toolchain supportata, non un'assunzione sull'ABI di `shared_ptr`.

Il beneficio è circoscritto: elimina il `malloc` per shell/control block/`DeltaState` nelle
publication incremental costruite nello slot. Non elimina le allocazioni necessarie per nuove
`DeltaPage`, spine COW, blocchi dell'arena, chiavi esterne, nuove basi o pin. Mantiene inoltre il
refcount Writer-side; il Reader continua a usare il puntatore raw adottato una volta per turn.

### Owner inline strutturale

`PairReadGenerationInlineSlotPool` chiude il lifetime del backing per costruzione: contiene la
banca inline prima del pool, quindi l'ordine inverso di distruzione elimina tutte le generation
prima dei relativi byte. Il builder con observer non owning è privato, la generation costruita non
esce mai dal wrapper e il Reader riceve soltanto il borrow governato dall'epoch. Allocation,
deallocation e contatori dello storage inline sono Writer-only e non usano atomiche.
Il wrapper non accetta un `PairReadMerge` esterno: il merge conserverebbe uno `shared_ptr` capace di
sopravvivere alla banca. L'integrazione futura dovrà possedere anche lo stato merge nello stesso
dominio. La distruzione di uno storage ancora occupato termina fail-fast.

Lo stress concorrente Writer publication / Reader adoption copre 10.000 reincarnazioni sotto TSan.
Il microbenchmark a protocollo equivalente misura +7,53% rispetto al backing posseduto tramite
`shared_ptr`, ma resta 7,09% dietro il `make_shared` senza slot protocol nello stesso run. Il
candidato dimostra quindi il lifetime corretto, non la convenienza della soluzione finale. Non va
integrato: il prossimo design deve eliminare il control block stesso tramite costruzione in-place
dell'oggetto generation.

### Direct-object ring

Il primo passo direct-object riusa la stessa routine di validazione e costruzione delta del percorso
ufficiale, poi esegue `construct_at` sul tipo concreto direttamente in due slot alternati. Non
esistono allocator, control block o ownership `shared_ptr` della generation. `destroy_at` viene
eseguito dal Writer prima della reincarnazione; uno storage occupato alla distruzione termina
fail-fast.

Il differential test confronta 256 publication ufficiali e direct su epoch, visibility, delta,
versioni e valore. Il benchmark misura +17,08% rispetto a `make_shared`. Questo ring non ha ancora
Reader concorrente: ritira sincronicamente la generation precedente e quindi non costituisce il
pool ADR 0036. Il sottoblocco successivo combina l'oggetto direct con token, safe epoch e reclaim
bounded, mantenendo ancora separata qualunque integrazione.

### Direct-object slot pool

`PairReadGenerationDirectSlotPool` completa quel sottoblocco in isolamento. Tutte le generation,
inclusa epoch zero, sono costruite direttamente negli slot inline. Il pool non contiene
`shared_ptr<PairReadGeneration>`, control block o allocator della shell. La reservation avviene
prima della linearizzazione Store; publication usa un singolo token release `{epoch, slot}`;
adoption e safe frontier sono Reader-only; `destroy_at` è eseguito dal Writer soltanto per epoch
ritirati strettamente precedenti alla frontiera acquire.

La capacità resta compile-time e bounded. La distruzione ordinaria richiede la sequenza terminale
`stop_admission → mark_reader_quiescent → try_finish_shutdown`; uno storage ancora occupato o un
pool distrutto senza drain termina fail-fast. Merge esterno e snapshot replacement restano
deliberatamente esclusi perché potrebbero introdurre ownership fuori dal dominio del pool.

Le prove coprono saturazione senza Store entry, reincarnazione fisica, cold borrow, rifiuto di una
frontiera regressiva, fail-closed dopo build invalida post-linearizzazione e 10.000 publication
concorrenti Reader/Writer sotto TSan. Il microbenchmark sincrono misura 3.186.299 publication/s:
+7,43% rispetto a `make_shared`, ma -8,28% rispetto al ring direct privo del protocollo Reader.
Questa è evidenza positiva del protocollo, non V11/V12 e non autorizza ancora l'integrazione nel
runtime ufficiale.

### Diagnostica Reader–Writer a due thread

`glyphastore_generation_publication_benchmark` confronta il pool direct e quello con generation
`shared_ptr` usando due thread persistenti distinti. Entrambi attraversano reservation, publication
token, adoption, reclaim e shutdown; cambia soltanto l'ownership della generation. Il Reader adotta
continuamente e verifica la vista finale, mentre il Writer pubblica 20.000 mutation reali già
materializzate nel Segment.

La mediana locale è 1.275.537 publication/s direct contro 1.190.497 shared: **+7,14%**, con tempo
complessivo per publication inferiore del 6,67%. La p50 campionata è invariata a 1.000 ns; la p99
campionata direct è invece peggiore del 5,86%. Il risultato mantiene quindi la direzione throughput,
ma non dimostra ancora il miglioramento delle code di latenza.

La richiesta di affinity è risultata `unavailable` su questo host macOS. Il test prova concorrenza
Reader–Writer, non pinning su core fisici. V11/V12 restano aperti fino all'integrazione nel percorso
paired sperimentale con GET reali e a una riga Linux effettivamente pinned (più una riga macOS
etichettata correttamente come advisory o unavailable).

La seconda modalità esegue un vero `PairReadGeneration::get` a ogni adozione. Sul dataset L1-hot da
2 byte, direct raggiunge 919.357 publication/s contro 817.965 shared (**+12,40%**), migliora p50 e
p99 publication campionate del 9,09% e 9,54%, e lascia invariati GET p50/p99 a 83/250 ns. Questo
rimuove il segnale p99 negativo del puro adoption storm, ma non sostituisce dataset LLC/oltre-LLC,
valori realistici, socket e affinity verificata.

Il Writer inizializza lo slot completo, quindi pubblica con release un token lock-free a 64 bit:

```text
bits 63..16  epoch della reincarnazione
bits 15..0   slot + 1 (zero riservato)
```

Il Reader acquire-carica il token, verifica l'epoch dello slot e adotta il puntatore raw. Epoch,
`visible_through` e grafo diventano visibili attraverso lo stesso edge release/acquire. Un epoch
strettamente crescente impedisce ABA; l'esaurimento dei 48 bit fallisce esplicitamente.

## Reclamation e cold I/O

Una sola frontiera governa ogni borrow:

```text
reader_safe_epoch = min(adopted_epoch, minimum_borrowed_epoch)
```

Il Reader la pubblica con release dopo aver terminato l'accesso necessario all'adozione. Il Writer
la carica con acquire e libera esclusivamente slot ritirati con:

```text
retired_epoch < reader_safe_epoch
```

Il confronto stretto mantiene viva la generation attualmente adottata. Un cold read avviato
nell'epoch `N` continua a pubblicare `N` durante adozioni successive; compaction e rotation possono
pubblicare `N+1`, ma lo slot `N` non viene resettato fino al completamento I/O. Il GET normale non
incrementa refcount e non esegue atomiche: l'adozione avviene una volta per turn.

La release della frontiera ordina tutti gli accessi Reader alla generation vecchia prima del reset
Writer. La monotonicità del token impedisce al Reader di adottare un epoch inferiore alla frontiera
già pubblicata.

Anche la frontiera deve essere monotona. Un tentativo di annunciare dopo l'adozione un borrow più
vecchio della frontiera già pubblicata viene rifiutato senza cambiare lo stato Reader: quell'epoch
potrebbe essere già stato reclamato e non può essere resuscitato. Il borrow deve essere registrato
nel turn in cui nasce, prima di avanzare la frontiera.

## Boundedness

Quando nessuno slot è libero dopo il reclaim opportunistico, `try_publish` restituisce
`pool_exhausted` senza cambiare token o generation Writer corrente. Non esiste attesa interna,
allocazione di crescita o overwrite. Admission/backpressure e la formula definitiva della capacità
restano responsabilità dell'eventuale integrazione production; la formula candidate è definita qui,
ma non chiude V9 per il runtime ufficiale.

La capacità production-congruent è ora derivata dal limite già normativo del runtime:

```text
slot_capacity = 1 current + maximum_retired_generations
              = 1 + 64
              = 65
```

Non dipende dalla mutation queue: il Writer è seriale e può avere una sola publication `building`.
Quando il debito è 63, l'ultimo slot libero viene riservato prima della mutazione; dopo il commit lo
slot `building` diventa current e la precedente current diventa la sessantaquattresima retired,
lasciando invariato il totale di 65. Al debito 64 non si entra più nello Store finché il Reader non
avanza. Un margine `+2` sarebbe quindi memoria inutilizzata, non sicurezza aggiuntiva.

Il test V9 mantiene fermo il Reader, riempie esattamente 65 slot, verifica il rifiuto pre-Store del
tentativo successivo, poi avanza la frontiera e prova reclaim e ripresa. La formula è codificata da
`GenerationSlotCapacity<MaximumRetired>` e compilata contro
`ShardPairRuntime::kMaximumRetiredReadGenerations`, così una variazione del limite non resta
silenziosamente disallineata.

## Reservation e failure dopo la linearizzazione

Una publication associata a PUT/ERASE deve riservare uno slot **prima** di entrare nella mutazione
Store:

```text
try_reserve slot
→ Store mutation
→ mark_store_linearized (committed o indeterminate)
→ costruzione generation
→ commit reservation + release publication
→ ACK
```

Se `try_reserve` fallisce, la richiesta è ancora sicuramente pre-mutation e può ricevere overload
senza ambiguità. Una reservation abbandonata prima della linearizzazione torna semplicemente
`free`. Dopo `mark_store_linearized`, invece, la guardia RAII non può essere distrutta senza armare
il fail-closed hook e incrementare `unpublished_linearizations`.

La guardia non interpreta un semplice `ErrorCode`: il chiamante deve marcarla per ogni outcome in
cui l'autorità Store può essere cambiata, quindi almeno `committed` e `indeterminate`. Il metodo è
`noexcept` e deve essere chiamato immediatamente dopo la classificazione Store, prima di qualsiasi
costruzione o allocazione successiva.

Il test durevole forza una publication invalida dopo un PUT già committato. La guardia marca lo
Store non operativo, libera la reservation, acquisisce poi lo snapshot con `allow_fail_closed`,
ricostruisce la generation e verifica che sia la chiave seed sia l'autorità appena committata siano
presenti. Nessun ACK di successo viene emesso dal candidato.

## Shutdown deterministico

Lo stop rispetta l'ownership della coppia. Prima il Reader smette di alimentare la mutation lane;
poi il Writer, sul proprio thread, esegue `stop_admission()`. Questa è la linearizzazione dello stop
per il pool: una reservation che aveva già osservato admission aperta è ammessa e deve essere
committata o cancellata; ogni tentativo successivo viene rifiutato prima della mutazione Store.

```text
Reader stop mutation admission
→ Writer stop_admission
→ commit/cancel ogni reservation building
→ drain completamenti e output/cold I/O Reader
→ mark_reader_quiescent
→ Writer try_finish_shutdown + reclaim
```

`mark_reader_quiescent()` fallisce finché admission è aperta o esiste uno slot `building`. La
transizione è terminale: azzera il puntatore locale Reader, pubblica una safe frontier successiva
all'ultimo epoch Writer e impedisce nuove adozioni. Il chiamante può invocarla soltanto dopo aver
completato tutti i borrow di output e cold I/O. `try_finish_shutdown()` riesce soltanto quando non
restano reservation, il Reader è quiescente e il reclaim ha lasciato una sola generation: quella
finale pubblicata.

Una reservation abbandonata durante lo shutdown segue la stessa regola del data path: prima della
linearizzazione è una cancellazione sicura; dopo la linearizzazione invoca il fail-closed hook. Lo
shutdown non converte quindi una mutazione ambigua in perdita silenziosa.

Il candidato non modella la SPSC esterna, il drain dei completion o la chiusura socket. Queste
responsabilità restano necessarie nell'integrazione `ShardPairRuntime`; il pool fornisce e prova
soltanto il confine publication/reclamation.

## Prove attuali

- exhaustion deterministico e recovery dopo avanzamento Reader;
- borrow di un epoch esatto attraverso publication e reclaim;
- rifiuto fail-closed di una frontiera borrow regressiva;
- reservation pre-mutation, cancellazione sicura e fail-closed RAII post-linearizzazione;
- stop admission Writer-owned, rifiuto delle reservation tardive e drain di quelle già ammesse;
- capacità normativa di 65 slot, saturazione bounded e recovery dopo quiescenza;
- quiescenza terminale rifiutata in presenza di slot `building`;
- borrow lento e grafo durevole reale trattenuti fino al completamento Reader;
- reclaim finale con una sola generation corrente ancora posseduta;
- snapshot drain reale dopo una mutazione durevole committata ma non pubblicata;
- stress concorrente di 20.000 publication/adoption/reclaim;
- snapshot durevole reale dopo compaction con cold read del vecchio Segment ancora in volo;
- snapshot durevole reale dopo rotazione Writer-owned;
- co-allocazione della vera generation in storage fisso, rifiuto deterministico dello storage
  occupato e riuso dello stesso indirizzo dopo la distruzione dell'ultimo weak owner;
- composizione reservation → shell con lo stesso indice di slot e reincarnazione dello storage;
- owner inline con ordine di distruzione strutturale, zero ownership/refcount del backing nel data
  path e stress concorrente da 10.000 publication;
- direct-object slot pool senza allocator/control block della generation, con reservation, token,
  safe epoch, cold borrow, reclaim bounded, shutdown terminale e stress Reader/Writer TSan;
- microbenchmark locale direct pool +7,43% su `make_shared` e -8,28% rispetto al ring senza
  protocollo; la matrice worker-affine resta aperta;
- diagnostica a due thread direct +7,14% sul protocollo shared equivalente, con p99 campionata
  +5,86% e affinity macOS non disponibile: evidenza positiva ma non gate V11/V12;
- con un GET reale per adozione: direct +12,40%, publication p50/p99 migliori e GET p50/p99
  invariati; dataset L1-hot e assenza di pinning mantengono aperti i gate;
- Release, ASan+UBSan e TSan focalizzati verdi.

Queste prove qualificano V5, V6, V8 e V9 come **candidate evidence** e rimuovono il blocker tecnico
che lasciava l'allocazione shell fuori dal candidato. Non chiudono la matrice production:
integrazione nel runtime ufficiale, drain delle lane/socket, crash matrix e soprattutto A/B
worker-affine V11/V12 rimangono obbligatori.
