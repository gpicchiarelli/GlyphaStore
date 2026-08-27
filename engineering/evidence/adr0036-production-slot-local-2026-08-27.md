# ADR 0036 Wave 1 production slot pool — local evidence

- Date: 2026-08-27
- Label: `local`
- Host: developer workstation (macOS)
- Status: Wave 1 opt-in path only; ADR remains **proposed**
- Claim ceiling: architectural prototype (no E3/E4, no production-ready)

## Scope closed locally

- Production `GenerationSlotPool` (capacity 65) with `{epoch:48, slot+1:16}` token
- `PairReadGeneration` direct constructors + merge-compatible direct publish
- Opt-in `PairedConcurrencyConfig::generation_slot_pool` (default **false**)
- Embedded combiner / dedicated Writer sync paths reserve-before-mutate
- Dedicated Writer async batch path reserves before Store and publishes via the pool
- Unit: V1 reincarnation (≥10k), V6 fail-closed, V9 exhaustion, token width
- Paired Store: V5 shutdown + V10 put_batch FIFO under the flag

## Still open

- Durable-group Writer publish sites under the flag (partial; sync/async incremental dual-pathed)
- V7/V8 production-flag integration soaks (merge pin + durable refresh APIs exist; long campaigns open)
- V11 / V12 worker-affine performance
- Multi-OS CI matrix / soak under the flag
- ADR acceptance

## Commands (local)

```bash
cmake --build build -j
ctest -R 'ADR 0036|generation_slot|paired Store put_batch' --output-on-failure
python3 engineering/tools/validate_assurance.py --write-generated
```
