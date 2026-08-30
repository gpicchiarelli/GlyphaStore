## Summary

<!-- What changed and why? -->

## Safety and architecture

- [ ] Lengths, offsets, and allocation arithmetic are checked.
- [ ] Ownership and view lifetimes are explicit.
- [ ] Persisted format or architectural decisions are documented in an ADR when applicable.
- [ ] The change does not add unbounded work to a latency-sensitive path.

## Validation

- [ ] Formatting and `./scripts/dev.sh verify`
- [ ] Unit/integration tests
- [ ] ASan + UBSan
- [ ] TSan when concurrency is affected
- [ ] Fuzz regression/corpus when decoding or recovery is affected
- [ ] Focused benchmark when a performance claim is made

## Notes

<!-- Risks, migration concerns, follow-up work, or intentionally omitted checks. -->
