# ADR 0001: Project scope

- Status: accepted
- Date: 2026-07-11

GlyphaStore is a native C++ key-value store with one logical key-space, a future binary-only
protocol, automatic many-core execution, safety, and SLA predictability. Redis/RESP compatibility,
SQL, and generic querying are non-goals because they would constrain the fast path and expand the
correctness surface before the engine is measured.
