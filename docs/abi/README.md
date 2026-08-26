# C ABI

The supported shared-library surface is the small C ABI declared in
[`glyphastore.h`](../../include/glyphastore/abi/glyphastore.h). Its normative contract is
[`c-abi-v1.md`](../spec/c-abi-v1.md). The daemon continues to link the C++ core directly; server
internals are not part of the shared library.

ABI v1 intentionally contains only version/error, open/close, GET, PUT, ERASE, and PUT batch. This
keeps the permanent compatibility surface smaller than the engine's internal feature set.

Consumers may use the installed CMake target `GlyphaStore::abi` or `pkg-config glyphastore-abi`.
The project remains an architectural prototype: a stable interface contract is not evidence of
production readiness or platform durability certification.

- [Versioning](versioning.md)
- [Lifetime](lifetime.md)
- [Threading](threading.md)
- [Error model](error-model.md)
- [Compatibility policy](compatibility-policy.md)
