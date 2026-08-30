# Build hardening contract

GlyphaStore requires ISO C++23 as its minimum language mode, disables compiler language extensions,
and accepts a newer explicitly selected ISO standard. The strict Release preset additionally treats
the project's pedantic warning set as errors.

When `GLYPHASTORE_ENABLE_HARDENING` is enabled, supported targets receive
`-fstack-protector-strong`; executables are compiled with `-fPIE` and linked as PIE. Optimized Linux
configurations define `_FORTIFY_SOURCE=3`. Linux executables also request RELRO and immediate dynamic
binding. Fortification is intentionally absent from Debug builds because glibc requires compiler
optimization for it to be effective.

The `unix-strict` CI job does not infer protection from CMake cache checks. It inspects the emitted
`daemon_main.cpp` compile command, the generated `glyphastored` link command, and the final ELF with
`readelf`. The job requires ISO rather than GNU language mode, optimization, warnings-as-errors,
stack protection, fortification, PIE, `PT_GNU_RELRO`, and `BIND_NOW`/`DF_1_NOW` evidence.

The separate `macos-correctness` and `unix-correctness` presets enable diagnostic-only standard
library checks after a compile probe. They select libc++ debug hardening when the installed libc++
supports `_LIBCPP_HARDENING_MODE_DEBUG`, or `_GLIBCXX_ASSERTIONS` with libstdc++. Configuration
fails when neither mode is available. These presets are intentionally isolated from production and
release builds because standard-library diagnostic modes may affect ABI, layout, and performance.

The intentional local correctness driver also compile- and run-probes ASan pointer
compare/subtract instrumentation with a standard-library container before enabling it in the
sanitizer build. A toolchain that accepts the flags but rejects valid one-past-end container
arithmetic therefore remains disabled. The driver also queries the linked ASan runtime's option
list, probes each advertised option for platform rejection, and then enables the supported
initialization-order, stack-use-after-return, strict-string, allocation-pair, and (when the
functional probe passes) invalid-pointer-pair checks. These diagnostics remain isolated from normal
and release builds.

The static-analysis build runs the repository clang-tidy profile and the pinned clang-format
version. A second pass derives the exact production source set from `compile_commands.json` and
treats unchecked `optional` access, use-after-move, analyzer-confirmed dead stores, and mismatched
declaration/definition parameter names as fail-closed diagnostics. The wider all-target bugprone,
analyzer, performance, portability, and selected CERT families remain visible for triage; warnings
known to be test-macro- or toolchain-sensitive are not silently presented as a zero-warning claim.
On macOS only, the allocation-fault executable is excluded from clang-tidy because its deliberate
global `operator new`/`operator delete` replacements conflict with clang-tidy 21's libc++ handling
of `__builtin_operator_delete`. The target is still compiled and executed, and this exception does
not reduce the production-source pass derived from the compile database.

Primary references:

- [CMake `CheckLinkerFlag`](https://cmake.org/cmake/help/latest/module/CheckLinkerFlag.html)
- [GCC instrumentation options](https://gcc.gnu.org/onlinedocs/gcc/Instrumentation-Options.html)
- [GCC link options](https://gcc.gnu.org/onlinedocs/gcc/Link-Options.html)
- [GNU C Library source fortification](https://sourceware.org/glibc/manual/latest/html_node/Source-Fortification.html)
- [GNU linker options](https://sourceware.org/binutils/docs/ld/Options.html)
