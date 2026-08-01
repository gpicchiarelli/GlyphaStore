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

Primary references:

- [CMake `CheckLinkerFlag`](https://cmake.org/cmake/help/latest/module/CheckLinkerFlag.html)
- [GCC instrumentation options](https://gcc.gnu.org/onlinedocs/gcc/Instrumentation-Options.html)
- [GCC link options](https://gcc.gnu.org/onlinedocs/gcc/Link-Options.html)
- [GNU C Library source fortification](https://sourceware.org/glibc/manual/latest/html_node/Source-Fortification.html)
- [GNU linker options](https://sourceware.org/binutils/docs/ld/Options.html)
