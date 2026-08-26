# BSD reference packaging

Status: structural reference; native package evidence pending

`packaging/freebsd/` and `packaging/openbsd/` are upstream reference ports, not evidence that an
official ports tree has accepted GlyphaStore or that a package has passed clean-host installation.
`engineering/tools/validate_bsd_packaging.py` checks their structural invariants and ties product
and shared-library versions back to `VERSION` and `ABI_VERSION`.

The FreeBSD reference uses staged CMake installation, system TLS, `USE_LDCONFIG`, a static plist,
the `rc.subr` framework, `/usr/local/etc/glyphastored.conf`, `/var/db/glyphastore`, and a dedicated
`glyphastore` account. The service is disabled by default, uses `daemon(8)` only as a supervisor,
records the child PID, drops privileges, and forwards normal stop into the daemon's SIGTERM drain.

The OpenBSD reference uses the ports CMake module, base LibreSSL, `SHARED_LIBS` major/minor
authority, WANTLIB, `${TRUEPREFIX}`, `@sample`, `@rcscript`, `/etc/glyphastored.conf`,
`/var/glyphastore`, and `_glyphastore`. The daemon's existing kqueue and pledge/unveil paths remain
native; no TLS implementation is bundled.

The `--release` validator additionally requires explicit account-registration markers. Those
markers do not exist before ports-tree review. A `distinfo` file is deliberately not committed here:
including the digest of the source archive inside that same source archive would be circular. Each
native release producer must generate and retain `distinfo` from the already sealed source archive,
then run the native checksum target before package construction. This prevents structural reference
material from being promoted as a tested package.

The normal FreeBSD and OpenBSD workflows now also install the C ABI into a temporary native prefix,
build external consumers without source include/library paths, execute the CMake and pkg-config ABI
smokes, and retain their VM logs. This raises the portability signal but deliberately does not emit
`freebsd-package-evidence.json` or `openbsd-package-evidence.json`: no native package manager or
service lifecycle was exercised by that installed-prefix test.

The tag-only release graph additionally contains a FreeBSD package producer. It consumes the exact
sealed source archive, requires the account in the native ports authority, generates same-run
`distinfo`, builds through the ports framework, and exercises the installed package and rc.subr
lifecycle. It is implementation, not proof: the account marker and retained tagged run are still
absent. The equivalent OpenBSD producer remains to be implemented.

Before either artifact enters a manifest, retain native evidence for build/fake or stage,
packing-list and shared-symbol checks, package creation, install, service start/stop, protocol
PUT/GET/ERASE, durable restart/recovery, upgrade/reinstall, deinstall, configuration preservation,
and expected data-directory preservation. Cross-compilation is not accepted.
