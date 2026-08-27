# OpenBSD reference port

This is upstream reference material for a future OpenBSD ports submission. It uses the native
CMake module, base LibreSSL, kqueue, the existing pledge/unveil path, an `_glyphastore` account,
`rc.subr`/`rcctl`, configuration samples, and OpenBSD major/minor shared-library notation.

It is intentionally **not yet represented as a proven package**: the `_glyphastore` account details
must be reviewed with the ports project. Do **not** create
`packaging/openbsd/PORTS_ACCOUNT_REGISTERED` until that upstream registration exists; the marker is
a honesty latch for `validate_bsd_packaging.py --release` and the fail-closed OpenBSD package
producer, not a claim of ports acceptance. A native release producer must generate `distinfo`
outside the source archive from its already sealed bytes; committing that self-digest here would be
circular. Retain native-VM evidence for fake installation, plist/shared-symbol checks, package
install, `rcctl` lifecycle, PUT/GET/ERASE/recovery, upgrade, and deinstall.

Cross-compilation or structural validation is not package evidence.
