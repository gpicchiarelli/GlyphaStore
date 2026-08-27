# FreeBSD reference port

This is upstream reference material for a future FreeBSD ports submission. It uses staged CMake
installation, system TLS, `USE_LDCONFIG`, a dedicated service account, a `.sample` configuration,
and native `rc.subr` integration.

It is intentionally **not yet represented as a proven package**: the `glyphastore` UID/GID still
requires allocation by the FreeBSD ports project. Do **not** create
`packaging/freebsd/PORTS_ACCOUNT_REGISTERED` until that upstream registration exists; the marker is
a honesty latch for `validate_bsd_packaging.py --release` and the fail-closed FreeBSD package
producer, not a claim of ports acceptance. The release producer generates `distinfo` outside the
source archive from its already sealed bytes; committing that self-digest here would be circular.
After the service account is registered, retain evidence for `make stage`, `check-plist`,
`package`, clean-host install, service PUT/GET/ERASE/recovery, upgrade, and deinstall.

Do not set a release gate to accepted merely because this reference tree parses.
