# Security policy

## Supported versions

GlyphaStore is pre-release software and currently has no production-supported version.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub private vulnerability
reporting for this repository, or contact the repository owner privately. Include affected commit,
reproduction steps, impact, and any sanitizer or crash output. Expect an acknowledgement within
three business days.

Persisted segment files, future network frames, and recovery metadata are all treated as untrusted
input. A crash, out-of-bounds access, integer overflow, use-after-free, data race, or silent
corruption is considered security-relevant.
