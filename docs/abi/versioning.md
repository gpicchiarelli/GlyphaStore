# C ABI versioning

`ABI_VERSION` contains `major.minor` and is independent of `VERSION`, wire v2, and persistence v1.
The exported `gs_abi_major/minor` functions report it at runtime; product version components
are independently available through `gs_product_version_major/minor/patch` and the static-lifetime
`gs_product_version_string`. Header constants report the ABI version at compile time. Consumers must compare the major before using a library selected
outside the platform loader's normal SONAME rules.

An additive entry point or a tail-appended optional struct field increments the ABI minor after
compatibility tests. Removing a symbol, changing a signature/calling convention/layout/value, or
changing an established semantic contract increments the ABI major. Product-only, wire-only, and
storage-format-only changes do not change `ABI_VERSION`.

The loader major is derived from this same file. Packaging must not duplicate or infer the major
from the product version.
