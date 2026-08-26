# C ABI threading contract

After a successful open, GET, PUT, ERASE, and PUT batch are thread-safe and internally synchronized
by the paired Store. Concurrent GET/GET and GET/mutation calls are supported. The facade adds no
global data-path mutex and no thread-local allocation.

A single `gs_store` may be shared by caller threads, but the handle variable itself remains caller
owned. `gs_store_close` is the exclusive terminal operation: the caller must prevent new calls and
wait for foreign-language tasks using that handle before invoking close. Operations already
admitted inside the C++ Store follow its drain contract, but the C wrapper cannot make an arbitrary
stale pointer safe after its allocation is consumed.

ABI v1 has no callbacks, reentrancy hooks, asynchronous completion objects, or thread-affine output
buffers. Batch input/result arrays must not be concurrently modified during the call.
