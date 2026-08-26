# C ABI lifetime and ownership

`gs_store_open` creates one opaque owning handle. Input byte views and option path bytes are borrowed
only until the synchronous call returns. GET writes an owning copy into caller storage, so no
`RecordRef`, Segment mapping, file handle, or `ReadGeneration` lifetime escapes the engine.

`gs_store_close` closes the engine and consumes the handle regardless of its returned status. The
pointer must not be reused or freed by the caller. Close must be externally serialized with every
other operation; this constraint prevents a foreign runtime from racing a call with destruction of
the opaque allocation.

Version strings have static lifetime and must not be modified or freed. Status messages are copied
into caller memory and require no release function.
