# Ruby SDK client benchmark

- SDK version: `0.1.0`
- Ops (PUT/GET pairs): `100000`
- Warmup/repeats: `1` / `7`
- Modes: sequential drain + concurrent `threaded per-Worker pipelines` (when workers>1)

- workers=1 listen=127.0.0.1:64371
- workers=2 listen=127.0.0.1:64903
- workers=4 listen=127.0.0.1:50034
