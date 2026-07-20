# Perl SDK client benchmark

- SDK version: `0.1.0`
- Ops (PUT/GET pairs): `100000`
- Warmup/repeats: `1` / `7`
- Modes: sequential drain + concurrent `execute_worker_pipelines` (when workers>1)

- workers=1 listen=127.0.0.1:51039
- workers=2 listen=127.0.0.1:51394
- workers=4 listen=127.0.0.1:52093
