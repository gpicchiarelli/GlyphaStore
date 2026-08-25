# Packaging and publishing (PyPI)

This directory is a self-contained Python distribution. Runtime dependencies: none (stdlib only).

## Preconditions

- Python ≥ 3.11
- Build tools: `python -m pip install build twine`
- Canonical wire fixtures under `tests/fixtures/` (vendored; must match repository
  `tests/fixtures/wire_*_v2.hex`). Refresh with `./scripts/sync-sdk-fixtures.sh`
  after changing the repository corpus.
- License: BSD-3-Clause (`LICENSE`), matching the GlyphaStore project

## Local verification

From the repository root:

```bash
./scripts/package-python-client.sh
```

The script:

1. Confirms vendored fixtures match the repository corpus
2. Builds an sdist and a wheel with `python -m build`
3. Runs `twine check` on the artifacts
4. Installs the wheel and normalized sdist into separate clean temporary environments
5. Runs the complete unittest suite against each installed artifact

## Manual build

```bash
cd sdk/python
python -m build
python -m twine check dist/*
```

Artifacts land in `sdk/python/dist/`:

- `glyphastore-0.1.0.tar.gz` (sdist)
- `glyphastore-0.1.0-py3-none-any.whl` (wheel)

## Publish to TestPyPI

```bash
python -m twine upload --repository testpypi dist/*
pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple glyphastore
```

## Publish to PyPI

```bash
python -m twine upload dist/*
```

Use a PyPI API token scoped to the `glyphastore` project. Do not commit tokens.

## Version bump checklist

1. Bump `__version__` in `src/glyphastore/__init__.py` (single source; `pyproject.toml` is dynamic)
2. Update root `VERSION` and every other official SDK in lockstep (or land an ADR)
3. Update `CHANGELOG.md`
4. Run `./scripts/check-sdk-versions.sh` and `./scripts/package-python-client.sh`
5. Tag/release and upload
