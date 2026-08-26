# Packaging and publishing (RubyGems)

Gem name: `glyphastore` · Module: `GlyphaStore` · License: BSD-3-Clause · Ruby ≥ 3.2

Runtime dependencies: none (stdlib). Optional async stack: `async` gem (loaded only via
`require "glypha_store/async_client"`).

## Version

`GlyphaStore::VERSION` in `lib/glypha_store/version.rb` is the single source. It must match the
repository root `VERSION` file (`./scripts/check-sdk-versions.sh`).

## Local verification

```bash
./scripts/package-ruby-client.sh
```

The script:

1. Confirms vendored wire fixtures match the repository corpus
2. Asserts `GlyphaStore::VERSION` matches `VERSION`
3. Builds `sdk/ruby/dist/glyphastore-VERSION.gem`
4. Installs the gem into a clean `GEM_HOME`
5. Runs the installed `glyphastore-interop` executable with no source-tree load path
6. Runs the Minitest suite against the installed gem (plus development gems `async`, `minitest`)

## Manual build

```bash
cd sdk/ruby
gem build glyphastore.gemspec --output dist/glyphastore-0.1.0.gem
```

## Publish to RubyGems

```bash
gem push sdk/ruby/dist/glyphastore-0.1.0.gem
```

Requires an account with MFA. `allowed_push_host` is restricted to `https://rubygems.org`.
Do not commit API keys.

## Version bump checklist

1. Bump `lib/glypha_store/version.rb`
2. Update `CHANGELOG.md`
3. Ensure root `VERSION` matches (SDK lockstep)
4. Run `./scripts/check-sdk-versions.sh` and `./scripts/package-ruby-client.sh`
5. Tag/release and `gem push`
