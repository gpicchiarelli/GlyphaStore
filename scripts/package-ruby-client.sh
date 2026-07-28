#!/usr/bin/env bash
# Build and verify the Ruby gem from a clean install path.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sdk="$root/sdk/ruby"
ruby_bin="${RUBY:-}"
if [[ -z "$ruby_bin" && -x "${HOME:-}/.local/bin/mise" ]]; then
  ruby_bin="$("${HOME}/.local/bin/mise" exec ruby@3.3 -- which ruby 2>/dev/null || true)"
fi
ruby_bin="${ruby_bin:-$(command -v ruby)}"
gem_bin="${GEM:-$("$ruby_bin" -e 'print Gem.default_bindir + "/gem"')}"
if [[ ! -x "$gem_bin" ]]; then
  gem_bin="$(dirname "$ruby_bin")/gem"
fi

for fixture in wire_requests_v2.hex wire_responses_v2.hex; do
  if ! cmp -s "$root/tests/fixtures/$fixture" "$sdk/test/fixtures/$fixture"; then
    echo "vendored fixture drift: $fixture" >&2
    echo "run ./scripts/sync-sdk-fixtures.sh" >&2
    exit 1
  fi
done

expected="$(tr -d '[:space:]' <"$root/VERSION")"
got="$(RUBYLIB="$sdk/lib" "$ruby_bin" -e 'require "glypha_store/version"; print GlyphaStore::VERSION')"
if [[ "$got" != "$expected" ]]; then
  echo "GlyphaStore::VERSION='$got' does not match VERSION='$expected'" >&2
  exit 1
fi

rm -rf "$sdk/dist" "$sdk"/glyphastore-*.gem
mkdir -p "$sdk/dist"
(
  cd "$sdk"
  "$gem_bin" build glyphastore.gemspec
  mv -f "glyphastore-$got.gem" "dist/glyphastore-$got.gem"
)

work="$(mktemp -d "${TMPDIR:-/tmp}/glyphastore-ruby-pack.XXXXXX")"
cleanup() { rm -rf "$work"; }
trap cleanup EXIT

export GEM_HOME="$work/gem_home"
export GEM_PATH="$GEM_HOME"
export PATH="$GEM_HOME/bin:$PATH"
mkdir -p "$GEM_HOME"
"$gem_bin" install --local --no-document "$sdk/dist/glyphastore-$got.gem"
"$gem_bin" install --no-document async minitest >/dev/null

# Run the test suite against the *installed* gem (not the tree lib/).
cp -R "$sdk/test" "$work/test"
(
  cd "$work"
  unset RUBYLIB || true
  "$ruby_bin" -Itest -e 'Dir["test/test_*.rb"].sort.each { |f| require "./#{f}" }'
)

echo "Ruby packaging verification OK ($sdk/dist/glyphastore-$got.gem)"
