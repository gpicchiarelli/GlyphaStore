#!/usr/bin/env bash
# Build and verify the Ruby gem from a clean install path.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export GLYPHASTORE_ROOT="$root"
# shellcheck disable=SC1091
source "$root/scripts/export-reproducible-build-env.sh"
sdk="$root/sdk/ruby"
ruby_bin="${RUBY:-}"
if [[ -z "$ruby_bin" ]]; then
  for candidate in \
    "$HOME/.local/share/mise/installs/ruby/3.3.12/bin/ruby" \
    "$HOME/.local/share/mise/installs/ruby/3.3.0/bin/ruby"; do
    if [[ -x "$candidate" ]]; then
      ruby_bin="$candidate"
      break
    fi
  done
fi
ruby_bin="${ruby_bin:-$(command -v ruby || true)}"
if [[ -z "$ruby_bin" || ! -x "$ruby_bin" ]]; then
  echo "ruby >= 3.2 required for packaging" >&2
  exit 1
fi
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
if ! "$ruby_bin" -e 'v=RUBY_VERSION.split(".").map!(&:to_i); exit(v[0] > 3 || (v[0]==3 && v[1] >= 2) ? 0 : 1)'; then
  echo "ruby >= 3.2 required to package gem (got $($ruby_bin -v))" >&2
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

# Gem must ship copyright notices (BSD-3-Clause redistribution).
"$gem_bin" unpack --target "$work" "$sdk/dist/glyphastore-$got.gem" >/dev/null
gem_dir="$work/glyphastore-$got"
for required in LICENSE NOTICE; do
  if [[ ! -f "$gem_dir/$required" ]]; then
    echo "ERROR: gem missing $required" >&2
    exit 1
  fi
done

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
