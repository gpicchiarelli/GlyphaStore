#!/usr/bin/env bash
# Prove installed Python/Perl/Ruby/Go/Erlang artifacts against the real secure-profile daemon.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python="${PYTHON:-python3}"
perl="${PERL:-perl}"
ruby="${RUBY:-}"
make="${MAKE:-make}"
daemon="${GLYPHASTORED:-}"
cpp_source="${GLYPHASTORE_INTEROP_CLIENT:-}"
go_bin="${GO:-go}"

if [[ -z "$daemon" || ! -x "$daemon" ]]; then
  echo "GLYPHASTORED must name a TLS-capable daemon" >&2
  exit 1
fi
if [[ -z "$cpp_source" || ! -x "$cpp_source" ]]; then
  echo "GLYPHASTORE_INTEROP_CLIENT must name the built C++ interop peer" >&2
  exit 1
fi
if ! command -v "$go_bin" >/dev/null 2>&1; then
  echo "Go is required for the installed secure-profile matrix" >&2
  exit 1
fi
if ! "$perl" -MIO::Socket::SSL -e1 >/dev/null 2>&1; then
  echo "PERL must provide IO::Socket::SSL for the installed secure-profile matrix" >&2
  exit 1
fi
if ! "$perl" -MIO::Socket::SSL -e \
  'IO::Socket::SSL::SSL_Context->new(SSL_version => "TLSv1_3") or die IO::Socket::SSL::errstr()' \
  >/dev/null 2>&1; then
  echo "PERL IO::Socket::SSL backend must support TLS 1.3" >&2
  exit 1
fi
if [[ -z "$ruby" && -x "$HOME/.local/bin/mise" ]]; then
  ruby="$("$HOME/.local/bin/mise" which ruby@3.3 2>/dev/null || true)"
fi
if [[ -z "$ruby" ]]; then
  ruby="$(command -v ruby || true)"
fi
if [[ -z "$ruby" || ! -x "$ruby" ]] || ! "$ruby" -e \
  'v=RUBY_VERSION.split(".").map!(&:to_i); exit(v[0] > 3 || (v[0] == 3 && v[1] >= 2) ? 0 : 1)' \
  >/dev/null 2>&1; then
  echo "RUBY must be version 3.2 or newer for the installed secure-profile matrix" >&2
  exit 1
fi
if ! command -v erl >/dev/null 2>&1 || ! command -v escript >/dev/null 2>&1 || \
  ! command -v rebar3 >/dev/null 2>&1; then
  echo "Erlang/OTP and rebar3 are required for the installed secure-profile matrix" >&2
  exit 1
fi

shopt -s nullglob
wheels=("$root"/sdk/python/dist/glyphastore-*.whl)
perl_tarballs=("$root"/sdk/perl/dist/GlyphaStore-*.tar.gz)
ruby_gems=("$root"/sdk/ruby/dist/glyphastore-*.gem)
go_archives=("$root"/sdk/go/dist/glyphastore-go-*.tar.gz)
erlang_archives=("$root"/sdk/erlang/dist/glyphastore-erlang-*.tar.gz)
shopt -u nullglob
if [[ "${#wheels[@]}" -ne 1 ]]; then
  echo "expected exactly one built Python wheel under sdk/python/dist" >&2
  exit 1
fi
if [[ "${#perl_tarballs[@]}" -ne 1 ]]; then
  echo "expected exactly one built Perl tarball under sdk/perl/dist" >&2
  exit 1
fi
if [[ "${#ruby_gems[@]}" -ne 1 ]]; then
  echo "expected exactly one built Ruby gem under sdk/ruby/dist" >&2
  exit 1
fi
if [[ "${#go_archives[@]}" -ne 1 ]]; then
  echo "expected exactly one built Go archive under sdk/go/dist" >&2
  exit 1
fi
if [[ "${#erlang_archives[@]}" -ne 1 ]]; then
  echo "expected exactly one built Erlang archive under sdk/erlang/dist" >&2
  exit 1
fi
wheel="${wheels[0]}"
perl_tarball="${perl_tarballs[0]}"
ruby_gem="${ruby_gems[0]}"
go_archive="${go_archives[0]}"
erlang_archive="${erlang_archives[0]}"

work="$(mktemp -d "${TMPDIR:-/tmp}/glyphastore-installed-secure.XXXXXX")"
cleanup() { rm -rf "$work"; }
trap cleanup EXIT
venv="$work/python-venv"
perl_artifact="$work/perl-artifact"
perl_install="$work/perl-install"
ruby_gem_home="$work/ruby-gems"
go_artifact="$work/go-artifact"
erlang_artifact="$work/erlang-artifact"
mkdir -p "$work/bin" "$perl_artifact" "$perl_install" "$ruby_gem_home" "$go_artifact" \
  "$erlang_artifact"
cp "$cpp_source" "$work/bin/glyphastore-interop-cpp"

"$python" -m venv "$venv"
"$venv/bin/python" -m pip install --disable-pip-version-check -q --no-deps "$wheel"
python_loaded_from="$("$venv/bin/python" -c 'import glyphastore; print(glyphastore.__file__)')"
if [[ "$python_loaded_from" == "$root/"* ]]; then
  echo "wheel smoke resolved Python SDK from source: $python_loaded_from" >&2
  exit 1
fi

tar -xzf "$perl_tarball" -C "$perl_artifact"
perl_roots=("$perl_artifact"/GlyphaStore-*)
if [[ "${#perl_roots[@]}" -ne 1 || ! -d "${perl_roots[0]}" ]]; then
  echo "Perl tarball must contain exactly one GlyphaStore-VERSION root" >&2
  exit 1
fi
(
  cd "${perl_roots[0]}"
  "$perl" Makefile.PL INSTALL_BASE="$perl_install" >/dev/null
  "$make" >/dev/null
  "$make" install >/dev/null
)
perl_lib="$(cd "$perl_install/lib/perl5" && pwd -P)"
perl_loaded_from="$(PERL5LIB="$perl_lib" "$perl" -MGlyphaStore::Client -e \
  'use Cwd qw(abs_path); print abs_path($INC{q{GlyphaStore/Client.pm}})')"
if [[ "$perl_loaded_from" != "$perl_lib/"* || "$perl_loaded_from" == "$root/"* ]]; then
  echo "tarball smoke resolved Perl SDK outside isolated install: $perl_loaded_from" >&2
  exit 1
fi

GEM_HOME="$ruby_gem_home" GEM_PATH="$ruby_gem_home" \
  "$ruby" -S gem install --local --no-document "$ruby_gem" >/dev/null
ruby_package_root="$(cd "$ruby_gem_home/gems/glyphastore-$(tr -d '[:space:]' <"$root/VERSION")" && pwd -P)"
ruby_helper="$ruby_package_root/exe/glyphastore-interop"
if [[ ! -f "$ruby_helper" ]]; then
  echo "installed Ruby gem is missing its interop executable: $ruby_helper" >&2
  exit 1
fi
ruby_loaded_from="$(GEM_HOME="$ruby_gem_home" GEM_PATH="$ruby_gem_home" "$ruby" -e \
  'require "glypha_store"; print $LOADED_FEATURES.find { |p| p.end_with?("/glypha_store.rb") }')"
if [[ "$ruby_loaded_from" != "$ruby_package_root/"* || "$ruby_loaded_from" == "$root/"* ]]; then
  echo "gem smoke resolved Ruby SDK outside isolated install: $ruby_loaded_from" >&2
  exit 1
fi

tar -xzf "$go_archive" -C "$go_artifact"
go_roots=("$go_artifact"/glyphastore-go-*)
if [[ "${#go_roots[@]}" -ne 1 || ! -d "${go_roots[0]}" ]]; then
  echo "Go archive must contain exactly one glyphastore-go-VERSION root" >&2
  exit 1
fi
go_root="$(cd "${go_roots[0]}" && pwd -P)"
(
  cd "$go_root"
  "$go_bin" build -o "$work/bin/glyphastore-interop-go" ./cmd/glyphastore-interop
)
if [[ ! -x "$work/bin/glyphastore-interop-go" ]]; then
  echo "Go archive did not produce glyphastore-interop" >&2
  exit 1
fi

tar -xzf "$erlang_archive" -C "$erlang_artifact"
erlang_roots=("$erlang_artifact"/glyphastore-erlang-*)
if [[ "${#erlang_roots[@]}" -ne 1 || ! -d "${erlang_roots[0]}" ]]; then
  echo "Erlang archive must contain exactly one glyphastore-erlang-VERSION root" >&2
  exit 1
fi
erlang_root="$(cd "${erlang_roots[0]}" && pwd -P)"
(
  cd "$erlang_root"
  rebar3 compile >/dev/null
)
erlang_lib="$erlang_root/_build/default/lib"
erlang_ebin="$erlang_lib/glyphastore/ebin"
erlang_loaded_from="$(ERL_LIBS="$erlang_lib" erl -noshell -pa "$erlang_ebin" \
  -eval 'io:format("~s", [code:which(glyphastore_client)]), halt().')"
if [[ "$erlang_loaded_from" != "$erlang_root/"* || "$erlang_loaded_from" == "$root/"* ]]; then
  echo "archive smoke resolved Erlang SDK outside isolated build: $erlang_loaded_from" >&2
  exit 1
fi

echo "installed Python wheel: $wheel"
echo "loaded Python SDK: $python_loaded_from"
echo "installed Perl tarball: $perl_tarball"
echo "loaded Perl SDK: $perl_loaded_from"
echo "installed Ruby gem: $ruby_gem"
echo "loaded Ruby SDK: $ruby_loaded_from"
echo "installed Go archive: $go_archive"
echo "built Go interop peer: $work/bin/glyphastore-interop-go"
echo "installed Erlang archive: $erlang_archive"
echo "loaded Erlang SDK: $erlang_loaded_from"

GLYPHASTORE_INTEROP_USE_INSTALLED=1 \
SECURE_INTEROP_SKIP_RUBY=0 \
SECURE_INTEROP_SKIP_ERLANG=0 \
PYTHON="$venv/bin/python" \
PERL="$perl" \
RUBY="$ruby" \
PYTHONPATH= \
PERL5LIB="$perl_lib" \
RUBYLIB= \
GEM_HOME="$ruby_gem_home" \
GEM_PATH="$ruby_gem_home" \
ERL_LIBS="$erlang_lib" \
GLYPHASTORED="$daemon" \
GLYPHASTORE_INTEROP_CLIENT="$work/bin/glyphastore-interop-cpp" \
GLYPHASTORE_GO_INTEROP="$work/bin/glyphastore-interop-go" \
GLYPHASTORE_RUBY_INTEROP="$ruby_helper" \
GLYPHASTORE_ERLANG_INTEROP="$erlang_root/scripts/glyphastore-interop.escript" \
"$root/scripts/test-secure-profile-interop.sh"

echo "Installed Python/Perl/Ruby/Go/Erlang secure-profile interop PASSED"
