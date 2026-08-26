#!/usr/bin/env bash
# Prove installed Python/Perl artifacts against the real secure-profile daemon.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python="${PYTHON:-python3}"
perl="${PERL:-perl}"
make="${MAKE:-make}"
daemon="${GLYPHASTORED:-}"
cpp_source="${GLYPHASTORE_INTEROP_CLIENT:-}"
go_source="${GLYPHASTORE_GO_INTEROP:-$root/sdk/go/bin/glyphastore-interop}"

if [[ -z "$daemon" || ! -x "$daemon" ]]; then
  echo "GLYPHASTORED must name a TLS-capable daemon" >&2
  exit 1
fi
if [[ -z "$cpp_source" || ! -x "$cpp_source" ]]; then
  echo "GLYPHASTORE_INTEROP_CLIENT must name the built C++ interop peer" >&2
  exit 1
fi
if [[ ! -x "$go_source" ]]; then
  echo "missing Go interop peer: $go_source" >&2
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

shopt -s nullglob
wheels=("$root"/sdk/python/dist/glyphastore-*.whl)
perl_tarballs=("$root"/sdk/perl/dist/GlyphaStore-*.tar.gz)
shopt -u nullglob
if [[ "${#wheels[@]}" -ne 1 ]]; then
  echo "expected exactly one built Python wheel under sdk/python/dist" >&2
  exit 1
fi
if [[ "${#perl_tarballs[@]}" -ne 1 ]]; then
  echo "expected exactly one built Perl tarball under sdk/perl/dist" >&2
  exit 1
fi
wheel="${wheels[0]}"
perl_tarball="${perl_tarballs[0]}"

work="$(mktemp -d "${TMPDIR:-/tmp}/glyphastore-installed-secure.XXXXXX")"
cleanup() { rm -rf "$work"; }
trap cleanup EXIT
venv="$work/python-venv"
perl_artifact="$work/perl-artifact"
perl_install="$work/perl-install"
mkdir -p "$work/bin" "$perl_artifact" "$perl_install"
cp "$cpp_source" "$work/bin/glyphastore-interop-cpp"
cp "$go_source" "$work/bin/glyphastore-interop-go"

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
perl_lib="$perl_install/lib/perl5"
perl_loaded_from="$(PERL5LIB="$perl_lib" "$perl" -MGlyphaStore::Client -e \
  'print $INC{q{GlyphaStore/Client.pm}}')"
if [[ "$perl_loaded_from" != "$perl_lib/"* || "$perl_loaded_from" == "$root/"* ]]; then
  echo "tarball smoke resolved Perl SDK outside isolated install: $perl_loaded_from" >&2
  exit 1
fi

echo "installed Python wheel: $wheel"
echo "loaded Python SDK: $python_loaded_from"
echo "installed Perl tarball: $perl_tarball"
echo "loaded Perl SDK: $perl_loaded_from"

GLYPHASTORE_INTEROP_USE_INSTALLED=1 \
SECURE_INTEROP_SKIP_RUBY=1 \
SECURE_INTEROP_SKIP_ERLANG=1 \
PYTHON="$venv/bin/python" \
PERL="$perl" \
PYTHONPATH= \
PERL5LIB="$perl_lib" \
RUBYLIB= \
GLYPHASTORED="$daemon" \
GLYPHASTORE_INTEROP_CLIENT="$work/bin/glyphastore-interop-cpp" \
GLYPHASTORE_GO_INTEROP="$work/bin/glyphastore-interop-go" \
"$root/scripts/test-secure-profile-interop.sh"

echo "Installed Python/Perl secure-profile interop PASSED"
