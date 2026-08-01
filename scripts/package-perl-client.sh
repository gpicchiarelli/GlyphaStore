#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export GLYPHASTORE_ROOT="$root"
# shellcheck disable=SC1091
source "$root/scripts/export-reproducible-build-env.sh"
perl="${PERL:-perl}"
make="${MAKE:-make}"
sdk="$root/sdk/perl"

for fixture in wire_requests_v2.hex wire_responses_v2.hex; do
  if ! cmp -s "$root/tests/fixtures/$fixture" "$sdk/t/fixtures/$fixture"; then
    echo "vendored fixture drift: $fixture" >&2
    echo "copy repository fixtures into sdk/perl/t/fixtures/ before packaging" >&2
    exit 1
  fi
done

expected="$(tr -d '[:space:]' <"$root/VERSION")"
# Prefer portable tools over ripgrep (not installed on all CI images).
perl_versions="$(
  find "$sdk/lib" -type f -name '*.pm' -print0 |
    xargs -0 "$perl" -ne "print \$1, \"\\n\" if /our \\\$VERSION = '([^']+)'/" |
    sort -u
)"
perl_count="$(printf '%s\n' "$perl_versions" | grep -c . || true)"
if [[ "$perl_count" -ne 1 || "$perl_versions" != "$expected" ]]; then
  echo "Perl \$VERSION drift or mismatch: '${perl_versions:-<none>}' (expected $expected)" >&2
  exit 1
fi
echo "Perl VERSION $perl_versions OK"

work="$(mktemp -d "${TMPDIR:-/tmp}/glyphastore-perl-pack.XXXXXX")"
cleanup() { rm -rf "$work"; }
trap cleanup EXIT

# Build from a clean copy so disttest does not fight the live tree.
cp -R "$sdk/." "$work/sdk"
cd "$work/sdk"
rm -rf blib GlyphaStore-* Makefile Makefile.old pm_to_blib MYMETA.* *.tar.gz

"$perl" Makefile.PL
"$make" manifest
# ExtUtils::Manifest skips dotfiles by default; keep the brutal critic profile in the tarball.
for profile in .perlcriticrc .perltidyrc; do
  if [[ -f "$profile" ]] && ! grep -qxF "$profile" MANIFEST; then
    echo "$profile" >>MANIFEST
  fi
done
"$make" disttest
"$make" dist

tarball=(GlyphaStore-*.tar.gz)
if [[ ! -f "${tarball[0]}" ]]; then
  echo "expected GlyphaStore-*.tar.gz was not produced" >&2
  exit 1
fi

"$perl" -MJSON::PP=decode_json -e '
  open my $fh, "<:raw", "MYMETA.json" or die $!;
  local $/;
  my $meta = decode_json(<$fh>);
  die "license missing\n" unless ($meta->{license} // "") =~ /bsd/i
      || grep { /bsd/i } @{$meta->{license} // []};
  for my $module (qw(
    GlyphaStore GlyphaStore::Client GlyphaStore::Protocol
    GlyphaStore::Error GlyphaStore::SendFailure
  )) {
    die "$module missing from provides\n" unless $meta->{provides}{$module};
  }
  print "META provides and license OK\n";
' 2>/dev/null || "$perl" -e '
  open my $fh, "<", "MYMETA.yml" or die $!;
  local $/;
  my $text = <$fh>;
  die "license missing\n" unless $text =~ /license:\s*.*bsd/i;
  for my $module (qw(
    GlyphaStore GlyphaStore::Client GlyphaStore::Protocol
    GlyphaStore::Error GlyphaStore::SendFailure
  )) {
    die "$module missing from provides\n" unless $text =~ /\Q$module\E/;
  }
  print "META provides and license OK\n";
'

mkdir -p "$sdk/dist"
cp "${tarball[0]}" "$sdk/dist/"
"$root/scripts/normalize-tar-gz.sh" "$sdk/dist/$(basename "${tarball[0]}")"
echo "Perl packaging verification OK ($sdk/dist/$(basename "${tarball[0]}"))"
