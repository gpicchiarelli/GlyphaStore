#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
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

work="$(mktemp -d "${TMPDIR:-/tmp}/glyphastore-perl-pack.XXXXXX")"
cleanup() { rm -rf "$work"; }
trap cleanup EXIT

# Build from a clean copy so disttest does not fight the live tree.
cp -R "$sdk/." "$work/sdk"
cd "$work/sdk"
rm -rf blib GlyphaStore-* Makefile Makefile.old pm_to_blib MYMETA.* *.tar.gz

"$perl" Makefile.PL
"$make" manifest
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
  for my $module (qw(GlyphaStore GlyphaStore::Client GlyphaStore::Protocol)) {
    die "$module missing from provides\n" unless $meta->{provides}{$module};
  }
  print "META provides and license OK\n";
' 2>/dev/null || "$perl" -e '
  open my $fh, "<", "MYMETA.yml" or die $!;
  local $/;
  my $text = <$fh>;
  die "license missing\n" unless $text =~ /license:\s*.*bsd/i;
  for my $module (qw(GlyphaStore GlyphaStore::Client GlyphaStore::Protocol)) {
    die "$module missing from provides\n" unless $text =~ /\Q$module\E/;
  }
  print "META provides and license OK\n";
'

mkdir -p "$sdk/dist"
cp "${tarball[0]}" "$sdk/dist/"
echo "Perl packaging verification OK ($sdk/dist/$(basename "${tarball[0]}"))"
