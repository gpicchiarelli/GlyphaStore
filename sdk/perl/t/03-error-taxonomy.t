#!/usr/bin/env perl
use v5.32;
use strict;
use warnings;
use FindBin;
use JSON::PP qw(decode_json);
use Test::More;

use GlyphaStore::Client;
use GlyphaStore::Error;
use GlyphaStore::Protocol qw(
    STATUS_INVALID_REQUEST STATUS_UNSUPPORTED STATUS_INTERNAL_ERROR STATUS_NOT_FOUND
    STATUS_OVERLOADED STATUS_WRONG_OWNER STATUS_NOT_BOUND STATUS_PERMISSION_DENIED
);

sub fixture_path {
    my ($name) = @_;
    my $local = "$FindBin::Bin/fixtures/$name";
    return $local if -f $local;
    return "$FindBin::Bin/../../../tests/fixtures/$name";
}

my $path = fixture_path('error_taxonomy_v1.json');
ok(-f $path, 'error taxonomy fixture present');
open my $fh, '<:raw', $path or die "open $path: $!";
my $raw = do { local $/; <$fh> };
close $fh;
my $fixture = decode_json($raw);

for my $case (@{ $fixture->{cases} }) {
    subtest $case->{id} => sub {
        my $error = GlyphaStore::Client::_status_error($case->{wire_status});
        is($error->category, $case->{category}, 'category');
        is($error->wire_status, $case->{wire_status}, 'wire_status');
        is($error->retryability, $case->{read_retryability}, 'read retryability');

        my $enriched = GlyphaStore::Client::_status_error($case->{wire_status})->enrich(
            bytes_sent       => 1,
            mutation_outcome => $case->{mutation_outcome},
        );
        is($enriched->retryability, $case->{mutation_retryability}, 'mutation retryability');

        my $want_unhealthy =
             $case->{wire_status} == STATUS_WRONG_OWNER
          || $case->{wire_status} == STATUS_NOT_BOUND;
        ok(($case->{unhealthy} ? 1 : 0) == ($want_unhealthy ? 1 : 0), 'unhealthy flag');
    };
}

done_testing();
