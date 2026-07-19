#!/usr/bin/env perl
use v5.32;
use strict;
use warnings;
use FindBin;
use Getopt::Long qw(GetOptions);
use Time::HiRes qw(time);

use lib "$FindBin::Bin/../lib";
use GlyphaStore;
use GlyphaStore::Client;

my %options = (
    host     => '127.0.0.1',
    workers  => 1,
    ops      => 100_000,
    pipeline => 128,
    warmup   => 1,
    repeats  => 7,
);
GetOptions(
    'host=s'     => \$options{host},
    'port=i'     => \$options{port},
    'workers=i'  => \$options{workers},
    'ops=i'      => \$options{ops},
    'pipeline=i' => \$options{pipeline},
    'warmup=i'   => \$options{warmup},
    'repeats=i'  => \$options{repeats},
) or die "invalid benchmark arguments\n";
die "--port is required\n" if !$options{port};
die "numeric arguments are outside benchmark limits\n"
    if $options{workers} < 1 || $options{ops} < 1 || $options{pipeline} < 1
    || $options{warmup} < 0 || $options{repeats} < 1;

my @requests = map { [] } 1 .. $options{workers};
my @remaining = map {
    int($options{ops} / $options{workers})
        + ($_ < $options{ops} % $options{workers} ? 1 : 0)
} 0 .. $options{workers} - 1;
my $candidate = 0;
while (grep { $_ } @remaining) {
    my $key = sprintf('perl-bench-%012d', $candidate);
    my $owner = GlyphaStore::Protocol::worker_for($key, $options{workers});
    if ($remaining[$owner]) {
        my $value = chr($candidate & 0xFF) x 64;
        push @{$requests[$owner]}, { opcode => 'put', key => $key, value => $value };
        push @{$requests[$owner]}, { opcode => 'get', key => $key };
        --$remaining[$owner];
    }
    ++$candidate;
}
my $batch_frames = $options{pipeline} * 2;
my @batches;
for my $worker_requests (@requests) {
    my @worker_batches;
    for (my $offset = 0; $offset < @$worker_requests; $offset += $batch_frames) {
        my $last = $offset + $batch_frames - 1;
        $last = $#$worker_requests if $last > $#$worker_requests;
        push @worker_batches, [@$worker_requests[$offset .. $last]];
    }
    push @batches, \@worker_batches;
}

my $client = GlyphaStore::Client->connect(host => $options{host}, port => $options{port});
die "server Worker count does not match --workers\n"
    if $client->worker_count != $options{workers};

sub run_once {
    my $started = time;
    for my $worker_batches (@batches) {
        for my $batch (@$worker_batches) {
            my $responses = $client->execute_pipeline($batch);
            die "pipeline response count mismatch\n" if @$responses != @$batch;
            for my $index (0 .. $#$batch) {
                die "pipeline request failed\n"
                    if $responses->[$index]->{outcome} ne 'succeeded';
                if ($batch->[$index]->{opcode} eq 'get') {
                    die "pipeline GET value mismatch\n"
                        if $responses->[$index]->{value} ne $batch->[$index - 1]->{value};
                }
            }
        }
    }
    return time - $started;
}

sub median {
    my @sorted = sort { $a <=> $b } @_;
    return $sorted[int(@sorted / 2)] if @sorted % 2;
    return ($sorted[@sorted / 2 - 1] + $sorted[@sorted / 2]) / 2;
}

run_once() for 1 .. $options{warmup};
my @samples = map { run_once() } 1 .. $options{repeats};
$client->close;

my $sdk_version = $GlyphaStore::VERSION;
my $operation_count = $options{ops} * 2;
my @rates = map { $operation_count / $_ } @samples;
printf "# glyphastore Perl client benchmark\n";
printf "# sdk_version=%s runtime=sync execution=single-process-worker-sequential "
    . "workers=%d pipeline_pairs=%d operations=%d\n",
    $sdk_version, $options{workers}, $options{pipeline}, $operation_count;
printf "name=perl_client_pipeline_read_after_write sdk_version=%s runtime=sync "
    . "execution=single-process-worker-sequential workers=%d pipeline_pairs=%d "
    . "operations=%d samples=%d median_seconds=%.9f min_seconds=%.9f max_seconds=%.9f "
    . "median_ops_per_second=%.3f min_ops_per_second=%.3f max_ops_per_second=%.3f\n",
    $sdk_version, $options{workers}, $options{pipeline}, $operation_count,
    scalar(@samples), median(@samples), (sort { $a <=> $b } @samples)[0],
    (sort { $b <=> $a } @samples)[0], median(@rates),
    (sort { $a <=> $b } @rates)[0], (sort { $b <=> $a } @rates)[0];
