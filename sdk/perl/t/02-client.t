use v5.32;
use strict;
use warnings;
use FindBin;
use IO::Select;
use IO::Socket::INET;
use POSIX qw(_exit);
use Test::More;

use lib "$FindBin::Bin/../lib";
use GlyphaStore::Client;
use GlyphaStore::Protocol qw(
    OP_INIT OP_PING OP_GET OP_PUT OP_ERASE OP_BIND_WORKER
    STATUS_OK STATUS_NOT_FOUND STATUS_NOT_BOUND STATUS_INVALID_REQUEST STATUS_WRONG_OWNER
    decode_request encode_response worker_for
);

sub read_exact {
    my ($socket, $size) = @_;
    my $output = '';
    while (length($output) < $size) {
        my $received = sysread($socket, my $chunk, $size - length($output));
        die "peer closed" if !$received;
        $output .= $chunk;
    }
    return $output;
}

sub receive_request {
    my ($socket) = @_;
    my $prefix = read_exact($socket, 4);
    my $size = unpack('V', $prefix);
    return decode_request($prefix . read_exact($socket, $size - 4));
}

sub send_response {
    my ($socket, %response) = @_;
    my $frame = encode_response(%response);
    my $offset = 0;
    while ($offset < length($frame)) {
        my $written = syswrite($socket, $frame, length($frame) - $offset, $offset);
        die "send failed: $!" if !$written;
        $offset += $written;
    }
}

sub start_server {
    my (%options) = @_;
    my $worker_count = $options{worker_count} // 1;
    my $listener = IO::Socket::INET->new(
        LocalAddr => '127.0.0.1', LocalPort => 0, Proto => 'tcp',
        Listen => $worker_count, ReuseAddr => 1)
        or die "listen failed: $!";
    my $port = $listener->sockport;
    my $pid = fork();
    die "fork failed: $!" if !defined($pid);
    if (!$pid) {
        my %values;
        eval {
            my $selector = IO::Select->new($listener);
            my %bound;
            my $accepted = 0;
            while ($selector->count) {
                my @ready = $selector->can_read(2)
                    or last;
                for my $handle (@ready) {
                    if (fileno($handle) == fileno($listener)) {
                        my $socket = $listener->accept or next;
                        $selector->add($socket);
                        ++$accepted;
                        $selector->remove($listener) if $accepted >= $worker_count;
                        next;
                    }
                    my $request = eval { receive_request($handle) };
                    if (!$request) {
                        $selector->remove($handle);
                        close($handle);
                        next;
                    }
                    my $opcode = $request->{opcode};
                    my $owner = $bound{$handle} // 0;
                    if ($opcode == OP_INIT) {
                        send_response($handle,
                            status => STATUS_OK,
                            request_id => $request->{request_id},
                            value => 'GlyphaStore/2',
                            owner_worker => 0,
                            worker_count => $worker_count,
                            routing_epoch => 9);
                    } elsif ($opcode == OP_BIND_WORKER) {
                        if ($request->{target_worker} >= $worker_count) {
                            send_response($handle,
                                status => STATUS_INVALID_REQUEST,
                                request_id => $request->{request_id},
                                owner_worker => 0,
                                worker_count => $worker_count,
                                routing_epoch => 9);
                            next;
                        }
                        $bound{$handle} = $request->{target_worker};
                        send_response($handle,
                            status => STATUS_OK,
                            request_id => $request->{request_id},
                            owner_worker => $request->{target_worker},
                            worker_count => $worker_count,
                            routing_epoch => 9);
                    } elsif (!exists $bound{$handle}) {
                        send_response($handle,
                            status => STATUS_NOT_BOUND,
                            request_id => $request->{request_id},
                            owner_worker => 0,
                            worker_count => $worker_count,
                            routing_epoch => 9);
                    } elsif ($opcode == OP_PING) {
                        send_response($handle,
                            status => STATUS_OK,
                            request_id => $request->{request_id},
                            value => $request->{value},
                            owner_worker => $owner,
                            worker_count => $worker_count,
                            routing_epoch => 9);
                    } elsif ($opcode == OP_PUT) {
                        my $owner = worker_for($request->{key}, $worker_count);
                        if ($owner != $bound{$handle}) {
                            send_response($handle,
                                status => STATUS_WRONG_OWNER,
                                request_id => $request->{request_id},
                                owner_worker => $owner,
                                worker_count => $worker_count,
                                routing_epoch => 9);
                            next;
                        }
                        $values{$request->{key}} = $request->{value};
                        if ($options{disconnect_on_put}) {
                            $selector->remove($handle);
                            close($handle);
                            next;
                        }
                        send_response($handle,
                            status => STATUS_OK,
                            request_id => $request->{request_id},
                            owner_worker => $owner,
                            worker_count => $worker_count,
                            routing_epoch => 9);
                    } elsif ($opcode == OP_GET) {
                        my $owner = worker_for($request->{key}, $worker_count);
                        if ($owner != $bound{$handle}) {
                            send_response($handle,
                                status => STATUS_WRONG_OWNER,
                                request_id => $request->{request_id},
                                owner_worker => $owner,
                                worker_count => $worker_count,
                                routing_epoch => 9);
                            next;
                        }
                        send_response($handle,
                            status => exists($values{$request->{key}}) ? STATUS_OK : STATUS_NOT_FOUND,
                            request_id => $request->{request_id},
                            value => $values{$request->{key}} // '',
                            owner_worker => $owner,
                            worker_count => $worker_count,
                            routing_epoch => 9);
                    } elsif ($opcode == OP_ERASE) {
                        my $owner = worker_for($request->{key}, $worker_count);
                        if ($owner != $bound{$handle}) {
                            send_response($handle,
                                status => STATUS_WRONG_OWNER,
                                request_id => $request->{request_id},
                                owner_worker => $owner,
                                worker_count => $worker_count,
                                routing_epoch => 9);
                            next;
                        }
                        my $found = exists($values{$request->{key}});
                        delete $values{$request->{key}};
                        send_response($handle,
                            status => $found ? STATUS_OK : STATUS_NOT_FOUND,
                            request_id => $request->{request_id},
                            owner_worker => $owner,
                            worker_count => $worker_count,
                            routing_epoch => 9);
                    }
                }
            }
            close($_) for grep { $_ != $listener } $selector->handles;
        };
        close($listener);
        _exit(0);
    }
    close($listener);
    return ($port, $pid);
}

my ($port, $pid) = start_server();
my $client = GlyphaStore::Client->connect(port => $port);
is($client->worker_count, 1, 'client discovers Worker count');
is($client->routing_epoch, 9, 'client records routing epoch');
is($client->worker_for("binary\x00key"), 0, 'client routes binary key');
is($client->put("binary\x00key", "value\x00\xff")->{outcome}, 'committed', 'PUT commits');
is($client->get("binary\x00key"), "value\x00\xff", 'GET preserves binary value');
is($client->ping('hello'), 'hello', 'PING echoes payload');

my $rejected = $client->put('bad-expiry', 'value', expire_at_ns => -1);
is($rejected->{outcome}, 'rejected', 'negative expire_at_ns is rejected before send');
unlike("$rejected->{error}", qr/ at .* line /, 'rejected errors omit Perl location noise');

my (@pipeline, @expected);
for my $index (0 .. 63) {
    my $value = "pipeline-$index";
    push @expected, $value;
    push @pipeline, { opcode => 'put', key => 'key', value => $value };
    push @pipeline, { opcode => 'get', key => 'key' };
}
my $responses = $client->execute_pipeline(\@pipeline);
is(scalar(@$responses), scalar(@pipeline), 'pipeline returns one positional response');
for my $index (0 .. $#expected) {
    is($responses->[$index * 2]->{outcome}, 'succeeded', "pipeline PUT $index succeeds");
    is($responses->[$index * 2 + 1]->{value}, $expected[$index], "pipeline GET $index ordered");
}
is($client->erase("binary\x00key")->{outcome}, 'committed', 'ERASE commits');
my $loaded = eval { $client->get("binary\x00key") };
ok(!$loaded && ref($@) eq 'GlyphaStore::Error' && $@->category eq 'not_found',
    'missing GET raises categorized error');
$client->close;
waitpid($pid, 0);

($port, $pid) = start_server(disconnect_on_put => 1);
$client = GlyphaStore::Client->connect(port => $port);
my $indeterminate = $client->put('key', 'value');
is($indeterminate->{outcome}, 'indeterminate', 'standalone PUT disconnect is indeterminate');
$client->close;
waitpid($pid, 0);

($port, $pid) = start_server(disconnect_on_put => 1);
$client = GlyphaStore::Client->connect(port => $port);
$responses = $client->execute_pipeline([
    { opcode => 'put', key => 'key', value => 'value' },
    { opcode => 'get', key => 'key' },
    { opcode => 'erase', key => 'key' },
]);
is($responses->[0]->{outcome}, 'indeterminate', 'sent PUT is indeterminate after disconnect');
is($responses->[1]->{outcome}, 'failed', 'unresolved GET is failed');
is($responses->[2]->{outcome}, 'indeterminate', 'sent ERASE is indeterminate');
$client->close;
waitpid($pid, 0);

($port, $pid) = start_server();
$client = GlyphaStore::Client->connect(
    port => $port,
    maximum_pipeline_requests => 1,
);
ok(!eval {
    $client->execute_pipeline([
        { opcode => 'get', key => 'key' },
        { opcode => 'get', key => 'key' },
    ]);
    1;
} && ref($@) eq 'GlyphaStore::Error' && $@->category eq 'invalid_argument',
    'pipeline request limit fails before transmission');
$client->close;
waitpid($pid, 0);

($port, $pid) = start_server(worker_count => 2);
$client = GlyphaStore::Client->connect(port => $port);
is($client->worker_count, 2, 'multi-worker bootstrap discovers both Workers');
my @keys = map { sprintf('mw-%02d', $_) } 0 .. 31;
my %owners = map { $_ => $client->worker_for($_) } @keys;
my %unique_owners = map { $_ => 1 } values %owners;
is_deeply([sort { $a <=> $b } keys %unique_owners], [0, 1], 'keys span both Workers');
for my $key (@keys) {
    my $value = scalar reverse $key;
    is($client->put($key, $value)->{outcome}, 'committed', "multi-worker PUT $key");
}
for my $key (@keys) {
    my $value = scalar reverse $key;
    is($client->get($key), $value, "multi-worker GET $key");
    is(worker_for($key, 2), $owners{$key}, "routing stays stable for $key");
}
$client->close;
waitpid($pid, 0);

($port, $pid) = start_server(worker_count => 2);
$client = GlyphaStore::Client->connect(port => $port);
{
    my @wave = ([], []);
    for my $key (@keys) {
        my $owner = $owners{$key};
        my $value = scalar reverse $key;
        push @{$wave[$owner]}, { opcode => 'put', key => $key, value => "$value-c" };
        push @{$wave[$owner]}, { opcode => 'get', key => $key };
    }
    my $all = $client->execute_worker_pipelines(\@wave);
    is(scalar(@$all), 2, 'concurrent worker pipelines return one slot per Worker');
    for my $worker (0, 1) {
        my $responses = $all->[$worker];
        is(scalar(@$responses), scalar(@{$wave[$worker]}), "worker $worker response count");
        for (my $index = 0; $index < @{$wave[$worker]}; $index += 2) {
            is($responses->[$index]->{outcome}, 'succeeded', "concurrent PUT worker $worker");
            is($responses->[$index + 1]->{value}, $wave[$worker][$index]{value},
                "concurrent GET worker $worker ordered");
        }
    }
}
$client->close;
waitpid($pid, 0);

done_testing;
