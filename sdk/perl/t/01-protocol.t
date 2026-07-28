use v5.32;
use strict;
use warnings;
use FindBin;
use Test::More;

use lib "$FindBin::Bin/../lib";
use GlyphaStore::Protocol qw(
    OP_INIT OP_PING OP_GET OP_PUT OP_ERASE OP_BIND_WORKER OP_HEALTH OP_READY OP_STATS
    encode_request decode_request encode_response decode_response worker_for
);

sub fixture {
    my ($name) = @_;
    my $path = "$FindBin::Bin/fixtures/$name";
    open(my $handle, '<', $path) or die "cannot read $path: $!";
    local $/;
    my $text = <$handle>;
    close($handle);
    return pack('C*', map { hex($_) } split(/\s+/, $text));
}

sub frames {
    my ($corpus) = @_;
    my @frames;
    my $offset = 0;
    while ($offset < length($corpus)) {
        my $size = unpack('V', substr($corpus, $offset, 4));
        push @frames, substr($corpus, $offset, $size);
        $offset += $size;
    }
    return @frames;
}

my @expected_requests = frames(fixture('wire_requests_v2.hex'));
my @encoded_requests = (
    encode_request(opcode => OP_INIT, request_id => 1),
    encode_request(opcode => OP_PING, request_id => 2, value => "\x00ping\xff"),
    encode_request(opcode => OP_GET, request_id => 3, key => "get\x00key"),
    encode_request(opcode => OP_PUT, request_id => 4, key => "put\x00key",
        value => "\x10\x20\xff", expire_at_ns => 123_456_789),
    encode_request(opcode => OP_ERASE, request_id => 5, key => 'erase-key'),
    encode_request(opcode => OP_BIND_WORKER, request_id => 6, target_worker => 2),
    encode_request(opcode => OP_HEALTH, request_id => 7),
    encode_request(opcode => OP_READY, request_id => 8),
    encode_request(opcode => OP_STATS, request_id => 9),
);
is_deeply(\@encoded_requests, \@expected_requests, 'request encoder matches canonical corpus');

my @decoded_requests = map { decode_request($_) } @expected_requests;
is_deeply([map { $_->{opcode} } @decoded_requests], [1 .. 9], 'request decoder covers every opcode');
is($decoded_requests[1]->{value}, "\x00ping\xff", 'PING payload is binary exact');
is($decoded_requests[3]->{expire_at_ns}, 123_456_789, 'PUT expiry is preserved');
my @reencoded_requests = map {
    encode_request(
        opcode => $_->{opcode},
        request_id => $_->{request_id},
        key => $_->{key},
        value => $_->{value},
        expire_at_ns => $_->{expire_at_ns},
        target_worker => $_->{target_worker},
    )
} @decoded_requests;
is_deeply(\@reencoded_requests, \@expected_requests, 'request codec round-trips fixtures');

my @expected_responses = frames(fixture('wire_responses_v2.hex'));
my @responses = map { decode_response($_) } @expected_responses;
is_deeply([map { $_->{status} } @responses], [0 .. 8], 'response decoder covers every status');
is($responses[0]->{value}, 'GlyphaStore/2', 'identity value is binary exact');
is($responses[6]->{owner_worker}, 2, 'wrong-owner fixture preserves owner');
my @reencoded_responses = map {
    encode_response(
        status => $_->{status},
        request_id => $_->{request_id},
        value => $_->{value},
        owner_worker => $_->{owner_worker},
        worker_count => $_->{worker_count},
        routing_epoch => $_->{routing_epoch},
    )
} @responses;
is_deeply(\@reencoded_responses, \@expected_responses, 'response codec round-trips fixtures');

is(worker_for('', 4), 1, 'empty-key FNV routing matches canonical implementation');
is(worker_for("key\x00\xff", 17), worker_for("key\x00\xff", 17), 'binary routing is stable');

my $noncanonical = $expected_requests[0];
substr($noncanonical, 36, 1, "\x01");
ok(!eval { decode_request($noncanonical); 1 }, 'decoder rejects noncanonical reserved fields');

ok(!eval { encode_request(opcode => OP_PUT, request_id => -1, key => 'k', value => 'v'); 1 },
    'request_id rejects negatives');
ok(!eval { encode_request(opcode => OP_PUT, request_id => 1, key => 'k', value => 'v',
    expire_at_ns => '18446744073709551616'); 1 },
    'expire_at_ns rejects values above u64 max');

is($GlyphaStore::Protocol::VERSION, '0.1.0', 'module VERSION is not shadowed by wire constant');
is(GlyphaStore::Protocol::PROTOCOL_VERSION(), 2, 'wire protocol version constant');

ok(!eval {
    encode_request(opcode => OP_PUT, request_id => 1, key => "\x{100}", value => 'v'); 1
}, 'wide-character keys are rejected');

my $big = encode_request(
    opcode => OP_PUT,
    request_id => '9007199254740993',
    key => 'k',
    value => 'v',
    expire_at_ns => '18446744073709551615',
);
my $decoded_big = decode_request($big);
is($decoded_big->{request_id}, 9_007_199_254_740_993, 'u64 request_id above 2^53 round-trips');
is($decoded_big->{expire_at_ns}, '18446744073709551615' + 0, 'u64 max expire_at_ns round-trips');

done_testing;
