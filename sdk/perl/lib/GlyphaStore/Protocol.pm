package GlyphaStore::Protocol;

use v5.32;
use strict;
use warnings;
use Config;
use Exporter 'import';

our $VERSION = '0.1.0';

BEGIN {
    die "GlyphaStore::Protocol requires a Perl with 64-bit integers (pack 'Q<')\n"
        unless defined($Config{use64bitint}) && $Config{use64bitint} eq 'define'
            && ($Config{uvsize} // 0) >= 8;
}

our @EXPORT_OK = qw(
    PROTOCOL_VERSION REQUEST_HEADER_BYTES RESPONSE_HEADER_BYTES MAX_FRAME_BYTES NO_WORKER
    OP_INIT OP_PING OP_GET OP_PUT OP_ERASE OP_BIND_WORKER
    STATUS_OK STATUS_INVALID_REQUEST STATUS_UNSUPPORTED STATUS_INTERNAL_ERROR
    STATUS_NOT_FOUND STATUS_OVERLOADED STATUS_WRONG_OWNER STATUS_NOT_BOUND
    encode_request decode_request encode_response decode_response worker_for
);

use constant PROTOCOL_VERSION      => 2;
use constant REQUEST_HEADER_BYTES  => 40;
use constant RESPONSE_HEADER_BYTES => 40;
use constant MAX_FRAME_BYTES       => 2 * 1024 * 1024;
use constant NO_WORKER             => 0xFFFF_FFFF;
use constant U64_MAX               => '18446744073709551615';

use constant OP_INIT        => 1;
use constant OP_PING        => 2;
use constant OP_GET         => 3;
use constant OP_PUT         => 4;
use constant OP_ERASE       => 5;
use constant OP_BIND_WORKER => 6;

use constant STATUS_OK              => 0;
use constant STATUS_INVALID_REQUEST => 1;
use constant STATUS_UNSUPPORTED     => 2;
use constant STATUS_INTERNAL_ERROR  => 3;
use constant STATUS_NOT_FOUND       => 4;
use constant STATUS_OVERLOADED      => 5;
use constant STATUS_WRONG_OWNER     => 6;
use constant STATUS_NOT_BOUND       => 7;

# Two little-endian u32 limbs avoid NV conversion for values above 2^53.
my $REQUEST_FORMAT  = 'VvCC VV VV VV VV';
my $RESPONSE_FORMAT = 'Vvv VV VV VV VV';

sub _require_bytes {
    my ($value, $field) = @_;
    $value = '' unless defined $value;
    my $copy = $value;
    utf8::downgrade($copy, 1)
        or die "$field contains wide characters; encode to octets before use\n";
    return $copy;
}

sub _u64_digits {
    my ($value, $field) = @_;
    die "$field must be a non-negative integer\n"
        if !defined($value) || $value !~ /\A\d+\z/;
    die "$field is outside unsigned 64-bit range\n"
        if length($value) > 20 || (length($value) == 20 && $value gt U64_MAX);
    return $value;
}

sub _pack_u64 {
    my ($digits) = @_;
    my ($high, $low) = (0, 0);
    for my $ch (split //, $digits) {
        $low = $low * 10 + ord($ch) - 48;
        my $carry = int($low / 4_294_967_296);
        $low %= 4_294_967_296;
        $high = $high * 10 + $carry;
        $high %= 4_294_967_296;
    }
    return ($low, $high);
}

sub _u64_from_limbs {
    my ($low, $high) = @_;
    return unpack('Q<', pack('VV', $low, $high));
}

sub encode_request {
    my (%request) = @_;
    my $opcode = $request{opcode};
    die "unknown protocol-v2 opcode\n"
        if !defined($opcode) || $opcode < OP_INIT || $opcode > OP_BIND_WORKER;
    my $request_id = _u64_digits($request{request_id}, 'request_id');
    my $key = _require_bytes($request{key}, 'key');
    my $value = _require_bytes($request{value}, 'value');
    my $expire_at_ns = _u64_digits($request{expire_at_ns} // 0, 'expire_at_ns');
    my $target_worker = $request{target_worker} // NO_WORKER;
    die "target_worker is outside unsigned 32-bit range\n"
        if $target_worker < 0 || $target_worker > 0xFFFF_FFFF;

    if ($opcode == OP_INIT && (length($key) || length($value) || $expire_at_ns ne '0'
        || $target_worker != NO_WORKER)) {
        die "INIT request cannot carry key, value, expiry, or target_worker\n";
    }
    if ($opcode == OP_PING && (length($key) || $expire_at_ns ne '0' || $target_worker != NO_WORKER)) {
        die "PING request cannot carry key, expiry, or target_worker\n";
    }
    if ($opcode == OP_GET && (length($value) || $expire_at_ns ne '0' || $target_worker != NO_WORKER)) {
        die "GET request cannot carry value, expiry, or target_worker\n";
    }
    if ($opcode == OP_PUT && $target_worker != NO_WORKER) {
        die "PUT request cannot carry target_worker\n";
    }
    if ($opcode == OP_ERASE && (length($value) || $expire_at_ns ne '0' || $target_worker != NO_WORKER)) {
        die "ERASE request cannot carry value, expiry, or target_worker\n";
    }
    if ($opcode == OP_BIND_WORKER && (length($key) || length($value) || $expire_at_ns ne '0')) {
        die "BIND_WORKER request cannot carry key, value, or expiry\n";
    }
    if ($opcode == OP_BIND_WORKER && $target_worker == NO_WORKER) {
        die "BIND_WORKER request requires an explicit target_worker\n";
    }

    my $frame_size = REQUEST_HEADER_BYTES + length($key) + length($value);
    die "request exceeds the protocol frame limit\n" if $frame_size > MAX_FRAME_BYTES;
    my ($rid_lo, $rid_hi) = _pack_u64($request_id);
    my ($exp_lo, $exp_hi) = _pack_u64($expire_at_ns);
    return pack(
        $REQUEST_FORMAT,
        $frame_size, PROTOCOL_VERSION, $opcode, 0,
        $rid_lo, $rid_hi,
        length($key), length($value),
        $exp_lo, $exp_hi,
        $target_worker, 0,
    ) . $key . $value;
}

sub decode_request {
    my ($frame, $maximum) = @_;
    $maximum //= MAX_FRAME_BYTES;
    die "request is shorter than its header\n" if length($frame) < REQUEST_HEADER_BYTES;
    my ($frame_size, $version, $opcode, $flags,
        $rid_lo, $rid_hi, $key_size, $value_size,
        $exp_lo, $exp_hi, $target_worker, $reserved) = unpack($REQUEST_FORMAT, $frame);
    die "request frame extent is invalid\n"
        if $frame_size != length($frame) || $frame_size > $maximum;
    die "request protocol version is unsupported\n" if $version != PROTOCOL_VERSION;
    die "request canonical fields are invalid\n" if $flags != 0 || $reserved != 0;
    die "unknown protocol-v2 opcode\n" if $opcode < OP_INIT || $opcode > OP_BIND_WORKER;
    die "request payload extent is invalid\n"
        if REQUEST_HEADER_BYTES + $key_size + $value_size != $frame_size;
    return {
        opcode         => $opcode,
        request_id     => _u64_from_limbs($rid_lo, $rid_hi),
        expire_at_ns   => _u64_from_limbs($exp_lo, $exp_hi),
        target_worker  => $target_worker,
        key             => substr($frame, REQUEST_HEADER_BYTES, $key_size),
        value           => substr($frame, REQUEST_HEADER_BYTES + $key_size, $value_size),
    };
}

sub encode_response {
    my (%response) = @_;
    my $status = $response{status};
    die "unknown protocol-v2 status\n"
        if !defined($status) || $status < STATUS_OK || $status > STATUS_NOT_BOUND;
    my $request_id = _u64_digits($response{request_id}, 'request_id');
    my $value = _require_bytes($response{value}, 'value');
    my $owner_worker = $response{owner_worker} // NO_WORKER;
    my $worker_count = $response{worker_count} // 0;
    my $routing_epoch = _u64_digits($response{routing_epoch} // 0, 'routing_epoch');
    die "owner_worker is outside unsigned 32-bit range\n"
        if $owner_worker < 0 || $owner_worker > 0xFFFF_FFFF;
    die "worker_count is outside unsigned 32-bit range\n"
        if $worker_count < 0 || $worker_count > 0xFFFF_FFFF;
    my $frame_size = RESPONSE_HEADER_BYTES + length($value);
    die "response exceeds the protocol frame limit\n" if $frame_size > MAX_FRAME_BYTES;
    my ($rid_lo, $rid_hi) = _pack_u64($request_id);
    my ($epoch_lo, $epoch_hi) = _pack_u64($routing_epoch);
    return pack(
        $RESPONSE_FORMAT,
        $frame_size, PROTOCOL_VERSION, $status,
        $rid_lo, $rid_hi,
        length($value),
        $owner_worker, $worker_count, 0,
        $epoch_lo, $epoch_hi,
    ) . $value;
}

sub decode_response {
    my ($frame, $maximum) = @_;
    $maximum //= MAX_FRAME_BYTES;
    die "response is shorter than its header\n" if length($frame) < RESPONSE_HEADER_BYTES;
    my ($frame_size, $version, $status,
        $rid_lo, $rid_hi, $value_size, $owner_worker,
        $worker_count, $reserved, $epoch_lo, $epoch_hi) = unpack($RESPONSE_FORMAT, $frame);
    die "response frame extent is invalid\n"
        if $frame_size != length($frame) || $frame_size > $maximum;
    die "response protocol version is unsupported\n" if $version != PROTOCOL_VERSION;
    die "response reserved field is noncanonical\n" if $reserved != 0;
    die "unknown protocol-v2 status\n" if $status < STATUS_OK || $status > STATUS_NOT_BOUND;
    die "response value extent is invalid\n"
        if RESPONSE_HEADER_BYTES + $value_size != $frame_size;
    return {
        status        => $status,
        request_id    => _u64_from_limbs($rid_lo, $rid_hi),
        owner_worker  => $owner_worker,
        worker_count  => $worker_count,
        routing_epoch => _u64_from_limbs($epoch_lo, $epoch_hi),
        value          => substr($frame, RESPONSE_HEADER_BYTES, $value_size),
    };
}

sub worker_for {
    my ($key, $worker_count) = @_;
    die "worker_count must be positive\n" if !$worker_count || $worker_count < 0;
    $key = _require_bytes($key, 'key');
    my $high = 0xCBF29CE4;
    my $low  = 0x84222325;
    for my $byte (unpack('C*', $key)) {
        $low ^= $byte;
        my $product = $low * 0x1B3;
        my $carry = int($product / 4_294_967_296);
        my $next_low = $product % 4_294_967_296;
        my $next_high = ($high * 0x1B3 + $low * 0x100 + $carry) % 4_294_967_296;
        ($high, $low) = ($next_high, $next_low);
    }
    return (($high % $worker_count) * (4_294_967_296 % $worker_count) +
        ($low % $worker_count)) % $worker_count;
}

1;

__END__

=encoding utf8

=head1 NAME

GlyphaStore::Protocol - wire-protocol v2 codec and Worker routing

=head1 VERSION

Version 0.1.0

=head1 SYNOPSIS

    use GlyphaStore::Protocol qw(
        OP_PUT encode_request decode_response worker_for
    );

    my $frame = encode_request(
        opcode => OP_PUT,
        request_id => 1,
        key => "k",
        value => "v",
    );

=head1 DESCRIPTION

Canonical little-endian framing for GlyphaStore wire protocol v2. Keys and
values must be B<octet strings> (UTF8 flag off). Wide characters are rejected.

Requires a Perl built with 64-bit integers.

=head1 AUTHOR

Giacomo Picchiarelli

=head1 LICENSE

BSD 3-Clause. See the distribution F<LICENSE>.

=cut
