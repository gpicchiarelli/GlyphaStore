package GlyphaStore::Client;

use v5.32;
use strict;
use warnings;
use Errno qw(EAGAIN EWOULDBLOCK EINTR);
use IO::Select;
use IO::Socket::INET;
use Socket qw(IPPROTO_TCP TCP_NODELAY);
use Time::HiRes qw(time);

use GlyphaStore::Protocol qw(
    MAX_FRAME_BYTES NO_WORKER RESPONSE_HEADER_BYTES
    OP_INIT OP_PING OP_GET OP_PUT OP_ERASE OP_BIND_WORKER
    STATUS_OK STATUS_INVALID_REQUEST STATUS_UNSUPPORTED STATUS_INTERNAL_ERROR
    STATUS_NOT_FOUND STATUS_OVERLOADED STATUS_WRONG_OWNER STATUS_NOT_BOUND
    encode_request decode_response
);

our $VERSION = '0.1.0';

{
    package GlyphaStore::Error;
    use overload '""' => sub { $_[0]->{message} }, fallback => 1;
    sub new { bless { category => $_[1], message => $_[2] }, $_[0] }
    sub category { $_[0]->{category} }
    sub message { $_[0]->{message} }
}

{
    package GlyphaStore::SendFailure;
    use overload '""' => sub { "$_[0]->{error}" }, fallback => 1;
    sub new { bless { error => $_[1], bytes_sent => $_[2] }, $_[0] }
}

sub _error { GlyphaStore::Error->new($_[0], $_[1]) }
sub _throw { die _error($_[0], $_[1]) }

sub _plain_message {
    my ($error) = @_;
    return $error->message if ref($error) && eval { $error->can('message') };
    return "$error" if ref($error);
    $error =~ s/\s+at \S+ line \d+\.?\n?\z//;
    chomp $error;
    return $error;
}

sub connect {
    my ($class, %config) = @_;
    my $self = bless {
        host                      => $config{host} // '127.0.0.1',
        port                      => $config{port} // 7379,
        connect_timeout           => $config{connect_timeout} // 3.0,
        request_timeout           => $config{request_timeout} // 5.0,
        maximum_frame_bytes       => $config{maximum_frame_bytes} // MAX_FRAME_BYTES,
        maximum_pipeline_requests => $config{maximum_pipeline_requests} // 256,
        maximum_pipeline_bytes    => $config{maximum_pipeline_bytes} // 1024 * 1024,
        worker_count              => 0,
        routing_epoch             => 0,
        next_request_id           => 1,
        healthy                   => 1,
        connections               => [],
    }, $class;
    $self->_validate_config;
    my $ok = eval {
        my $first = $self->_new_connection(0);
        my ($worker_count, $routing_epoch) = $self->_bootstrap($first, undef);
        $self->{worker_count} = $worker_count;
        $self->{routing_epoch} = $routing_epoch;
        push @{$self->{connections}}, $first;
        for my $worker (1 .. $worker_count - 1) {
            my $connection = $self->_new_connection($worker);
            $self->_bootstrap($connection, [$worker_count, $routing_epoch]);
            push @{$self->{connections}}, $connection;
        }
        1;
    };
    if (!$ok) {
        my $error = $@;
        $self->close;
        die $error;
    }
    return $self;
}

sub _validate_config {
    my ($self) = @_;
    _throw('invalid_argument', 'client configuration is outside protocol limits')
        if !length($self->{host}) || $self->{port} < 1 || $self->{port} > 65_535
        || $self->{connect_timeout} <= 0 || $self->{request_timeout} <= 0
        || $self->{maximum_frame_bytes} < RESPONSE_HEADER_BYTES
        || $self->{maximum_frame_bytes} > MAX_FRAME_BYTES
        || $self->{maximum_pipeline_requests} < 1
        || $self->{maximum_pipeline_bytes} < 40;
}

sub _new_connection {
    my ($self, $worker) = @_;
    return { worker => $worker, socket => undef, input => '', input_offset => 0 };
}

sub worker_count  { $_[0]->{worker_count} }
sub routing_epoch { $_[0]->{routing_epoch} }
sub healthy       { $_[0]->{healthy} ? 1 : 0 }

sub worker_for {
    my ($self, $key) = @_;
    return GlyphaStore::Protocol::worker_for($key, $self->{worker_count});
}

sub _next_request_id {
    my ($self) = @_;
    my $current = $self->{next_request_id};
    # Match Python/C++: wrap after the maximum unsigned 64-bit value, skipping 0.
    $self->{next_request_id} = ($current == ~0) ? 1 : $current + 1;
    return $current;
}

sub _encode_request {
    my ($self, $opcode, $request_id, $key, $value, $expire_at_ns, $target_worker) = @_;
    return encode_request(
        opcode        => $opcode,
        request_id    => $request_id,
        key           => $key // '',
        value         => $value // '',
        expire_at_ns  => $expire_at_ns // 0,
        target_worker => $target_worker // NO_WORKER,
    );
}

sub _reset_connection {
    my ($self, $connection) = @_;
    if (my $socket = delete $connection->{socket}) {
        close($socket);
    }
    $connection->{input} = '';
    $connection->{input_offset} = 0;
}

sub close {
    my ($self) = @_;
    $self->{healthy} = 0;
    $self->_reset_connection($_) for @{$self->{connections}};
    return;
}

sub DESTROY {
    my ($self) = @_;
    $self->close if $self->{connections};
}

sub _open_socket {
    my ($self) = @_;
    my $socket = IO::Socket::INET->new(
        PeerAddr => $self->{host},
        PeerPort => $self->{port},
        Proto    => 'tcp',
        Timeout  => $self->{connect_timeout},
    );
    _throw('unavailable', "could not connect to GlyphaStore: $!") if !$socket;
    setsockopt($socket, IPPROTO_TCP, TCP_NODELAY, pack('i', 1));
    $socket->blocking(0);
    return $socket;
}

sub _remaining {
    my ($deadline) = @_;
    my $remaining = $deadline - time;
    _throw('transport', 'request deadline expired') if $remaining <= 0;
    return $remaining;
}

sub _wait_io {
    my ($selector, $mode, $deadline) = @_;
    while (1) {
        my $remaining = _remaining($deadline);
        my @ready = $mode eq 'write'
            ? $selector->can_write($remaining)
            : $selector->can_read($remaining);
        return 1 if @ready;
        next if $! == EINTR;
        return 0;
    }
}

sub _send {
    my ($self, $connection, $frame, $deadline) = @_;
    local $SIG{PIPE} = 'IGNORE';
    my $socket = $connection->{socket};
    my $selector = IO::Select->new($socket);
    $deadline //= time + $self->{request_timeout};
    my $sent = 0;
    while ($sent < length($frame)) {
        if (!_wait_io($selector, 'write', $deadline)) {
            die GlyphaStore::SendFailure->new(
                _error('transport', 'request send deadline expired'), $sent);
        }
        my $written = syswrite($socket, $frame, length($frame) - $sent, $sent);
        if (defined($written) && $written > 0) {
            $sent += $written;
            next;
        }
        next if !defined($written) && ($! == EINTR || $! == EAGAIN || $! == EWOULDBLOCK);
        my $message = defined($written) ? 'socket closed during send' : "request send failed: $!";
        die GlyphaStore::SendFailure->new(_error('transport', $message), $sent);
    }
    return $sent;
}

sub _receive_response {
    my ($self, $connection, $deadline, $selector) = @_;
    my $socket = $connection->{socket};
    $selector //= IO::Select->new($socket);
    $deadline //= time + $self->{request_timeout};
    while (1) {
        my $available = length($connection->{input}) - $connection->{input_offset};
        if ($available >= 4) {
            my $frame_size = unpack('V', substr($connection->{input}, $connection->{input_offset}, 4));
            _throw('protocol', 'server response size is outside client limits')
                if $frame_size < RESPONSE_HEADER_BYTES
                || $frame_size > $self->{maximum_frame_bytes};
            if ($available >= $frame_size) {
                my $frame = substr($connection->{input}, $connection->{input_offset}, $frame_size);
                $connection->{input_offset} += $frame_size;
                if ($connection->{input_offset} == length($connection->{input})) {
                    $connection->{input} = '';
                    $connection->{input_offset} = 0;
                }
                my $response = eval { decode_response($frame, $self->{maximum_frame_bytes}) };
                _throw('protocol', 'invalid server response: ' . _plain_message($@)) if !$response;
                return $response;
            }
        }
        if ($connection->{input_offset}) {
            substr($connection->{input}, 0, $connection->{input_offset}, '');
            $connection->{input_offset} = 0;
        }
        _throw('transport', 'response receive deadline expired')
            if !_wait_io($selector, 'read', $deadline);
        my $chunk = '';
        my $received = sysread($socket, $chunk, 64 * 1024);
        if (defined($received) && $received > 0) {
            $connection->{input} .= $chunk;
            next;
        }
        next if !defined($received) && ($! == EINTR || $! == EAGAIN || $! == EWOULDBLOCK);
        _throw('transport', defined($received)
            ? 'server closed the connection' : "response receive failed: $!");
    }
}

sub _exchange {
    my ($self, $connection, $frame, $deadline) = @_;
    $deadline //= time + $self->{request_timeout};
    $self->_send($connection, $frame, $deadline);
    return $self->_receive_response($connection, $deadline);
}

sub _bootstrap {
    my ($self, $connection, $expected) = @_;
    $self->_reset_connection($connection);
    $connection->{socket} = $self->_open_socket;
    my $result = eval {
        my $init_id = $self->_next_request_id;
        my $response = $self->_exchange(
            $connection, $self->_encode_request(OP_INIT, $init_id));
        _throw('protocol', 'server INIT response is inconsistent')
            if $response->{status} != STATUS_OK || $response->{request_id} != $init_id
            || $response->{value} ne 'GlyphaStore/2'
            || $response->{worker_count} < 1 || $response->{worker_count} > 256
            || !$response->{routing_epoch};
        my @metadata = ($response->{worker_count}, $response->{routing_epoch});
        _throw('unavailable', 'server routing metadata changed during bootstrap')
            if $expected && ($metadata[0] != $expected->[0] || $metadata[1] != $expected->[1]);
        my $bind_id = $self->_next_request_id;
        my $bound = $self->_exchange($connection,
            $self->_encode_request(OP_BIND_WORKER, $bind_id, '', '', 0, $connection->{worker}));
        _throw('protocol', 'server BIND_WORKER response is inconsistent')
            if $bound->{status} != STATUS_OK || $bound->{request_id} != $bind_id
            || $bound->{owner_worker} != $connection->{worker}
            || $bound->{worker_count} != $metadata[0]
            || $bound->{routing_epoch} != $metadata[1];
        \@metadata;
    };
    if (!$result) {
        my $error = $@;
        $self->_reset_connection($connection);
        if (ref($error) eq 'GlyphaStore::SendFailure') {
            _throw('unavailable', $error->{error}->{message});
        }
        if (ref($error) eq 'GlyphaStore::Error' && $error->{category} eq 'transport') {
            _throw('unavailable', $error->{message});
        }
        die $error;
    }
    return @$result;
}

sub _ensure_connected {
    my ($self, $connection) = @_;
    return if $connection->{socket};
    $self->_bootstrap($connection, [$self->{worker_count}, $self->{routing_epoch}]);
}

sub _validate_response {
    my ($self, $response, $request_id, $worker) = @_;
    _throw('protocol', 'server response request ID does not match')
        if $response->{request_id} != $request_id;
    if ($response->{worker_count} != $self->{worker_count}
        || $response->{routing_epoch} != $self->{routing_epoch}) {
        $self->{healthy} = 0;
        _throw('unavailable', 'server routing metadata changed');
    }
    if ($response->{owner_worker} != $worker && $response->{status} != STATUS_WRONG_OWNER) {
        $self->{healthy} = 0;
        _throw('protocol', 'server response came from the wrong Worker');
    }
}

sub _status_error {
    my ($status) = @_;
    return _error('not_found', 'key was not found') if $status == STATUS_NOT_FOUND;
    return _error('overloaded', 'server is overloaded') if $status == STATUS_OVERLOADED;
    return _error('unavailable', 'server connection is not bound') if $status == STATUS_NOT_BOUND;
    return _error('protocol', 'server rejected Worker routing') if $status == STATUS_WRONG_OWNER;
    return _error('invalid_argument', 'server rejected the request')
        if $status == STATUS_INVALID_REQUEST || $status == STATUS_UNSUPPORTED;
    return _error('internal', 'server reported an internal error');
}

sub get   { $_[0]->_read(OP_GET, $_[1], '') }
sub ping  { $_[0]->_read(OP_PING, '', $_[1] // '') }

sub put {
    my ($self, $key, $value, %options) = @_;
    return $self->_mutate(OP_PUT, $key, $value, $options{expire_at_ns} // 0);
}

sub erase { $_[0]->_mutate(OP_ERASE, $_[1], '', 0) }

sub _read {
    my ($self, $opcode, $key, $value) = @_;
    _throw('unavailable', 'client is closed or routing metadata changed') if !$self->{healthy};
    my $worker = $opcode == OP_PING ? 0 : $self->worker_for($key);
    my $connection = $self->{connections}->[$worker];
    my $last_error = _error('unavailable', 'request was not attempted');
    for (1 .. 2) {
        my $response = eval {
            $self->_ensure_connected($connection);
            my $request_id = $self->_next_request_id;
            my $frame = eval {
                $self->_encode_request($opcode, $request_id, $key, $value)
            };
            _throw('invalid_argument', _plain_message($@)) if !$frame;
            _throw('invalid_argument', 'request exceeds the configured frame limit')
                if length($frame) > $self->{maximum_frame_bytes};
            my $deadline = time + $self->{request_timeout};
            my $received = $self->_exchange($connection, $frame, $deadline);
            $self->_validate_response($received, $request_id, $worker);
            [$received, $request_id];
        };
        if (!$response) {
            my $error = $@;
            $self->_reset_connection($connection);
            if (ref($error) eq 'GlyphaStore::SendFailure') {
                $last_error = $error->{error};
                next;
            }
            if (ref($error) eq 'GlyphaStore::Error') {
                if ($error->{category} eq 'transport') {
                    $last_error = $error;
                    next;
                }
                if ($error->{category} eq 'unavailable') {
                    die $error if !$self->{healthy};
                    $last_error = $error;
                    next;
                }
            }
            die $error;
        }
        my $received = $response->[0];
        if ($received->{status} != STATUS_OK) {
            $self->{healthy} = 0
                if $received->{status} == STATUS_WRONG_OWNER || $received->{status} == STATUS_NOT_BOUND;
            die _status_error($received->{status});
        }
        return $received->{value};
    }
    die $last_error;
}

sub _mutate {
    my ($self, $opcode, $key, $value, $expire_at_ns) = @_;
    return {
        outcome => 'rejected',
        error   => _error('unavailable', 'client is closed or routing metadata changed'),
    } if !$self->{healthy};
    my $worker = $self->worker_for($key);
    my $connection = $self->{connections}->[$worker];
    for my $attempt (0 .. 1) {
        my $connected = eval { $self->_ensure_connected($connection); 1 };
        if (!$connected) {
            my $error = $@;
            return {
                outcome => 'rejected',
                error   => ref($error) eq 'GlyphaStore::Error'
                    ? $error
                    : _error('unavailable', _plain_message($error)),
            };
        }
        my $request_id = $self->_next_request_id;
        my $frame = eval {
            $self->_encode_request($opcode, $request_id, $key, $value, $expire_at_ns)
        };
        return {
            outcome => 'rejected',
            error   => _error('invalid_argument', _plain_message($@)),
        } if !$frame;
        return {
            outcome => 'rejected',
            error   => _error('invalid_argument', 'request exceeds the configured frame limit'),
        } if length($frame) > $self->{maximum_frame_bytes};
        my $deadline = time + $self->{request_timeout};
        my $response = eval {
            my $received = $self->_exchange($connection, $frame, $deadline);
            $self->_validate_response($received, $request_id, $worker);
            $received;
        };
        if (!$response) {
            my $error = $@;
            $self->_reset_connection($connection);
            if (ref($error) eq 'GlyphaStore::SendFailure') {
                if (!$error->{bytes_sent}) {
                    next if !$attempt;
                    return { outcome => 'rejected', error => $error->{error} };
                }
                return { outcome => 'indeterminate', error => $error->{error} };
            }
            return {
                outcome => 'indeterminate',
                error   => ref($error) eq 'GlyphaStore::Error'
                    ? $error
                    : _error('transport', _plain_message($error)),
            };
        }
        if ($response->{status} == STATUS_OK) {
            if (length($response->{value})) {
                $self->_reset_connection($connection);
                return {
                    outcome => 'indeterminate',
                    error   => _error('protocol', 'mutation response value must be empty'),
                };
            }
            return { outcome => 'committed', error => undef };
        }
        my $error = _status_error($response->{status});
        return { outcome => 'indeterminate', error => $error }
            if $response->{status} == STATUS_INTERNAL_ERROR;
        $self->{healthy} = 0
            if $response->{status} == STATUS_WRONG_OWNER || $response->{status} == STATUS_NOT_BOUND;
        return { outcome => 'rejected', error => $error };
    }
    return { outcome => 'rejected', error => _error('unavailable', 'could not send mutation') };
}

sub execute_pipeline {
    my ($self, $requests) = @_;
    _throw('invalid_argument', 'pipeline requests must be an array reference')
        if ref($requests) ne 'ARRAY';
    return [] if !@$requests;
    _throw('unavailable', 'client is closed or routing metadata changed') if !$self->{healthy};
    _throw('invalid_argument', 'pipeline exceeds the configured request limit')
        if @$requests > $self->{maximum_pipeline_requests};
    my (%wire_opcode) = (get => OP_GET, put => OP_PUT, erase => OP_ERASE);
    my (@normalized, @metadata, @frames);
    my ($worker, $output_size) = (undef, 0);
    for my $request (@$requests) {
        _throw('invalid_argument', 'pipeline request must be a hash reference')
            if ref($request) ne 'HASH';
        my $name = $request->{opcode} // '';
        _throw('invalid_argument', 'pipeline request contains an invalid opcode')
            if !exists($wire_opcode{$name});
        my $key = $request->{key} // '';
        my $value = $request->{value} // '';
        my $expire_at_ns = $request->{expire_at_ns} // 0;
        my $owner = $self->worker_for($key);
        $worker //= $owner;
        _throw('invalid_argument', 'every pipeline key must route to the same Worker')
            if $owner != $worker;
        _throw('invalid_argument', 'GET and ERASE pipeline requests cannot carry PUT fields')
            if ($name eq 'get' || $name eq 'erase') && (length($value) || $expire_at_ns);
        my $request_id = $self->_next_request_id;
        my $frame = eval {
            $self->_encode_request(
                $wire_opcode{$name}, $request_id, $key, $value, $expire_at_ns)
        };
        _throw('invalid_argument', _plain_message($@)) if !$frame;
        _throw('invalid_argument', 'pipeline request exceeds the configured frame limit')
            if length($frame) > $self->{maximum_frame_bytes};
        _throw('invalid_argument', 'pipeline exceeds the configured aggregate byte limit')
            if length($frame) > $self->{maximum_pipeline_bytes} - $output_size;
        push @normalized, $name;
        push @metadata, [$request_id, $output_size];
        push @frames, $frame;
        $output_size += length($frame);
    }
    my $output = join('', @frames);
    my @responses = map { { outcome => 'failed', value => '', error => undef } } @$requests;
    my $connection = $self->{connections}->[$worker];
    _throw('unavailable', 'client closed before pipeline admission') if !$self->{healthy};
    $self->_ensure_connected($connection);
    my $mark_unresolved = sub {
        my ($first, $error, $bytes_sent) = @_;
        if (ref($error) ne 'GlyphaStore::Error') {
            $error = _error('transport', _plain_message($error));
        }
        for my $index ($first .. $#normalized) {
            my $mutation_may_have_arrived = ($normalized[$index] eq 'put'
                || $normalized[$index] eq 'erase') && $bytes_sent > $metadata[$index]->[1];
            $responses[$index] = {
                outcome => $mutation_may_have_arrived ? 'indeterminate' : 'failed',
                value => '', error => $error,
            };
        }
    };
    my $deadline = time + $self->{request_timeout};
    my $sent = eval { $self->_send($connection, $output, $deadline) };
    if (!defined($sent)) {
        my $failure = $@;
        $self->_reset_connection($connection);
        my $error = ref($failure) eq 'GlyphaStore::SendFailure'
            ? $failure->{error} : $failure;
        my $bytes_sent = ref($failure) eq 'GlyphaStore::SendFailure'
            ? $failure->{bytes_sent} : 0;
        $mark_unresolved->(0, $error, $bytes_sent);
        return \@responses;
    }
    my $receive_selector = IO::Select->new($connection->{socket});
    for my $index (0 .. $#normalized) {
        my $response = eval {
            my $received = $self->_receive_response(
                $connection, $deadline, $receive_selector);
            $self->_validate_response($received, $metadata[$index]->[0], $worker);
            $received;
        };
        if (!$response) {
            my $error = $@;
            $self->_reset_connection($connection);
            $mark_unresolved->($index, $error, length($output));
            return \@responses;
        }
        if ($response->{status} == STATUS_OK) {
            $responses[$index] = { outcome => 'succeeded', value => $response->{value}, error => undef };
            next;
        }
        my $error = _status_error($response->{status});
        $responses[$index] = {
            outcome => (($normalized[$index] eq 'put' || $normalized[$index] eq 'erase')
                && $response->{status} == STATUS_INTERNAL_ERROR) ? 'indeterminate' : 'failed',
            value => '', error => $error,
        };
        $self->{healthy} = 0
            if $response->{status} == STATUS_WRONG_OWNER || $response->{status} == STATUS_NOT_BOUND;
    }
    return \@responses;
}

1;

__END__

=encoding utf8

=head1 NAME

GlyphaStore::Client - synchronous TCP client for GlyphaStore

=head1 VERSION

Version 0.1.0

=head1 SYNOPSIS

    use GlyphaStore::Client;

    my $cache = GlyphaStore::Client->connect(port => 7379);
    my $put = $cache->put("key", "value");
    my $value = $cache->get("key") if $put->{outcome} eq 'committed';
    my $responses = $cache->execute_pipeline([
        { opcode => 'put', key => 'key', value => 'next' },
        { opcode => 'get', key => 'key' },
    ]);
    $cache->close;

=head1 DESCRIPTION

C<GlyphaStore::Client> opens one bound TCP connection per Worker advertised by
the server C<INIT> response. Keys and values are opaque byte strings. Mutations
return a hash reference with C<outcome> set to C<committed>, C<rejected>, or
C<indeterminate>. Reads throw L<GlyphaStore::Error> objects with a C<category>
and C<message>.

Perl ithreads are not a shared-client concurrency model; use one client per
process or thread.

=head1 METHODS

=over 4

=item connect(%config)

=item get($key)

=item put($key, $value, %options)

=item erase($key)

=item ping($payload?)

=item execute_pipeline(\@requests)

=item worker_for($key)

=item worker_count

=item routing_epoch

=item healthy

=item close

=back

=head1 AUTHOR

Giacomo Picchiarelli

=head1 LICENSE

BSD 3-Clause. See the distribution F<LICENSE>.

=cut
