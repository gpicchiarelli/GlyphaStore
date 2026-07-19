package GlyphaStore;

use v5.32;
use strict;
use warnings;

our $VERSION = '0.1.0';

1;

__END__

=encoding utf8

=head1 NAME

GlyphaStore - native Perl client for GlyphaStore wire protocol v2

=head1 VERSION

Version 0.1.0

=head1 SYNOPSIS

    use GlyphaStore::Client;

    my $cache = GlyphaStore::Client->connect(
        host => '127.0.0.1',
        port => 7379,
    );
    my $stored = $cache->put("session\x00key", "payload");
    my $value = $cache->get("session\x00key")
        if $stored->{outcome} eq 'committed';
    $cache->close;

=head1 DESCRIPTION

GlyphaStore is a native Perl distribution that speaks GlyphaStore wire protocol
v2 over TCP. It opens and binds one connection per Worker, routes complete
binary keys with canonical FNV-1a 64-bit, retries safe reads after a transient
disconnect, and never reports an uncertain mutation as C<rejected>.

The public runtime modules are:

=over 4

=item L<GlyphaStore::Client>

Synchronous TCP client API (C<get>, C<put>, C<erase>, C<ping>,
C<execute_pipeline>).

=item L<GlyphaStore::Protocol>

Bidirectional request/response codec and Worker routing helpers.

=back

Runtime dependencies are core modules only.

=head1 SUBROUTINES/METHODS

This distribution root module exports no runtime API. Use
L<GlyphaStore::Client> or L<GlyphaStore::Protocol>.

=head1 DIAGNOSTICS

None at this level.

=head1 CONFIGURATION AND ENVIRONMENT

None.

=head1 DEPENDENCIES

Core modules only.

=head1 INCOMPATIBILITIES

Requires Perl 5.32 or newer and a 64-bit integer build for protocol codecs.

=head1 BUGS AND LIMITATIONS

None known.

=head1 SEE ALSO

L<GlyphaStore::Client>, L<GlyphaStore::Protocol>,
L<https://github.com/gpicchiarelli/GlyphaStore>

=head1 AUTHOR

Giacomo Picchiarelli

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2026, Giacomo Picchiarelli.

This software is licensed under the BSD 3-Clause License. See the F<LICENSE>
file included with this distribution.

=cut
