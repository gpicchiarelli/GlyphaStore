package GlyphaStore::Error;

use v5.32;
use strict;
use warnings;

our $VERSION = '0.1.0';

use overload '""' => sub { $_[0]->{message} }, fallback => 1;

sub new {
    my ($class, $category, $message) = @_;
    return bless { category => $category, message => $message }, $class;
}

sub category {
    my ($self) = @_;
    return $self->{category};
}

sub message {
    my ($self) = @_;
    return $self->{message};
}

1;

__END__

=encoding utf8

=head1 NAME

GlyphaStore::Error - structured client exception

=head1 VERSION

Version 0.1.0

=head1 SYNOPSIS

    eval { GlyphaStore::Client->connect(host => '127.0.0.1', port => 7379) };
    if (my $error = $@) {
        warn $error->category, ': ', $error->message if ref($error);
    }

=head1 DESCRIPTION

Exception object thrown by L<GlyphaStore::Client> for transport, protocol, and
routing failures. Stringifies to the human-readable C<message>.

=head1 SUBROUTINES/METHODS

=over 4

=item new($category, $message)

Construct an error with a stable C<category> string and C<message>.

=item category()

Return the category string (for example C<transport> or C<not_found>).

=item message()

Return the message string.

=back

=head1 DIAGNOSTICS

Objects of this class are thrown via C<die> from L<GlyphaStore::Client>; they
do not set C<$!>.

=head1 CONFIGURATION AND ENVIRONMENT

None.

=head1 DEPENDENCIES

Core modules only.

=head1 INCOMPATIBILITIES

None known.

=head1 BUGS AND LIMITATIONS

None known.

=head1 AUTHOR

Giacomo Picchiarelli

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2026, Giacomo Picchiarelli.

BSD 3-Clause. See the distribution F<LICENSE>.

=cut
