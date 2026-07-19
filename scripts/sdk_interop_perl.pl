#!/usr/bin/env perl
use v5.32;
use strict;
use warnings;
use FindBin;
use Getopt::Long qw(GetOptions);
use lib "$FindBin::Bin/../sdk/perl/lib";
use GlyphaStore::Client;

sub parse_hex {
    my ($text) = @_;
    $text //= '';
    $text =~ s/\s+//g;
    return '' if $text eq '';
    die "odd hex length\n" if length($text) % 2;
    return pack('H*', $text);
}

sub to_hex {
    return unpack('H*', $_[0] // '');
}

my %options = (host => '127.0.0.1', expire_at_ns => 0);
GetOptions(
    'host=s'        => \$options{host},
    'port=i'        => \$options{port},
    'key-hex=s'     => \$options{key_hex},
    'value-hex=s'   => \$options{value_hex},
    'expire-at-ns=i'=> \$options{expire_at_ns},
) or die "invalid arguments\n";
my $command = shift @ARGV // '';
die "--port and command are required\n"
    if !$options{port} || $command !~ /\A(?:put|get|erase|pipeline-put-get)\z/;

my $key = parse_hex($options{key_hex});
my $value = parse_hex($options{value_hex});
my $client = GlyphaStore::Client->connect(host => $options{host}, port => $options{port});

if ($command eq 'put') {
    my $result = $client->put($key, $value, expire_at_ns => $options{expire_at_ns});
    die "put not committed\n" if $result->{outcome} ne 'committed';
}
elsif ($command eq 'get') {
    print to_hex($client->get($key)), "\n";
}
elsif ($command eq 'erase') {
    my $result = $client->erase($key);
    die "erase not committed\n" if $result->{outcome} ne 'committed';
}
else {
    my $responses = $client->execute_pipeline([
        { opcode => 'put', key => $key, value => $value },
        { opcode => 'get', key => $key },
    ]);
    die "pipeline outcomes failed\n"
        if @$responses != 2
        || $responses->[0]{outcome} ne 'succeeded'
        || $responses->[1]{outcome} ne 'succeeded';
    die "pipeline value mismatch\n" if $responses->[1]{value} ne $value;
    print to_hex($responses->[1]{value}), "\n";
}

$client->close;
