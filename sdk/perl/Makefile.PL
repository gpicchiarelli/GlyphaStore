use strict;
use warnings;
use ExtUtils::MakeMaker 7.12;
use Config;

die "GlyphaStore requires a Perl with 64-bit integers\n"
    unless defined($Config{use64bitint}) && $Config{use64bitint} eq 'define'
        && ($Config{uvsize} // 0) >= 8;

my $version = '0.1.0';

WriteMakefile(
    NAME             => 'GlyphaStore',
    VERSION_FROM     => 'lib/GlyphaStore.pm',
    ABSTRACT_FROM    => 'lib/GlyphaStore.pm',
    AUTHOR           => 'Giacomo Picchiarelli',
    LICENSE          => 'bsd',
    MIN_PERL_VERSION => '5.032',
    CONFIGURE_REQUIRES => {
        'ExtUtils::MakeMaker' => '7.12',
        'Config'              => '0',
    },
    TEST_REQUIRES => {
        'Test::More'       => '0.98',
        'FindBin'          => '0',
        'IO::Select'       => '0',
        'IO::Socket::INET' => '0',
        'POSIX'            => '0',
    },
    PREREQ_PM => {
        'Config'           => '0',
        'Errno'            => '0',
        'Exporter'         => '0',
        'IO::Select'       => '0',
        'IO::Socket::INET' => '0',
        'Socket'           => '0',
        'Time::HiRes'      => '0',
    },
    META_MERGE => {
        'meta-spec' => { version => 2 },
        release_status => 'testing',
        dynamic_config => 0,
        no_index => {
            directory => [ 'benchmarks', 't' ],
        },
        resources => {
            license    => ['https://opensource.org/licenses/BSD-3-Clause'],
            bugtracker => {
                web => 'https://github.com/gpicchiarelli/GlyphaStore/issues',
            },
            repository => {
                type => 'git',
                url  => 'https://github.com/gpicchiarelli/GlyphaStore.git',
                web  => 'https://github.com/gpicchiarelli/GlyphaStore',
            },
            homepage => 'https://github.com/gpicchiarelli/GlyphaStore',
        },
        prereqs => {
            runtime => {
                requires => {
                    'perl'             => '5.032',
                    'Errno'            => '0',
                    'Exporter'         => '0',
                    'IO::Select'       => '0',
                    'IO::Socket::INET' => '0',
                    'Socket'           => '0',
                    'Time::HiRes'      => '0',
                },
            },
            test => {
                requires => {
                    'Test::More'       => '0.98',
                    'FindBin'          => '0',
                    'IO::Select'       => '0',
                    'IO::Socket::INET' => '0',
                    'POSIX'            => '0',
                },
            },
        },
        provides => {
            'GlyphaStore' => {
                file    => 'lib/GlyphaStore.pm',
                version => $version,
            },
            'GlyphaStore::Client' => {
                file    => 'lib/GlyphaStore/Client.pm',
                version => $version,
            },
            'GlyphaStore::Protocol' => {
                file    => 'lib/GlyphaStore/Protocol.pm',
                version => $version,
            },
        },
        x_static_install => 1,
    },
    dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz' },
    clean => { FILES => 'GlyphaStore-*' },
);
