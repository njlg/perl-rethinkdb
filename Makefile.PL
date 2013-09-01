use 5.010001;

use Config;
use ExtUtils::MakeMaker;

use strict;
use warnings;

WriteMakefile(
  NAME         => 'Rethinkdb',
  VERSION_FROM => 'lib/Rethinkdb.pm',
  ABSTRACT     => 'Pure Perl RethinkDB Driver',
  AUTHOR       => 'Nathan Levin-Greenhaw <njlg@cpan.org>',
  LICENSE      => 'artistic_2',
  META_MERGE   => {
    requires  => { perl => '5.010001' },
    resources => {
      homepage    => 'https://github.com/njlg/perl-rethinkdb',
      license     => 'http://www.opensource.org/licenses/artistic-license-2.0',
      repository  => 'https://github.com/njlg/perl-rethinkdb',
      bugtracker  => 'https://github.com/njlg/perl-rethinkdb/issues'
    },
  },
  PREREQ_PM => { 'Google::ProtocolBuffers' => '0.08' },
  test      => { TESTS       => 't/*.t' }
);