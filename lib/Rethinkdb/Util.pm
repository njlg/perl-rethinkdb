package Rethinkdb::Util;
use Rethinkdb::Base -strict;

use Sys::Hostname 'hostname';
use Digest::MD5 qw{md5 md5_hex};

my $MACHINE = join '', (md5_hex(hostname) =~ /\d/g);
my $COUNTER = 0;

sub token {
  # my $t = time . q{} . ($COUNTER++) . $$ . $MACHINE;
  # $t = substr $t, 0, 13;

  return ($COUNTER++);
}

sub to_term {
  my $value = shift;
  my $hash = {};

  if( !ref $value && $value =~ /^\d+$/ ) {
    $hash = {
      type => Term::TermType::NUMBER,
      number => int $value
    };
  }
  elsif( !ref $value ) {
    $hash = {
      type => Term::TermType::STRING,
      valuestring => $value
    };
  }
  elsif( ref $value eq 'Rethinkdb::_True' || ref $value eq 'Rethinkdb::_False' ) {
    $hash = {
      type => Term::TermType::BOOL,
      valuebool => $value == 1
    };
  }

  return $hash;
}

1;