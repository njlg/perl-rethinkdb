package Rethinkdb::Query::Datum;
use Rethinkdb::Base 'Rethinkdb::Query';

use Carp 'croak';
use Rethinkdb::Protocol;

has 'data';

sub new {
  my $class = shift;
  my $data  = shift;

  my $self = bless { data => $data }, $class;

  return $self;
}

sub build {
  my $self  = shift;
  my $data = $self->data;

  my $hash = {};

  if ( ! $data ) {
    $hash = {
      type  => Datum::DatumType::R_NULL,
    };
  }
  elsif ( !ref $data && $data =~ /^\d+$/ ) {
    $hash = {
      type  => Datum::DatumType::R_NUM,
      r_num => int $data
    };
  }
  elsif ( !ref $data ) {
    $hash = {
      type  => Datum::DatumType::R_STR,
      r_str => $data
    };
  }
  elsif ( ref $data eq 'Rethinkdb::_True' || ref $data eq 'Rethinkdb::_False' ) {
    $hash = {
      type   => Datum::DatumType::R_BOOL,
      r_bool => $data == 1
    };
  }
  else {
    croak "Got crazy Datum: $data";
  }

  return  {
    type  => Term::TermType::DATUM,
    datum => $hash,
  };
}

1;
