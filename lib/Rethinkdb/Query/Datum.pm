package Rethinkdb::Query::Datum;
use Rethinkdb::Base 'Rethinkdb::Query';

use Carp 'croak';
use Scalar::Util 'looks_like_number';
use Rethinkdb::Protocol;

has 'data';

sub new {
  my $class = shift;
  my $data  = shift;

  my $self = bless { data => $data }, $class;

  return $self;
}

sub build {
  my $self = shift;
  my $data = $self->data;

  my $hash = {};

  if ( !ref $data && !$data && $data != '0' ) {
    $hash = { type => Datum::DatumType::R_NULL, };
  }
  elsif ( looks_like_number $data ) {
    $hash = { type => Datum::DatumType::R_NUM, r_num => $data };
  }
  elsif ( !ref $data ) {
    $hash = { type => Datum::DatumType::R_STR, r_str => $data };
  }
  elsif ( ref $data eq 'Rethinkdb::_True' || ref $data eq 'Rethinkdb::_False' )
  {
    $hash = { type => Datum::DatumType::R_BOOL, r_bool => $data == 1 };
  }
  else {
    croak "Got unknown Datum: $data";
  }

  return { type => Term::TermType::DATUM, datum => $hash, };
}

1;
