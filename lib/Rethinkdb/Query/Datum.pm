package Rethinkdb::Query::Datum;
use Rethinkdb::Base 'Rethinkdb::Query';

use Carp 'croak';
use Scalar::Util 'looks_like_number';
use Rethinkdb::Protocol;

has 'data';
has 'datumType' => sub { Rethinkdb::Protocol->new->datum->datumType; };

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
    $hash = { type => $self->datumType->r_null, };
  }
  elsif ( looks_like_number $data ) {
    $hash = { type => $self->datumType->r_num, r_num => $data };
  }
  elsif ( !ref $data ) {
    $hash = { type => $self->datumType->r_str, r_str => $data };
  }
  elsif ( ref $data eq 'Rethinkdb::_True' || ref $data eq 'Rethinkdb::_False' )
  {
    $hash = { type => $self->datumType->r_bool, r_bool => $data == 1 };
  }
  else {
    croak "Got unknown Datum: $data";
  }

  return { type => $self->termType->datum, datum => $hash, };
}

1;
