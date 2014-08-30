package Rethinkdb::Query::Datum;
use Rethinkdb::Base 'Rethinkdb::Query';

use Carp 'croak';
use Scalar::Util 'looks_like_number';
use Rethinkdb::Protocol;

has 'data';
has 'datumType' => sub { Rethinkdb::Protocol->new->datum->datumType; };

sub _build {
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

  return { type => $self->_termType->datum, datum => $hash, };
}

1;

=encoding utf8

=head1 NAME

Rethinkdb::Query::Datum - RethinkDB Query Datum

=head1 SYNOPSIS

=head1 DESCRIPTION

L<Rethinkdb::Query::Datum> is the smallest building block in the RethinkDB
Query Language. A datum can be thought of as a primative. A datum can have the
following types: C<null>, C<number>, C<string>, or C<boolean>.

=head1 ATTRIBUTES

L<Rethinkdb::Query::Datum> implements the following attributes.

=head2 data

  my $datum = r->expr('Lorem Ipsum');
  say $datum->data;

The actual datum value of this instance.

=head2 datumType

  my $datum = r->expr('Lorem Ipsum');
  say $datum->datumType;

The actual RQL (RethinkDB Query Language) datum type of this instance.

=head1 SEE ALSO

L<Rethinkdb>, L<http://rethinkdb.com>

=cut
