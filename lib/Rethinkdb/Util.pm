package Rethinkdb::Util;
use Rethinkdb::Base -base;

use Scalar::Util 'blessed';
use JSON::PP 'encode_json';
use Carp 'croak';

use Rethinkdb::Query::Datum;
use Rethinkdb::Protocol;

my $PROTOCOL = Rethinkdb::Protocol->new;
my $COUNTER  = 0;

sub _token {
  return $COUNTER++;
}

sub _wrap_func_helper {
  my $node = shift;

  if ( !( blessed $node && $node->isa('Rethinkdb::Query') ) ) {
    return;
  }

  if ( blessed $node
    && $node->_type
    && $node->_type eq $PROTOCOL->term->termType->implicit_var )
  {
    return 1;
  }

  if ( $node->args ) {
    foreach ( @{ $node->args } ) {
      if ( _wrap_func_helper($_) ) {
        return 1;
      }
    }
  }

  return;
}

sub _wrap_func {
  my $self = shift;
  my $arg  = shift;
  my $force  = shift;

  my $val = $self->_expr($arg);

  if ( _wrap_func_helper $val ) {
    return $self->_make_func( sub ($) { $val; } );
  }
  elsif( $force ) {
    return $self->_make_func( sub ($) { $val; } );
  }

  return $val;
}

sub _expr {
  my $self  = shift;
  my $value = shift;

  if ( blessed($value) && $value->can('_build') ) {
    return $value;
  }
  elsif ( ref $value eq 'ARRAY' ) {
    return $self->_make_array($value);
  }
  elsif ( ref $value eq 'HASH' ) {
    return $self->_make_obj($value);
  }
  elsif ( ref $value eq 'CODE' ) {
    return $self->_make_func($value);
  }
  else {
    return Rethinkdb::Query::Datum->new( { data => $value } );
  }

  # to croak or not?
  return;
}

# try to make expr mostly JSON
sub _expr_json {
  my $self  = shift;
  my $value = shift;

  if ( blessed($value) && $value->can('_build') ) {
    return $value;
  }

  my $retval;
  eval { $retval = encode_json $value; };

  if ( !$@ && $retval ) {
    return Rethinkdb::Query->new(
      _type => $PROTOCOL->term->termType->json,
      args  => $retval
    );
  }
  elsif ( ref $value eq 'ARRAY' ) {
    return $self->_make_array($value);
  }
  elsif ( ref $value eq 'HASH' ) {
    return $self->_make_obj($value);
  }
  elsif ( ref $value eq 'CODE' ) {
    return $self->_make_func($value);
  }
  else {
    return Rethinkdb::Query::Datum->new( { data => $value } );
  }

  # to croak or not?
  return;
}

sub _make_array {
  my $self = shift;
  my $args = @_ ? @_ > 1 ? [@_] : [ @{ $_[0] } ] : [];

  my $obj = Rethinkdb::Query->new(
    _type => $PROTOCOL->term->termType->make_array,
    args  => $args,
  );

  return $obj;
}

sub _make_obj {
  my $self = shift;
  my $optargs = @_ ? @_ > 1 ? {@_} : { %{ $_[0] } } : {};

  my $obj = Rethinkdb::Query->new(
    _type   => $PROTOCOL->term->termType->make_obj,
    optargs => $optargs,
  );

  return $obj;
}

sub _make_func {
  my $self = shift;
  my $func = shift;

  my $params    = [];
  my $prototype = prototype $func;
  $prototype ||= '$';
  my $param_length = length $prototype;

  foreach ( 1 .. $param_length ) {
    push @{$params},
      Rethinkdb::Query->new(
      _type => $PROTOCOL->term->termType->var,
      args  => $_,
      );
  }

  my $body = $func->( @{$params} );
  my $args = $self->_make_array( [ 1 .. $param_length ] );

  my $obj = Rethinkdb::Query->new(
    _type => $PROTOCOL->term->termType->func,
    args  => [ $args, $body ],
  );

  return $obj;
}

1;

=encoding utf8

=head1 NAME

Rethinkdb::Util - RethinkDB Utilities

=head1 DESCRIPTION

This module contains internal utilities used by the RethinkDB perl driver.

=head1 SEE ALSO

L<Rethinkdb>, L<http://rethinkdb.com>

=cut
