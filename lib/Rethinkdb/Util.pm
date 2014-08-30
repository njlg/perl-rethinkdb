package Rethinkdb::Util;
use Rethinkdb::Base -base;

use Scalar::Util qw{ blessed looks_like_number };
use JSON::PP 'encode_json';
use Carp 'croak';

use Rethinkdb::Query::Datum;
use Rethinkdb::Protocol;

my $PROTOCOL = Rethinkdb::Protocol->new;
my $COUNTER = 0;

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

  my $val = $self->_expr($arg);

  if ( _wrap_func_helper $val ) {
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
    return Rethinkdb::Query::Datum->new({ data => $value });
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

  use feature ':5.10';
  use Data::Dumper;

  my $retval;
  eval { $retval = encode_json $value; };

  if ( !$@ && $retval ) {
    return Rethinkdb::Query->new(
      _type => $PROTOCOL->term->termType->json,
      args => $retval
    );
  }
  elsif ( ref $value eq 'ARRAY' ) {
    return $self->_make_array($value);
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
    return Rethinkdb::Query::Datum->new({ data => $value });
  }

  # to croak or not?
  return;
}

sub _to_json {
  my $self  = shift;
  my $value = shift;

  if ( !$value ) {
    return;
  }

  my $json = encode_json $value;

  return $json;
}

sub _to_term {
  my $self  = shift;
  my $value = shift;

  if ( !$value ) {
    return;
  }

  my $datum = $self->_to_datum($value);
  my $term = { type => $PROTOCOL->term->termType->datum, datum => $datum };

  return $term;
}

sub _to_datum {
  my $self  = shift;
  my $value = shift;
  my $hash  = {};

  if ( !$value ) {
    return;
  }

  if ( ref $value eq 'ARRAY' ) {
    return $self->_make_array($value);
  }
  elsif ( ref $value eq 'HASH' ) {
    return $self->_make_obj($value);
  }
  elsif ( looks_like_number $value ) {
    $hash = { type => $PROTOCOL->datum->datumType->r_num, r_num => $value };
  }
  elsif ( !ref $value ) {
    $hash = { type => $PROTOCOL->datum->datumType->r_str, r_str => $value };
  }
  elsif ( ref $value eq 'Rethinkdb::_True'
    || ref $value eq 'Rethinkdb::_False' )
  {
    $hash = { type => $PROTOCOL->datum->datumType->r_bool, r_bool => $value == 1 };
  }

  return $hash;
}

sub _make_array {
  my $self = shift;
  my $args = @_ ? @_ > 1 ? [@_] : [ @{ $_[0] } ] : [];

  my $obj = Rethinkdb::Query->new(
    _type => $PROTOCOL->term->termType->make_array,
    args => $args,
  );

  return $obj;
}

sub _make_obj {
  my $self = shift;
  my $optargs = @_ ? @_ > 1 ? {@_} : { %{ $_[0] } } : {};

  my $obj = Rethinkdb::Query->new(
    _type    => $PROTOCOL->term->termType->make_obj,
    optargs => $optargs,
  );

  return $obj;
}

sub _make_func {
  my $self = shift;
  my $func = shift;

  my $params       = [];
  my $param_length = length prototype $func;
  $param_length ||= 1;

  foreach ( 1 .. $param_length ) {
    push @{$params},
      Rethinkdb::Query->new( _type => $PROTOCOL->term->termType->var, args => $_, );
  }

  my $body = $func->( @{$params} );
  my $args = $self->_make_array( [ 1 .. $param_length ] );

  my $obj = Rethinkdb::Query->new(
    _type => $PROTOCOL->term->termType->func,
    args => [ $args, $body ],
  );

  return $obj;
}

sub _to_datum_object {
  my $self   = shift;
  my $values = shift;

  my $object = [];
  foreach ( keys %{$values} ) {
    push @{$object},
      {
      key => $_,
      val => {
        type  => $PROTOCOL->term->termType->datum,
        datum => $self->_to_datum( $values->{$_} )
      }
      };
  }

  my $expr = { type => $PROTOCOL->datum->datumType->r_object, r_object => $object };

  return $expr;
}

sub _to_datum_array {
  my $self   = shift;
  my $values = shift;

  my $list = [];
  foreach ( @{$values} ) {
    push @{$list}, $self->_to_datum($_);
  }

  my $expr = { type => $PROTOCOL->datum->datumType->r_array, r_array => $list };

  return $expr;
}

sub _from_datum {
  my $self  = shift;
  my $datum = shift;

  if ( $datum->_type == $PROTOCOL->datum->datumType->r_null ) {
    return undef;
  }
  elsif ( $datum->_type == $PROTOCOL->datum->datumType->r_bool ) {
    if ( $datum->r_bool ) {
      return Rethinkdb::_True->new;
    }
    else {
      return Rethinkdb::_False->new;
    }
  }
  elsif ( $datum->_type == $PROTOCOL->datum->datumType->r_num ) {
    return $datum->r_num;
  }
  elsif ( $datum->_type == $PROTOCOL->datum->datumType->r_str ) {
    return $datum->r_str;
  }
  elsif ( $datum->_type == $PROTOCOL->datum->datumType->r_array ) {
    my $r_array = $datum->r_array;
    my $array   = [];

    foreach ( @{$r_array} ) {
      push @{$array}, $self->_from_datum($_);
    }

    return $array;
  }
  elsif ( $datum->_type == $PROTOCOL->datum->datumType->r_object ) {
    my $r_object = $datum->r_object;
    my $object   = {};

    foreach ( @{$r_object} ) {
      $object->{ $_->key } = $self->_from_datum( $_->val );
    }

    return $object;
  }

  croak 'Invalid datum type (' . $datum->_type . ')';
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
