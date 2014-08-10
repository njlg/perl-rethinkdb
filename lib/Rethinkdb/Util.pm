package Rethinkdb::Util;
use Rethinkdb::Base -base;

use Scalar::Util qw{ blessed looks_like_number };
use JSON::PP 'encode_json';
use Carp 'croak';

use Rethinkdb::Query::Datum;
use Rethinkdb::Protocol;

my $PROTOCOL = Rethinkdb::Protocol->new;
my $COUNTER = 0;

sub token {
  return $COUNTER++;
}

sub _wrap_func {
  my $node = shift;

  if ( !( blessed $node && $node->isa('Rethinkdb::Query') ) ) {
    return;
  }

  if ( blessed $node
    && $node->type
    && $node->type eq $PROTOCOL->term->termType->implicit_var )
  {
    return 1;
  }

  if ( $node->args ) {
    foreach ( @{ $node->args } ) {
      if ( _wrap_func($_) ) {
        return 1;
      }
    }
  }

  return;
}

sub wrap_func {
  my $self = shift;
  my $arg  = shift;

  my $val = $self->expr($arg);

  if ( _wrap_func $val ) {
    return $self->make_func( sub ($) { $val; } );
  }

  return $val;
}

sub expr {
  my $self  = shift;
  my $value = shift;

  if ( blessed($value) && $value->can('build') ) {
    return $value;
  }
  elsif ( ref $value eq 'ARRAY' ) {
    return $self->make_array($value);
  }
  elsif ( ref $value eq 'HASH' ) {
    return $self->make_obj($value);
  }
  elsif ( ref $value eq 'CODE' ) {
    return $self->make_func($value);
  }
  else {
    return Rethinkdb::Query::Datum->new($value);
  }

  # to croak or not?
  return;
}

# try to make expr mostly JSON
sub expr_json {
  my $self  = shift;
  my $value = shift;

  if ( blessed($value) && $value->can('build') ) {
    return $value;
  }

  use feature ':5.10';
  use Data::Dumper;

  my $retval;
  eval { $retval = encode_json $value; };

  # say Dumper $@;
  # say Dumper $retval;

  if ( !$@ && $retval ) {
    return Rethinkdb::Query->new( type => $PROTOCOL->term->termType->json,
      args => $retval );
  }
  elsif ( ref $value eq 'ARRAY' ) {
    return $self->make_array($value);
  }
  elsif ( ref $value eq 'ARRAY' ) {
    return $self->make_array($value);
  }
  elsif ( ref $value eq 'HASH' ) {
    return $self->make_obj($value);
  }
  elsif ( ref $value eq 'CODE' ) {
    return $self->make_func($value);
  }
  else {
    return Rethinkdb::Query::Datum->new($value);
  }

  # to croak or not?
  return;
}

sub to_json {
  my $self  = shift;
  my $value = shift;

  if ( !$value ) {
    return;
  }

  my $json = encode_json $value;

  return $json;
}

sub to_term {
  my $self  = shift;
  my $value = shift;

  if ( !$value ) {
    return;
  }

  my $datum = $self->to_datum($value);
  my $term = { type => $PROTOCOL->term->termType->datum, datum => $datum };

  return $term;
}

sub to_datum {
  my $self  = shift;
  my $value = shift;
  my $hash  = {};

  if ( !$value ) {
    return;
  }

  if ( ref $value eq 'ARRAY' ) {
    return $self->make_array($value);
  }
  elsif ( ref $value eq 'HASH' ) {
    return $self->make_obj($value);
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

sub make_array {
  my $self = shift;
  my $args = @_ ? @_ > 1 ? [@_] : [ @{ $_[0] } ] : [];

  my $obj = Rethinkdb::Query->new(
    type => $PROTOCOL->term->termType->make_array,
    args => $args,
  );

  return $obj;
}

sub make_obj {
  my $self = shift;
  my $optargs = @_ ? @_ > 1 ? {@_} : { %{ $_[0] } } : {};

  my $obj = Rethinkdb::Query->new(
    type    => $PROTOCOL->term->termType->make_obj,
    optargs => $optargs,
  );

  return $obj;
}

sub make_func {
  my $self = shift;
  my $func = shift;

  my $params       = [];
  my $param_length = length prototype $func;
  $param_length ||= 1;

  foreach ( 1 .. $param_length ) {
    push @{$params},
      Rethinkdb::Query->new( type => $PROTOCOL->term->termType->var, args => $_, );
  }

  my $body = $func->( @{$params} );
  my $args = $self->make_array( [ 1 .. $param_length ] );

  my $obj = Rethinkdb::Query->new(
    type => $PROTOCOL->term->termType->func,
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
        datum => Rethinkdb::Util->to_datum( $values->{$_} )
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
    push @{$list}, Rethinkdb::Util->to_datum($_);
  }

  my $expr = { type => $PROTOCOL->datum->datumType->r_array, r_array => $list };

  return $expr;
}

sub from_datum {
  my $self  = shift;
  my $datum = shift;

  if ( $datum->type == $PROTOCOL->datum->datumType->r_null ) {
    return undef;
  }
  elsif ( $datum->type == $PROTOCOL->datum->datumType->r_bool ) {
    if ( $datum->r_bool ) {
      return Rethinkdb::_True->new;
    }
    else {
      return Rethinkdb::_False->new;
    }
  }
  elsif ( $datum->type == $PROTOCOL->datum->datumType->r_num ) {
    return $datum->r_num;
  }
  elsif ( $datum->type == $PROTOCOL->datum->datumType->r_str ) {
    return $datum->r_str;
  }
  elsif ( $datum->type == $PROTOCOL->datum->datumType->r_array ) {
    my $r_array = $datum->r_array;
    my $array   = [];

    foreach ( @{$r_array} ) {
      push @{$array}, $self->from_datum($_);
    }

    return $array;
  }
  elsif ( $datum->type == $PROTOCOL->datum->datumType->r_object ) {
    my $r_object = $datum->r_object;
    my $object   = {};

    foreach ( @{$r_object} ) {
      $object->{ $_->key } = $self->from_datum( $_->val );
    }

    return $object;
  }

  croak 'Invalid datum type (' . $datum->type . ')';
}

1;
