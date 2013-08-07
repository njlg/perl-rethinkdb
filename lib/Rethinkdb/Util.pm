package Rethinkdb::Util;
use Rethinkdb::Base -strict;

use Scalar::Util 'blessed';
use JSON::PP 'encode_json';
use Carp 'croak';

use Rethinkdb::Query::Datum;

my $COUNTER = 0;

sub token {
  return $COUNTER++;
}

sub expr {
  my $self = shift;
  my $value = shift;

# use feature ':5.10';
# use Data::Dumper;
# say 'expr -----------------';
# say Dumper $value;

  if( blessed($value) && $value->isa('Rethinkdb::Query') ) {
    return $value;
  }
  elsif( ref $value eq 'ARRAY' ) {
    return $self->make_array($value);
  }
  elsif( ref $value eq 'HASH' ) {
    return $self->make_obj($value);
  }
  # elsif( ref $value eq 'FUNC' ) {
  #   return $self->make_obj($value);
  # }
  else {
    return Rethinkdb::Query::Datum->new($value);
  }

  # to croak or not?
  return;
}

sub to_json {
  my $self  = shift;
  my $value = shift;

  if( ! $value ) {
    return;
  }

  my $json = encode_json $value;

  return $json;
}

sub to_term {
  my $self  = shift;
  my $value = shift;

  if( ! $value ) {
    return;
  }

  my $datum  = $self->to_datum($value);
  my $term = {
    type  => Term::TermType::DATUM,
    datum => $datum
  };

  return $term;
}

sub to_datum {
  my $self  = shift;
  my $value = shift;
  my $hash  = {};

  if( ! $value ) {
    return;
  }

  # if ( ref $value eq 'ARRAY' ) {
  #   return $self->_to_datum_array($value);
  # }

  # if ( ref $value eq 'HASH' ) {
  #   return $self->_to_datum_object($value);
  # }

  if ( ref $value eq 'ARRAY' ) {
    return $self->make_array($value);
  }

  if ( ref $value eq 'HASH' ) {
    return $self->make_obj($value);
  }

  if ( !ref $value && $value =~ /^\d+$/ ) {
    $hash = {
      type  => Datum::DatumType::R_NUM,
      r_num => int $value
    };
  }
  elsif ( !ref $value ) {
    $hash = {
      type  => Datum::DatumType::R_STR,
      r_str => $value
    };
  }
  elsif ( ref $value eq 'Rethinkdb::_True' || ref $value eq 'Rethinkdb::_False' ) {
    $hash = {
      type   => Datum::DatumType::R_BOOL,
      r_bool => $value == 1
    };
  }

  return $hash;
}

sub make_array {
  my $self = shift;
  my $args = @_ ? @_ > 1 ? [@_] : [@{$_[0]}] : [];

  my $obj = Rethinkdb::Query->new(
    type => Term::TermType::MAKE_ARRAY,
    args => $args,
  );

  return $obj;
}

sub make_obj {
  my $self    = shift;
  my $optargs = @_ ? @_ > 1 ? {@_} : {%{$_[0]}} : {};

  my $obj = Rethinkdb::Query->new(
    type    => Term::TermType::MAKE_OBJ,
    optargs => $optargs,
  );

  return $obj;
}

sub _to_datum_object {
  my $self   = shift;
  my $values = shift;

  my $object = [];
  foreach ( keys %{$values} ) {
    push @{$object}, {
      key  => $_,
      val => {
        type => Term::TermType::DATUM,
        datum => Rethinkdb::Util->to_datum( $values->{$_} )
      }
    };
  }

  my $expr = {
    type     => Datum::DatumType::R_OBJECT,
    r_object => $object
  };

  return $expr;
}

sub _to_datum_array {
  my $self   = shift;
  my $values = shift;

  my $list = [];
  foreach ( @{$values} ) {
    push @{$list}, Rethinkdb::Util->to_datum($_);
  }

  my $expr = {
    type    => Datum::DatumType::R_ARRAY,
    r_array => $list
  };

  return $expr;
}

sub from_datum {
  my $self = shift;
  my $datum = shift;

  if( $datum->type == Datum::DatumType::R_NULL ) {
    return undef;
  }
  elsif( $datum->type == Datum::DatumType::R_BOOL ) {
    return $datum->r_bool;
  }
  elsif( $datum->type == Datum::DatumType::R_NUM ) {
    return $datum->r_num;
  }
  elsif( $datum->type == Datum::DatumType::R_STR ) {
    return $datum->r_str;
  }
  elsif( $datum->type == Datum::DatumType::R_ARRAY ) {
    my $r_array = $datum->r_array;
    my $array = [];

    foreach( @{$r_array} ) {
      push @{$array}, $self->from_datum($_);
    }

    return $array;
  }
  elsif( $datum->type == Datum::DatumType::R_OBJECT ) {
    my $r_object = $datum->r_object;
    my $object = {};

    foreach( @{$r_object} ) {
      $object->{$_->key} = $self->from_datum($_->val);
    }

    return $object;
  }

  croak 'Invalid datum type (' . $datum->type . ')';
}

1;
