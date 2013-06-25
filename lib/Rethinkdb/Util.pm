package Rethinkdb::Util;
use Rethinkdb::Base -strict;

use Carp 'croak';
use Sys::Hostname 'hostname';
use Digest::MD5 qw{md5 md5_hex};

use Data::Dumper;
use feature ':5.10';

my $MACHINE = join '', ( md5_hex(hostname) =~ /\d/g );
my $COUNTER = 0;

sub token {
  # my $t = time . q{} . ($COUNTER++) . $$ . $MACHINE;
  # $t = substr $t, 0, 13;

  return ( $COUNTER++ );
}

sub to_term {
  my $value = shift;
  my $hash  = {};

  return $hash;
}

sub to_datum {
  my $self = shift;
  my $value = shift;
  my $hash  = {};

  say Dumper $value;

  if ( ref $value eq 'ARRAY' ) {
    return $self->_to_datum_array($value);
  }

  if ( ref $value eq 'HASH' ) {
    return $self->_to_datum_object($value);
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

sub _to_datum_object {
  my $self   = shift;
  my $values = shift;

  my $object = [];
  foreach ( keys %{$values} ) {
    push @{$object}, {
      var  => $_,
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
    return $datum->r_null;
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
