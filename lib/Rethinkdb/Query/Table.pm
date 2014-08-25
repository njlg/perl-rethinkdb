package Rethinkdb::Query::Table;
use Rethinkdb::Base 'Rethinkdb::Query';

use Carp qw'croak carp';
use Scalar::Util 'weaken';

use Rethinkdb::Protocol;
use Rethinkdb::Util;

has [qw{ rdb name }];

# primary_key = None
# datacenter = None
# durability = hard|soft
# cache_size = '1024MB'
sub create {
  my $self = shift;
  my $optargs = ref $_[0] ? $_[0] : {@_};

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    type    => $self->termType->table_create,
    args    => $self->name,
    optargs => $optargs,
  );

  return $q;
}

sub drop {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self->rdb,
    type => $self->termType->table_drop,
    args => $self->name,
  );

  return $q;
}

sub list {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self->rdb,
    type => $self->termType->table_list,
  );

  return $q;
}

sub index_create {
  my $self  = shift;
  my $index = shift;
  my $func  = shift;
  my $multi = shift;

  if ($func) {
    carp 'table->index_create does not accept functions yet';
  }

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->index_create,
    args    => $index
  );

  return $q;
}

sub index_drop {
  my $self  = shift;
  my $index = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->index_drop,
    args    => $index
  );

  return $q;
}

sub index_list {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->index_list,
  );

  return $q;
}

sub index_status {
  my $self    = shift;
  my $indices = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->index_status,
    args    => $indices,
  );

  return $q;
}

sub index_wait {
  my $self    = shift;
  my $indices = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->index_wait,
    args    => $indices,
  );

  return $q;
}

sub changes {
  my $self    = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->changes,
  );

  return $q;
}

sub insert {
  my $self   = shift;
  my $args   = shift;
  my $params = shift;

  # my $args = Rethinkdb::Query->new(
  #   type => $self->termType->json,
  #   args => Rethinkdb::Util->to_json($data),
  # );

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->insert,
    args    => Rethinkdb::Util->expr_json($args),
    optargs => $params,
  );

  return $q;
}

sub delete {
  my $self = shift;
  my $optargs = ref $_[0] ? $_[0] : {@_};

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->delete,
    optargs => $optargs,
  );

  return $q;
}

sub sync {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->sync,
  );

  return $q;
}

# get a document by primary key
# TODO: key can be other things besides string
sub get {
  my $self = shift;
  my ($key) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->get,
    args    => $key,
  );

  return $q;
}

# Get all documents where the given value matches the value of the requested index
sub get_all {
  my $self = shift;

  # extract values
  my $values = \@_;
  my $params = {};

  if ( ref $values->[0] eq 'ARRAY' ) {
    ( $values, $params ) = @{$values};
  }

  if ( ref $values->[ $#{$values} ] eq 'HASH' ) {
    $params = pop @{$values};
  }

  if ( !$params->{index} ) {
    $params->{index} = 'id';
  }

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->get_all,
    args    => $values,
    optargs => $params,
  );

  return $q;
}

sub between {
  my $self = shift;
  my ( $lower, $upper, $index, $left_bound, $right_bound ) = @_;

  my $optargs = {};
  if ( ref $index ) {
    $optargs = $index;
  }
  else {
    $optargs->{index} = $index || 'id';

    if ($left_bound) {
      $optargs->{left_bound} = $left_bound;
    }

    if ($right_bound) {
      $optargs->{right_bound} = $right_bound;
    }
  }

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->between,
    args    => [ $lower, $upper ],
    optargs => $optargs,
  );

  return $q;
}

1;
