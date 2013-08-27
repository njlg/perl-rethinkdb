package Rethinkdb::Table;
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
    type    => Term::TermType::TABLE_CREATE,
    args    => $self->name,
    optargs => $optargs,
  );

  return $q;
}

sub drop {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self->rdb,
    type => Term::TermType::TABLE_DROP,
    args => $self->name,
  );

  return $q;
}

sub list {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self->rdb,
    type => Term::TermType::TABLE_LIST,
  );

  return $q;
}

sub index_create {
  my $self  = shift;
  my $index = shift;
  my $func  = shift;

  if( $func ) {
    carp 'table->index_create does not accept functions yet';
  }

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::INDEX_CREATE,
    args    => $index
  );

  return $q;
}

sub index_drop {
  my $self = shift;
  my $index = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::INDEX_DROP,
    args    => $index
  );

  return $q;
}

sub index_list {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::INDEX_LIST,
  );

  return $q;
}

sub insert {
  my $self   = shift;
  my $data   = shift;
  my $params = shift;

  my $args = Rethinkdb::Query->new(
    type => Term::TermType::JSON,
    args => Rethinkdb::Util->to_json($data),
  );

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::INSERT,
    args    => $args,
    optargs => $params,
  );

  return $q;
}

sub delete {
  my $self = shift;
  my $optargs = ref $_[0] ? $_[0] : {@_};

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::DELETE,
    optargs => $optargs,
  );

  return $q;
}

# get a document by primary key
# TODO: key can be other things besides string
sub get {
  my $self = shift;
  my ( $key ) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::GET,
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

  if( ref $values->[0] eq 'ARRAY' ) {
    ($values, $params) = @{$values};
  }

  if( ref $values->[$#{$values}] eq 'HASH' ) {
    $params = pop @{$values};
  }

  if( !$params->{index} ) {
    $params->{index} = 'id';
  }

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::GET_ALL,
    args    => $values,
    optargs => $params,
  );

  return $q;
}

1;
