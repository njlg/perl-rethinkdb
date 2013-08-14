package Rethinkdb::Table;
use Rethinkdb::Base 'Rethinkdb::Query';

use Carp qw'croak carp';
use Scalar::Util 'weaken';

use Rethinkdb::Protocol;
use Rethinkdb::Util;

has [qw{rdb name}];

# primary_key = None
# datacenter = None
# durability = hard|soft
# cache_size = '1024MB'
sub create {
  my $self = shift;
  my $optargs = ref $_[0] ? $_[0] : {@_};

  say 'name: ' . $self->name;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self->_parent,
    type    => Term::TermType::TABLE_CREATE,
    args    => $self->name,
    optargs => $optargs,
  );

  weaken $q->{rdb};
  return $q;
}

sub drop {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self->_parent,
    type    => Term::TermType::TABLE_DROP,
    args    => $self->name,
  );

  weaken $q->{rdb};
  return $q;
}

sub list {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self->_parent,
    type    => Term::TermType::TABLE_LIST,
  );

  weaken $q->{rdb};
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
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::INDEX_CREATE,
    args    => $index
  );

  weaken $q->{rdb};
  return $q;
}

sub index_drop {
  my $self = shift;
  my $index = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::INDEX_DROP,
    args    => $index
  );

  weaken $q->{rdb};
  return $q;
}

sub index_list {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::INDEX_LIST,
  );

  weaken $q->{rdb};
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
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::INSERT,
    args    => $args,
    optargs => $params,
  );

  weaken $q->{rdb};
  return $q;
}

sub delete {
  my $self = shift;
  my $optargs = ref $_[0] ? $_[0] : {@_};

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::DELETE,
    optargs => $optargs,
  );

  weaken $q->{rdb};
  return $q;
}


# get a document by primary key
# TODO: key can be other things besides string
sub get {
  my $self = shift;
  my ( $key ) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::GET,
    args    => $key,
  );

  weaken $q->{rdb};
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

# use feature ':5.10';
# use Data::Dumper;
# say Dumper $values;
# say Dumper $params;
# exit;
  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::GET_ALL,
    args    => $values,
    optargs => $params,
  );



  weaken $q->{rdb};
  return $q;
}

sub between {
  my $self = shift;
  my ( $lower, $upper, $attr ) = @_;

  $attr ||= 'id';

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::BETWEEN,
    args    => [$lower, $upper],
    optargs => {index => $attr},
  );

  weaken $q->{rdb};
  return $q;
}


# predicate = JSON, expr, or sub
# TODO: fix predicate for expr or sub
sub filter {
  my $self      = shift;
  my $predicate = shift;
  # my $predicate = @_ ? @_ > 1 ? {@_} : {%{$_[0]}} : {};

  my $args = Rethinkdb::Util->wrap_func($predicate);

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::FILTER,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}


# TODO
sub inner_join {
  my $self = shift;
  my ( $table, $predicate ) = @_;

  croak 'inner_join is not implemented';

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::INNER_JOIN,
    args    => $predicate,
  );

  weaken $q->{rdb};
  return $q;
}

# TODO
sub outer_join {
  my $self = shift;
  my ( $table, $predicate ) = @_;

  croak 'outer_join is not implemented';
}

sub eq_join {
  my $self = shift;
  my ($left, $table, $optargs) = @_;

  if( ! $optargs ) {
    $optargs = { index => 'id' };
  }

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::EQ_JOIN,
    args    => [$left, $table],
  );

  weaken $q->{rdb};
  return $q;
}

sub map {
  my $self = shift;
  my ( $args ) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::MAP,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub reduce {
  my $self = shift;
  my ( $function ) = @_;

  croak 'reduce is not implemented';

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::REDUCE,
    args    => $function,
  );

  weaken $q->{rdb};
  return $q;
}

# TODO
sub concat_map {
  my $self = shift;
  my ( $table, $predicate ) = @_;

  croak 'concat_map is not implemented';
}

sub order_by {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::ORDERBY,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub skip {
  my $self   = shift;
  my $number = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::SKIP,
    args    => $number,
  );

  weaken $q->{rdb};
  return $q;
}

sub limit {
  my $self   = shift;
  my $number = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::LIMIT,
    args    => $number,
  );

  weaken $q->{rdb};
  return $q;
}

sub slice {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::SLICE,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub nth {
  my $self   = shift;
  my $number = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::NTH,
    args    => $number,
  );

  weaken $q->{rdb};
  return $q;
}

# TODO: fix this
sub pluck {
  my $self  = shift;
  my $args = @_ ? @_ > 1 ? [@_] : [ @{ $_[0] } ] : [];

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::PLUCK,
    args    => $args
  );

  weaken $q->{rdb};
  return $q;
}

sub without {
  my $self  = shift;
  my $args = @_ ? @_ > 1 ? [@_] : [ @{ $_[0] } ] : [];

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::WITHOUT,
    args    => $args
  );

  weaken $q->{rdb};
  return $q;
}

sub count {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::COUNT,
    args    => $args
  );

  weaken $q->{rdb};
  return $q;
}

sub distinct {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::DISTINCT,
  );

  weaken $q->{rdb};
  return $q;
}

sub union {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    type    => Term::TermType::UNION,
    args    => [$self, $args],
  );

  weaken $q->{rdb};
  return $q;
}

sub sample {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::SAMPLE,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub grouped_map_reduce {
  my $self = shift;
  croak 'grouped_map_reduce is not implemented';
}

sub stream_to_array {
  my $self = shift;
  croak 'stream_to_array is not implemented';
}

sub with_fields {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::WITH_FIELDS,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub indexes_of {
  my $self = shift;
  my ($args) = @_;

  if( ref $args ) {
    croak 'Unsupported argument to indexes_of';
  }

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::INDEXES_OF,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub is_empty {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::IS_EMPTY,
  );

  weaken $q->{rdb};
  return $q;
}

sub group_by {
  my $self = shift;
  my $args = [@_];

  my $reductor;
  if( ref $args->[$#{$args}] ) {
    $reductor = pop @{$args};
    $args = [$args, $reductor];
  }

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::GROUPBY,
    args    => $args
  );

  weaken $q->{rdb};
  return $q;
}

sub contains {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::CONTAINS,
    args    => $args
  );

  weaken $q->{rdb};
  return $q;
}

1;
