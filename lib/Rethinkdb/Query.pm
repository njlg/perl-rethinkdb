package Rethinkdb::Query;
use Rethinkdb::Base -base;

use Carp 'croak';
use Scalar::Util 'weaken';

use Rethinkdb;
use Rethinkdb::Protocol;

has [qw{rdb args optargs type _parent}];

sub new {
  my $class = shift;
  my $self = bless @_ ? @_ > 1 ? {@_} : { %{ $_[0] } } : {},
    ref $class || $class;

  if ( $self->_parent && $self->_parent->rdb ) {
    my $rdb = $self->_parent->rdb;
    delete $self->_parent->{rdb};
    $self->rdb($rdb);
  }

  # process args and optargs
  $self->_args;
  $self->_optargs;

  # ditch parent
  delete $self->{_parent};

  return $self;
}

sub build {
  my $self = shift;

  my $q = { type => $self->type };

  if ( $self->args ) {
    foreach ( @{ $self->args } ) {
      if ( ref $_ && UNIVERSAL::can( $_, 'can' ) && $_->can('build') ) {
        push @{ $q->{args} }, $_->build;
      }
      else {
        push @{ $q->{args} }, $_;
      }
    }
  }

  if ( $self->optargs ) {
    foreach ( keys %{ $self->optargs } ) {
      my $value = $self->{optargs}->{$_};
      if ( ref $value
        && UNIVERSAL::can( $value, 'can' )
        && $value->can('build') )
      {
        push @{ $q->{optargs} }, { key => $_, val => $value->build };
      }
      else {
        push @{ $q->{optargs} }, { key => $_, val => $value };
      }
    }
  }

  return $q;
}

sub _args {
  my $self   = shift;
  my $args   = $self->args;
  my $parent = $self->_parent;
  delete $self->{args};

  if ( defined $args ) {

    if ( ref $args ne 'ARRAY' ) {
      $args = [$args];
    }

    my $expr_args = [];

    if ($parent) {
      push @{$expr_args}, $parent;
    }

    foreach ( @{$args} ) {
      push @{$expr_args}, Rethinkdb::Util->expr($_);
    }

    $self->args($expr_args);
  }
  elsif ( defined $parent ) {
    $self->args( [$parent] );
  }

  return;
}

sub _optargs {
  my $self    = shift;
  my $optargs = $self->optargs;
  delete $self->{optargs};

  if ($optargs) {
    if ( ref $optargs ) {
      my $expr_optargs = {};

      foreach ( keys %{$optargs} ) {
        $expr_optargs->{$_} = Rethinkdb::Util->expr( $optargs->{$_} );
      }

      $self->optargs($expr_optargs);
    }
  }

  return;
}

sub run {
  my $self = shift;
  my ( $connection, $args ) = @_;

  if ( ref $connection ne 'Rethinkdb::IO' ) {
    $args = $connection;
    if ( $self->rdb && $self->rdb->io ) {
      $connection = $self->rdb->io;
    }
    else {
      croak 'ERROR: run() was not given a connection';
    }
  }

  return $connection->_start( $self, $args );
}

sub update {
  my $self    = shift;
  my $args    = shift;
  my $optargs = @_ ? @_ > 1 ? {@_} : { %{ $_[0] } } : {};

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::UPDATE,
    args    => $args,
    optargs => $optargs,
  );

  return $q;
}

sub replace {
  my $self    = shift;
  my $args    = shift;
  my $optargs = @_ ? @_ > 1 ? {@_} : { %{ $_[0] } } : {};

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::REPLACE,
    args    => $args,
    optargs => $optargs,
  );

  return $q;
}

sub merge {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::MERGE,
    args    => $args
  );

  return $q;
}

sub has_fields {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::HAS_FIELDS,
    args    => $args
  );

  return $q;
}

# TODO: replace this with AUTOLOAD or overload %{}
# to get something like r->table->get()->{attr}->run;
# or like r->table->get()->attr->run;
sub attr {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::GET_FIELD,
    args    => $args
  );

  return $q;
}

sub append {
  my $self = shift;

  # my $args = @_ ? @_ > 1 ? [@_] : [ @{ $_[0] } ] : [];
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::APPEND,
    args    => $args
  );

  return $q;
}

sub prepend {
  my $self = shift;

  # my $args = @_ ? @_ > 1 ? [@_] : [ @{ $_[0] } ] : [];
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::PREPEND,
    args    => $args
  );

  return $q;
}

sub difference {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::DIFFERENCE,
    args    => [$args],
  );

  return $q;
}

sub set_insert {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::SET_INSERT,
    args    => $args,
  );

  return $q;
}

sub set_union {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::SET_UNION,
    args    => [$args],
  );

  return $q;
}

sub set_intersection {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::SET_INTERSECTION,
    args    => [$args],
  );

  return $q;
}

sub set_difference {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::SET_DIFFERENCE,
    args    => [$args],
  );

  return $q;
}

sub pluck {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::PLUCK,
    args    => $args
  );

  return $q;
}

sub without {
  my $self = shift;
  my $args = @_ ? @_ > 1 ? [@_] : [ @{ $_[0] } ] : [];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::WITHOUT,
    args    => $args
  );

  return $q;
}

sub delete {
  my $self = shift;
  my $optargs = @_ ? @_ > 1 ? {@_} : { %{ $_[0] } } : {};

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::DELETE,
    optargs => $optargs,
  );

  return $q;
}

sub insert_at {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::INSERT_AT,
    args    => $args,
  );

  return $q;
}

sub splice_at {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::SPLICE_AT,
    args    => $args,
  );

  return $q;
}

sub delete_at {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::DELETE_AT,
    args    => $args,
  );

  return $q;
}

sub change_at {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::CHANGE_AT,
    args    => $args,
  );

  return $q;
}

sub keys {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::KEYS,
    args    => $args,
  );

  return $q;
}

sub with_fields {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::WITH_FIELDS,
    args    => $args,
  );

  return $q;
}

sub slice {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::SLICE,
    args    => $args,
  );

  return $q;
}

sub indexes_of {
  my $self = shift;
  my ($args) = @_;

  # if( ref $args ) {
  #   croak 'Unsupported argument to indexes_of';
  # }

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::INDEXES_OF,
    args    => Rethinkdb::Util->wrap_func($args),
  );

  return $q;
}

sub is_empty {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::IS_EMPTY,
  );

  return $q;
}

sub sample {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::SAMPLE,
    args    => $args,
  );

  return $q;
}

sub zip {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, type => Term::TermType::ZIP, );

  return $q;
}

sub distinct {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::DISTINCT,
  );

  return $q;
}

sub contains {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::CONTAINS,
    args    => $args
  );

  return $q;
}

sub match {
  my $self = shift;
  my ($expr) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::MATCH,
    args    => $expr
  );

  return $q;
}

sub nth {
  my $self   = shift;
  my $number = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::NTH,
    args    => $number,
  );

  return $q;
}

sub add {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::ADD,
    args    => [$args],
  );

  return $q;
}

sub sub {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::SUB,
    args    => $args,
  );

  return $q;
}

sub mul {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::MUL,
    args    => $args,
  );

  return $q;
}

sub div {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::DIV,
    args    => $args,
  );

  return $q;
}

sub mod {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::MOD,
    args    => $args,
  );

  return $q;
}

sub and {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::ALL,
    args    => $args,
  );

  return $q;
}

sub or {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::ANY,
    args    => $args,
  );

  return $q;
}

sub eq {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::EQ,
    args    => $args,
  );

  return $q;
}

sub ne {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::NE,
    args    => $args,
  );

  return $q;
}

sub gt {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::GT,
    args    => $args,
  );

  return $q;
}

sub ge {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::GE,
    args    => $args,
  );

  return $q;
}

sub lt {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::LT,
    args    => $args,
  );

  return $q;
}

sub le {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::LE,
    args    => $args,
  );

  return $q;
}

sub not {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, type => Term::TermType::NOT, );

  return $q;
}

sub map {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::MAP,
    args    => Rethinkdb::Util->wrap_func($args),
  );

  return $q;
}

sub filter {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::FILTER,
    args    => Rethinkdb::Util->wrap_func($args),
  );

  return $q;
}

# TODO: figure out why the arguments have to be reversed here
sub do {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(

    # _parent => $self,
    rdb  => $self->rdb,
    type => Term::TermType::FUNCALL,
    args => [ $args, $self ],
  );

  return $q;
}

sub for_each {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::FOREACH,
    args    => $args,
  );

  return $q;
}

sub default {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::DEFAULT,
    args    => $args,
  );

  return $q;
}

sub coerce_to {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::COERCE_TO,
    args    => $args,
  );

  return $q;
}

sub type_of {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, type => Term::TermType::TYPEOF,
    );

  return $q;
}

sub info {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, type => Term::TermType::INFO, );

  return $q;
}

sub order_by {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::ORDERBY,
    args    => $args,
  );

  return $q;
}

sub concat_map {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::CONCATMAP,
    args    => $args,
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
    type    => Term::TermType::BETWEEN,
    args    => [ $lower, $upper ],
    optargs => $optargs,
  );

  return $q;
}

sub inner_join {
  my $self = shift;
  my ( $table, $predicate ) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::INNER_JOIN,
    args    => [ $table, $predicate ],
  );

  return $q;
}

sub outer_join {
  my $self = shift;
  my ( $table, $predicate ) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::OUTER_JOIN,
    args    => [ $table, $predicate ],
  );

  return $q;
}

sub eq_join {
  my $self = shift;
  my ( $left, $table, $optargs ) = @_;

  # if( ! $optargs ) {
  #   $optargs = { index => 'id' };
  # }

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::EQ_JOIN,
    args    => [ $left, $table ],
    optargs => $optargs,
  );

  return $q;
}

sub reduce {
  my $self = shift;
  my ( $function, $base ) = @_;

  my $optargs = {};
  if ($base) {
    $optargs->{base} = $base;
  }

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::REDUCE,
    args    => $function,
    optargs => $optargs,
  );

  return $q;
}

sub skip {
  my $self   = shift;
  my $number = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::SKIP,
    args    => $number,
  );

  return $q;
}

sub limit {
  my $self   = shift;
  my $number = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::LIMIT,
    args    => $number,
  );

  return $q;
}

sub count {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::COUNT,
    args    => $args
  );

  return $q;
}

sub union {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::UNION,
    args    => $args,
  );

  return $q;
}

sub grouped_map_reduce {
  my $self = shift;
  my ( $grouping, $reduction, $mapping, $base ) = @_;

  my $optargs = {};
  if ($base) {
    $optargs->{base} = $base;
  }

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::GROUPED_MAP_REDUCE,
    args    => [ $grouping, $reduction, $mapping ],
    optargs => $optargs,
  );

  return $q;
}

sub group_by {
  my $self = shift;
  my $args = [@_];

  my $reductor;
  if ( ref $args->[ $#{$args} ] ) {
    $reductor = pop @{$args};
    $args = [ $args, $reductor ];
  }

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::GROUPBY,
    args    => $args
  );

  return $q;
}

#
# time functions
#

sub to_iso8601 {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::TO_ISO8601,
  );

  return $q;
}

sub to_epoch_time {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::TO_EPOCH_TIME,
  );

  return $q;
}

sub during {
  my $self    = shift;
  my $start   = shift;
  my $end     = shift;
  my $optargs = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::DURING,
    args    => [ $start, $end ],
    optargs => $optargs,
  );

  return $q;
}

sub date {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, type => Term::TermType::DATE, );

  return $q;
}

sub time_of_day {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::TIME_OF_DAY,
  );

  return $q;
}

sub timezone {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::TIMEZONE,
  );

  return $q;
}

sub year {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, type => Term::TermType::YEAR, );

  return $q;
}

sub month {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, type => Term::TermType::MONTH,
    );

  return $q;
}

sub day {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, type => Term::TermType::DAY, );

  return $q;
}

sub day_of_week {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::DAY_OF_WEEK,
  );

  return $q;
}

sub day_of_year {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::DAY_OF_YEAR,
  );

  return $q;
}

sub hours {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, type => Term::TermType::HOURS,
    );

  return $q;
}

sub minutes {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::MINUTES,
  );

  return $q;
}

sub seconds {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::SECONDS,
  );

  return $q;
}

sub in_timezone {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::IN_TIMEZONE,
    args    => $args,
  );

  return $q;
}

1;
