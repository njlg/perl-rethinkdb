package Rethinkdb::Query;
use Rethinkdb::Base -base;

use Carp 'croak';
use Scalar::Util 'weaken';

use Rethinkdb;
use Rethinkdb::Protocol;

has [qw{ _rdb _parent _type args optargs }];
has '_termType' => sub { Rethinkdb::Protocol->new->term->termType; };

sub new {
  my $class = shift;
  my $self = bless @_ ? @_ > 1 ? {@_} : { %{ $_[0] } } : {},
    ref $class || $class;

  if ( $self->_parent && $self->_parent->_rdb ) {
    my $rdb = $self->_parent->_rdb;
    delete $self->_parent->{_rdb};
    $self->_rdb($rdb);
  }

  # process args and optargs
  $self->_args;
  $self->_optargs;

  # ditch parent
  delete $self->{_parent};

  return $self;
}

sub _build {
  my $self = shift;

  my $q = { type => $self->_type };

  if ( $self->args ) {
    foreach ( @{ $self->args } ) {
      if ( ref $_ && UNIVERSAL::can( $_, 'can' ) && $_->can('_build') ) {
        push @{ $q->{args} }, $_->_build;
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
        && $value->can('_build') )
      {
        push @{ $q->{optargs} }, { key => $_, val => $value->_build };
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
      push @{$expr_args}, Rethinkdb::Util->_expr($_);
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
        $expr_optargs->{$_} = Rethinkdb::Util->_expr( $optargs->{$_} );
      }

      $self->optargs($expr_optargs);
    }
  }

  return;
}

sub run {
  my $self = shift;
  my ( $connection, $args, $callback ) = @_;

  if ( ref $connection ne 'Rethinkdb::IO' ) {
    $callback = $args;
    $args     = $connection;
    if ( $self->_rdb && $self->_rdb->io ) {
      $connection = $self->_rdb->io;
    }
    else {
      croak 'ERROR: run() was not given a connection';
    }
  }

  if ( ref $args eq 'CODE' ) {
    $callback = $args;
    $args     = {};
  }

  return $connection->_start( $self, $args, $callback );
}

# WRITING DATA

sub update {
  my $self    = shift;
  my $args    = shift;
  my $optargs = @_ ? @_ > 1 ? {@_} : { %{ $_[0] } } : {};

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->update,
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
    _type   => $self->_termType->replace,
    args    => $args,
    optargs => $optargs,
  );

  return $q;
}

sub delete {
  my $self = shift;
  my $optargs = @_ ? @_ > 1 ? {@_} : { %{ $_[0] } } : {};

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->delete,
    optargs => $optargs,
  );

  return $q;
}

# SELECTING DATA

sub filter {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->filter,
    args    => Rethinkdb::Util->_wrap_func($args),
  );

  return $q;
}

# JOINS

sub inner_join {
  my $self = shift;
  my ( $table, $predicate ) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->inner_join,
    args    => [ $table, $predicate ],
  );

  return $q;
}

sub outer_join {
  my $self = shift;
  my ( $table, $predicate ) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->outer_join,
    args    => [ $table, $predicate ],
  );

  return $q;
}

sub eq_join {
  my $self = shift;
  my ( $left, $table, $optargs ) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->eq_join,
    args    => [ $left, $table ],
    optargs => $optargs,
  );

  return $q;
}

sub zip {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, _type => $self->_termType->zip,
    );

  return $q;
}

# TRANSFORMATIONS

sub map {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->map,
    args    => Rethinkdb::Util->_wrap_func($args),
  );

  return $q;
}

sub with_fields {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->with_fields,
    args    => $args,
  );

  return $q;
}

sub concat_map {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->concat_map,
    args    => $args,
  );

  return $q;
}

sub order_by {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->order_by,
    args    => $args,
  );

  return $q;
}

sub skip {
  my $self   = shift;
  my $number = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->skip,
    args    => $number,
  );

  return $q;
}

sub limit {
  my $self   = shift;
  my $number = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->limit,
    args    => $number,
  );

  return $q;
}

sub slice {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->slice,
    args    => $args,
  );

  return $q;
}

sub nth {
  my $self   = shift;
  my $number = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->nth,
    args    => $number,
  );

  return $q;
}

sub offsets_of {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->offsets_of,
    args    => Rethinkdb::Util->_wrap_func($args),
  );

  return $q;
}

sub is_empty {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->is_empty,
  );

  return $q;
}

sub union {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->union,
    args    => $args,
  );

  return $q;
}

sub sample {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->sample,
    args    => $args,
  );

  return $q;
}

# AGGREGATION

sub group {
  my $self = shift;
  my $args = [@_];

  my $reductor;
  if ( ref $args->[ $#{$args} ] ) {
    $reductor = pop @{$args};
    $args = [ $args, $reductor ];
  }

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->group,
    args    => $args
  );

  return $q;
}

sub ungroup {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->ungroup,
  );

  return $q;
}

sub reduce {
  my $self     = shift;
  my $function = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->reduce,
    args    => $function,
  );

  return $q;
}

sub count {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->count,
    args    => $args
  );

  return $q;
}

sub sum {
  my $self = shift;
  my $args = {@_};

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->sum,
    args    => $args
  );

  return $q;
}

sub avg {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->avg,
    args    => $args
  );

  return $q;
}

sub min {
  my $self = shift;
  my $args = {@_};

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->min,
    args    => $args
  );

  return $q;
}

sub max {
  my $self = shift;
  my $args = {@_};

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->max,
    args    => $args
  );

  return $q;
}

sub distinct {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->distinct,
  );

  return $q;
}

sub contains {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->contains,
    args    => $args
  );

  return $q;
}

# DOCUMENT MANIPULATION

sub pluck {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->pluck,
    args    => $args
  );

  return $q;
}

sub without {
  my $self = shift;
  my $args = @_ ? @_ > 1 ? [@_] : [ @{ $_[0] } ] : [];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->without,
    args    => $args
  );

  return $q;
}

sub merge {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->merge,
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
    _type   => $self->_termType->append,
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
    _type   => $self->_termType->prepend,
    args    => $args
  );

  return $q;
}

sub difference {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->difference,
    args    => [$args],
  );

  return $q;
}

sub set_insert {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->set_insert,
    args    => $args,
  );

  return $q;
}

sub set_union {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->set_union,
    args    => [$args],
  );

  return $q;
}

sub set_intersection {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->set_intersection,
    args    => [$args],
  );

  return $q;
}

sub set_difference {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->set_difference,
    args    => [$args],
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
    _type   => $self->_termType->get_field,
    args    => $args
  );

  return $q;
}

sub has_fields {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->has_fields,
    args    => $args
  );

  return $q;
}

sub insert_at {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->insert_at,
    args    => $args,
  );

  return $q;
}

sub splice_at {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->splice_at,
    args    => $args,
  );

  return $q;
}

sub delete_at {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->delete_at,
    args    => $args,
  );

  return $q;
}

sub change_at {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->change_at,
    args    => $args,
  );

  return $q;
}

sub keys {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->keys,
    args    => $args,
  );

  return $q;
}

# STRING MANIPULATION

sub match {
  my $self = shift;
  my ($expr) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->match,
    args    => $expr
  );

  return $q;
}

sub split {
  my $self = shift;
  my ($expr) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->split,
    args    => $expr
  );

  return $q;
}

sub upcase {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->upcase,
  );

  return $q;
}

sub downcase {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->downcase,
  );

  return $q;
}

# MATH AND LOGIC


sub add {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->add,
    args    => [$args],
  );

  return $q;
}

sub sub {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->sub,
    args    => $args,
  );

  return $q;
}

sub mul {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->mul,
    args    => $args,
  );

  return $q;
}

sub div {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->div,
    args    => $args,
  );

  return $q;
}

sub mod {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->mod,
    args    => $args,
  );

  return $q;
}

sub and {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->and,
    args    => $args,
  );

  return $q;
}

sub or {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->or,
    args    => $args,
  );

  return $q;
}

sub eq {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->eq,
    args    => $args,
  );

  return $q;
}

sub ne {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->ne,
    args    => $args,
  );

  return $q;
}

sub gt {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->gt,
    args    => $args,
  );

  return $q;
}

sub ge {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->ge,
    args    => $args,
  );

  return $q;
}

sub lt {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->lt,
    args    => $args,
  );

  return $q;
}

sub le {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->le,
    args    => $args,
  );

  return $q;
}

sub not {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, _type => $self->_termType->not,
    );

  return $q;
}

# DATES AND TIMES

sub in_timezone {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->in_timezone,
    args    => $args,
  );

  return $q;
}

sub timezone {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->timezone,
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
    _type   => $self->_termType->during,
    args    => [ $start, $end ],
    optargs => $optargs,
  );

  return $q;
}

sub date {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->date,
  );

  return $q;
}

sub time_of_day {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->time_of_day,
  );

  return $q;
}

sub year {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->year,
  );

  return $q;
}

sub month {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->month,
  );

  return $q;
}

sub day {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, _type => $self->_termType->day,
    );

  return $q;
}

sub day_of_week {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->day_of_week,
  );

  return $q;
}

sub day_of_year {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->day_of_year,
  );

  return $q;
}

sub hours {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->hours,
  );

  return $q;
}

sub minutes {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->minutes,
  );

  return $q;
}

sub seconds {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->seconds,
  );

  return $q;
}

sub to_iso8601 {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->to_iso8601,
  );

  return $q;
}

sub to_epoch_time {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->to_epoch_time,
  );

  return $q;
}

# CONTROL STRUCTURES

# TODO: figure out why the arguments have to be reversed here
sub do {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _rdb  => $self->_rdb,
    _type => $self->_termType->funcall,
    args  => [ $args, $self ],
  );

  return $q;
}

sub for_each {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->for_each,
    args    => $args,
  );

  return $q;
}

sub default {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->default,
    args    => $args,
  );

  return $q;
}

sub coerce_to {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->coerce_to,
    args    => $args,
  );

  return $q;
}

sub type_of {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->type_of,
  );

  return $q;
}

sub info {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->info,
  );

  return $q;
}

1;

=encoding utf8

=head1 NAME

Rethinkdb::Query - RethinkDB Query

=head1 SYNOPSIS

=head1 DESCRIPTION

L<Rethinkdb::Query> is a type of query.

=head1 ATTRIBUTES

L<Rethinkdb::Query> implements the following attributes.

=head2 args

  my $query = r->table('marvel')->get(1);
  say $query->args;

The arguments for this instance of a query.

=head2 optargs

  my $query = r->table('marvel')->get_all(1, { index => 'rank' });
  say $query->optargs;

The optional arguments for this instance of a query.

=head1 METHODS

L<Rethinkdb::Query> implements the following methods.

=head2 new

This is a specialized constructor that enables chaining the queries together
in a rational way. This constructor should never be called directly by
consumers of this library.

=head2 run

  r->table('marvel')->run;

Run a query on a connection.

The callback will get either an error, a single JSON result, or a cursor,
depending on the query.

=head2 update

  r->table('posts')->get(1)->update({status => 'published'})->run;

Update JSON documents in a table. Accepts a JSON document, a ReQL expression,
or a combination of the two.

=head2 replace

  r->table('posts')->get(1)->replace({
    id      => 1,
    title   => 'Lorem ipsum',
    content => 'Aleas jacta est',
    status  => 'draft'
  })->run;

Replace documents in a table. Accepts a JSON document or a ReQL expression, and
replaces the original document with the new one. The new document must have the
same primary key as the original document.

=head2 delete

  r->table('comments')->
    get('7eab9e63-73f1-4f33-8ce4-95cbea626f59')->delete->run;

Delete one or more documents from a table.

=head2 filter

  r->table('users')->filter({'age' => 30})->run;

Get all the documents for which the given predicate is true.

L</filter> can be called on a sequence, selection, or a field containing an
array of elements. The return type is the same as the type on which the
function was called on.

The body of every filter is wrapped in an implicit C<< default(r->false) >>,
which means that if a non-existence errors is thrown (when you try to access a
field that does not exist in a document), RethinkDB will just ignore the
document. The C<default> value can be changed by passing the named argument
C<default>. Setting this optional argument to C<< r->error >> will cause any
non-existence errors to return a C<runtime_error>.

=head2 inner_join

  r->table('marvel')->inner_join(r->table('dc'), sub($$) {
    my ($marvel, $dc) = @_;
    return marvel->attr('strength')->lt($dc->attr('strength'));
  })->run;

Returns the inner product of two sequences (e.g. a table, a filter result)
filtered by the predicate. The query compares each row of the left sequence
with each row of the right sequence to find all pairs of rows which satisfy
the predicate. When the predicate is satisfied, each matched pair of rows of
both sequences are combined into a result row.

=head2 outer_join

  r->table('marvel')->outer_join(r->table('dc'), sub ($$) {
    my ($marvel, $dc) = @_;
    return $marvel->attr('strength')->lt($dc->attr('strength'));
  })->run;

Computes a left outer join by retaining each row in the left table even if no
match was found in the right table.

=head2 eq_join

  r->table('players')->eq_join('gameId', r->table('games'))->run;

Join tables using a field on the left-hand sequence matching primary keys or
secondary indexes on the right-hand table. L</eq_join> is more efficient than
other ReQL join types, and operates much faster. Documents in the result set
consist of pairs of left-hand and right-hand documents, matched when the
field on the left-hand side exists and is non-null and an entry with that
field's value exists in the specified index on the right-hand side.

=head2 zip

  r->table('marvel')->eq_join('main_dc_collaborator',
    r->table('dc'))->zip()->run;

Used to zip up the result of a join by merging the right fields into
left fields of each member of the sequence.

=head2 map

  r->table('marvel')->map(sub {
    my $hero = shift;
    return $hero->attr('combatPower')->add(
      $hero->('compassionPower')->mul(2)
    );
  })->run;

Transform each element of the sequence by applying the given mapping function.

=head2 with_fields

  r->table('users')->with_fields('id', 'username', 'posts')->run;

Plucks one or more attributes from a sequence of objects, filtering out any
objects in the sequence that do not have the specified fields. Functionally,
this is identical to L</has_fields> followed by L</pluck> on a sequence.

=head2 concat_map

  r->table('marvel')->concatMap(sub {
    my $hero = shift;
    return $hero->attr('defeatedMonsters');
  })->run;

Concatenate one or more elements into a single sequence using a mapping
function.

=head2 order_by

  r->table('posts')->order_by({index => 'date'})->run;
  r->table('posts')->order_by({index => r->desc('date')})->run;

Sort the sequence by document values of the given key(s). To specify the
ordering, wrap the attribute with either L<C<< r->asc >>|Rethinkdb/asc> or
L<C<< r->desc >>|Rethinkdb/desc> (defaults to ascending).

Sorting without an index requires the server to hold the sequence in memory,
and is limited to 100,000 documents. Sorting with an index can be done on
arbitrarily large tables, or after a L<Rethinkdb::Query::Table/between> command
using the same index.

=head2 skip

  r->table('marvel')->order_by('successMetric')->skip(10)->run;

Skip a number of elements from the head of the sequence.

=head2 limit

  r->table('marvel')->order_by('belovedness')->limit(10)->run;

End the sequence after the given number of elements.

=head2 slice

  r->table('players')->order_by({index => 'age'})->slice(3, 6)->run;

Return the elements of a sequence within the specified range.

=head2 nth

  r->expr([1,2,3])->nth(1)->run;

Get the nth element of a sequence.

=head2 offsets_of

  r->expr(['a','b','c'])->offsets_of('c')->run;

Get the indexes of an element in a sequence. If the argument is a predicate,
get the indexes of all elements matching it.

=head2 is_empty

  r->table('marvel')->is_empty->run;

Test if a sequence is empty.

=head2 union

  r->table('marvel')->union(r->table('dc'))->run;

Concatenate two sequences.

=head2 sample

  r->table('marvel')->sample(3)->run;

Select a given number of elements from a sequence with uniform random
distribution. Selection is done without replacement.

=head2 group

  r->table('games')->group('player')->max('points')->run;

Takes a stream and partitions it into multiple groups based on the fields or
functions provided. Commands chained after L</group> will be called on each of
these grouped sub-streams, producing grouped data.

=head2 ungroup

  r->table('games')
    ->group('player')->max('points')->attr('points')
    ->ungroup()->order_by(r->desc('reduction'))->run;

Takes a grouped stream or grouped data and turns it into an array of objects
representing the groups. Any commands chained after L</ungroup> will operate on
this array, rather than operating on each group individually. This is useful
if you want to e.g. order the groups by the value of their reduction.

=head2 reduce

  r->table('posts')->map(sub { return 1; })->reduce(sub($$) {
    my ($left, $right) = @_;
    return $left->add($right);
  })->run;

Produce a single value from a sequence through repeated application of a
reduction function.

=head2 count

  r->table('marvel')->count->add(r->table('dc')->count->run

Count the number of elements in the sequence. With a single argument, count the
number of elements equal to it. If the argument is a function, it is equivalent
to calling filter before count.

=head2 sum

  r->expr([3, 5, 7])->sum->run;

Sums all the elements of a sequence. If called with a field name, sums all the
values of that field in the sequence, skipping elements of the sequence that
lack that field. If called with a function, calls that function on every
element of the sequence and sums the results, skipping elements of the sequence
where that function returns C<null> or a non-existence error.

=head2 avg

  r->expr([3, 5, 7])->avg->run;

Averages all the elements of a sequence. If called with a field name, averages
all the values of that field in the sequence, skipping elements of the sequence
that lack that field. If called with a function, calls that function on every
element of the sequence and averages the results, skipping elements of the
sequence where that function returns C<null> or a non-existence error.

=head2 min

  r->expr([3, 5, 7])->min->run;

Finds the minimum of a sequence. If called with a field name, finds the element
of that sequence with the smallest value in that field. If called with a
function, calls that function on every element of the sequence and returns the
element which produced the smallest value, ignoring any elements where the
function returns C<null> or produces a non-existence error.

=head2 max

  r->expr([3, 5, 7])->max->run;

Finds the maximum of a sequence. If called with a field name, finds the element
of that sequence with the largest value in that field. If called with a
function, calls that function on every element of the sequence and returns the
element which produced the largest value, ignoring any elements where the
function returns null or produces a non-existence error.

=head2 distinct

  r->table('marvel')->concat_map(sub {
    my $hero = shift;
    return $hero->attr('villainList')
  })->distinct->run;

Remove duplicate elements from the sequence.

=head2 contains

  r->table('marvel')->get('ironman')->
    attr('opponents')->contains('superman')->run;

Returns whether or not a sequence contains all the specified values, or if
functions are provided instead, returns whether or not a sequence contains
values matching all the specified functions.

=head2 pluck

  r->table('marvel')->get('IronMan')->
    pluck('reactorState', 'reactorPower')->run;

Plucks out one or more attributes from either an object or a sequence of
objects (projection).

=head2 without

  r->table('marvel')->get('IronMan')->without('personalVictoriesList')->run;

The opposite of pluck; takes an object or a sequence of objects, and returns
them with the specified paths removed.

=head2 merge

  r->table('marvel')->get('IronMan')->merge(
    r->table('loadouts')->get('alienInvasionKit')
  )->run;

Merge two objects together to construct a new object with properties from both.
Gives preference to attributes from other when there is a conflict.

=head2 append

  r->table('marvel')->get('IronMan')->
    attr('equipment')->append('newBoots')->run;

Append a value to an array.

=head2 prepend

  r->table('marvel')->get('IronMan')->
    attr('equipment')->prepend('newBoots')->run;

Prepend a value to an array.

=head2 difference

  r->table('marvel')->get('IronMan')->
    attr('equipment')->difference(['Boots'])->run;

Remove the elements of one array from another array.

=head2 set_insert

  r->table('marvel')->get('IronMan')->
    attr('equipment')->set_insert('newBoots')->run;

Add a value to an array and return it as a set (an array with distinct values).

=head2 set_union

  r->table('marvel')->get('IronMan')->
    attr('equipment')->set_union(['newBoots', 'arc_reactor'])->run;

Add a several values to an array and return it as a set (an array with distinct
values).

=head2 set_intersection

  r->table('marvel')->get('IronMan')->attr('equipment')->
    set_intersection(['newBoots', 'arc_reactor'])->run;

Intersect two arrays returning values that occur in both of them as a set (an
array with distinct values).

=head2 set_difference

  r->table('marvel')->get('IronMan')->attr('equipment')->
    set_difference(['newBoots', 'arc_reactor'])->run;

Remove the elements of one array from another and return them as a set (an
array with distinct values).

=head2 attr

  r->table('marvel')->get('IronMan')->attr('firstAppearance')->run;

Get a single field from an object. If called on a sequence, gets that field
from every object in the sequence, skipping objects that lack it.

=head2 has_fields

  r->table('players')->has_fields('games_won')->run;

Test if an object has one or more fields. An object has a field if it has that
key and the key has a non-null value. For instance, the object
C<< {'a' => 1, 'b' => 2, 'c' => null} >> has the fields C<a> and C<b>.

=head2 insert_at

  r->expr(['Iron Man', 'Spider-Man'])->insert_at(1, 'Hulk')->run;

Insert a value in to an array at a given index. Returns the modified array.

=head2 splice_at

  r->expr(['Iron Man', 'Spider-Man'])->splice_at(1, ['Hulk', 'Thor'])->run;

Insert several values in to an array at a given index. Returns the modified
array.

=head2 delete_at

  r->expr(['a','b','c','d','e','f'])->delete_at(1)->run;

Remove one or more elements from an array at a given index. Returns the
modified array.

=head2 change_at

  r->expr(['Iron Man', 'Bruce', 'Spider-Man'])->change_at(1, 'Hulk')->run;

Change a value in an array at a given index. Returns the modified array.

=head2 keys

  r->table('marvel')->get('ironman')->keys->run;

Return an array containing all of the object's keys.

=head2 match

  r->table('users')->filter(sub {
    my $doc = shift;
    return $doc->attr('name')->match('^A')
  })->run;

Matches against a regular expression. If there is a match, returns an object
with the fields:

=head2 split

  r->expr('foo  bar bax')->split->run;
  r->expr('foo,bar,bax')->split(",")->run;
  r->expr('foo,bar,bax')->split(",", 1)->run;

Splits a string into substrings. Splits on whitespace when called with no
arguments. When called with a separator, splits on that separator. When called
with a separator and a maximum number of splits, splits on that separator at
most C<max_splits> times. (Can be called with C<undef> as the separator if you
want to split on whitespace while still specifying C<max_splits>.)

Mimics the behavior of Python's C<string.split> in edge cases, except for
splitting on the empty string, which instead produces an array of
single-character strings.

=head2 upcase

  r->expr('Sentence about LaTeX.')->upcase->run;

Uppercases a string.

=head2 downcase

  r->expr('Sentence about LaTeX.')->downcase->run;

Lowercases a string.

=head2 add

  r->expr(2)->add(2)->run;

Sum two numbers, concatenate two strings, or concatenate 2 arrays.

=head2 sub

  r->expr(2)->sub(2)->run;

Subtract two numbers.

=head2 mul

  r->expr(2)->mul(2)->run;

Multiply two numbers, or make a periodic array.

=head2 div

  r->expr(2)->div(2)->run;

Divide two numbers.

=head2 mod

  r->expr(2)->mod(2)->run;

Find the remainder when dividing two numbers.

=head2 and

  r->expr(r->true)->and(r->false)->run;

Compute the logical C<and> of two or more values.

=head2 or

  r->expr(r->true)->or(r->false)->run;

Compute the logical C<or> of two or more values.

=head2 eq

  r->expr(2)->eq(2)->run;

Test if two values are equal.

=head2 ne

  r->expr(2)->ne(2)->run;

Test if two values are not equal.

=head2 gt

  r->expr(2)->gt(2)->run;

Test if the first value is greater than other.

=head2 ge

  r->expr(2)->ge(2)->run;

Test if the first value is greater than or equal to other.

=head2 lt

  r->expr(2)->lt(2)->run;

Test if the first value is less than other.

=head2 le

  r->expr(2)->le(2)->run;

Test if the first value is less than or equal to other.

=head2 not

  r->expr(r->true)->not->run;

Compute the logical inverse (not) of an expression.

=head2 in_timezone

  r->now->in_timezone('-08:00')->hours->run;

Return a new time object with a different timezone. While the time stays the
same, the results returned by methods such as L</hours> will change since they
take the timezone into account. The timezone argument has to be of the ISO 8601
format.

=head2 timezone

  r->table('users')->filter(sub {
    my $user = shift;
    return $user->attr('subscriptionDate')->timezone->eql('-07:00');
  })->run;

Return the timezone of the time object.

=head2 during

  r->table('posts')->filter(
    r->row->attr('date')->during(
      r->time(2013, 12, 1, 'Z'),
      r->time(2013, 12, 10, 'Z')
    )
  )->run;

Return if a time is between two other times (by default, inclusive for the
start, exclusive for the end).

=head2 date

  r->table('users')->filter(sub {
    my $user = shift;
    return user->attr('birthdate')->date->eql(r->now->date);
  })->run;

Return a new time object only based on the day, month and year (ie. the same
day at 00:00).

=head2 time_of_day

  r->table('posts')->filter(
    r->row->attr('date')->time_of_day->le(12*60*60)
  )->run;

Return the number of seconds elapsed since the beginning of the day stored in
the time object.

=head2 year

  r->table('users')->filter(sub {
    my $user = shift;
    return user->attr('birthdate')->year->eql(1986);
  })->run;

Return the year of a time object.

=head2 month

  r->table('users')->filter(
    r->row->attr('birthdate')->month->eql(11)
  )->run;

Return the month of a time object as a number between 1 and 12. For your
convenience, the terms L<C<< r->january >>|Rethinkdb/january>,
L<C<< r->february >>|Rethinkdb/february> etc. are defined and map to the
appropriate integer.

=head2 day

  r->table('users')->filter(
    r->row->attr('birthdate')->day->eql(24)
  )->run;

Return the day of a time object as a number between 1 and 31.

=head2 day_of_week

  r->now->day_of_week->run;

Return the day of week of a time object as a number between 1 and 7 (following
ISO 8601 standard). For your convenience, the terms r.monday, r.tuesday etc.
are defined and map to the appropriate integer.

=head2 day_of_year

  r->now->day_of_year->run;

Return the day of the year of a time object as a number between 1 and 366
(following ISO 8601 standard).

=head2 hours

  r->table('posts')->filter(sub {
    my $post = shift;
    return $post->attr('date')->hours->lt(4);
  })->run;

Return the hour in a time object as a number between 0 and 23.

=head2 minutes

  r->table('posts')->filter(sub {
    my $post = shift;
    return $post->attr('date')->minutes->lt(10);
  })->run;

Return the minute in a time object as a number between 0 and 59.

=head2 seconds

  r->table('posts')->filter(sub {
    my $post = shift;
    return $post->attr('date')->seconds->lt(30);
  })->run;

Return the seconds in a time object as a number between 0 and 59.999 (double
precision).

=head2 to_iso8601

  r->now->to_iso8601->run;

Convert a time object to its ISO 8601 format.

=head2 to_epoch_time

  r->now->to_epoch_time->run;

Convert a time object to its epoch time.

=head2 do

  r->table('players')->get('86be93eb-a112-48f5-a829-15b2cb49de1d')->do(sub {
    my $player = shift;
    return $player->attr('gross_score')->sub($player->attr('course_handicap'));
  })->run

Evaluate an expression and pass its values as arguments to a function or to an
expression.

=head2 for_each

  r->table('marvel')->for_each(sub {
    my $hero = shift;
    r->table('villains')->get($hero->attr('villainDefeated'))->delete;
  })->run;

Loop over a sequence, evaluating the given write query for each element.

=head2 default

  r->table('posts')->map(sub {
    my $post = shift;
    return {
      title => $post->attr('title'),
      author => $post->attr('author')->default('Anonymous')
    };
  })->run

Handle non-existence errors. Tries to evaluate and return its first argument.
If an error related to the absence of a value is thrown in the process, or if
its first argument returns C<null>, returns its second argument.
(Alternatively, the second argument may be a function which will be called with
either the text of the non-existence error or C<null>.)

=head2 coerce_to

  r->table('posts')->map(sub {
    my $post = shift;
    return $post->merge({
      'comments' => r->table('comments')->get_all(
        $post->attr('id'), { index => 'post_id' })->coerce_to('array')
    });
  )->run

Convert a value of one type into another.

=head2 type_of

  r->expr('foo')->type_of->run;

Gets the type of a value.

=head2 info

  r->table('marvel')->info->run;

Get information about a ReQL value.

=head1 SEE ALSO

L<Rethinkdb>, L<http://rethinkdb.com>

=cut
