package Rethinkdb::Query;
use Rethinkdb::Base -base;

use Carp 'croak';
use Scalar::Util 'weaken';

use Rethinkdb;
use Rethinkdb::Protocol;

has [qw{rdb args optargs type _parent}];
has 'termType' => sub { Rethinkdb::Protocol->new->term->termType; };

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

# WRITING DATA

sub update {
  my $self    = shift;
  my $args    = shift;
  my $optargs = @_ ? @_ > 1 ? {@_} : { %{ $_[0] } } : {};

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->update,
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
    type    => $self->termType->replace,
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
    type    => $self->termType->delete,
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
    type    => $self->termType->filter,
    args    => Rethinkdb::Util->wrap_func($args),
  );

  return $q;
}

# JOINS

sub inner_join {
  my $self = shift;
  my ( $table, $predicate ) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->inner_join,
    args    => [ $table, $predicate ],
  );

  return $q;
}

sub outer_join {
  my $self = shift;
  my ( $table, $predicate ) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->outer_join,
    args    => [ $table, $predicate ],
  );

  return $q;
}

sub eq_join {
  my $self = shift;
  my ( $left, $table, $optargs ) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->eq_join,
    args    => [ $left, $table ],
    optargs => $optargs,
  );

  return $q;
}

sub zip {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, type => $self->termType->zip, );

  return $q;
}

# TRANSFORMATIONS

sub map {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->map,
    args    => Rethinkdb::Util->wrap_func($args),
  );

  return $q;
}

sub with_fields {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->with_fields,
    args    => $args,
  );

  return $q;
}

sub concat_map {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->concatmap,
    args    => $args,
  );

  return $q;
}

sub order_by {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->orderby,
    args    => $args,
  );

  return $q;
}

sub skip {
  my $self   = shift;
  my $number = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->skip,
    args    => $number,
  );

  return $q;
}

sub limit {
  my $self   = shift;
  my $number = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->limit,
    args    => $number,
  );

  return $q;
}

sub slice {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->slice,
    args    => $args,
  );

  return $q;
}

sub nth {
  my $self   = shift;
  my $number = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->nth,
    args    => $number,
  );

  return $q;
}

sub indexes_of {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->indexes_of,
    args    => Rethinkdb::Util->wrap_func($args),
  );

  return $q;
}

sub is_empty {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->is_empty,
  );

  return $q;
}

sub union {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->union,
    args    => $args,
  );

  return $q;
}

sub sample {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->sample,
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
    type    => $self->termType->group,
    args    => $args
  );

  return $q;
}

sub ungroup {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->ungroup,
  );

  return $q;
}

sub reduce {
  my $self     = shift;
  my $function = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->reduce,
    args    => $function,
  );

  return $q;
}

sub count {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->count,
    args    => $args
  );

  return $q;
}

sub sum {
  my $self = shift;
  my $args = {@_};

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->sum,
    args    => $args
  );

  return $q;
}

sub avg {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->avg,
    args    => $args
  );

  return $q;
}

sub min {
  my $self = shift;
  my $args = {@_};

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->min,
    args    => $args
  );

  return $q;
}

sub max {
  my $self = shift;
  my $args = {@_};

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->max,
    args    => $args
  );

  return $q;
}

sub distinct {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->distinct,
  );

  return $q;
}

sub contains {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->contains,
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
    type    => $self->termType->pluck,
    args    => $args
  );

  return $q;
}

sub without {
  my $self = shift;
  my $args = @_ ? @_ > 1 ? [@_] : [ @{ $_[0] } ] : [];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->without,
    args    => $args
  );

  return $q;
}

sub merge {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->merge,
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
    type    => $self->termType->append,
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
    type    => $self->termType->prepend,
    args    => $args
  );

  return $q;
}

sub difference {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->difference,
    args    => [$args],
  );

  return $q;
}

sub set_insert {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->set_insert,
    args    => $args,
  );

  return $q;
}

sub set_union {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->set_union,
    args    => [$args],
  );

  return $q;
}

sub set_intersection {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->set_intersection,
    args    => [$args],
  );

  return $q;
}

sub set_difference {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->set_difference,
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
    type    => $self->termType->get_field,
    args    => $args
  );

  return $q;
}

sub has_fields {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->has_fields,
    args    => $args
  );

  return $q;
}

sub insert_at {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->insert_at,
    args    => $args,
  );

  return $q;
}

sub splice_at {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->splice_at,
    args    => $args,
  );

  return $q;
}

sub delete_at {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->delete_at,
    args    => $args,
  );

  return $q;
}

sub change_at {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->change_at,
    args    => $args,
  );

  return $q;
}

sub keys {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->keys,
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
    type    => $self->termType->match,
    args    => $expr
  );

  return $q;
}

sub split {
  my $self = shift;
  my ($expr) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->split,
    args    => $expr
  );

  return $q;
}

sub upcase {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->upcase,
  );

  return $q;
}

sub downcase {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->downcase,
  );

  return $q;
}

# MATH AND LOGIC




sub add {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->add,
    args    => [$args],
  );

  return $q;
}

sub sub {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->sub,
    args    => $args,
  );

  return $q;
}

sub mul {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->mul,
    args    => $args,
  );

  return $q;
}

sub div {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->div,
    args    => $args,
  );

  return $q;
}

sub mod {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->mod,
    args    => $args,
  );

  return $q;
}

sub and {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->all,
    args    => $args,
  );

  return $q;
}

sub or {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->any,
    args    => $args,
  );

  return $q;
}

sub eq {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->eq,
    args    => $args,
  );

  return $q;
}

sub ne {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->ne,
    args    => $args,
  );

  return $q;
}

sub gt {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->gt,
    args    => $args,
  );

  return $q;
}

sub ge {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->ge,
    args    => $args,
  );

  return $q;
}

sub lt {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->lt,
    args    => $args,
  );

  return $q;
}

sub le {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->le,
    args    => $args,
  );

  return $q;
}

sub not {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, type => $self->termType->not, );

  return $q;
}

# DATES AND TIMES

sub in_timezone {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->in_timezone,
    args    => $args,
  );

  return $q;
}

sub timezone {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->timezone,
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
    type    => $self->termType->during,
    args    => [ $start, $end ],
    optargs => $optargs,
  );

  return $q;
}

sub date {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, type => $self->termType->date,
    );

  return $q;
}

sub time_of_day {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->time_of_day,
  );

  return $q;
}

sub year {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, type => $self->termType->year,
    );

  return $q;
}

sub month {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, type => $self->termType->month,
    );

  return $q;
}

sub day {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, type => $self->termType->day, );

  return $q;
}

sub day_of_week {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->day_of_week,
  );

  return $q;
}

sub day_of_year {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->day_of_year,
  );

  return $q;
}

sub hours {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, type => $self->termType->hours,
    );

  return $q;
}

sub minutes {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->minutes,
  );

  return $q;
}

sub seconds {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->seconds,
  );

  return $q;
}

sub to_iso8601 {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->to_iso8601,
  );

  return $q;
}

sub to_epoch_time {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->to_epoch_time,
  );

  return $q;
}

# CONTROL STRUCTURES

# TODO: figure out why the arguments have to be reversed here
sub do {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(

    # _parent => $self,
    rdb  => $self->rdb,
    type => $self->termType->funcall,
    args => [ $args, $self ],
  );

  return $q;
}

sub for_each {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->foreach,
    args    => $args,
  );

  return $q;
}

sub default {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->default,
    args    => $args,
  );

  return $q;
}

sub coerce_to {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->coerce_to,
    args    => $args,
  );

  return $q;
}

sub type_of {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => $self->termType->typeof,
  );

  return $q;
}

sub info {
  my $self = shift;

  my $q
    = Rethinkdb::Query->new( _parent => $self, type => $self->termType->info,
    );

  return $q;
}

1;

=head1 METHODS

blah blah

=head2 count

  r->table('marvel')->count->add(r->table('dc')->count)->run;

  r->table('marvel')->concat_map(
    sub {
      my $row = shift;
      $row->attr('dc_buddies');
    }
  )->count('Batman')->run;

  r->table('marvel')->count(
    sub {
      my $hero = shift;
      $hero->attr('dc_buddies')->contains('Batman');
    }
  )->run;

Count the number of elements in the sequence. With a single argument, count
the number of elements equal to it. If the argument is a function, it is
equivalent to calling filter before count.
