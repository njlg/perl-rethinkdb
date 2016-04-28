package Rethinkdb;
use Rethinkdb::Base -base;

use Carp 'croak';
use Scalar::Util 'weaken';

use Rethinkdb::IO;
use Rethinkdb::Protocol;
use Rethinkdb::Query::Database;
use Rethinkdb::Query::Table;
use Rethinkdb::Query;
use Rethinkdb::Util;

our $VERSION = '0.12';

# this is set only when connect->repl()
has 'io';
has 'term' => sub { Rethinkdb::Protocol->new->term; };

sub import {
  my $class   = shift;
  my $package = caller;

  no strict;
  *{"$package\::r"} = \&r;

  return;
}

sub r {
  my $package = caller;
  my $self;

  if ($package::_rdb_io) {
    $self = __PACKAGE__->new( io => $package::_rdb_io );
    $self->io->_rdb($self);
  }
  else {
    $self = __PACKAGE__->new;
  }

  return $self;
}

sub connect {
  my $self     = shift;
  my $host     = shift || 'localhost';
  my $port     = shift || 28015;
  my $db       = shift || 'test';
  my $auth_key = shift || '';
  my $timeout  = shift || 20;

  my $io = Rethinkdb::IO->new(
    _rdb       => $self,
    host       => $host,
    port       => $port,
    default_db => $db,
    auth_key   => $auth_key,
    timeout    => $timeout
  );

  weaken $io->{_rdb};
  return $io->connect;
}

# DATABASES

sub db_create {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _rdb  => $self,
    _type => $self->term->termType->db_create,
    args  => $args,
  );

  weaken $q->{_rdb};
  return $q;
}

sub db_drop {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _rdb  => $self,
    _type => $self->term->termType->db_drop,
    args  => $args
  );

  weaken $q->{_rdb};
  return $q;
}

sub db_list {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _rdb  => $self,
    _type => $self->term->termType->db_list,
  );

  weaken $q->{_rdb};
  return $q;
}

sub db {
  my $self = shift;
  my $name = shift;

  my $db = Rethinkdb::Query::Database->new(
    _rdb  => $self,
    name  => $name,
    args  => $name,
  );

  weaken $db->{_rdb};
  return $db;
}

# TABLE

sub table {
  my $self     = shift;
  my $name     = shift;
  my $outdated = shift;

  my $optargs = {};
  if ($outdated) {
    $optargs = { use_outdated => 1 };
  }

  my $t = Rethinkdb::Query::Table->new(
    _rdb    => $self,
    _type   => $self->term->termType->table,
    name    => $name,
    args    => $name,
    optargs => $optargs,
  );

  weaken $t->{_rdb};
  return $t;
}

# DOCUMENT MANIPULATION

sub row {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _rdb  => $self,
    _type => $self->term->termType->implicit_var,
  );

  weaken $q->{_rdb};
  return $q;
}

sub literal {
  my $self = shift;
  my $args = ref $_[0] ? $_[0] : {@_};

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->literal,
    args  => $args,
  );

  return $q;
}

sub object {
  my $self = shift;
  my $args = ref $_[0] ? $_[0] : {@_};

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->object,
    args  => $args,
  );

  return $q;
}

# MATH AND LOGIC

sub and {
  my $self = shift;
  my $args = \@_;

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->and,
    args  => $args,
  );

  return $q;
}

sub or {
  my $self = shift;
  my $args = \@_;

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->or,
    args  => $args,
  );

  return $q;
}

sub random {
  my $self    = shift;
  my $args    = [@_];
  my $optargs = {};

  if ( ref $args->[2] eq 'HASH' ) {
    $optargs = $args->[2];
  }
  elsif ( scalar @{$args} > 2 and $args->[2] ) {
    $optargs->{float} = r->true;
  }

  # only keep the first two elements
  $args = [ splice @{$args}, 0, 2 ];

  my $q = Rethinkdb::Query->new(
    _type   => $self->term->termType->random,
    args    => $args,
    optargs => $optargs,
  );

  return $q;
}

# DATES AND TIMES

sub now {
  my $self = shift;

  my $q = Rethinkdb::Query->new( _type => $self->term->termType->now );

  return $q;
}

sub time {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->time,
    args  => $args
  );

  return $q;
}

sub epoch_time {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->epoch_time,
    args  => $args
  );

  return $q;
}

sub iso8601 {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->iso8601,
    args  => $args
  );

  return $q;
}

sub monday {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->monday,
    args  => $args
  );

  return $q;
}

sub tuesday {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->tuesday,
    args  => $args
  );

  return $q;
}

sub wednesday {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->wednesday,
    args  => $args
  );

  return $q;
}

sub thursday {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->thursday,
    args  => $args
  );

  return $q;
}

sub friday {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->friday,
    args  => $args
  );

  return $q;
}

sub saturday {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->saturday,
    args  => $args
  );

  return $q;
}

sub sunday {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->sunday,
    args  => $args
  );

  return $q;
}

sub january {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->january,
    args  => $args
  );

  return $q;
}

sub february {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->february,
    args  => $args
  );

  return $q;
}

sub march {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->march,
    args  => $args
  );

  return $q;
}

sub april {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->april,
    args  => $args
  );

  return $q;
}

sub may {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->may,
    args  => $args
  );

  return $q;
}

sub june {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->june,
    args  => $args
  );

  return $q;
}

sub july {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->july,
    args  => $args
  );

  return $q;
}

sub august {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->august,
    args  => $args
  );

  return $q;
}

sub september {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->september,
    args  => $args
  );

  return $q;
}

sub october {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->october,
    args  => $args
  );

  return $q;
}

sub november {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->november,
    args  => $args
  );

  return $q;
}

sub december {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->december,
    args  => $args
  );

  return $q;
}

# CONTROL STRUCTURES

sub args {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->args,
    args  => $args
  );

  return $q;
}

# TODO: figure out why the arguments have to be reversed here
sub do {
  my $self = shift;
  my ( $one, $two ) = @_;

  my $q = Rethinkdb::Query->new(
    _rdb  => $self,
    _type => $self->term->termType->funcall,
    args => [ $two, $one ],
  );

  weaken $q->{_rdb};
  return $q;
}

sub branch {
  my $self = shift;
  my ( $predicate, $true, $false ) = @_;

  my $q = Rethinkdb::Query->new(
    _rdb  => $self,
    _type => $self->term->termType->branch,
    args => [ $predicate, $true, $false ],
  );

  weaken $q->{_rdb};
  return $q;
}

sub error {
  my $self = shift;
  my ($message) = @_;

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->error,
    args  => $message,
  );

  return $q;
}

sub expr {
  my $self  = shift;
  my $value = shift;

  return Rethinkdb::Util->_expr($value);
}

sub js {
  my $self    = shift;
  my $args    = shift;
  my $timeout = shift;

  my $optargs = {};
  if ($timeout) {
    $optargs = { timeout => $timeout };
  }

  my $q = Rethinkdb::Query->new(
    _rdb    => $self,
    _type   => $self->term->termType->javascript,
    args    => $args,
    optargs => $optargs,
  );

  weaken $q->{_rdb};
  return $q;
}

sub json {
  my $self  = shift;
  my $value = shift;

  my $q = Rethinkdb::Query->new(
    _rdb  => $self,
    _type => $self->term->termType->json,
    args  => $value,
  );

  weaken $q->{_rdb};
  return $q;
}

sub http {
  my $self    = shift;
  my $value   = shift;
  my $optargs = shift;

  my $q = Rethinkdb::Query->new(
    _rdb    => $self,
    _type   => $self->term->termType->http,
    args    => $value,
    optargs => $optargs,
  );

  weaken $q->{_rdb};
  return $q;
}

sub uuid {
  my $self  = shift;
  my $value = shift;

  my $q = Rethinkdb::Query->new(
    _rdb  => $self,
    _type => $self->term->termType->uuid,
    args  => $value,
  );

  weaken $q->{_rdb};
  return $q;
}

# GEO

sub circle {
  my $self    = shift;
  my $point   = shift;
  my $radius  = shift;
  my $optargs = shift;

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->circle,
    args    => [ $point, $radius ],
    optargs => $optargs,
  );

  return $q;
}

sub distance {
  my $self    = shift;
  my $point1  = shift;
  my $point2  = shift;
  my $optargs = shift;

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->distance,
    args    => [ $point1, $point2 ],
    optargs => $optargs,
  );

  return $q;
}

sub geojson {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->geojson,
    args  => $args,
  );

  return $q;
}

sub line {
  my $self = shift;
  my $args = \@_;

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->line,
    args  => $args,
  );

  return $q;
}

sub point {
  my $self = shift;
  my $args = \@_;

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->point,
    args  => $args,
  );

  return $q;
}

sub polygon {
  my $self = shift;
  my $args = \@_;

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->polygon,
    args  => $args,
  );

  return $q;
}


# MISC

sub asc {
  my $self = shift;
  my $name = shift;

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->asc,
    args  => $name,
  );

  return $q;
}

sub desc {
  my $self = shift;
  my $name = shift;

  my $q = Rethinkdb::Query->new(
    _type => $self->term->termType->desc,
    args  => $name,
  );

  return $q;
}

sub wait {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _rdb  => $self,
    _type => $self->term->termType->wait,
  );

  return $q;
}

sub minval {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _rdb  => $self,
    _type => $self->term->termType->minval,
  );

  return $q;
}

sub maxval {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _rdb  => $self,
    _type => $self->term->termType->maxval,
  );

  return $q;
}

sub round {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _rdb  => $self,
    _type => $self->term->termType->round,
    args  => $args
  );

  return $q;
}

sub ceil {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _rdb  => $self,
    _type => $self->term->termType->ceil,
    args  => $args
  );

  return $q;
}

sub floor {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _rdb  => $self,
    _type => $self->term->termType->floor,
    args  => $args
  );

  return $q;
}

sub true  { return Rethinkdb::_True->new; }
sub false { return Rethinkdb::_False->new; }

package Rethinkdb::_True;

use overload
  '""'   => sub {'true'},
  'bool' => sub {1},
  'eq'   => sub { $_[1] eq 'true' ? 1 : 0; },
  '=='   => sub { $_[1] == 1 ? 1 : 0; },
  fallback => 1;

sub new { return bless {}, $_[0] }

package Rethinkdb::_False;

use overload
  '""'   => sub {'false'},
  'bool' => sub {0},
  'eq'   => sub { $_[1] eq 'false' ? 1 : 0; },
  '=='   => sub { $_[1] == 0 ? 1 : 0; },
  fallback => 1;

sub new { return bless {}, $_[0] }

1;

__END__

=encoding utf8

=head1 NAME

Rethinkdb - Pure Perl RethinkDB Driver

=head1 SYNOPSIS

  package MyApp;
  use Rethinkdb;

  r->connect->repl;
  r->table('agents')->get('007')->update(
    r->branch(
      r->row->attr('in_centrifuge'),
      {'expectation': 'death'},
      {}
    )
  )->run;

=head1 DESCRIPTION

Rethinkdb enables Perl programs to interact with RethinkDB in a easy-to-use
way. This particular driver is based on the official Python, Javascript, and
Ruby drivers.

To learn more about RethinkDB take a look at the L<official documentation|http://rethinkdb.com/api/>.

=head1 ATTRIBUTES

L<Rethinkdb> implements the following attributes.

=head2 io

  my $io = r->io;
  r->io(Rethinkdb::IO->new);

The C<io> attribute returns the current L<Rethinkdb::IO> instance that
L<Rethinkdb> is currently set to use. If C<io> is not set by the time C<run>
is called, then an error will occur.

=head2 term

  my $term = r->term;

The C<term> attribute returns an instance of the RethinkDB Query Langague
protocol.

=head1 METHODS

L<Rethinkdb> inherits all methods from L<Rethinkdb::Base> and implements the
following methods.

=head2 r

  my $r = r;
  my $conn = r->connect;

C<r> is a factory method to begin a new Rethink DB query. The C<r> sub is
exported in the importer's namespace so that it can be used as short-hand;
similar to what the official drivers provide. In addition, to creating a
new instance, if a L<Rethinkdb::IO> connection has been repl-ized, then that
connection will be set via C<io> in the new instance.

=head2 connect

  my $conn1 = r->connect;
  my $conn2 = r->connect('localhost', 28015, 'test', 'auth_key', 20);

Create a new connection to a RethinkDB shard. Creating a connection tries to
contact the RethinkDB shard immediately and will fail if the connection fails.

=head2 db_create

  r->db_create('test')->run;

Create a database. A RethinkDB database is a collection of tables, similar to
relational databases.

=head2 db_drop

  r->db_drop('test')->run;

Drop a database. The database, all its tables, and corresponding data will be
deleted.

=head2 db_list

  r->db_list->run;

List all database names in the system.

=head2 db

  r->db('irl')->table('marvel')->run;

Reference a database.

=head2 table

  r->table('marvel')->run;
  r->table('marvel', 1)->run;
  r->table('marvel')->get('Iron Man')->run;
  r->table('marvel', r->true)->get('Iron Man')->run;

Select all documents in a table. This command can be chained with other
commands to do further processing on the data.

=head2 row

  r->table('users')->filter(r->row->attr('age')->lt(5))->run;
  r->table('users')->filter(
    r->row->attr('embedded_doc')->attr('child')->gt(5)
  )->run;
  r->expr([1, 2, 3])->map(r->row->add(1))->run;
  r->table('users')->filter(sub {
    my $row = shift;
    $row->attr('name')->eq(r->table('prizes')->get('winner'));
  })->run;

Returns the currently visited document.

=head2 literal

  r->table('users')->get(1)->update({
    data: r->literal({ age => 19, job => 'Engineer' })
  })->run;

Replace an object in a field instead of merging it with an existing object in a
merge or update operation.

=head2 object

  r->object('id', 5, 'data', ['foo', 'bar'])->run;

Creates an object from a list of key-value pairs, where the keys must be
strings. C<r.object(A, B, C, D)> is equivalent to
C<r.expr([[A, B], [C, D]]).coerce_to('OBJECT')>.

=head2 and

  r->and(true, false)->run;

Compute the logical C<and> of two or more values.

=head2 or

  r->or(true, false)->run;

Compute the logical C<or> of two or more values.

=head2 random

  r->random()
  r->random(number[, number], {float => true})
  r->random(integer[, integer])

Generate a random number between given (or implied) bounds. C<random> takes
zero, one or two arguments.

=head2 now

  r->table("users")->insert({
    name => "John",
    subscription_date => r->now()
  })->run($conn);

Return a time object representing the current time in UTC. The command C<now()>
is computed once when the server receives the query, so multiple instances of
C<r.now()> will always return the same time inside a query.

=head2 time

  r->table('user')->get('John')->update({
    birthdate => r->time(1986, 11, 3, 'Z')
  })->run;

Create a time object for a specific time.

=head2 epoch_time

  r->table('user')->get('John')->update({
    "birthdate" => r->epoch_time(531360000)
  })->run;

Create a time object based on seconds since epoch. The first argument is a
double and will be rounded to three decimal places (millisecond-precision).

=head2 iso8601

  r->table('user')->get('John')->update({
    birth => r->iso8601('1986-11-03T08:30:00-07:00')
  })->run;

Create a time object based on an ISO 8601 date-time string
(e.g. '2013-01-01T01:01:01+00:00'). We support all valid ISO 8601 formats
except for week dates. If you pass an ISO 8601 date-time without a time zone,
you must specify the time zone with the default_timezone argument. Read more
about the ISO 8601 format at L<Wikipedia|http://en.wikipedia.org/wiki/ISO_8601>.

=head2 monday

  r->table('users')->filter(
    r->row('birthdate')->day_of_week()->eq(r->monday)
  )->run;

L</monday> is a literal day of the week for comparisions.

=head2 tuesday

  r->table('users')->filter(
    r->row('birthdate')->day_of_week()->eq(r->tuesday)
  )->run;

L</tuesday> is a literal day of the week for comparisions.

=head2 wednesday

  r->table('users')->filter(
    r->row('birthdate')->day_of_week()->eq(r->wednesday)
  )->run;

L</wednesday> is a literal day of the week for comparisions.

=head2 thursday

  r->table('users')->filter(
    r->row('birthdate')->day_of_week()->eq(r->thursday)
  )->run;

L</thursday> is a literal day of the week for comparisions.

=head2 friday

  r->table('users')->filter(
    r->row('birthdate')->day_of_week()->eq(r->friday)
  )->run;

L</friday> is a literal day of the week for comparisions.

=head2 saturday

  r->table('users')->filter(
    r->row('birthdate')->day_of_week()->eq(r->saturday)
  )->run;

L</saturday> is a literal day of the week for comparisions.

=head2 sunday

  r->table('users')->filter(
    r->row('birthdate')->day_of_week()->eq(r->sunday)
  )->run;

L</sunday> is a literal day of the week for comparisions.

=head2 january

  r->table('users')->filter(
    r->row('birthdate')->month()->eq(r->january)
  )->run;

L</january> is a literal month for comparisions.

=head2 february

  r->table('users')->filter(
    r->row('birthdate')->month()->eq(r->february)
  )->run;

L</february> is a literal month for comparisions.

=head2 march

  r->table('users')->filter(
    r->row('birthdate')->month()->eq(r->march)
  )->run;

L</march> is a literal month for comparisions.

=head2 april

  r->table('users')->filter(
    r->row('birthdate')->month()->eq(r->april)
  )->run;

L</april> is a literal month for comparisions.

=head2 may

  r->table('users')->filter(
    r->row('birthdate')->month()->eq(r->may)
  )->run;

L</may> is a literal month for comparisions.

=head2 june

  r->table('users')->filter(
    r->row('birthdate')->month()->eq(r->june)
  )->run;

L</june> is a literal month for comparisions.

=head2 july

  r->table('users')->filter(
    r->row('birthdate')->month()->eq(r->july)
  )->run;

L</july> is a literal month for comparisions.

=head2 august

  r->table('users')->filter(
    r->row('birthdate')->month()->eq(r->august)
  )->run;

L</august> is a literal month for comparisions.

=head2 september

  r->table('users')->filter(
    r->row('birthdate')->month()->eq(r->september)
  )->run;

L</september> is a literal month for comparisions.

=head2 october

  r->table('users')->filter(
    r->row('birthdate')->month()->eq(r->october)
  )->run;

L</october> is a literal month for comparisions.

=head2 november

  r->table('users')->filter(
    r->row('birthdate')->month()->eq(r->november)
  )->run;

L</november> is a literal month for comparisions.

=head2 december

  r->table('users')->filter(
    r->row('birthdate')->month()->eq(r->december)
  )->run;

L</december> is a literal month for comparisions.

=head2 args

  r->table('people')->get_all('Alice', 'Bob')->run;
  # or
  r->table('people')->get_all(r->args(['Alice', 'Bob']))->run;

C<< r->args >> is a special term that's used to splice an array of arguments into
another term. This is useful when you want to call a variadic term such as
L<Rethinkdb::Query::Table/"get_all"> with a set of arguments produced at runtime.

=head2 do

  r->do(r->table('marvel')->get('IronMan'), sub {
    my $ironman = shift;
    return $ironman->attr('name');
  })->run;

Evaluate the expr in the context of one or more value bindings. The type of
the result is the type of the value returned from expr.

=head2 branch

  r->table('marvel')->map(
    r->branch(
      r->row->attr('victories')->lt(100),
      r->row->attr('name')->add(' is a superhero'),
      r->row->attr('name')->add(' is a hero')
    )
  )->run;

Evaluate one of two control paths based on the value of an expression. C<branch>
is effectively an C<if> renamed due to language constraints. The type of the
result is determined by the type of the branch that gets executed.

=head2 error

  r->table('marvel')->get('IronMan')->do(sub {
    my $ironman = shift;
    r->branch(
      $ironman->attr('victories')->lt($ironman->attr('battles')),
      r->error('impossible code path'),
      $ironman
    );
  })->run;

Throw a runtime error. If called with no arguments inside the second argument
to default, re-throw the current error.

=head2 expr

  r->expr({a => 'b'})->merge({b => [1,2,3]})->run($conn);

Construct a RQL JSON object from a native object.

=head2 js

  r->js("'str1' + 'str2'")->run($conn);
  r->table('marvel')->filter(
    r->js('(function (row) { return row.age > 90; })')
  )->run($conn);
  r->js('while(true) {}', 1.3)->run($conn);

Create a javascript expression.

=head2 json

  r->json("[1,2,3]")->run($conn);

Parse a JSON string on the server.

=head2 http

  r->table('posts')->insert(r->http('httpbin.org/get'))->run;
  r->http('http://httpbin.org/post', {
    method => 'POST',
    data   => {
      player => 'Bob',
      game   => 'tic tac toe'
    }
  })->run($conn);

Retrieve data from the specified URL over HTTP. The return type depends on the
C<result_format> option, which checks the C<Content-Type> of the response by
default.

=head2 uuid

  r->uuid->run;

Return a UUID (universally unique identifier), a string that can be used as a
unique ID.

=head2 circle

  r->circle( [ -122.423246, 37.770378359 ], 10, { unit => 'mi' } )

Construct a circular line or polygon. A circle in RethinkDB is a polygon or
line approximating a circle of a given radius around a given center,
consisting of a specified number of vertices (default 32).

=head2 distance

  r->distance(
    r->point( -122.423246, 37.779388 ),
    r->point( -117.220406, 32.719464 ),
    { unit => 'km' }
  )->run;

Compute the distance between a point and another geometry object. At least
one of the geometry objects specified must be a point.

=head2 geojson

  r->geojson(
    { 'type' => 'Point', 'coordinates' => [ -122.423246, 37.779388 ] } )

Convert a L<GeoJSON|http://geojson.org/> object to a ReQL geometry object.

=head2 line

  r->line( [ -122.423246, 37.779388 ], [ -121.886420, 37.329898 ] )

Construct a geometry object of type Line. The line can be specified in one of
two ways:
(1) Two or more two-item arrays, specifying latitude and longitude numbers of
the line's vertices;
(2) Two or more L</point> objects specifying the line's vertices.

=head2 point

  r->point( -122.423246, 37.779388 )

Construct a geometry object of type Point. The point is specified by two
floating point numbers, the longitude (-180 to 180) and latitude (-90 to 90)
of the point on a perfect sphere.

=head2 polygon

  r->polygon(
    [ -122.423246, 37.779388 ],
    [ -122.423246, 37.329898 ],
    [ -121.886420, 37.329898 ],
    [ -121.886420, 37.779388 ]
  )

Construct a geometry object of type Polygon. The Polygon can be specified in
one of two ways:
(1) Three or more two-item arrays, specifying longitude and latitude numbers
of the polygon's vertices;
(2) Three or more L</point> objects specifying the polygon's vertices.

=head2 asc

  r->table('marvel')->order_by(r->asc('enemies_vanquished'))->run;

Specifies that a column should be ordered in ascending order.

=head2 desc

  r->table('marvel')->order_by(r->desc('enemies_vanquished'))->run;

Specifies that a column should be ordered in descending order.

=head2 wait

  r->wait->run;

Wait on all the tables in the default database (set with the L</connect>
command's C<db> parameter, which defaults to C<test>). A table may be
temporarily unavailable after creation, rebalancing or reconfiguring. The
L</wait> command blocks until the given all the tables in database is fully up
to date.

=head2 minval

  r->table('marvel')->between( r->minval, 7 )->run;

The special constants L</minval> is used for specifying a boundary, which
represent "less than any index key". For instance, if you use L</minval> as the
lower key, then L<Rethinkdb::Query::Table/between> will return all documents
whose primary keys (or indexes) are less than the specified upper key.

=head2 maxval

  r->table('marvel')->between( 8, r->maxval )->run;

The special constants L</maxval> is used for specifying a boundary, which
represent "greater than any index key". For instance, if you use L</maxval> as
the upper key, then L<Rethinkdb::Query::Table/between> will return all
documents whose primary keys (or indexes) are greater than the specified lower
key.

=head2 round

  r->round(-12.567)->run;

Rounds the given value to the nearest whole integer. For example, values of
1.0 up to but not including 1.5 will return 1.0, similar to L</floor>; values
of 1.5 up to 2.0 will return 2.0, similar to L</ceil>.

=head2 ceil

  r->ceil(-12.567)->run;

Rounds the given value up, returning the smallest integer value greater than
or equal to the given value (the value's ceiling).

=head2 floor

  r->floor(-12.567)->run;

Rounds the given value down, returning the largest integer value less than or
equal to the given value (the value's floor).

=head2 true

  r->true->run;

Helper literal since Perl does not have a C<true> literal.

=head2 false

  r->false->run;

Helper literal since Perl does not have a C<false> literal.

=head1 AUTHOR

Nathan Levin-Greenhaw, C<njlg@cpan.org>.

=head1 COPYRIGHT AND LICENSE

Unless otherwise noted:

Copyright (C) 2013-2014, Nathan Levin-Greenhaw

A lot of the above documentation above was taken from the
L<official documentation|http://rethinkdb.com/api/>.
Copyright (C) 2010-2014 RethinkDB.

This program is free software, you can redistribute it and/or modify it under
the terms of the Artistic License version 2.0.

=cut
