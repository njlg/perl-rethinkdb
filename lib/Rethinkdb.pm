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

our $VERSION = '0.03';

# this is set only when connect->repl()
has 'io';
has 'term' => sub { Rethinkdb::Protocol->new->term; };

sub import {
  my $class   = shift;
  my $package = caller;

  no strict;
  *{"$package\::r"} = \&r;
}

sub r {
  my $package = caller;
  my $self;

  if ($package::_rdb_io) {
    $self = __PACKAGE__->new( io => $package::_rdb_io );
    $self->io->rdb($self);
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
    rdb      => $self,
    host     => $host,
    port     => $port,
    db       => $db,
    auth_key => $auth_key,
    timeout  => $timeout
  );

  weaken $io->{rdb};
  return $io->connect;
}

sub db_create {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => $self->term->termType->db_create,
    args => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub db_drop {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => $self->term->termType->db_drop,
    args => $args
  );

  weaken $q->{rdb};
  return $q;
}

sub db_list {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => $self->term->termType->db_list,
  );

  weaken $q->{rdb};
  return $q;
}

sub db {
  my $self = shift;
  my $name = shift;

  my $db = Rethinkdb::Query::Database->new(
    rdb  => $self,
    type => $self->term->termType->db,
    name => $name,
    args => $name,
  );

  weaken $db->{rdb};
  return $db;
}

sub table_create {
  my $self    = shift;
  my $args    = shift;
  my $optargs = ref $_[0] ? $_[0] : {@_};

  my $q = Rethinkdb::Query->new(
    rdb     => $self,
    type    => $self->term->termType->table_create,
    args    => $args,
    optargs => $optargs,
  );

  weaken $q->{rdb};
  return $q;
}

sub table_drop {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => $self->term->termType->table_drop,
    args => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub table_list {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => $self->term->termType->table_list,
  );

  weaken $q->{rdb};
  return $q;
}

sub table {
  my $self     = shift;
  my $name     = shift;
  my $outdated = shift;

  my $optargs = {};
  if ($outdated) {
    $optargs = { use_outdated => 1 };
  }

  my $t = Rethinkdb::Query::Table->new(
    rdb     => $self,
    type    => $self->term->termType->table,
    name    => $name,
    args    => $name,
    optargs => $optargs,
  );

  weaken $t->{rdb};
  return $t;
}

sub row {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => $self->term->termType->implicit_var,
  );

  weaken $q->{rdb};
  return $q;
}

sub asc {
  my $self = shift;
  my $name = shift;

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->asc,
    args => $name,
  );

  return $q;
}

sub desc {
  my $self = shift;
  my $name = shift;

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->desc,
    args => $name,
  );

  return $q;
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
    rdb     => $self,
    type    => $self->term->termType->javascript,
    args    => $args,
    optargs => $optargs,
  );

  weaken $q->{rdb};
  return $q;
}

sub expr {
  my $self  = shift;
  my $value = shift;

  return Rethinkdb::Util->expr($value);
}

sub json {
  my $self  = shift;
  my $value = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => $self->term->termType->json,
    args => $value,
  );

  weaken $q->{rdb};
  return $q;
}

# TODO: figure out why the arguments have to be reversed here
sub do {
  my $self = shift;
  my ( $one, $two ) = @_;

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => $self->term->termType->funcall,
    args => [ $two, $one ],
  );

  weaken $q->{rdb};
  return $q;
}

sub branch {
  my $self = shift;
  my ( $predicate, $true, $false ) = @_;

  # $predicate = Rethinkdb::Util->wrap_func($predicate);
  # $true      = Rethinkdb::Util->wrap_func($true);
  # $false     = Rethinkdb::Util->wrap_func($false);

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => $self->term->termType->branch,
    args => [ $predicate, $true, $false ],
  );

  weaken $q->{rdb};
  return $q;
}

sub error {
  my $self = shift;
  my ($message) = @_;

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->error,
    args => $message,
  );

  return $q;
}

sub now {
  my $self = shift;

  my $q = Rethinkdb::Query->new( type => $self->term->termType->now, );

  return $q;
}

sub time {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->time,
    args => $args
  );

  return $q;
}

sub epoch_time {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->epoch_time,
    args => $args
  );

  return $q;
}

sub iso8601 {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->iso8601,
    args => $args
  );

  return $q;
}

sub monday {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->monday,
    args => $args
  );

  return $q;
}

sub tuesday {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->tuesday,
    args => $args
  );

  return $q;
}

sub wednesday {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->wednesday,
    args => $args
  );

  return $q;
}

sub thursday {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->thursday,
    args => $args
  );

  return $q;
}

sub friday {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->friday,
    args => $args
  );

  return $q;
}

sub saturday {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->saturday,
    args => $args
  );

  return $q;
}

sub sunday {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->sunday,
    args => $args
  );

  return $q;
}

sub january {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->january,
    args => $args
  );

  return $q;
}

sub february {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->february,
    args => $args
  );

  return $q;
}

sub march {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->march,
    args => $args
  );

  return $q;
}

sub april {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->april,
    args => $args
  );

  return $q;
}

sub may {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new( type => $self->term->termType->may,
    args => $args );

  return $q;
}

sub june {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->june,
    args => $args
  );

  return $q;
}

sub july {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->july,
    args => $args
  );

  return $q;
}

sub august {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->august,
    args => $args
  );

  return $q;
}

sub september {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->september,
    args => $args
  );

  return $q;
}

sub october {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->october,
    args => $args
  );

  return $q;
}

sub november {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->november,
    args => $args
  );

  return $q;
}

sub december {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => $self->term->termType->december,
    args => $args
  );

  return $q;
}


sub true  { Rethinkdb::_True->new; }
sub false { Rethinkdb::_False->new; }

package Rethinkdb::_True;

use overload
  '""'   => sub {'true'},
  'bool' => sub {1},
  'eq'   => sub { $_[1] eq 'true' ? 1 : 0; },
  '=='   => sub { $_[1] == 1 ? 1 : 0; },
  fallback => 1;

sub new { bless {}, $_[0] }

package Rethinkdb::_False;

use overload
  '""'   => sub {'false'},
  'bool' => sub {0},
  'eq'   => sub { $_[1] eq 'false' ? 1 : 0; },
  '=='   => sub { $_[1] == 0 ? 1 : 0; },
  fallback => 1;

sub new { bless {}, $_[0] }

1;

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
connection will be set via L<io> in the new instance.

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

Drop a database. The database, all its tables, and corresponding data will be deleted.

=head2 db_list

  r->db_list->run;

List all database names in the system.

=head2 db

  r->db('irl')->table('marvel')->run;

Reference a database.

=head2 table_create

  r->table_create('marvel')->run;
  r->table_create('marvel', {
    primary_key => 'superhero',
    durability  => 'soft',
    cache_size  => 512,
    datacenter  => 'obscurus',
  })->run;

Create a table. A RethinkDB table is a collection of JSON documents.

The second argument contains optional arguments:

=over 4

=item primary_key (string)

The name of the primary key. The default primary key is id.

=item durability (string)

If set to 'soft', this enables soft durability on this table: writes will be acknowledged by the server immediately and flushed to disk in the background. Default is 'hard' (acknowledgement of writes happens after data has been written to disk).

=item cache_size (number)

Set the cache size (in MB) to be used by the table. Default is 1024MB.

=item datacenter (string)

The name of the datacenter this table should be assigned to.

=back

=head2 table_drop

  r->table_drop('marvel')->run;

Drop a table. The table and all its data will be deleted.

=head2 table_list

  r->table_list->run;

List all table names in a database.

=head2 table

  r->table('marvel')->run;
  r->table('marvel', 1)->run;
  r->table('marvel')->get('Iron Man')->run;
  r->table('marvel', r->true)->get('Iron Man')->run;

Select all documents in a table. This command can be chained with other
commands to do further processing on the data.

The second argument specifies whether using outdated results is okay or not.
By default the results will be accurate.

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

=head2 asc

  r->table('marvel')->order_by(r->asc('enemies_vanquished'))->run;

Specifies that a column should be ordered in ascending order.

=head2 desc

  r->table('marvel')->order_by(r->desc('enemies_vanquished'))->run;

Specifies that a column should be ordered in descending order.

=head2 js

  r->js("'str1' + 'str2'")->run($conn);
  r->table('marvel')->filter(
    r->js('(function (row) { return row.age > 90; })')
  )->run($conn);
  r->js('while(true) {}', 1.3)->run($conn);

Create a javascript expression.

=head2 expr

  r->expr({a => 'b'})->merge({b => [1,2,3]})->run($conn);

Construct a RQL JSON object from a native object.

=head2 json

  r->json("[1,2,3]")->run($conn);

Parse a JSON string on the server.

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

=head2 now

=head2 time

=head2 epoch_time

=head2 iso8601

=head2 monday

=head2 tuesday

=head2 wednesday

=head2 thursday

=head2 friday

=head2 saturday

=head2 sunday

=head2 january

=head2 february

=head2 march

=head2 april

=head2 may

=head2 june

=head2 july

=head2 august

=head2 september

=head2 october

=head2 november

=head2 december

=head2 true

=head2 false

=head1 AUTHOR

Nathan Levin-Greenhaw, C<njlg@cpan.org>.

=head1 COPYRIGHT AND LICENSE

Unless otherwise noted:

Copyright (C) 2013, Nathan Levin-Greenhaw

A lot of the above documentation above was taken from the L<official documentation|http://rethinkdb.com/api/>.
Copyright (C) 2010-2013 RethinkDB.

This program is free software, you can redistribute it and/or modify it under
the terms of the Artistic License version 2.0.

=cut
