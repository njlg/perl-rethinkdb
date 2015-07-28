package Rethinkdb::Query::Table;
use Rethinkdb::Base 'Rethinkdb::Query';

use Carp qw'croak carp';
use Scalar::Util 'weaken';

use Rethinkdb::Protocol;
use Rethinkdb::Util;

has [qw{ _rdb name }];

# primary_key = None
# datacenter = None
# durability = hard|soft
# cache_size = '1024MB'
sub create {
  my $self = shift;
  my $optargs = ref $_[0] ? $_[0] : {@_};

  my $q = Rethinkdb::Query->new(
    _rdb    => $self->_rdb,
    _type   => $self->_termType->table_create,
    args    => $self->name,
    optargs => $optargs,
  );

  weaken $q->{_rdb};
  return $q;
}

sub drop {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _rdb  => $self->_rdb,
    _type => $self->_termType->table_drop,
    args  => $self->name,
  );

  weaken $q->{_rdb};
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
    _type   => $self->_termType->index_create,
    args    => $index
  );

  return $q;
}

sub index_drop {
  my $self  = shift;
  my $index = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->index_drop,
    args    => $index
  );

  return $q;
}

sub index_list {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->index_list,
  );

  return $q;
}

sub index_rename {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->index_rename,
    args    => $args
  );

  return $q;
}

sub index_status {
  my $self    = shift;
  my $indices = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->index_status,
    args    => $indices,
  );

  return $q;
}

sub index_wait {
  my $self    = shift;
  my $indices = [@_];

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->index_wait,
    args    => $indices,
  );

  return $q;
}

sub changes {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->changes,
  );

  return $q;
}

sub insert {
  my $self   = shift;
  my $args   = shift;
  my $params = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->insert,
    args    => Rethinkdb::Util->_expr_json($args),
    optargs => $params,
  );

  return $q;
}

sub sync {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->sync,
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
    _type   => $self->_termType->get,
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
    _type   => $self->_termType->get_all,
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
    _type   => $self->_termType->between,
    args    => [ $lower, $upper ],
    optargs => $optargs,
  );

  return $q;
}

sub config {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->config,
  );

  return $q;
}

sub rebalance {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->rebalance,
  );

  return $q;
}

sub reconfigure {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->reconfigure,
    optargs => $args
  );

  return $q;
}

sub status {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->status,
  );

  return $q;
}

sub wait {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    _type   => $self->_termType->wait,
  );

  return $q;
}

1;

=encoding utf8

=head1 NAME

Rethinkdb::Query::Table - RethinkDB Query Table

=head1 SYNOPSIS

=head1 DESCRIPTION

L<Rethinkdb::Query::Table> is a type of query that represents a table in a
database. This classes contains methods to interact with said table.

=head1 ATTRIBUTES

L<Rethinkdb::Query::Table> implements the following attributes.

=head2 name

  my $table = r->db('comics')->table('superheros');
  say $table->name;

The name of the table.

=head1 METHODS

L<Rethinkdb::Query::Table> implements the following methods.

=head2 create

  r->db('test')->table('dc_universe')->create->run;

Create this table. A RethinkDB table is a collection of JSON documents.

If successful, the operation returns an object: C<< {created => 1} >>. If a
table with the same name already exists, the operation returns a
C<runtime_error>.

B<Note:> that you can only use alphanumeric characters and underscores for the
table name.

=head2 drop

  r->db('test')->table('dc_universe')->drop->run(conn)

Drop this table. The table and all its data will be deleted.

If successful, the operation returns an object: C<< {dropped => 1} >>. If the
specified table doesn't exist a C<runtime_error> is returned.

=head2 index_create

  r->table('comments')->index_create('post_id')->run;

Create a new secondary index on a table.

=head2 index_drop

  r->table('dc')->index_drop('code_name')->run;

Delete a previously created secondary index of this table.

=head2 index_list

  r->table('marvel')->index_list->run;

List all the secondary indexes of this table.

=head2 index_rename

  r->table('marvel')->index_rename('heroId', 'awesomeId')->run;

Rename an existing secondary index on a table. If the optional argument
C<overwrite> is specified as C<true>, a previously existing index with the new
name will be deleted and the index will be renamed. If C<overwrite> is C<false>
(the default) an error will be raised if the new index name already exists.

=head2 index_status

  r->table('test')->index_status->run;
  r->table('test')->index_status('timestamp')->run;

Get the status of the specified indexes on this table, or the status of all
indexes on this table if no indexes are specified.

=head2 index_wait

  r->table('test')->index_wait->run;
  r->table('test')->index_wait('timestamp')->run;

Wait for the specified indexes on this table to be ready, or for all indexes on
this table to be ready if no indexes are specified.

=head2 changes

  my $stream = r->table('games')->changes->run;
  foreach( @{$stream} ) {
    say Dumper $_;
  }

Return an infinite stream of objects representing changes to a table. Whenever
an C<insert>, C<delete>, C<update> or C<replace> is performed on the table, an
object of the form C<< {'old_val' => ..., 'new_val' => ...} >> will be appended
to the stream. For an C<insert>, C<old_val> will be C<null>, and for a
C<delete>, C<new_val> will be C<null>.

=head2 insert

  r->table('posts')->insert({
    id => 1,
    title => 'Lorem ipsum',
    content => 'Dolor sit amet'
  })->run;

Insert documents into a table. Accepts a single document or an array of
documents.

=head2 sync

L</sync> ensures that writes on a given table are written to permanent storage.
Queries that specify soft durability C<< {durability => 'soft'} >> do not give
such guarantees, so sync can be used to ensure the state of these queries. A
call to sync does not return until all previous writes to the table are
persisted.

=head2 get

  r->table('posts')->get('a9849eef-7176-4411-935b-79a6e3c56a74')->run;

Get a document by primary key.

If no document exists with that primary key, L</get> will return C<null>.

=head2 get_all

  r->table('marvel')->get_all('man_of_steel', { index => 'code_name' })->run;

Get all documents where the given value matches the value of the requested
index.

=head2 between

  r->table('marvel')->between(10, 20)->run;

Get all documents between two keys. Accepts three optional arguments: C<index>,
C<left_bound>, and C<right_bound>. If C<index> is set to the name of a
secondary index, L</between> will return all documents where that index's value
is in the specified range (it uses the primary key by default). C<left_bound>
or C<right_bound> may be set to open or closed to indicate whether or not to
include that endpoint of the range (by default, C<left_bound> is closed and
C<right_bound> is open).

=head2 config

  r->table('marvel')->config->run;

Query (read and/or update) the configurations for individual tables.

=head2 rebalance

  r->table('marvel')->rebalance->run;

Rebalances the shards of a table.

=head2 reconfigure

  r->table('marvel')->reconfigure({ shards => 2, replicas => 1 })->run;
  r->table('marvel')->reconfigure(
    {
      shards              => 2,
      replicas            => { wooster => 1, wayne => 1 },
      primary_replica_tag => 'wooster'
    }
  )->run;

Reconfigure a table's sharding and replication.

=head2 status

  r->table('marvel')->status->run;

Return the status of a table. The return value is an object providing
information about the table's shards, replicas and replica readiness states

=head2 wait

  r->table('marvel')->wait->run;

Wait for a table to be ready. A table may be temporarily unavailable
after creation, rebalancing or reconfiguring. The L</wait> command
blocks until the given table is fully up to date.

=cut
