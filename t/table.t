use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use Rethinkdb;

# setup
r->connect->repl;
r->db('test')->drop->run;
r->db('test')->create->run;
r->db('test')->table_create('geo')->run;

#
# db class methods for table
#

# db->table_create(table_name[, primary_key=None, primary_datacenter=None, cache_size=None])
isa_ok r->db('test')->table_create('dcuniverse'), 'Rethinkdb::Query',
  'Correct class';
my $res = r->db('test')->table_create('dcuniverse')->run;
isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->type, 1, 'Correct status code';

# db->table_list
isa_ok r->db('test')->table_list, 'Rethinkdb::Query', 'Correct class';
$res = r->db('test')->table_list->run;
isa_ok $res, 'Rethinkdb::Response', 'Correct class';
ok grep {/dcuniverse/} @{ $res->response }, 'Table was listed';

# db->table_drop
isa_ok r->db('test')->table_drop('dcuniverse'), 'Rethinkdb::Query',
  'Correct class';
$res = r->db('test')->table_drop('dcuniverse')->run;
isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->type, 1, 'Correct status code';

# check table_list to make sure table_drop worked
$res = r->db('test')->table_list->run;
ok !grep {/dcuniverse/} @{ $res->response }, 'Table was listed';

TODO: {
  local $TODO = 'Need to write tests for table parameters';

# r->db('test')->table_create('dcuniverse', { primary_key => 'name' })->run;
# r->db('test')->table_create('dcuniverse', { primary_key => 'name', primary_datacenter => '' })->run;
# r->db('test')->table_create('dcuniverse', { primary_key => 'name', cache_size => 500 })->run;
# r->db('test')->table_create('dcuniverse', { primary_key => 'name', durability => 'soft' })->run;
}

#
# table class methods
#
isa_ok r->db('test')->table('dcuniverse'), 'Rethinkdb::Query::Table',
  'Correct class';
isa_ok r->db('test')->table('dcuniverse')->_rdb, 'Rethinkdb',
  'Correctly has reference';

# create table
isa_ok r->db('test')->table('dcuniverse')->create, 'Rethinkdb::Query',
  'Correct class';
$res = r->db('test')->table('dcuniverse')->create->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->type, 1, 'Correct status code';

# create a simple secondary index
$res = r->db('test')->table('dcuniverse')->index_create('alias')->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->response->{created}, 1, 'Index was created';

# list secondary index
$res = r->db('test')->table('dcuniverse')->index_list->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is_deeply $res->response, ['alias'], 'Indexes were listed';

# rename index
$res = r->db('test')->table('dcuniverse')->index_rename( 'alias', 'pseudonym' )
  ->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->response->{renamed}, 1, 'Index was renamed';

# index_status - for one particular index
$res = r->db('test')->table('dcuniverse')->index_status('pseudonym')->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
isa_ok $res->response, 'ARRAY', 'Correct return type';
is scalar @{ $res->response }, 1, 'Correct return type';

# index_status - for all indexes on table
$res = r->db('test')->table('dcuniverse')->index_status->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
isa_ok $res->response, 'ARRAY', 'Correct return type';
is scalar @{ $res->response }, 1, 'Correct return type';

# index_wait - for one particular index
$res = r->db('test')->table('dcuniverse')->index_wait('pseudonym')->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
isa_ok $res->response, 'ARRAY', 'Correct return type';
is scalar @{ $res->response }, 1, 'Correct return type';

# index_wait - for all indexes on table
$res = r->db('test')->table('dcuniverse')->index_wait->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
isa_ok $res->response, 'ARRAY', 'Correct return type';
is scalar @{ $res->response }, 1, 'Correct return type';

# drop secondary index
$res = r->db('test')->table('dcuniverse')->index_drop('pseudonym')->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->response->{dropped}, 1, 'Index was dropped';

# create a simple index based on the field
$res
  = r->db('test')->table('dcuniverse')
  ->index_create( 'alias_name', r->row->bracket('alias')->bracket('name') )
  ->run;

is $res->response->{created}, 1, 'Index was created';

# create a geospatial index
$res = r->db('test')->table('dcuniverse')
  ->index_create( 'last_seen', { geo => r->true } )->run;

is $res->response->{created}, 1, 'Index was created';

# create a compound index based on the fields post_id and date
$res
  = r->db('test')->table('dcuniverse')
  ->index_create( 'location_and_date',
  [ r->row->bracket('location'), r->row->bracket('date') ] )->run;

is $res->response->{created}, 1, 'Index was created';

# create a multi index
$res = r->db('test')->table('dcuniverse')
  ->index_create( 'friends', { multi => r->true } )->run;

is $res->response->{created}, 1, 'Index was created';

# create a geospatial multi index based on the field
$res = r->db('test')->table('dcuniverse')
  ->index_create( 'cities', { multi => r->true, geo => r->true } )->run;

is $res->response->{created}, 1, 'Index was created';

# create an index based on an arbitrary expression
$res = r->table('dcuniverse')->index_create(
  'authors',
  sub {
    my $doc = shift;
    return r->branch(
      $doc->has_fields('updated_at'),
      $doc->bracket('updated_at'),
      $doc->bracket('created_at')
    );
  }
)->run;

is $res->response->{created}, 1, 'Index was created';

# create a new secondary index based on an existing one
$res = r->table('dcuniverse')->index_status('authors')->nth(0)
  ->bracket('function')->run;
$res = r->table('dcuniverse')->index_create( 'authors_bkup', $res->response )
  ->run;

is $res->response->{created}, 1, 'Index was created';

# changes
TODO: {
  local $TODO = 'Need to write tests for table changes';

  # r->table('dcuniverse')->changes->run(sub {
  #   my $res = shift;
  # });
}

# sync
$res = r->table('dcuniverse')->sync->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->response->{synced}, 1, 'Index was dropped';

# drop table
isa_ok r->db('test')->table('dcuniverse')->drop, 'Rethinkdb::Query',
  'Correct class';
$res = r->db('test')->table('dcuniverse')->drop->run;
isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->type, 1, 'Correct status code';

TODO: {
  local $TODO = 'Need to write tests for table parameters';

# r->db('test')->table('dcuniverse', { primary_key => 'name' })->create->run;
# r->db('test')->table('dcuniverse', { primary_key => 'name', primary_datacenter => '' })->create->run;
# r->db('test')->table('dcuniverse', { primary_key => 'name', cache_size => 500 })->create->run;
# r->db('test')->table('dcuniverse', { primary_key => 'name', durability => 'soft' })->create->run;
}

# clean up
r->db('test')->drop->run;

done_testing();
