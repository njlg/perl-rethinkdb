use Test::More;

use Rethinkdb;

# setup
r->connect->repl;
r->db('test')->drop->run;
r->db('test')->create->run;

#
# db class methods for table
#

# db->table_create(table_name[, primary_key=None, primary_datacenter=None, cache_size=None])
isa_ok r->db('test')->table_create('dcuniverse'), 'Rethinkdb::Query', 'Correct class';
my $res = r->db('test')->table_create('dcuniverse')->run;
isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->type, 1, 'Correct status code';

# db->table_list
isa_ok r->db('test')->table_list, 'Rethinkdb::Query', 'Correct class';
$res = r->db('test')->table_list->run;
isa_ok $res, 'Rethinkdb::Response', 'Correct class';
ok grep { /dcuniverse/ } @{$res->response}, 'Table was listed';

# db->table_drop
isa_ok r->db('test')->table_drop('dcuniverse'), 'Rethinkdb::Query', 'Correct class';
$res = r->db('test')->table_drop('dcuniverse')->run;
isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->type, 1, 'Correct status code';

# check table_list to make sure table_drop worked
$res = r->db('test')->table_list->run;
ok !grep { /dcuniverse/ } @{$res->response}, 'Table was listed';

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

isa_ok r->db('test')->table('dcuniverse'), 'Rethinkdb::Table', 'Correct class';
isa_ok r->db('test')->table('dcuniverse')->rdb, 'Rethinkdb', 'Correctly has reference';

# create
isa_ok r->db('test')->table('dcuniverse')->create, 'Rethinkdb::Query', 'Correct class';
$res = r->db('test')->table('dcuniverse')->create->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->type, 1, 'Correct status code';

# list
r->db('test')->table->list, 'Rethinkdb::Query', 'Correct class';
$res = r->db('test')->table->list->run;
isa_ok $res, 'Rethinkdb::Response', 'Correct class';
ok grep { /dcuniverse/ } @{$res->response}, 'Table was listed';

# drop
isa_ok r->db('test')->table('dcuniverse')->drop, 'Rethinkdb::Query', 'Correct class';
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

done_testing();