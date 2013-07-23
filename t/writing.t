use Test::More;

use Rethinkdb;

# setup
r->connect;
r->db('test')->drop->run;
r->db('test')->create->run;
r->db('test')->table('marvel')->create(primary_key => 'superhero')->run;

my $res = r->table('marvel')->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->type, 2, 'Correct status code';
is scalar @{$res->response}, 0, 'Correctly shows table empty';

# insert one entry
$res = r->table('marvel')->insert({ superhero => 'Iron Man', superpower => 'Arc Reactor' })->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->type, 1, 'Correct status code';
isa_ok $res->response, 'HASH', 'Response has correct type';
is $res->response->{inserted}, 1, 'Correct number of inserted';
# these are only set if the object we inserted did not have a primary_key value:
# is scalar @{$res->response->{generated_keys}}, 1, 'Response has correct number of keys';

# list table entries just to double-check
$res = r->db('test')->table('marvel')->run;

is scalar @{$res->response}, 1, 'Table contains correct number of entries';
is $res->response->[0]->{superhero}, 'Iron Man', 'Table contains correct first entry';

# insert multiple entries
$res = r->table('marvel')->insert([
  { superhero => 'Wolverine', superpower => 'Adamantium' },
  { superhero => 'Spiderman', superpower => 'Spidy Sense' }
])->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->type, 1, 'Correct status code';
isa_ok $res->response, 'HASH', 'Response has correct type';
is $res->response->{inserted}, 2, 'Correct number of inserted';
# these are only set if the object we inserted idd not have a primary_key value:
# is scalar @{$res->response->{generated_keys}}, 2, 'Correct number of generated keys';

# list table entries just to double-check
$res = r->db('test')->table('marvel')->run;

is scalar @{$res->response}, 3, 'Table contains correct number of entries';
# should we check all the names?

# insert an entry with an existing primary_key should fail
$res = r->table('marvel')->insert({ superhero => 'Iron Man', superpower => 'Arc Reactor' })->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->response->{errors}, 1, 'Correct number of errors';
is $res->response->{inserted}, 0, 'Correct number of inserts';

$res = r->table('marvel')->insert([
  { superhero => 'Iron Man', superpower => 'Arc Reactor' },
  { superhero => 'Wolverine', superpower => 'Adamantium' },
  { superhero => 'Spiderman', superpower => 'Spidy Sense' }
])->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->response->{errors}, 3, 'Correct number of errors';
is $res->response->{inserted}, 0, 'Correct number of inserts';

# forcing an insert should work tho
$res = r->table('marvel')->insert(
  { superhero => 'Iron Man', superpower => 'Mach 5' },
  { upsert => 1 }
)->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->response->{errors}, 0, 'Correct number of errors';
is $res->response->{inserted}, 0, 'Correct number of inserts';
is $res->response->{replaced}, 1, 'Correct number replaced';

# forcing an insert should work with "true" value too
$res = r->table('marvel')->insert(
  { superhero => 'Iron Man', superpower => 'Arc Reactor' },
  { upsert => r->true }
)->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->response->{replaced}, 1, 'Correct number replaced';

# Update
$res = r->table('marvel')->get('Iron Man', 'superhero')->update({ age => 30 })->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->response->{replaced}, 1, 'Correct number of updates';

# TODO:
# $res = r->table('marvel')->update({ age => r->row('age')->add(1) })->run;

# Replace / Modify
$res = r->table('marvel')->get('Iron Man', 'superhero')->replace({ superhero => 'Iron Man', age => 30 })->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->response->{replaced}, 1, 'Correct number of modified documents';

# Delete one document
$res = r->table('marvel')->get('Iron Man', 'superhero')->delete->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->response->{deleted}, 1, 'Correct number of deleted documents';

# Delete all the documents
$res = r->table('marvel')->delete->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->response->{deleted}, 2, 'Correct number of deleted documents';

# clean up
r->db('test')->drop->run;

done_testing();