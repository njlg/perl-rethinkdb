use Test::More;

use Rethinkdb;

# setup
r->connect->repl;
r->db('test')->drop->run;
r->db('test')->create->run;
r->db('test')->table('marvel')->create(primary_key => 'superhero')->run;
r->table('marvel')->insert([
  { user_id => 1, superhero => 'Iron Man', superpower => 'Arc Reactor', active => 1, age => 35 },
  { user_id => 8, superhero => 'Wolverine', superpower => 'Adamantium', active => 0, age => 35 },
  { user_id => 9, superhero => 'Spiderman', superpower => 'Spidy Sense', active => 0, age => 20 }
])->run;

# two ways to do the same thing:
my $res = r->db('test')->table('marvel')->run;
my $res2 = r->table('marvel')->run;
# everything should be the same but the tokens
$res2->token($res->token);
is_deeply $res, $res2;


# fetch (possibly) out-dated results
r->table('marvel')->insert([
  { user_id => 2, superhero => 'Hulk', superpower => 'Smash', active => 1, age => 35 },
  { user_id => 3, superhero => 'Captain America', superpower => 'Super Strength', active => 1, age => 135 },
  { user_id => 4, superhero => 'Thor', superpower => 'God-like powers', active => 1, age => 1035 },
  { user_id => 5, superhero => 'Hawk-eye', superpower => 'Bow-n-arrow', active => 0, age => 35 },
  { user_id => 6, superhero => 'Wasp', superpower => 'Bio-lasers', active => 0, age => 35 },
  { user_id => 7, superhero => 'Ant-Man', superpower => 'Size', active => 1, age => 35 },
])->run;

# TODO: how to really test for this
$res = r->db('test')->table('marvel', 1)->run;
$res2 = r->db('test')->table('marvel', r->true)->run;
# everything should be the same but the tokens
$res2->token($res->token);
is_deeply $res, $res2;

# get Document
$res = r->table('marvel')->get('Spiderman')->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->type, 1, 'Correct status code';
is $res->response->{superhero}, 'Spiderman', 'Correct resposne';

# get all Documents with correct key
$res = r->table('marvel')->get_all('Size', 'Smash', index => 'superpower')->run;
use feature ':5.10';
use Data::Dumper;
$Data::Dumper::Indent = 1;
say Dumper $res;
exit;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->type, 1, 'Correct status code';
isa_ok $res->response, 'ARRAY', 'Correct resposne';
is scalar @{$res->response}, 1, 'Correct number of documents returned';
is $res->response->[0]->{superhero}, 'Spiderman', 'Correct documents returned';

# Select a couple items (should fail because we there is no ID key)
$res = r->table('marvel')->between(2, 7)->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->type, 103, 'Correct status code';
is $res->response, undef, 'Correct resposne';
like $res->error_message, qr/has no attribute id/, 'Correct error message';

# Select a couple items with correct key
$res = r->table('marvel')->between(2, 7, 'user_id')->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->type, 3, 'Correct status code';
isa_ok $res->response, 'ARRAY', 'Correct resposne type';
is scalar @{$res->response}, 6, 'Correct number of documents returned';

# Filter results
$res = r->table('marvel')->filter({active => 1})->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->type, 3, 'Correct status code';
isa_ok $res->response, 'ARRAY', 'Correct resposne type';
is scalar @{$res->response}, 5, 'Correct number of documents returned';

# Filter on multiple attributes
$res = r->table('marvel')->filter({active => 1, age => 35, superpower => 'Size'})->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->type, 3, 'Correct status code';
isa_ok $res->response, 'ARRAY', 'Correct resposne type';
is scalar @{$res->response}, 1, 'Correct number of documents returned';
is $res->response->[0]->{superhero}, 'Ant-Man', 'Correct document returned';

# TODO: implement EXPR predicate
#$res = r->table('marvel')->filter(r->row('age')->gt(25))->run;

# TODO: implement CODE predicate
eval {
  r->table('marvel')->filter(sub {
    my $hero = shift;
    return $hero->{abilities}->contains('super-strength');
  })->run;
};
like $@, qr/Unsupported predicated passed to filter/;


# clean up
r->db('test')->drop->run;

done_testing();