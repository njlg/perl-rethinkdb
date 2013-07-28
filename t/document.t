use Test::More;

use Rethinkdb;

# setup
r->connect;
r->db('test')->drop->run;
r->db('test')->create->run;
r->db('test')->table('marvel')->create(primary_key => 'superhero')->run;
r->table('marvel')->insert([
  { user_id => 1, superhero => 'Iron Man', superpower => 'Arc Reactor', active => 1, age => 35 },
  { user_id => 8, superhero => 'Wolverine', superpower => 'Adamantium', active => 0, age => 35 },
  { user_id => 9, superhero => 'Spiderman', superpower => 'Spidy Sense', active => 0, age => 20 },
  { user_id => 2, superhero => 'Hulk', superpower => 'Smash', active => 1, age => 35 },
  { user_id => 3, superhero => 'Captain America', superpower => 'Super Strength', active => 1, age => 135 },
  { user_id => 4, superhero => 'Thor', superpower => 'God-like powers', active => 1, age => 1035 },
  { user_id => 5, superhero => 'Hawk-eye', superpower => 'Bow-n-arrow', active => 0, age => 35 },
  { user_id => 6, superhero => 'Wasp', superpower => 'Bio-lasers', active => 0, age => 35 },
  { user_id => 7, superhero => 'Ant-Man', superpower => 'Size', active => 1, age => 35 },
])->run;
r->db('test')->table('loadouts')->create(primary_key => 'kit')->run;
r->table('loadouts')->insert({kit => 'alienInvasionKit', equipment => ['alienHelm', 'alienArmour', 'alienBoots']})->run;


# replace one document
my $res = r->table('marvel')->get('Iron Man')->replace({ superhero => 'Iron Man', age => 30 })->run;

isa_ok $res, 'Rethinkdb::Response';
is $res->type, 1, 'Correct status code';
is $res->response->{replaced}, 1, 'Correct number of updates';

# merge to documents
$res = r->table('marvel')->get('Iron Man')->merge(r->table('loadouts')->get('alienInvasionKit'))->run;

isa_ok $res, 'Rethinkdb::Response';
is $res->type, 1, 'Correct status code';
is_deeply [sort keys %{$res->response}], ['age', 'equipment', 'kit', 'superhero'], 'Correct merged document attribute';

# TODO: this should go in a sequence test
# get several documents
# $res = r->table('marvel')->get_all('Iron Man', 'Spiderman', 'Ant-Man', {index => 'superhero'})->run;


# check for an attribute (that doesn't exist)
$res = r->table('marvel')->get('Iron Man')->has_fields('active_status')->run;

isa_ok $res, 'Rethinkdb::Response';
is $res->type, 1, 'Correct status code';
is $res->response, 0, 'Correct response';

# check for an attribute (that does exist)
$res = r->table('marvel')->get('Iron Man')->has_fields('age')->run;

isa_ok $res, 'Rethinkdb::Response';
is $res->type, 1, 'Correct status code';
is $res->response, 1, 'Correct response';

# get one attribute value
$res = r->table('marvel')->get('Iron Man')->attr('age')->run;

isa_ok $res, 'Rethinkdb::Response';
is $res->type, 1, 'Correct status code';
is $res->response, 30, 'Correct response';

# prep for next test:
r->table('marvel')->get('Iron Man')->update({
  equipment => { oldBoots => 1, oldHelm => 1 },
  reactorState => 'medium',
  reactorPower => 4500,
  personalVictoriesList => [
    'Fing Fang Foom',
    'Iron Monger',
    'Mandarin',
  ],
})->run;

use feature ':5.10';
use Data::Dumper;
say 'before append';

# update one attribute
$res = r->table('marvel')->get('Iron Man')->attr('equipment')->append({'newBoots' => 1, 'newHelm' => 1})->run;

$Data::Dumper::Indent = 1;
say Dumper $res;
exit;

# pick the attributes from the document
$res = r->table('marvel')->get('Iron Man')->pick('reactorState', 'reactorPower')->run;
is $res->type, 1, 'Correct status code';
is_deeply $res->response, [{
  reactorState => 'medium',
  reactorPower => 4500,
  }], 'Correct response';

# pick all but some attributes from the document
$res = r->table('marvel')->get('Iron Man')->unpick('personalVictoriesList', 'equipment')->run;
is $res->type, 1, 'Correct status code';
is_deeply $res->response, [{
  reactorState => 'medium',
  reactorPower => 4500,
  age => 30,
  superhero => 'Iron Man',
  }], 'Correct response';

# delete one document
$res = r->table('marvel')->get('Iron Man')->delete->run;
is $res->type, 1, 'Correct status code';
is_deeply $res->response, [{
  deleted => 1,
  }], 'Correct response';

# clean up
r->db('test')->drop->run;

done_testing();