use Test::More;

use Rethinkdb;

use lib '../';
use Carp::Always;

# setup
my $conn = r->connect->repl;
r->db('test')->drop->run;
r->db('test')->create->run;
r->db('test')->table('marvel')->create( primary_key => 'superhero' )->run;
r->table('marvel')->insert(
  [
    {
      user_id    => 1,
      superhero  => 'Iron Man',
      superpower => 'Arc Reactor',
      active     => 1,
      age        => 35,
      villians   => { count => 5 }
    },
    {
      user_id    => 2,
      superhero  => 'Hulk',
      superpower => 'Smash',
      active     => 1,
      age        => 35,
      villians   => { count => 6 }
    },
    {
      user_id    => 3,
      superhero  => 'Captain America',
      superpower => 'Super Strength',
      active     => 1,
      age        => 135,
      villians   => { count => 7 }
    },
    {
      user_id    => 4,
      superhero  => 'Thor',
      superpower => 'God-like powers',
      active     => 1,
      age        => 1035,
      villians   => { count => 8 }
    },
    {
      user_id    => 5,
      superhero  => 'Hawk-Eye',
      superpower => 'Bow-n-arrow',
      active     => 0,
      age        => 35,
      villians   => { count => 9 }
    },
    {
      user_id    => 6,
      superhero  => 'Wasp',
      superpower => 'Bio-lasers',
      active     => 0,
      age        => 35,
      villians   => { count => 5 }
    },
    {
      user_id    => 7,
      superhero  => 'Ant-Man',
      superpower => 'Size',
      active     => 1,
      age        => 35,
      villians   => { count => 10 }
    },
    {
      user_id    => 8,
      superhero  => 'Wolverine',
      superpower => 'Adamantium',
      active     => 0,
      age        => 35,
      villians   => { count => 1 }
    },
    {
      user_id    => 9,
      superhero  => 'Spider-Man',
      superpower => 'Spidy Sense',
      active     => 0,
      age        => 20,
      villians   => { count => 2 }
    },
  ]
)->run;
r->db('test')->table('loadouts')->create( primary_key => 'kit' )->run;
r->table('loadouts')->insert(
  {
    kit       => 'alienInvasionKit',
    equipment => [ 'alienHelm', 'alienArmour', 'alienBoots' ]
  }
)->run;
r->db('test')->table('prizes')->create( primary_key => 'status' )->run;
r->table('prizes')->insert( { status => 'winner', name => 'Hulk' } )->run;

my $res;

# row
$res = r->table('marvel')->filter( r->row->attr('age')->gt(50) )->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 2, 'Correct number of documents';

$res = r->table('marvel')
  ->filter( r->row->attr('villians')->attr('count')->ge(10) )->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 1, 'Correct number of documents';
is $res->response->[0]->{superhero}, 'Ant-Man', 'Correct document';

$res = r->expr( [ 1, 2, 3 ] )->map( r->row->add(1) )->run($conn);

is $res->type, 1, 'Correct response type';
is_deeply $res->response, [ '2', '3', '4' ], 'Correct response';

$res = r->table('marvel')->filter(
  sub ($) {
    my $doc = shift;
    return $doc->attr('superhero')->eq
      ( r->table('prizes')->get('winner')->attr('name') );
  }
)->run;

is $res->type, 2, 'Correct response type';
is_deeply $res->response->[0]->{superhero}, 'Hulk', 'Correct response';

# replace one document
$res = r->table('marvel')->get('Iron Man')
  ->replace( { superhero => 'Iron Man', age => 30 } )->run;

isa_ok $res, 'Rethinkdb::Response';
is $res->type, 1, 'Correct response type';
is $res->response->{replaced}, 1, 'Correct number of updates';

# merge to documents
$res = r->table('marvel')->get('Iron Man')
  ->merge( r->table('loadouts')->get('alienInvasionKit') )->run;

isa_ok $res, 'Rethinkdb::Response';
is $res->type, 1, 'Correct response type';
is_deeply [ sort keys %{ $res->response } ],
  [ 'age', 'equipment', 'kit', 'superhero' ],
  'Correct merged document attribute';

# check for an attribute (that doesn't exist)
$res = r->table('marvel')->get('Iron Man')->has_fields('active_status')->run;

isa_ok $res, 'Rethinkdb::Response';
is $res->type, 1, 'Correct response type';
is $res->response, r->false, 'Correct response';

# check for an attribute (that does exist)
$res = r->table('marvel')->get('Iron Man')->has_fields('age')->run;

isa_ok $res, 'Rethinkdb::Response';
is $res->type, 1, 'Correct response type';
is $res->response, r->true, 'Correct response';

# get one attribute value
$res = r->table('marvel')->get('Iron Man')->attr('age')->run;

isa_ok $res, 'Rethinkdb::Response';
is $res->type,     1,  'Correct response type';
is $res->response, 30, 'Correct response';

# prep for next test:
r->table('marvel')->get('Iron Man')->update(
  {
    equipment => [ 'oldBoots', 'oldHelm' ],
    stuff        => { laserCannons => 2, missels => 12 },
    reactorState => 'medium',
    reactorPower => 4500,
    personalVictoriesList => [ 'Fing Fang Foom', 'Iron Monger', 'Mandarin', ],
  }
)->run;

# append a value
$res
  = r->table('marvel')->get('Iron Man')->attr('equipment')->append('newBoots')
  ->run;

is $res->type, 1, 'Correct response type';
is_deeply $res->response, [ 'oldBoots', 'oldHelm', 'newBoots' ];

# prepend a value
$res
  = r->table('marvel')->get('Iron Man')->attr('equipment')->prepend('newHelm')
  ->run;

is $res->type, 1, 'Correct response type';
is_deeply $res->response, [ 'newHelm', 'oldBoots', 'oldHelm' ];

# get the difference between to arrays
$res = r->table('marvel')->get('Iron Man')->attr('equipment')
  ->difference( ['oldBoots'] )->run;

is $res->type, 1, 'Correct response type';
is_deeply $res->response, ['oldHelm'];

# Add a value to an array and return it as a set (an array with distinct values).
$res = r->table('marvel')->get('Iron Man')->attr('equipment')
  ->set_insert( ['newBoots'] )->run;

is $res->type, 1, 'Correct response type';
is_deeply $res->response, [ 'oldBoots', 'oldHelm', 'newBoots' ];

# Add a several values to an array and return it as a set (an array with distinct values)
$res = r->table('marvel')->get('Iron Man')->attr('equipment')
  ->set_union( [ 'newBoots', 'arc_reactor' ] )->run;

is $res->type, 1, 'Correct response type';
is_deeply $res->response, [ 'oldBoots', 'oldHelm', 'newBoots', 'arc_reactor' ];

# Intersect two arrays returning values that occur in both of them as a set (an array with distinct values).
$res = r->table('marvel')->get('Iron Man')->attr('equipment')
  ->set_intersection( [ 'newBoots', 'arc_reactor', 'oldBoots' ] )->run;

is $res->type, 1, 'Correct response type';
is_deeply $res->response, ['oldBoots'];

# Remove the elements of one array from another and return them as a set (an array with distinct values).
$res = r->table('marvel')->get('Iron Man')->attr('equipment')
  ->set_difference( [ 'newBoots', 'arc_reactor', 'oldBoots' ] )->run;

is $res->type, 1, 'Correct response type';
is_deeply $res->response, ['oldHelm'];

# Plucks out one or more attributes from either an object or a sequence of
# objects (projection).
$res = r->table('marvel')->get('Iron Man')
  ->pluck( 'reactorState', 'reactorPower' )->run;

is $res->type, 1, 'Correct response type';
is_deeply $res->response, { reactorState => 'medium', reactorPower => 4500, },
  'Correct response';

# The opposite of pluck; takes an object or a sequence of objects, and removes
# all attributes except for the ones specified.
$res = r->table('marvel')->get('Iron Man')
  ->without( 'personalVictoriesList', 'equipment', 'stuff' )->run;

is $res->type, 1, 'Correct response type';
is_deeply $res->response,
  {
  reactorState => 'medium',
  reactorPower => 4500,
  age          => 30,
  superhero    => 'Iron Man',
  },
  'Correct response';

# Insert a value in to an array at a given index. Returns the modified array.
$res = r->expr( [ 'Iron Man', 'Spider-Man' ] )->insert_at( 1, 'Hulk' )
  ->run($conn);

is $res->type, 1, 'Correct response type';
is_deeply $res->response, [ 'Iron Man', 'Hulk', 'Spider-Man', ],
  'Correct response type';

# Insert several values in to an array at a given index.
# Returns the modified array.
$res = r->expr( [ 'Iron Man', 'Spider-Man' ] )
  ->splice_at( 1, [ 'Hulk', 'Thor' ] )->run($conn);

is $res->type, 1, 'Correct response type';
is_deeply $res->response, [ 'Iron Man', 'Hulk', 'Thor', 'Spider-Man', ],
  'Correct response type';

# Remove an element from an array at a given index. Returns the modified array.
$res = r->expr( [ 'Iron Man', 'Spider-Man' ] )->delete_at(1)->run($conn);

is $res->type, 1, 'Correct response type';
is_deeply $res->response, [ 'Iron Man' ], 'Correct response type';

# same but use a starting and ending index
$res
  = r->expr( [ 'Iron Man', 'Hulk', 'Thor', 'Spider-Man' ] )->delete_at( 1, 3 )
  ->run($conn);

is $res->type, 1, 'Correct response type';
is_deeply $res->response, [ 'Iron Man', 'Spider-Man' ],
  'Correct response type';

# Change a value in an array at a given index. Returns the modified array.
$res
  = r->expr( [ 'Iron Man', 'Bruce Banner', 'Thor' ] )->change_at( 1, 'Hulk' )
  ->run($conn);

is $res->type, 1, 'Correct response type';
is_deeply $res->response, [ 'Iron Man', 'Hulk', 'Thor' ],
  'Correct response type';

# Return an array containing all of the object's keys.
$res = r->table('marvel')->get('Iron Man')->keys->run;

is $res->type, 1, 'Correct response type';
is_deeply $res->response,
  [
  'age',                   'equipment',
  'personalVictoriesList', 'reactorPower',
  'reactorState',          'stuff',
  'superhero'
  ],
  'Correct keys';

# clean up
r->db('test')->drop->run;

done_testing();
