use Test::More;

use Rethinkdb;

# setup
my $conn = r->connect->repl;
r->db('test')->drop->run;
r->db('test')->create->run;
r->db('test')->table('marvel')->create( primary_key => 'superhero' )->run;
r->table('marvel')->insert(
  [
    {
      user_id         => 1,
      superhero       => 'Iron Man',
      superpower      => 'Arc Reactor',
      active          => 1,
      age             => 35,
      victories       => 2,
      battles         => 3,
      villainDefeated => 'Mandarin',
      outfits         => 12,
    },
    {
      user_id         => 8,
      superhero       => 'Wolverine',
      superpower      => 'Adamantium',
      age             => 40,
      victories       => 12,
      battles         => 3,
      villainDefeated => 'Sabretooth',
      outfits         => 2,
    },
    {
      user_id         => 9,
      superhero       => 'Spider-Man',
      superpower      => 'Spidy Sense',
      age             => 20,
      victories       => 24,
      battles         => 3,
      villainDefeated => 'Green Goblin'
    }
  ]
)->run;
r->db('test')->table('villains')->create( primary_key => 'name' )->run;
r->table('villains')->insert(
  [
    { name => 'Mandarin', },
    { name => 'Sabretooth', },
    { name => 'Green Goblin' }
  ]
)->run;

# do
my $res = r->do(
  r->table('marvel')->get('Iron Man'),
  sub ($) {
    my $ironman = shift;
    $ironman->attr('superpower');
  }
)->run;

is $res->type,     1,             'Correct response type';
is $res->response, 'Arc Reactor', 'Correct response';

# branch
r->table('marvel')->map(
  r->branch(

    # r->row->attr('victories')->gt(100),
    sub { shift->attr('victories')->gt(1); },
    # r->true,
    sub { shift->attr('superhero')->add(' is a superhero'); },
    sub { shift->attr('superhero')->add(' is a hero'); }
  )
)->run;

# for_each
$res = r->table('marvel')->for_each(
  sub {
    my $hero = shift;
    return r->table('villains')->get( $hero->attr('villainDefeated') )->delete;
  }
)->run;

is $res->type, 1, 'Correct response type';
is $res->response->{deleted}, 3, 'Correct response';

# error
$res = r->table('marvel')->get('Iron Man')->do(
  sub {
    my $ironman = shift;
    r->branch( $ironman->attr('victories')->lt( $ironman->attr('battles') ),
      r->error('impossible code path'), $ironman );
  }
)->run;

is $res->type, 18, 'Correct response type';
is $res->response->[0], 'impossible code path', 'Correct response';

# default
$res = r->table('marvel')->map(
  sub {
    my $stuff = shift;
    $stuff->attr('outfits')->default(0)
      ->add( $stuff->attr('active')->default(0) );
  }
)->run;

is $res->type, 2, 'Correct response type';
is_deeply [ sort { $a <=> $b } @{ $res->response } ], [ '0', '2', '13' ],
  'Correct response';

# expr
$res = r->expr( { 'a' => 'b' } )->merge( { 'b' => [ 1, 2, 3 ] } )->run($conn);

is $res->type, 1, 'Correct response type';
is_deeply $res->response, { 'a' => 'b', 'b' => [ '1', '2', '3' ] },
  'Correct response';

# js
$res = r->js("'str1' + 'str2'")->run;

is $res->type,     1,          'Correct response type';
is $res->response, 'str1str2', 'Correct response';

# js with function
$res = r->table('marvel')
  ->filter( r->js('(function (row) { return row.age > 30; })') )->run($conn);

is $res->type, 2, 'Correct response type';
is_deeply [ map { $_->{superhero} } @{ $res->response } ],
  [ 'Wolverine', 'Iron Man' ], 'Correct response';

# js with timeout
$res = r->js( 'while(true) {}', 1.3 )->run($conn);

is $res->type, 18, 'Correct response type';
is $res->response->[0],
  'JavaScript query `while(true) {}` timed out after 1.300 seconds.',
  'Correct response';

# coerce_to
$res = r->table('marvel')->coerce_to('array')->run;

is $res->type,         1,       'Correct response type';
isa_ok $res->response, 'ARRAY', 'Correct response';

$res = r->expr( [ [ 'name', 'Iron Man' ], [ 'victories', 2000 ] ] )
  ->coerce_to('object')->run($conn);

is $res->type,         1,      'Correct response type';
isa_ok $res->response, 'HASH', 'Correct response';

$res = r->expr(1)->coerce_to('string')->run($conn);

is $res->type,     1,   'Correct response type';
is $res->response, '1', 'Correct response';

# type_of
$res = r->expr("foo")->type_of->run($conn);

is $res->type,     1,        'Correct response type';
is $res->response, 'STRING', 'Correct response';

# info
$res = r->table('marvel')->info->run($conn);

is $res->type, 1, 'Correct response type';
is_deeply $res->response,
  {
  primary_key => 'superhero',
  db          => { name => 'test', type => 'DB' },
  name        => 'marvel',
  type        => 'TABLE',
  indexes     => []
  },
  'Correct response';

# json
$res = r->json("[1,2,3]")->run($conn);

is $res->type, 1, 'Correct response type';
is_deeply $res->response, [ '1', '2', '3' ], 'Correct response';

# clean up
r->db('test')->drop->run;

done_testing();
