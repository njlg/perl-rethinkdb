use Test::More;

use Rethinkdb;

# setup
my $conn = r->connect->repl;
r->db('test')->drop->run;
r->db('test')->create->run;
r->db('test')->table('marvel')->create(primary_key => 'superhero')->run;
r->table('marvel')->insert([
  { user_id => 1, superhero => 'Iron Man', superpower => 'Arc Reactor', enemiesVanquished => 35, race => 'Human' },
  { user_id => 2, superhero => 'Hulk', superpower => 'Smash', enemiesVanquished => 35, race => 'Hulkman' },
  { user_id => 3, superhero => 'Captain America', superpower => 'Super Strength', enemiesVanquished => 135, race => 'Human' },
  { user_id => 4, superhero => 'Thor', superpower => 'God-like powers', enemiesVanquished => 1035, race => 'God' },
  { user_id => 5, superhero => 'Hawk-Eye', superpower => 'Bow-n-arrow', enemiesVanquished => 35, race => 'Human' },
  { user_id => 6, superhero => 'Wasp', superpower => 'Bio-lasers', enemiesVanquished => 35, race => 'Human' },
  { user_id => 7, superhero => 'Ant-Man', superpower => 'Size', enemiesVanquished => 35, race => 'Human' },
  { user_id => 8, superhero => 'Wolverine', superpower => 'Adamantium', enemiesVanquished => 35, race => 'Human' },
  { user_id => 9, superhero => 'Spider-Man', superpower => 'Spidy Sense', enemiesVanquished => 20, race => 'Human' },
])->run;

# r->count
$res = r->table('marvel')->group_by('enemiesVanquished', r->count)->run;

is $res->type, 1, 'Correct response type';
is_deeply $res->response, [
  { group => [ '20' ], reduction => '1' },
  { group => [ '35' ], reduction => '6' },
  { group => [ '135' ], reduction => '1' },
  { group => [ '1035' ], reduction => '1' }
], 'Correct result';

# r->sum
$res = r->table('marvel')->group_by('race', r->sum('enemiesVanquished'))->run;

is $res->type, 1, 'Correct response type';
is_deeply $res->response, [
  { group => ['God'], reduction => '1035' },
  { group => ['Hulkman'], reduction => '35' },
  { group => ['Human'], reduction => '330' }
], 'Correct result';


# r->avg
r->table('marvel')->group_by('race', r->avg('enemiesVanquished'))->run;

is $res->type, 1, 'Correct response type';
is_deeply $res->response, [
  { group => ['God'], reduction => '1035' },
  { group => ['Hulkman'], reduction => '35' },
  { group => ['Human'], reduction => '47.1428571428571' }
], 'Correct result';

# clean up
r->db('test')->drop->run;

done_testing();