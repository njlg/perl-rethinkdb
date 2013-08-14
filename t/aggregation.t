use Test::More;

use Rethinkdb;

# setup
my $conn = r->connect->repl;
r->db('test')->drop->run;
r->db('test')->create->run;
r->db('test')->table('marvel')->create(primary_key => 'superhero')->run;
r->table('marvel')->insert([
  { user_id => 1, superhero => 'Iron Man', superpower => 'Arc Reactor', active => 1, age => 35 },
  { user_id => 2, superhero => 'Hulk', superpower => 'Smash', active => 1, age => 35 },
  { user_id => 3, superhero => 'Captain America', superpower => 'Super Strength', active => 1, age => 135 },
  { user_id => 4, superhero => 'Thor', superpower => 'God-like powers', active => 1, age => 1035 },
  { user_id => 5, superhero => 'Hawk-Eye', superpower => 'Bow-n-arrow', active => 0, age => 35 },
  { user_id => 6, superhero => 'Wasp', superpower => 'Bio-lasers', active => 0, age => 35 },
  { user_id => 7, superhero => 'Ant-Man', superpower => 'Size', active => 1, age => 35, extra => 1 },
  { user_id => 8, superhero => 'Wolverine', superpower => 'Adamantium', active => 0, age => 35, extra => 1 },
  { user_id => 9, superhero => 'Spider-Man', superpower => 'Spidy Sense', active => 0, age => 20, extra => 1 },
])->run;

r->db('test')->table('dc')->create(primary_key => 'superhero')->run;
r->table('dc')->insert([
  { user_id => 10, superhero => 'Superman', superpower => 'Alien', active => 1, age => 35 },
  { user_id => 11, superhero => 'Batman', superpower => 'Cunning', active => 1, age => 35 },
  { user_id => 12, superhero => 'Flash', superpower => 'Super Speed', active => 1, age => 135 },
  { user_id => 13, superhero => 'Wonder Women', superpower => 'Super Stregth', active => 1, age => 1035 },
  { user_id => 14, superhero => 'Green Lantern', superpower => 'Ring', active => 0, age => 35 },
  { user_id => 15, superhero => 'Aquaman', superpower => 'Hydrokinesis', active => 0, age => 35 },
  { user_id => 16, superhero => 'Hawkman', superpower => 'Ninth Metal', active => 1, age => 35 },
  { user_id => 17, superhero => 'Martian Manhunter', superpower => 'Shapeshifting', active => 0, age => 35 },
])->run;

# reduce
eval {
  $res = r->table('marvel')->map(r->row('monstersKilled'))->reduce(sub {
    my ($acc, $val) = @_;
    $acc + $val;
  } , 0)->run;
};
unlink $@, qr/is not implemented/;

# count
$res = r->table('marvel')->count->run;

is $res->type, 1, 'Correct response type';
is $res->response, '9', 'Correct number of documents';

# count (with parameter)
r->table('marvel')->count('extra')->run;

is $res->type, 1, 'Correct response type';
is $res->response, '9', 'Correct number of documents';

# distinct (on table)
$res = r->table('marvel')->distinct->run;

is $res->type, 1, 'Correct response type';
is scalar @{$res->response}, 9, 'Correct number of documents';

# distinct (on query)
$res = r->expr([1, 1, 1, 1, 1, 2, 3])->distinct->run($conn);

is $res->type, 1, 'Correct response type';
is scalar @{$res->response}, 3, 'Correct number of documents';

# grouped_map_reduce
eval {
  r->table('marvel')->grouped_map_reduce(sub {
      my $hero = shift;
      return $hero->{weightClass}
    },
    sub {
      my $hero = shift;
      return $hero->pick('name', 'strength');
    },
    { name => 'none', strength => 0},
    sub {
      my $acc = shift;
      my $hero = shift;
      return $r->branch($acc->{strength}->lt($hero->{strength}), $hero, $acc);
  })->run;
};
unlink $@, qr/is not implemented/;

# group_by
eval {
  r->table('marvel')->group_by('weightClass', r->avg('strength'))->run;
};
unlink $@, qr/is not implemented/;

# contains
$res = r->table('marvel')->get('Ironman')->contains('superman')->run;

# clean up
r->db('test')->drop->run;

done_testing();