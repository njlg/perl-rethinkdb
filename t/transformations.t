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
  { user_id => 9, superhero => 'Spiderman', superpower => 'Spidy Sense', active => 0, age => 20 }
  { user_id => 2, superhero => 'Hulk', superpower => 'Smash', active => 1, age => 35 },
  { user_id => 3, superhero => 'Captain America', superpower => 'Super Strength', active => 1, age => 135 },
  { user_id => 4, superhero => 'Thor', superpower => 'God-like powers', active => 1, age => 1035 },
  { user_id => 5, superhero => 'Hawk-eye', superpower => 'Bow-n-arrow', active => 0, age => 35 },
  { user_id => 6, superhero => 'Wasp', superpower => 'Bio-lasers', active => 0, age => 35 },
  { user_id => 7, superhero => 'Ant-Man', superpower => 'Size', active => 1, age => 35 },
])->run;


# map
eval {
  r->table('marvel')->map(sub {
    my $hero = shift;
    return $hero->{combatPower}->add($hero->{compassionPower}.mult(2));
  })->run;
};
unlike $@, qr/is not implemented/;

# concat_map
eval {
  r->table('marvel')->concat_map(sub {
    my $hero = shift;
    return $hero->{defeatedMonsters};
  })->run;
};
unlike $@, qr/is not implemented/;

# order_by
my $order1 = ['Ant-Man', 'Captain America', 'Hawk-eye', 'Hulk', 'Iron Man', 'Spiderman', 'Thor', 'Wasp', 'Wolverine'];
my $order2 = ['Hawk-eye', 'Spiderman', 'Wasp', 'Wolverine', 'Ant-Man', 'Captain America', 'Hulk', 'Iron Man', 'Thor'];
my $order3 = ['Ant-Man', 'Captain America', 'Hulk', 'Iron Man', 'Thor', 'Hawk-eye', 'Spiderman', 'Wasp', 'Wolverine'];

$res = r->table('marvel')->order_by('superhero')->run;

isa_ok $res, 'Rethinkdb::Response', 'Correct class';
is $res->status_code, 3, 'Correct status code';
isa_ok $res->response, 'ARRAY', 'Correct resposne type';
is scalar @{$res->response}, 9, 'Correct number of documents returned';
is_deeply [map { $_->{superhero} } @{$res->response}], $order1, 'Correct order';

# order by two attributes
$res = r->table('marvel')->order_by('active', 'superhero')->run;
is_deeply [map { $_->{superhero} } @{$res->response}], $order2, 'Correct order';

# order with asc/desc
$res = r->table('marvel')->order_by(r->desc('active'), r->asc('superhero'))->run;
is_deeply [map { $_->{superhero} } @{$res->response}], $order3, 'Correct order';


# skip
eval {
  $res = r->table('marvel')->skip(10)->run;
};
unlike $@, qr/is not implemented/;


# limit
eval {
  $res = r->table('marvel')->limit(10)->run;
};
unlike $@, qr/is not implemented/;


# slice
# $r->table('marvel')->order_by('strength')[5:10]->run;
eval {
  $res = r->table('marvel')->slice(5, 10)->run;
};
unlike $@, qr/is not implemented/;


# nth
# $r->expr([1,2,3])[1]->run;
# $res = r->expr([1,2,3])->nth(1)->run;
eval {
  $res = r->table('marvel')->nth(1)->run;
};
unlike $@, qr/is not implemented/;


# pluck
eval {
  $res = r->table('marvel')->pluck('beauty', 'muscleTone', 'charm')->run;
};
unlike $@, qr/is not implemented/;


# without
eval {
  r->table('enemies')->without('weapons')->run;
};
unlike $@, qr/is not implemented/;

# union
eval {
  r->table('marvel')->union(r->table('dc'))->run;
};
unlike $@, qr/is not implemented/;


# clean up
r->db('test')->drop->run;

done_testing();