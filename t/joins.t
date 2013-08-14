use Test::More;

use Rethinkdb;


# setup
r->connect->repl;
r->db('test')->drop->run;
r->db('test')->create->run;
r->db('test')->table('marvel')->create(primary_key => 'age')->run;
r->table('marvel')->insert([
  { user_id => 1, superhero => 'Iron Man', superpower => 'Arc Reactor', active => 1, age => 35 },
  { user_id => 2, superhero => 'Hulk', superpower => 'Smash', active => 1, age => 35 },
  { user_id => 3, superhero => 'Captain America', superpower => 'Super Strength', active => 1, age => 135 },
  { user_id => 4, superhero => 'Thor', superpower => 'God-like powers', active => 1, age => 1035 },
  { user_id => 5, superhero => 'Hawk-Eye', superpower => 'Bow-n-arrow', active => 0, age => 35 },
  { user_id => 6, superhero => 'Wasp', superpower => 'Bio-lasers', active => 0, age => 35 },
  { user_id => 7, superhero => 'Ant-Man', superpower => 'Size', active => 1, age => 35 },
  { user_id => 8, superhero => 'Wolverine', superpower => 'Adamantium', active => 0, age => 35 },
  { user_id => 9, superhero => 'Spider-Man', superpower => 'Spidy Sense', active => 0, age => 20 },
])->run;

r->db('test')->table('dc')->create(primary_key => 'age')->run;
r->db('test')->table('dc')->index_create('secondary_id')->run;
r->table('dc')->insert([
  { user_id => 10, superhero => 'Superman', superpower => 'Alien', active => 1, age => 35, secondary_id => 35 },
  { user_id => 11, superhero => 'Batman', superpower => 'Cunning', active => 1, age => 35, secondary_id => 35 },
  { user_id => 12, superhero => 'Flash', superpower => 'Super Speed', active => 1, age => 135, secondary_id => 135 },
  { user_id => 13, superhero => 'Wonder Women', superpower => 'Super Stregth', active => 1, age => 1035, secondary_id => 1035 },
  { user_id => 14, superhero => 'Green Lantern', superpower => 'Ring', active => 0, age => 35, secondary_id => 35 },
  { user_id => 15, superhero => 'Aquaman', superpower => 'Hydrokinesis', active => 0, age => 35, secondary_id => 35 },
  { user_id => 16, superhero => 'Hawkman', superpower => 'Ninth Metal', active => 1, age => 35, secondary_id => 35 },
  { user_id => 17, superhero => 'Martian Manhunter', superpower => 'Shapeshifting', active => 0, age => 35, secondary_id => 35 },
])->run;


# inner_join
eval {
  r->table('marvel')->inner_join(r->table('dc'), sub {
    my ($marvelRow, $dcRow) = @_;
    return $marvelRow->{strength} < $dcRow->{strength};
  })->run;
};
like $@, qr/is not implemented/;

# outer_join
eval {
  r->table('marvel')->outer_join(r->table('dc'), sub {
    my ($marvelRow, $dcRow) = @_;
    return $marvelRow->{strength} < $dcRow->{strength};
  })->run;
};
like $@, qr/is not implemented/;


eval {
  r->table('marvel')->inner_join(r->table('dc'), sub {
    my ($left, $right) = @_;
    return $left->{main_dc_collaborator}->eq($right->{hero_name});
  })->run;
};
unlike $@, qr/is not implemented/;

# eq_join
$res = r->table('marvel')->eq_join('age', r->table('dc'))->run;

is $res->type, 2, 'Correct response type';
is scalar @{$res->response}, 5, 'Correct number of documents returned';

# eq_join with secondary index
r->table('marvel')->eq_join('age', r->table('dc'), 'secondary_id')->run;

is $res->type, 2, 'Correct response type';
is scalar @{$res->response}, 3, 'Correct number of documents returned';

# zip
$res = r->table('marvel')->eq_join('age', r->table('dc'))->zip->run;

is $res->type, 2, 'Correct response type';
is scalar @{$res->response}, 3, 'Correct number of documents returned';

# clean up
r->db('test')->drop->run;

done_testing();