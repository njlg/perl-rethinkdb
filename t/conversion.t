use Test::More;

use Rethinkdb;

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

eval {
  my $heroes;
  r->let({heroes => r->table('marvel')->stream_to_array}, $heroes)->run;
};
unlike $@, qr/is not implemented/;

eval {
  r->expr([1,2,3])->array_to_stream->union(r->table('marvel'))->run;
};
fail 'array_to_stream is not implemented';

# clean up
r->db('test')->drop->run;

done_testing();