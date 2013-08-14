use Test::More;

use Rethinkdb;

# setup
r->connect->repl;
r->db('test')->drop->run;
r->db('test')->create->run;
r->db('test')->table('marvel')->create(primary_key => 'superhero')->run;
r->db('test')->table('marvel')->index_create('superpower')->run;
r->db('test')->table('marvel')->index_create('user_id')->run;
r->table('marvel')->insert([
  { user_id => 1, superhero => 'Iron Man', superpower => 'Arc Reactor', active => 1, age => 35 },
  { user_id => 8, superhero => 'Wolverine', superpower => 'Adamantium', active => 0, age => 35 },
  { user_id => 9, superhero => 'Spider-Man', superpower => 'Spidy Sense', active => 0, age => 20 }
])->run;

# do
my $res = r->do(r->table('marvel')->get('Iron Man'), sub ($) {
  my $ironman = shift;
  $ironman->attr('superpower');
})->run;

is $res->type, 1, 'Correct response type';
is $res->response, 'Arc Reactor', 'Correct response';

# branch
r->table('marvel')->map(r->branch(r->row->attr('victories')->gt(100),
  r->row->attr('name')->add(' is a superhero'),
  r->row->attr('name')->add(' is a hero'))
)->run;

exit;

eval {
  r->let({'ironman' => r->table('marvel')->get('IronMan')}, r->letvar('ironman')->{'name'})->run;
  r->let({'ironman' => r->table('marvel')->get('IronMan')}, r->letvar('ironman')->{'name'})->run;
  r->let({'ironman' => r->table('marvel')->get('IronMan'), 'thor' => r->table('marvel')->get('Thor')},
    r->branch(
      r->letvar('ironman')->{'manliness'}->gt(r->letvar('thor')->{'manliness'}),
      r->letvar('ironman'),
      r->letvar('thor')
    )
  )->run;
};
like $@, qr/is not implemented/;

eval {
  r->table('marvel')->for_each(sub {
    my $hero = shift;
    return r->table('villains')->get($hero->{villainDefeated})->delete();
  })->run;
};
fail 'for_each is not implemented';

eval {
  r->let({'ironman' => r->table('marvel')->get('IronMan')},
   r->branch(r->letvar('ironman')->{'victories'} < r->letVar('ironman')->{'battles'},
    r->error('impossible code path'),
    r->letvar('ironman')
  ))->run;
};
like $@, qr/is not implemented/;

eval {
  r->expr({'a' => 'b'})->merge({'b' => [1,2,3]})->run;
};
fail 'expr -> merge is not implemented';

eval {
  r->js("'str1' + 'str2'")->run;
};
like $@, qr/is not implemented/;

# clean up
r->db('test')->drop->run;

done_testing();