use Test::More;

use Rethinkdb;

r->connect;

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
unlike $@, qr/is not implemented/;

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
unlike $@, qr/is not implemented/;

eval {
  r->expr({'a' => 'b'})->merge({'b' => [1,2,3]})->run;
};
fail 'expr -> merge is not implemented';

eval {
  r->js("'str1' + 'str2'")->run;
};
unlike $@, qr/is not implemented/;

done_testing();