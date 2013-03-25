use Test::More;

use Rethinkdb;

# setup
r->connect;

eval {
  r->table('marvel')->group_by('strength', r->count)->run;
  r->table('marvel')->group_by('strength', r->sum('enemiesVanquished'))->run;
  r->table('marvel')->group_by('strength', r->avg('agility'))->run;
};

fail 'group_by count is not implemented';
fail 'group_by sum is not implemented';
fail 'group_by avg is not implemented';

done_testing();