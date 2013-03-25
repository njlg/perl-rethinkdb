use Test::More;

use Rethinkdb;

r->expr(2)->add(2)->run;
r->expr(2)->div(2)->run;
r->expr(2)->eq(2)->run;
r->expr(2)->ge(2)->run;
r->expr(2)->gt(2)->run;
r->expr(2)->le(2)->run;
r->expr(2)->lt(2)->run;
r->expr(2)->mod(2)->run;
r->expr(2)->mul(2)->run;
r->expr(2)->ne(2)->run;
r->expr(2)->sub(2)->run;
r->expr([1,2,3])->add([4,5,6])->run;
r->expr(r->true)->and(r->false)->run;
r->expr(r->true)->not()->run;
r->expr(r->true)->or(r->false)->run;

done_testing();