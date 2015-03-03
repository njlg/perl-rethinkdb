use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use Rethinkdb;

# setup
my $conn = r->connect->repl;

# add
my $res = r->expr(2)->add(2)->run($conn);
is $res->response, 4, 'Addition: Mathematical response is okay';

$res = r->expr('Foo')->add('bar')->run($conn);
is $res->response, 'Foobar', 'Addition: String concatenation response is okay';

$res = r->expr( [ 'foo', 'bar' ] )->add( ['buzz'] )->run($conn);
is_deeply $res->response, [ 'foo', 'bar', 'buzz' ],
  'Addition: Array concatenation response is okay';

# sub
$res = r->expr(4)->sub(2)->run($conn);
is $res->response, 2, 'Subtraction response is okay';

# mul
$res = r->expr(2)->mul(2)->run($conn);
is $res->response, 4, 'Multiplication: Mathematical response is okay';

$res = r->expr( [ 'This', 'is', 'the', 'song', 'that', 'never', 'ends.' ] )
  ->mul(4)->run($conn);
is_deeply $res->response,
  [
  'This', 'is',   'the',   'song',  'that',  'never', 'ends.', 'This',
  'is',   'the',  'song',  'that',  'never', 'ends.', 'This',  'is',
  'the',  'song', 'that',  'never', 'ends.', 'This',  'is',    'the',
  'song', 'that', 'never', 'ends.'
  ],
  'Multiplication: Periodic  response is okay';

# div
$res = r->expr(2)->div(2)->run($conn);
is $res->response, 1, 'Division response is okay';

# mod
$res = r->expr(2)->mod(2)->run($conn);
is $res->response, 0, 'Mod response is okay';

# eq
$res = r->expr(2)->eq(2)->run($conn);
is $res->response, r->true, 'EQ response is okay';

# ne
$res = r->expr(2)->ne(2)->run($conn);
is $res->response, r->false, 'NE response is okay';

# gt
$res = r->expr(2)->gt(2)->run($conn);
is $res->response, r->false, 'GT response is okay';

# ge
$res = r->expr(2)->ge(2)->run($conn);
is $res->response, r->true, 'GE response is okay';

# lt
$res = r->expr(2)->lt(2)->run($conn);
is $res->response, r->false, 'LT response is okay';

# le
$res = r->expr(2)->le(2)->run($conn);
is $res->response, r->true, 'LE response is okay';

# and
$res = r->expr( r->true )->and( r->false )->run($conn);
is $res->response, r->false, 'AND response is okay';

# or
$res = r->expr( r->true )->or( r->false )->run($conn);
is $res->response, r->true, 'OR response is okay';

# not
$res = r->expr( r->true )->not->run($conn);
is $res->response, r->false, 'NOT response is okay';

# all
$res = r->all( r->true, r->false )->run($conn);
is $res->response, r->false, 'ALL response is okay';

# any
$res = r->any( r->true, r->false )->run($conn);
is $res->response, r->true, 'ANY response is okay';

# random
$res = r->random->run($conn);
cmp_ok $res->response, '>=', 0, 'Random response is okay';
cmp_ok $res->response, '<',  1, 'Random response is okay';

$res = r->random(100)->run($conn);
cmp_ok $res->response, '>=', 0,   'Random response is okay';
cmp_ok $res->response, '<',  100, 'Random response is okay';

$res = r->random( 75, 100 )->run($conn);
cmp_ok $res->response, '>=', 75,  'Random response is okay';
cmp_ok $res->response, '<',  100, 'Random response is okay';

$res = r->random( 1.59, -2.24, r->true )->run($conn);
cmp_ok $res->response, '>',  -2.24, 'Random response is okay';
cmp_ok $res->response, '<=', 1.59,  'Random response is okay';

done_testing();
