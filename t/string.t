use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use Rethinkdb;

my $conn = r->connect->repl;

# match
my $res
  = r->expr('id:0,name:mlucy,foo:bar')->match('name:(\w+)')->bracket('groups')
  ->nth(0)->bracket('str')->run($conn);

isa_ok $res, 'Rethinkdb::Response';
is $res->type,     1,       'Correct status code';
is $res->response, 'mlucy', 'Correct number of updates';

# $res = r->expr('id:0,foo:bar')->match('name:(\w+)')->bracket('groups')->nth(0)->bracket('str')->run($conn);
$res = r->expr('id:0,foo:bar')->match('name:(\w+)')->run($conn);

isa_ok $res, 'Rethinkdb::Response';
is $res->type,     1,     'Correct status code';
is $res->response, undef, 'Correct number of updates';

# split
$res = r->expr('foo  bar bax')->split->run($conn);

isa_ok $res, 'Rethinkdb::Response';
is $res->type,     1,     'Correct status code';
is_deeply $res->response, ['foo', 'bar', 'bax'], 'Correct split response';

$res = r->expr('id:0,foo:bar,stuff:good')->split(',')->run($conn);

isa_ok $res, 'Rethinkdb::Response';
is $res->type,     1,     'Correct status code';
is_deeply $res->response, ['id:0', 'foo:bar', 'stuff:good'], 'Correct split response';

# upcase
$res = r->expr('Sentence about LaTeX.')->upcase->run($conn);

isa_ok $res, 'Rethinkdb::Response';
is $res->type,     1,     'Correct status code';
is $res->response, 'SENTENCE ABOUT LATEX.', 'Correct split response';

# downcase
$res = r->expr('Sentence about LaTeX.')->downcase->run($conn);

isa_ok $res, 'Rethinkdb::Response';
is $res->type,     1,     'Correct status code';
is $res->response, 'sentence about latex.', 'Correct split response';

done_testing();
