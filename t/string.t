use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use Rethinkdb;

my $conn = r->connect->repl;

# match
my $res
  = r->expr('id:0,name:mlucy,foo:bar')->match('name:(\w+)')->attr('groups')
  ->nth(0)->attr('str')->run($conn);

isa_ok $res, 'Rethinkdb::Response';
is $res->type,     1,       'Correct status code';
is $res->response, 'mlucy', 'Correct number of updates';

# $res = r->expr('id:0,foo:bar')->match('name:(\w+)')->attr('groups')->nth(0)->attr('str')->run($conn);
$res = r->expr('id:0,foo:bar')->match('name:(\w+)')->run($conn);

isa_ok $res, 'Rethinkdb::Response';
is $res->type,     1,     'Correct status code';
is $res->response, undef, 'Correct number of updates';

done_testing();
