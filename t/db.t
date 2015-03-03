use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use Rethinkdb;

# setup
r->connect->repl;
r->db_drop('test')->run;
r->db_drop('superheroes')->run;

#
# db methods from main class
#
isa_ok r->db_create('superheroes'), 'Rethinkdb::Query', 'correct class';
my $res = r->db_create('superheroes')->run;

isa_ok $res, 'Rethinkdb::Response';
is $res->type, 1, 'Correct status code';

# list the databases
isa_ok r->db_list, 'Rethinkdb::Query', 'correct class';
$res = r->db_list->run;
isa_ok $res, 'Rethinkdb::Response';
is $res->type, 1, 'Correct status code';

my %dbs = map { $_ => 1 } @{$res->response};
ok $dbs{rethinkdb}, 'Db was created and listed';
ok $dbs{superheroes}, 'Db was created and listed';

# drop the database
isa_ok r->db_drop('superheroes'), 'Rethinkdb::Query', 'Correct class';
$res = r->db_drop('superheroes')->run;
isa_ok $res, 'Rethinkdb::Response';
is $res->type, 1, 'Correct status code';

# run list and double check the drop
$res = r->db_list->run;
ok !grep {/superheroes/} @{ $res->response }, 'Db is no longer listed';

#
# db class methods
#
isa_ok r->db('superheroes'), 'Rethinkdb::Query::Database', 'correct class';
isa_ok r->db('superheroes')->create, 'Rethinkdb::Query', 'correct class';
$res = r->db('superheroes')->create->run;

isa_ok $res, 'Rethinkdb::Response';
is $res->type, 1, 'Correct status code';

# list the databases
isa_ok r->db->list, 'Rethinkdb::Query', 'correct class';
$res = r->db->list->run;

isa_ok $res, 'Rethinkdb::Response';
is $res->type, 1, 'Correct status code';
ok grep {/superheroes/} @{ $res->response }, 'Db was created and listed';

# drop the database
isa_ok r->db('superheroes')->drop, 'Rethinkdb::Query', 'correct class';
$res = r->db('superheroes')->drop->run;
isa_ok $res, 'Rethinkdb::Response';
is $res->type, 1, 'Correct status code';

# double check the drop
$res = r->db->list->run;
ok !grep {/superheroes/} @{ $res->response }, 'Db is no longer listed';

done_testing();
