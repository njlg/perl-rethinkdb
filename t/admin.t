use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use Rethinkdb;

# setup
my $conn = r->connect->repl;
r->db('test')->drop->run;
r->db('test')->create->run;
r->db('test')->table('marvel')->create( primary_key => 'superhero' )->run;
r->table('marvel')->insert(
  [
    {
      user_id    => 1,
      superhero  => 'Iron Man',
      superpower => 'Arc Reactor',
      active     => 1,
      age        => 35,
      strength   => 1000,
      dc_buddies => [ 'Superman', 'Batman' ],
    },
    {
      user_id    => 2,
      superhero  => 'Hulk',
      superpower => 'Smash',
      active     => 1,
      age        => 35,
      strength   => 2012,
      dc_buddies => [ 'Superman', 'Flash' ],
    },
    {
      user_id    => 3,
      superhero  => 'Captain America',
      superpower => 'Super Strength',
      active     => 1,
      age        => 135,
      strength   => 1035,
      dc_buddies => [ 'Superman', 'Green Lantern' ],
    },
    {
      user_id    => 4,
      superhero  => 'Thor',
      superpower => 'God-like powers',
      active     => 1,
      age        => 1035,
      strength   => 2035,
      dc_buddies => [ 'Flash', 'Batman' ],
    },
    {
      user_id    => 5,
      superhero  => 'Hawk-Eye',
      superpower => 'Bow-n-arrow',
      active     => 0,
      age        => 35,
      strength   => 10,
      dc_buddies => [ 'Aquaman', 'Wonder Women' ],
    },
    {
      user_id    => 6,
      superhero  => 'Wasp',
      superpower => 'Bio-lasers',
      active     => 0,
      age        => 35,
      strength   => 52,
      dc_buddies => [ 'Superman', 'Batman' ],
    },
    {
      user_id    => 7,
      superhero  => 'Ant-Man',
      superpower => 'Size',
      active     => 1,
      age        => 35,
      strength   => 50,
      dc_buddies => [ 'Green Lantern', 'Aquaman' ],
      extra      => 1,
    },
    {
      user_id    => 8,
      superhero  => 'Wolverine',
      superpower => 'Adamantium',
      active     => 0,
      age        => 35,
      strength   => 135,
      dc_buddies => [ 'Hawkman', 'Batman' ],
      extra      => 1,
    },
    {
      user_id    => 9,
      superhero  => 'Spider-Man',
      superpower => 'Spidy Sense',
      active     => 0,
      age        => 20,
      strength   => 200,
      dc_buddies => [ 'Wonder Women', 'Martian Manhunter' ],
      extra      => 1,
    }
  ]
)->run;

# create a few users
r->db('rethinkdb')->table('users')->insert(
  [
    {
      id       => 'chatapp',
      password => { password => 'chatapp-secret', iterations => 1024 }
    },
    {
      id       => 'monitoring',
      password => { password => 'monitoring-secret', iterations => 1024 }
    },
    {
      id       => 'bob',
      password => { password => 'bob-secret', iterations => 1024 }
    }
  ]
)->run;

my $res;

# grant global
$res = r->grant( 'chatapp', { read => r->true, write => r->true } )->run;

is $res->type, 1, 'Correct response type';
is $res->response->{granted}, 1, 'Correct response';
is_deeply [ sort keys %{ $res->response->{permissions_changes}->[0] } ],
  [ 'new_val', 'old_val' ], 'Correct structure returned';
is_deeply $res->response->{permissions_changes}->[0],
  {
  'new_val' => { 'read'  => r->true, 'write' => r->true },
  'old_val' => undef
  },
  'Correct structure returned';

$res = r->grant(
  'monitoring',
  {
    read    => r->true,
    write   => r->false,
    connect => r->false,
    config  => r->false
  }
)->run;

is $res->type, 1, 'Correct response type';
is $res->response->{granted}, 1, 'Correct response';
is_deeply $res->response->{permissions_changes}->[0],
  {
  'new_val' => { 'read'  => r->true, 'write' => r->false, 'connect' => r->false, 'config' => r->false },
  'old_val' => undef
  },
  'Correct structure returned';

# grant database
$res = r->db('test')->grant( 'chatapp', { read => r->true, write => r->true } )->run;

is $res->type, 1, 'Correct response type';
is $res->response->{granted}, 1, 'Correct response';
is_deeply [ sort keys %{ $res->response->{permissions_changes}->[0] } ],
  [ 'new_val', 'old_val' ], 'Correct structure returned';
is_deeply $res->response->{permissions_changes}->[0],
  {
  'new_val' => { 'read'  => r->true, 'write' => r->true },
  'old_val' => { 'write' => r->true, 'read'  => r->true }
  },
  'Correct structure returned';

$res = r->db('test')->grant(
  'monitoring',
  {
    read    => r->true,
    write   => r->false,
    connect => r->false,
    config  => r->false
  }
)->run;

is $res->type, 1, 'Correct response type';
is $res->response->{granted}, 1, 'Correct response';
is_deeply $res->response->{permissions_changes}->[0],
  {
  'new_val' => { 'read'  => r->true, 'write' => r->false, 'connect' => r->false, 'config' => r->false },
  'old_val' => { 'read'  => r->true, 'write' => r->false, 'connect' => r->false, 'config' => r->false }
  },
  'Correct structure returned';

# grant table
$res = r->table('marvel')->grant( 'chatapp', { read => r->true, write => r->true } )->run;

is $res->type, 1, 'Correct response type';
is $res->response->{granted}, 1, 'Correct response';
is_deeply [ sort keys %{ $res->response->{permissions_changes}->[0] } ],
  [ 'new_val', 'old_val' ], 'Correct structure returned';
is_deeply $res->response->{permissions_changes}->[0],
  {
  'new_val' => { 'read'  => r->true, 'write' => r->true },
  'old_val' => { 'write' => r->true, 'read'  => r->true }
  },
  'Correct structure returned';

$res = r->table('marvel')->grant(
  'monitoring',
  {
    read    => r->true,
    write   => r->false,
    connect => r->false,
    config  => r->false
  }
)->run;

is $res->type, 1, 'Correct response type';
is $res->response->{granted}, 1, 'Correct response';
is_deeply $res->response->{permissions_changes}->[0],
  {
  'new_val' => { 'read'  => r->true, 'write' => r->false, 'connect' => r->false, 'config' => r->false },
  'old_val' => { 'read'  => r->true, 'write' => r->false, 'connect' => r->false, 'config' => r->false }
  },
  'Correct structure returned';


# config - database
$res = r->db('test')->config->run;

is $res->type, 1, 'Correct response type';
is_deeply [ sort keys %{ $res->response } ], [ 'id', 'name' ],
  'Correct structure returned';

# config - table
$res = r->table('marvel')->config->run;

is $res->type, 1, 'Correct response type';
is_deeply [ sort keys %{ $res->response } ],
  [
  'db',   'durability',  'id',     'indexes',
  'name', 'primary_key', 'shards', 'write_acks'
  ],
  'Correct structure returned';

# rebalance - database
$res = r->db('test')->rebalance->run;

is $res->type, 1, 'Correct response type';
isa_ok $res->response->{status_changes}, 'ARRAY', 'Correct structure returned';
is $res->response->{rebalanced},         1,       'Correct structure returned';

# rebalance - table
$res = r->table('marvel')->rebalance->run;

is $res->type, 1, 'Correct response type';
isa_ok $res->response->{status_changes}, 'ARRAY', 'Correct structure returned';
is $res->response->{rebalanced},         1,       'Correct structure returned';

# reconfigure - database
$res
  = r->db('test')->reconfigure( { shards => 1, replicas => 1, dry_run => 1 } )
  ->run;

is $res->type, 1, 'Correct response type';
isa_ok $res->response->{config_changes}, 'ARRAY', 'Correct structure returned';
is $res->response->{reconfigured},       0,       'Correct structure returned';

$res = r->db('test')->reconfigure( { shards => 1, replicas => 1 } )->run;

is $res->type, 1, 'Correct response type';
isa_ok $res->response->{config_changes}, 'ARRAY', 'Correct structure returned';
is $res->response->{reconfigured},       1,       'Correct structure returned';

# reconfigure - table
$res = r->table('marvel')
  ->reconfigure( { shards => 1, replicas => 1, dry_run => 1 } )->run;

is $res->type, 1, 'Correct response type';
isa_ok $res->response->{config_changes}, 'ARRAY', 'Correct structure returned';
is $res->response->{reconfigured},       0,       'Correct structure returned';

$res = r->table('marvel')->reconfigure( { shards => 1, replicas => 1 } )->run;

is $res->type, 1, 'Correct response type';
isa_ok $res->response->{config_changes}, 'ARRAY', 'Correct structure returned';
is $res->response->{reconfigured},       1,       'Correct structure returned';

# status
$res = r->table('marvel')->status->run;

is $res->type, 1, 'Correct response type';
isa_ok $res->response->{shards}, 'ARRAY',  'Correct structure returned';
is $res->response->{db},         'test',   'Correct structure returned';
is $res->response->{name},       'marvel', 'Correct structure returned';
is_deeply $res->response->{status},
  {
  ready_for_reads          => r->true,
  ready_for_outdated_reads => r->true,
  all_replicas_ready       => r->true,
  ready_for_writes         => r->true
  },
  'Correct structure returned';

# wait - database
$res = r->db('test')->wait->run;

is $res->type, 1, 'Correct response type';
is $res->response->{ready}, 1, 'Correct response type';

# wait - table
$res = r->table('marvel')->wait->run;

is $res->type, 1, 'Correct response type';
is $res->response->{ready}, 1, 'Correct response type';

# clean up
r->db('test')->drop->run;
r->db('rethinkdb')->table('users')->get('chatapp')->delete->run;
r->db('rethinkdb')->table('users')->get('monitoring')->delete->run;
r->db('rethinkdb')->table('users')->get('bob')->delete->run;

done_testing();
