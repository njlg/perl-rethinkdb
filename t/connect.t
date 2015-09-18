use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use Rethinkdb;

# initialization
my $r = Rethinkdb->new;
isa_ok $r, 'Rethinkdb';

$r = r;
isa_ok $r, 'Rethinkdb';

my $conn = r->connect;
isa_ok $conn, 'Rethinkdb::IO';

# connect default values
is $conn->host,       'localhost';
is $conn->port,       28015;
is $conn->default_db, 'test';
is $conn->auth_key,   '';
is $conn->timeout,    20;

# other values for connect
eval { my $r = r->connect('wiggle'); } or do {
  like $@, qr/ERROR: Could not connect to wiggle:28015/,
    'Correct host connection error message';
};

eval { my $r = r->connect( 'localhost', 48015 ); } or do {
  like $@, qr/ERROR: Could not connect to localhost:48015/,
    'Correct host connection error message';
};

$r = r->connect( 'localhost', 28015, 'better' );
isnt $r->default_db, 'test';
is $r->default_db, 'better', 'Correct `default_db` set';

# test auth_key
eval { r->connect( 'localhost', 28015, 'better', 'hiddenkey' ); } or do {
  like $@, qr/ERROR: Incorrect authorization key./,
    'Correct `auth_key` connection error message';
};

my $r = r->connect( 'localhost', 28015, 'better', '', 100 );
is $r->timeout, 100, 'Correct timeout set';

# internal stuff
r->connect;
is r->io, undef;

r->connect->repl;
isa_ok r->io, 'Rethinkdb::IO';

# close connection
$conn = r->connect;
isa_ok $conn->close, 'Rethinkdb::IO';
is $conn->_handle,   undef;

$conn = r->connect;
isa_ok $conn->close( noreply_wait => 0 ), 'Rethinkdb::IO';
is $conn->_handle, undef;

# reconnect
isa_ok $conn->reconnect, 'Rethinkdb::IO';
isa_ok $conn->_handle,   'IO::Socket::INET';
is $conn->_handle->peerport, 28015;
is $conn->_handle->peerhost, '127.0.0.1';

isa_ok $conn->reconnect( noreply_wait => 0 ), 'Rethinkdb::IO';
isa_ok $conn->_handle, 'IO::Socket::INET';
is $conn->_handle->peerport, 28015;
is $conn->_handle->peerhost, '127.0.0.1';

# switch default databases
$conn->use('test2');
is $conn->default_db, 'test2';

$conn->use('wiggle-waggle');
is $conn->default_db, 'wiggle-waggle';

# noreply_wait
my $res = $conn->noreply_wait;
is $res->type_description, 'wait_complete';

# testing run parameters

# profile
$res
  = r->db('rethinkdb')->table('logs')->nth(0)->run( { profile => r->true } );
isa_ok $res->profile, 'ARRAY', 'Correctly received profile data';

# durability (no real way to test the output)
r->db('test')->drop->run;
r->db('test')->create->run( { durability => 'soft' } );

r->db('test')->table('battle')->create->run;
r->db('test')->table('battle')->insert(
  [
    {
      id           => 1,
      superhero    => 'Iron Man',
      target       => 'Mandarin',
      damage_dealt => 100,
    },
    {
      id           => 2,
      superhero    => 'Wolverine',
      target       => 'Sabretooth',
      damage_dealt => 40,
    },
    {
      id           => 3,
      superhero    => 'Iron Man',
      target       => 'Magneto',
      damage_dealt => 90,
    },
    {
      id           => 4,
      superhero    => 'Wolverine',
      target       => 'Magneto',
      damage_dealt => 10,
    },
    {
      id           => 5,
      superhero    => 'Spider-Man',
      target       => 'Green Goblin',
      damage_dealt => 20,
    }
  ]
)->run;

# group_format
$res = r->db('test')->table('battle')->group('superhero')->run( { group_format => 'raw' } );

is $res->response->{'$reql_type$'}, 'GROUPED_DATA', 'Correct group_format response data';
isa_ok $res->response->{data}, 'ARRAY', 'Correct group_format response data';
isa_ok $res->response->{data}[0][1], 'ARRAY', 'Correct group_format response data';

# db
$res = r->table('cluster_config')->run({db => 'rethinkdb'});
ok ($res->response->[0]->{id} eq 'auth' or $res->response->[0]->{id} eq 'heartbeat'), 'Correct response for db change';

# array_limit (doesn't seem to change response)
r->db('test')->table('battle')->run({array_limit => 2});

# noreply
$res = r->db('test')->table('battle')->run({noreply => 1});
is $res, undef, 'Correct response for noreply';

# clean up
r->db('test')->drop->run;

done_testing();
