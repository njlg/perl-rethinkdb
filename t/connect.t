use Test::More;

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
TODO: {
  todo_skip 'Need to make testable', 9;

  $r = r->connect('wiggle');
  isnt $r->host,     'localhost';
  is $r->port,       28015;
  is $r->default_db, 'test';

  $r = r->connect( 'wiggle', 48015 );
  isnt $r->host,     'localhost';
  isnt $r->port,     28015;
  is $r->default_db, 'test';

  $r = r->connect( 'wiggle', 48015, 'best' );
  isnt $r->host,       'localhost';
  isnt $r->port,       28015;
  isnt $r->default_db, 'test';
}

# internal stuff
r->connect;
is r->io, undef;

r->connect->repl;
isa_ok r->io, 'Rethinkdb::IO';

# close connection
$conn = r->connect;
isa_ok $conn->close, 'Rethinkdb::IO';
is $conn->handle,    undef;

$conn = r->connect;
isa_ok $conn->close(noreply_wait => 0), 'Rethinkdb::IO';
is $conn->handle,    undef;

# reconnect
isa_ok $conn->reconnect, 'Rethinkdb::IO';
isa_ok $conn->handle,    'IO::Socket::INET';
is $conn->handle->peerport, 28015;
is $conn->handle->peerhost, '127.0.0.1';

isa_ok $conn->reconnect(noreply_wait => 0), 'Rethinkdb::IO';
isa_ok $conn->handle,    'IO::Socket::INET';
is $conn->handle->peerport, 28015;
is $conn->handle->peerhost, '127.0.0.1';

# switch default databases
$conn->use('test2');
is $conn->default_db, 'test2';

$conn->use('wiggle-waggle');
is $conn->default_db, 'wiggle-waggle';

# noreply_wait
my $res = $conn->noreply_wait;
is $res->type_description, 'wait_complete';

done_testing();
