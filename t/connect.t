use Test::More;

use Rethinkdb;

# initialization
my $r = Rethinkdb->new;
isa_ok $r, 'Rethinkdb';

$r = r;
isa_ok $r, 'Rethinkdb';

$r = r->connect;
isa_ok $r, 'Rethinkdb';

# connect default values
is $r->host, 'localhost';
is $r->port, 28015;
is $r->default_db, 'test';

# other values for connect
TODO: {
	todo_skip 'Need to make testable', 9;

	$r = r->connect('wiggle');
	isnt $r->host, 'localhost';
	is $r->port, 28015;
	is $r->default_db, 'test';

	$r = r->connect('wiggle', 48015);
	isnt $r->host, 'localhost';
	isnt $r->port, 28015;
	is $r->default_db, 'test';

	$r = r->connect('wiggle', 48015, 'best');
	isnt $r->host, 'localhost';
	isnt $r->port, 28015;
	isnt $r->default_db, 'test';
}

# internal stuff
r->connect;
isa_ok r->handle, 'IO::Socket::INET';

# close connection
isa_ok r->close, 'Rethinkdb';
isa_ok r->handle, 'IO::Socket::INET';
is r->handle->peerport, undef;
is r->handle->peerhost, undef;

# reconnect
isa_ok r->reconnect, 'Rethinkdb';
isa_ok r->handle, 'IO::Socket::INET';
is r->handle->peerport, 28015;
is r->handle->peerhost, '127.0.0.1';

# switch default databases
$r = r->use('test2');
isa_ok $r, 'Rethinkdb';
is $r->default_db, 'test2';

$r = r->use('wiggle-waggle');
isa_ok $r, 'Rethinkdb';
is $r->default_db, 'wiggle-waggle';

done_testing();