package Rethinkdb::IO;
use Rethinkdb::Base -base;

use Carp 'croak';
use IO::Socket::INET;

use Rethinkdb::Protocol;
use Rethinkdb::Response;

has host       => 'localhost';
has port       => 28015;
has default_db => 'test';
has auth_key   => '';
has timeout    => 20;
has [ 'rdb', 'handle' ];


sub connect {
  my $self = shift;

  $self->{handle} = IO::Socket::INET->new(
    PeerHost => $self->host,
    PeerPort => $self->port,
    Reuse    => 1,
    Timeout  => $self->timeout,
    )
    or croak 'ERROR: Could not connect to ' . $self->host . ':' . $self->port;

  $self->handle->send( pack 'L<', VersionDummy::Version::V0_2 );
  $self->handle->send(
    ( pack 'L<', length $self->auth_key ) . $self->auth_key );

  my $response;
  my $char = '';
  do {
    $self->handle->recv( $char, 1 );
    $response .= $char;
  } while ( $char ne "\0" );

  return $self;
}

sub close {
  my $self = shift;

  $self->handle->close if $self->handle;

  return $self;
}

sub reconnect {
  my $self = shift;

  $self->{handle} = IO::Socket::INET->new(
    PeerHost => $self->host,
    PeerPort => $self->port,
    Reuse    => 1,
    )
    or croak 'ERROR: Could not reconnect to '
    . $self->host . ':'
    . $self->port;

  $self->handle->send( pack 'L<', VersionDummy::Version::V0_2 );
  $self->handle->send(
    ( pack 'L<', length $self->auth_key ) . $self->auth_key );

  return $self;
}

# put the handle into main package
sub repl {
  my $self    = shift;
  my $package = caller;

  $package::_rdb_io = $self;
  return $self;
}

sub use {
  my $self = shift;
  my $db   = shift;

  $self->default_db($db);
  return $self;
}

sub _start {
  my $self = shift;
  my ( $query, $args ) = @_;

  my $q = {
    type  => Query::QueryType::START,
    token => Rethinkdb::Util::token(),
    query => $query->build
  };

  return $self->_send($q);
}

sub _send {
  my $self  = shift;
  my $query = shift;

  if ( $ENV{RDB_DEBUG} ) {
    use feature ':5.10';
    use Data::Dumper;
    $Data::Dumper::Indent = 1;
    say {*STDERR} 'SENDING:';
    say {*STDERR} Dumper $query;
  }

  my $serial = Query->encode($query);

  my $length = pack 'L<', length $serial;

  # send message
  $self->handle->send( $length . $serial );

  # receive message
  my $data;
  $self->handle->recv( $length, 4 );
  $length = unpack 'L<', $length;
  $self->handle->recv( $data, $length );

  # decode RQL data
  my $res_data = Response->decode($data);

  # put data in response
  my $res = Rethinkdb::Response->init($res_data);

  if ( $ENV{RDB_DEBUG} ) {
    say {*STDERR} 'RECEIVED:';
    say {*STDERR} Dumper $res;
  }

  return $res;
}


1;
