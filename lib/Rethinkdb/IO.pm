package Rethinkdb::IO;
use Rethinkdb::Base -base;

use Carp 'croak';
use IO::Socket::INET;
use JSON::PP;

use Rethinkdb::Protocol;
use Rethinkdb::Response;

has host       => 'localhost';
has port       => 28015;
has default_db => 'test';
has auth_key   => '';
has timeout    => 20;
has [ 'rdb', 'handle' ];
has 'protocol' => sub { Rethinkdb::Protocol->new; };

sub connect {
  my $self = shift;

  $self->{handle} = IO::Socket::INET->new(
    PeerHost => $self->host,
    PeerPort => $self->port,
    Reuse    => 1,
    Timeout  => $self->timeout,
    )
    or croak 'ERROR: Could not connect to ' . $self->host . ':' . $self->port;

  $self->handle->send( pack 'L<',
    $self->protocol->versionDummy->version->v0_3 );
  $self->handle->send(
    ( pack 'L<', length $self->auth_key ) . $self->auth_key );

  $self->handle->send( pack 'L<',
    $self->protocol->versionDummy->protocol->json );

  my $response;
  my $char = '';
  do {
    $self->handle->recv( $char, 1 );
    $response .= $char;
  } while ( $char ne "\0" );

  # trim string
  $response =~ s/^\s//;
  $response =~ s/\s$//;

  if ( $response eq 'SUCCESS' ) {
    croak "ERROR: Unable to connect to the database";
  }

  return $self;
}

sub close {
  my $self = shift;
  my $args = ref $_[0] ? $_[0] : {@_};

  if( $self->handle ) {
    if( !defined $args->{noreply_wait} || !$args->{noreply_wait} ) {
      say 'noreply_wait';
      $self->noreply_wait;
    }

    $self->handle->close;
    $self->handle(undef);
  }

  return $self;
}

sub reconnect {
  my $self = shift;
  my $args = ref $_[0] ? $_[0] : {@_};

  say 'reconnecting';

  return $self->close($args)->connect;
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

sub noreply_wait {
  my $self = shift;

  return $self->_send(
    {
      type  => $self->protocol->query->queryType->noreply_wait,
      token => Rethinkdb::Util::token(),
    }
  );
}

sub _start {
  my $self = shift;
  my ( $query, $args ) = @_;

  my $q = {
    type  => $self->protocol->query->queryType->start,
    token => Rethinkdb::Util::token(),
    query => $query->build
  };

  return $self->_send($q);
}

sub _encode {
  my $self = shift;
  my $data = shift;

  # only QUERY->START needs these:
  if ( $data->{type} == 1 ) {
    $data = $self->_encode_recurse($data);
    push @{$data}, {};
  }
  else {
    $data = [ $data->{type} ];
  }

  return encode_json $data;
}

sub _encode_recurse {
  my $self = shift;
  my $data = shift;
  my $json = [];

  if ( $data->{datum} ) {
    my $val = q{};
    if ( defined $data->{datum}->{r_bool} ) {
      if ( $data->{datum}->{r_bool} ) {
        return JSON::PP::true;
      }
      else {
        return JSON::PP::false;
      }
    }
    else {
      foreach ( keys %{ $data->{datum} } ) {
        if ( $_ ne 'type' ) {
          return $data->{datum}->{$_};
        }
      }
    }
  }

  if ( $data->{type} ) {
    push @{$json}, $data->{type};
  }

  if ( $data->{query} ) {
    push @{$json}, $self->_encode_recurse( $data->{query} );
  }

  if ( $data->{args} ) {
    my $args = [];
    foreach ( @{ $data->{args} } ) {
      push @{$args}, $self->_encode_recurse($_);
    }

    push @{$json}, $args;
  }

  if ( $data->{optargs} ) {
    my $args = {};
    foreach ( @{ $data->{optargs} } ) {
      $args->{ $_->{key} } = $self->_encode_recurse( $_->{val} );
    }

    if ( $data->{type} == $self->protocol->term->termType->make_obj ) {
      return $args;
    }

    push @{$json}, $args;
  }

  return $json;
}

sub _decode {
  my $self   = shift;
  my $data   = shift;
  my $decode = decode_json $data;
  my $clean  = [];

  foreach ( @{ $decode->{r} } ) {
    if ( ref $_ eq 'JSON::PP::Boolean' ) {
      if ($_) {
        push @{$clean}, $self->rdb->true;
      }
      else {
        push @{$clean}, $self->rdb->false;
      }
    }
    else {
      push @{$clean}, $_;
    }
  }

  $decode->{r} = $clean;
  return $decode;
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

  my $token;
  my $length;

  # croak 'dying';
  my $serial = $self->_encode($query);
  my $header = pack 'QL<', $query->{token}, length $serial;

  if ( $ENV{RDB_DEBUG} ) {
    say {*STDERR} Dumper $serial;
  }

  # send message
  $self->handle->send( $header . $serial );

  # receive message
  my $data;

  $self->handle->recv( $token, 8 );
  $token = unpack 'Q<', $token;

  $self->handle->recv( $length, 4 );
  $length = unpack 'L<', $length;

  $self->handle->recv( $data, $length );

  # decode RQL data
  my $res_data = $self->_decode($data);
  $res_data->{token} = $token;

  # fetch the rest of the data if steam/partial
  if ( $res_data->{t} == 3 ) {
    my $more
      = $self->_send(
      { type => $self->protocol->query->queryType->continue, token => $token }
      );

    push @{ $res_data->{r} }, @{ $more->response };
    $res_data->{t} = $more->type;
  }

  # put data in response
  my $res = Rethinkdb::Response->init($res_data);

  if ( $ENV{RDB_DEBUG} ) {
    say {*STDERR} 'RECEIVED:';
    say {*STDERR} Dumper $res;
  }

  return $res;
}

1;

=encoding utf8

=head1 NAME

Rethinkdb::IO - RethinkDB IO

=head1 SYNOPSIS

  package MyApp;
  use Rethinkdb::IO;

  my $io = Rethinkdb::IO->new->connect;
  $io->use('marvel');
  $io->close;

=head1 DESCRIPTION

This module handles communicating with the RethinkDB Database.

=head1 ATTRIBUTES

L<Rethinkdb::IO> implements the following attributes.

=head2 host

  my $io = Rethinkdb::IO->new->connect;
  my $host = $io->host;
  $io->host('r.example.com');

The C<host> attribute returns or sets the current host name that
L<Rethinkdb::IO> is currently set to use.

=head2 port

  my $io = Rethinkdb::IO->new->connect;
  my $port = $io->port;
  $io->port(1212);

The C<port> attribute returns or sets the current port number that
L<Rethinkdb::IO> is currently set to use.

=head2 default_db

  my $io = Rethinkdb::IO->new->connect;
  my $port = $io->default_db;
  $io->default_db('marvel');

The C<default_db> attribute returns or sets the current database name that
L<Rethinkdb::IO> is currently set to use.

=head2 auth_key

  my $io = Rethinkdb::IO->new->connect;
  my $port = $io->auth_key;
  $io->auth_key('setec astronomy');

The C<auth_key> attribute returns or sets the current authentication key that
L<Rethinkdb::IO> is currently set to use.

=head2 timeout

  my $io = Rethinkdb::IO->new->connect;
  my $timeout = $io->timeout;
  $io->timeout(60);

The C<timeout> attribute returns or sets the timeout length that
L<Rethinkdb::IO> is currently set to use.

=head1 METHODS

L<Rethinkdb> inherits all methods from L<Rethinkdb::Base> and implements the
following methods.

=head2 connect

  my $io = Rethinkdb::IO->new;
  $io->host('rdb.example.com');
  $io->connect->repl;

The C<connect> method initiates the connection to the RethinkDB database.

=head2 close

  my $io = Rethinkdb::IO->new;
  $io->host('rdb.example.com');
  $io->connect;
  $io->close;

The C<connect> method closes the current connection to the RethinkDB database.

=head2 reconnect

  my $io = Rethinkdb::IO->new;
  $io->host('rdb.example.com');
  $io->connect;
  $io->reconnect;

The C<reconnect> method closes and reopens a connection to the RethinkDB
database.

=head2 repl

  my $io = Rethinkdb::IO->new;
  $io->host('rdb.example.com');
  $io->connect->repl;

The C<repl> method caches the current connection in to the main program so that
it is available to for all L<Rethinkdb> queries without specifically specifying
one.

=head2 use

  my $io = Rethinkdb::IO->new;
  $io->use('marven');
  $io->connect;

The C<use> method sets the default database name to use for all queries that
use this connection.

=head2 noreply_wait

  my $io = Rethinkdb::IO->new;
  $io->noreply_wait;

The C<noreply_wait> method will tell the database to wait until all "no reply"
have executed before responding.
