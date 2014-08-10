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

  $self->handle->send( pack 'L<', $self->protocol->versionDummy->version->v0_3 );
  $self->handle->send(
    ( pack 'L<', length $self->auth_key ) . $self->auth_key );

  $self->handle->send( pack 'L<', $self->protocol->versionDummy->protocol->json );

  my $response;
  my $char = '';
  do {
    $self->handle->recv( $char, 1 );
    $response .= $char;
  } while ( $char ne "\0" );

  # trim string
  $response =~ s/^\s//;
  $response =~ s/\s$//;

  if( $response eq 'SUCCESS' ) {
    croak "ERROR: Unable to connect to the database";
  }

  return $self;
}

sub close {
  my $self = shift;

  $self->handle->close if $self->handle;

  return $self;
}

sub reconnect {
  return (shift)->connect;
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
  if( $data->{type} == 1 ) {
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

  # say 'rec what?';
  # say Dumper $data;

  if( $data->{datum} ) {
    my $val = q{};
    if( defined $data->{datum}->{r_bool} ) {
      if( $data->{datum}->{r_bool} ) {
        return JSON::PP::true;
      }
      else {
        return JSON::PP::false;
      }
    }
    else {
      foreach (keys %{$data->{datum}}) {
        if( $_ ne 'type' ) {
          return $data->{datum}->{$_};
        }
      }
    }
  }

  if( $data->{type} ) {
    push @{$json}, $data->{type};
  }

  if( $data->{query} ) {
    push @{$json}, $self->_encode_recurse($data->{query});
  }

  if( $data->{args} ) {
    my $args = [];
    foreach ( @{$data->{args}} ) {
      push @{$args}, $self->_encode_recurse($_);
    }

    push @{$json}, $args;
  }

  if( $data->{optargs} ) {
    my $args = {};
    foreach ( @{$data->{optargs}} ) {
      $args->{$_->{key}} = $self->_encode_recurse($_->{val});
    }

    if( $data->{type} == $self->protocol->term->termType->make_obj ) {
      return $args;
    }

    push @{$json}, $args;
  }

  return $json;
}

sub _decode {
  my $self = shift;
  my $data = shift;
  my $decode = decode_json $data;
  my $clean = [];

  foreach( @{$decode->{r}} ) {
    if( ref $_ eq 'JSON::PP::Boolean' ) {
      if( $_ ) {
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
  $self->handle->send( $header . $serial  );

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
  if( $res_data->{t} == 3 ) {
    my $more = $self->_send({
      type  => $self->protocol->query->queryType->continue,
      token => $token
    });

    push @{$res_data->{r}}, @{$more->response};
    $res_data->{t} = $more->type;
  }

  # put data in response
  my $res = Rethinkdb::Response->init($res_data);

  if ( $ENV{RDB_DEBUG} ) {
    say {*STDERR} 'RECEIVED:';
    say {*STDERR} Dumper $data;
    say {*STDERR} Dumper $res;
  }

  return $res;
}

1;
