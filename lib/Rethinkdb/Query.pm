package Rethinkdb::Query;
use Rethinkdb::Base -base;

use Scalar::Util 'weaken';

use Rethinkdb::Protocol;
use Rethinkdb::Response;

has [qw{rdb query}];

sub run {
  my $self = shift;

  my $length = pack 'L<', length $self->query;

  # send message
  $self->rdb->handle->send( $length . $self->query );

  # receive message
  my $data;
  $self->rdb->handle->recv( $length, 4 );
  $length = unpack 'L<', $length;
  $self->rdb->handle->recv( $data, $length );

  # decode RQL data
  my $res_data = Response->decode($data);

  # put data in response
  my $res = Rethinkdb::Response->init($res_data);

  return $res;
}

1;