package Rethinkdb::Query;
use Rethinkdb::Base -base;

use Scalar::Util 'weaken';

use Rethinkdb::Protocol;
use Rethinkdb::Response;

has [qw{rdb query}];

sub run {
  my $self = shift;

use feature ':5.10';
use Data::Dumper;
$Data::Dumper::Indent = 1;
say Dumper(Query->decode($self->query));

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

# use feature ':5.10';
# use Data::Dumper;
# $Data::Dumper::Indent = 1;
# say '---';
# say Dumper $res_data;
# say '---';

  # put data in response
  my $res = Rethinkdb::Response->init($res_data);

  return $res;
}

1;
