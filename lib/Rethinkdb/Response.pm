package Rethinkdb::Response;
use Rethinkdb::Base -base;

use JSON::PP;
use Rethinkdb::Protocol;

has [qw{ type response token error_message backtrace }];

sub init {
  my $class = shift;
  my $data  = shift;
  my $args  = { type => $data->type, token => $data->token, };

  my $response = [];
  if ( $data->response ) {
    foreach ( @{ $data->response } ) {
      push @{$response}, Rethinkdb::Util->from_datum($_);
    }
  }

  # not sure about this:
  if ( $data->type == 1 ) {
    $response = $response->[0];
  }

  $args->{response} = $response;

  if ( $data->backtrace ) {
    $args->{backtrace} = $data->backtrace;
  }

  return $class->new($args);
}

1;
