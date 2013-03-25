package Rethinkdb::Response;
use Rethinkdb::Base -base;

use JSON::PP;
use Rethinkdb::Protocol;

has [qw{ status_code response token error_message backtrace }];

sub init {
  my $class = shift;
  my $data = shift;
  my $args = {
    status_code => $data->status_code,
    response => $data->response,
    token => $data->token,
  };

  if( $data->status_code == 1 || ($data->status_code == 3 && $data->response) ) {
    my $json = JSON::PP->new->allow_nonref(1);
    if( ! ref $data->response ) {
      $args->{response} = $json->decode($data->response);
    }
    elsif( ref $data->response eq 'ARRAY' ) {
      $args->{response} = [];
      foreach( @{$data->response} ) {
        push @{$args->{response}}, $json->decode($_);
      }
    }
  }

  if( $data->error_message ) {
    $args->{error_message} = $data->error_message;
  }

  if( $data->backtrace ) {
    $args->{backtrace} = $data->backtrace;
  }

  return $class->new($args);
}

1;