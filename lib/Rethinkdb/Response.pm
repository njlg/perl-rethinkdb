package Rethinkdb::Response;
use Rethinkdb::Base -base;

use JSON::PP;
use Rethinkdb::Protocol;

has [qw{ type type_description response token error_message backtrace }];

sub init {
  my $class = shift;
  my $data  = shift;
  my $args  = {
    type => $data->{t},
    token => $data->{token},
  };

  my $types = {
     1 => 'success_atom',
     2 => 'success_sequence',
     3 => 'success_partial',
     5 => 'success_feed',
     4 => 'wait_complete',
    16 => 'client_error',
    17 => 'compile_error',
    18 => 'runtime_error',
  };

  $args->{type_description} = $types->{$data->{t}};

  my $response = [];
  if ( $data->{r} ) {
    foreach ( @{ $data->{r} } ) {
      # push @{$response}, Rethinkdb::Util->from_datum($_);
      push @{$response}, $_;
    }
  }

  # not sure about this:
  if ( $data->{t} == 1 ) {
    $response = $response->[0];
  }

  if( ref $response eq 'HASH' && $response->{'$reql_type$'} && $response->{'$reql_type$'} eq 'GROUPED_DATA' ) {
    my $group = {};

    foreach( @{$response->{data}} ) {
      $group->{$_->[0]} = $_->[1];
    }

    $response = $group;
  }

  $args->{response} = $response;

  if ( $data->{b} ) {
    $args->{backtrace} = $data->{b};
  }

  return $class->new($args);
}

1;
