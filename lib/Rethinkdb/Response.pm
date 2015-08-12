package Rethinkdb::Response;
use Rethinkdb::Base -base;

use JSON::PP;
use Rethinkdb::Protocol;

has [qw{ type type_description response token error_type backtrace profile }];

sub _init {
  my $class = shift;
  my $data  = shift;
  my $args  = { type => $data->{t}, token => $data->{token}, };

  my $types = {
    1  => 'success_atom',
    2  => 'success_sequence',
    3  => 'success_partial',
    4  => 'wait_complete',
    16 => 'client_error',
    17 => 'compile_error',
    18 => 'runtime_error',
  };

  $args->{type_description} = $types->{ $data->{t} };

  my $response = [];
  if ( $data->{r} ) {
    foreach ( @{ $data->{r} } ) {
      push @{$response}, $_;
    }
  }

  # not sure about this:
  if ( $data->{t} == 1 ) {
    $response = $response->[0];
  }

  if ( ref $response eq 'HASH'
    && $response->{'$reql_type$'}
    && $response->{'$reql_type$'} eq 'GROUPED_DATA' )
  {
    my $group = {};

    foreach ( @{ $response->{data} } ) {
      $group->{ $_->[0] } = $_->[1];
    }

    $response = $group;
  }

  $args->{response} = $response;

  if ( $data->{b} ) {
    $args->{backtrace} = $data->{b};
  }

  if ( $data->{p} ) {
    $args->{profile} = $data->{p};
  }

  if ( $data->{e} ) {
    $args->{error_type} = $data->{e};
  }

  return $class->new($args);
}

1;

=encoding utf8

=head1 NAME

Rethinkdb::Response - RethinkDB Response

=head1 SYNOPSIS

  package MyApp;
  use Rethinkdb;

  my $res = r->table('marvel')->run;
  say $res->type;
  say $res->type_description;
  say $res->response;
  say $res->token;
  say $res->error_type;
  say $res->profile;
  say $res->backtrace;

=head1 DESCRIPTION

All responses from the driver come as an instance of this class.

=head1 ATTRIBUTES

L<Rethinkdb::Response> implements the following attributes.

=head2 type

  my $res = r->table('marvel')->run;
  say $res->type;

The response type code. The current response types are:

  'success_atom' => 1
  'success_sequence' => 2
  'success_partial' => 3
  'success_feed' => 5
  'wait_complete' => 4
  'client_error' => 16
  'compile_error' => 17
  'runtime_error' => 18

=head2 type_description

  my $res = r->table('marvel')->run;
  say $res->type_description;

The response type description (e.g. C<success_atom>, C<runtime_error>).

=head2 response

  use Data::Dumper;
  my $res = r->table('marvel')->run;
  say Dumper $res->response;

The actual response value from the database.

=head2 token

  my $res = r->table('marvel')->run;
  say Dumper $res->token;

Each request made to the database must have a unique token. The response from
the database includes that token incase further actions are required.

=head2 error_type

  my $res = r->table('marvel')->run;
  say $res->error_type;

If the request cause an error, this attribute will contain the error message
from the database.

=head2 backtrace

  my $res = r->table('marvel')->run;
  say $res->backtrace;

If the request cause an error, this attribute will contain a backtrace for the
error.

=head2 profile

  my $res = r->table('marvel')->run;
  say $res->profile;

If profiling information was requested as a global argument for a query, then
this attribute will contain that profiling data.

=head1 SEE ALSO

L<Rethinkdb>, L<http://rethinkdb.com>

=cut
