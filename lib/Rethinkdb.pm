package Rethinkdb;
use Rethinkdb::Base -base;

use Carp 'croak';
use Scalar::Util 'weaken';
use IO::Socket::INET;

use Rethinkdb::Database;
# use Rethinkdb::Query;
use Rethinkdb::Table;
use Rethinkdb::Protocol;
use Rethinkdb::Util;

our $VERSION = '0.01';

my $handle;

has host       => 'localhost';
has port       => 28015;
has default_db => 'test';
has 'handle';

sub import {
  my $class   = shift;
  my $package = caller;
  no strict;
  *{"$package\::r"} = \&r;
}

sub r {
  return __PACKAGE__->new( handle => $handle );
}

sub connect {
  my $self = shift;
  my $host = shift || 'localhost';
  my $port = shift || 28015;
  my $db   = shift || 'test';

  $handle = IO::Socket::INET->new(
    PeerHost => $host,
    PeerPort => $port,
    Reuse    => 1,
  ) or croak "ERROR: Could not connect to $host:$port";

  $handle->send( pack 'L<', VersionDummy::Version::V0_1 );

  $self->host($host);
  $self->port($port);
  $self->default_db($db);

  return $self->new(
    handle     => $handle,
    host       => $host,
    port       => $port,
    default_db => $db
  );
}

sub close {
  my $self = shift;

  $self->handle->close if $self->handle;

  return $self;
}

sub reconnect {
  my $self = shift;

  $handle = IO::Socket::INET->new(
    PeerHost => $self->host,
    PeerPort => $self->port,
    Reuse    => 1,
  ) or croak "ERROR: Could not reconnect to $self->host:$self->port";

  $handle->send( pack 'L<', VersionDummy::Version::V0_1 );

  $self->handle($handle);
  return $self;
}

sub use {
  my $self = shift;
  my $db   = shift;

  $self->default_db($db);
  return $self;
}


sub db_create {
  my $self = shift;
  my $name = shift;

  my $db = Rethinkdb::Database->new(
    rdb  => $self,
    name => $name,
  );

  weaken $db->{rdb};
  return $db->create();
}

sub db_drop {
  my $self = shift;
  my $name = shift;

  my $db = Rethinkdb::Database->new(
    rdb  => $self,
    name => $name,
  );

  weaken $db->{rdb};
  return $db->drop;
}

sub db_list {
  my $self = shift;

  my $db = Rethinkdb::Database->new(
    rdb => $self,
  );

  weaken $db->{rdb};
  return $db->list;
}

sub db {
  my $self = shift;
  my $name = shift;

  my $db = Rethinkdb::Database->new(
    rdb => $self,
    name => $name
  );

  weaken $db->{rdb};
  return $db;
}

sub table_create {
  my $self   = shift;
  my $name   = shift;
  my @params = @_;

  return Rethinkdb::Table->new(
    db   => $self->default_db,
    name => $name,
  )->create(@params);
}

sub table_drop {
  my $self = shift;
  my $name = shift;

  return Rethinkdb::Table->new(
    db   => $self->default_db,
    name => $name,
  )->drop;
}

sub table_list {
  my $self = shift;

  return Rethinkdb::Table->new(
    db => $self->default_db,
  )->list;
}

sub table {
  my $self = shift;
  my $name = shift;

  my $t = Rethinkdb::Table->new(
    rdb  => $self,
    db   => $self->default_db,
    name => $name,
  );

  weaken $t->{rdb};
  return $t;
}

# TODO: fix this
sub row {
  my $self = shift;
  return $self;
}

sub desc {
  my $self = shift;
  my $name = shift;

  return { attr => $name, ascending => 0 };
}

sub let {
  my $self = shift;
  croak 'let is not implemented';
}

sub letvar {
  my $self = shift;
  croak 'letvar is not implemented';
}

sub js {
  my $self = shift;
  croak 'js is not implemented';
}

sub asc {
  my $self = shift;
  my $name = shift;

  return { attr => $name, ascending => 1 };
}

sub expr {
  my $self  = shift;
  my $value = shift;

  if ( !ref $value ) {
    return Rethinkdb::Util::to_term($value);
  }

  if ( ref $value eq 'ARRAY' ) {
    return $self->_expr_array($value);
  }

  my $obj = [];
  foreach ( keys %{$value} ) {
    push @{$obj}, {
      var  => $_,
      term => $self->expr( $value->{$_} ) };
  }

  my $expr = {
    type   => 'Term::TermType::OBJECT',
    object => $obj
  };

  return $expr;
}

sub _expr_array {
  my $self   = shift;
  my $values = shift;

  my $list = [];
  foreach ( @{$values} ) {
    push @{$list}, $self->expr($_);
  }

  my $expr = {
    type  => 'Term::TermType::ARRAY',
    array => $list
  };

  return $expr;
}

sub true  { Rethinkdb::_True->new; }
sub false { Rethinkdb::_False->new; }

package Rethinkdb::_True;

use overload '""' => sub { 'true' },
  'bool' => sub { 1 },
  'eq' => sub { $_[1] eq 'true' ? 1 : 0; },
  '==' => sub { $_[1] == 1 ? 1 : 0; },
  fallback => 1;

sub new { bless {}, $_[0] }

package Rethinkdb::_False;

use overload '""' => sub { 'false' },
  'bool' => sub { 0 },
  'eq' => sub { $_[1] eq 'false' ? 1 : 0; },
  '==' => sub { $_[1] == 0 ? 1 : 0; },
  fallback => 1;

sub new { bless {}, $_[0] }

1;
