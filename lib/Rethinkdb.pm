package Rethinkdb;
use Rethinkdb::Base -base;

use feature ':5.10';
use Data::Dumper;

use Carp 'croak';
use Scalar::Util 'weaken';

use Rethinkdb::IO;
use Rethinkdb::Database;
use Rethinkdb::Query;
use Rethinkdb::Table;
use Rethinkdb::Protocol;
use Rethinkdb::Util;

our $VERSION = '0.02';

# this is set only when connect->repl()
has 'io';

sub import {
  my $class   = shift;
  my $package = caller;

  no strict;
  *{"$package\::r"} = \&r;
}

sub r {
  my $package = caller;
  my $self;

  if( $package::_rdb_io ) {
    $self = __PACKAGE__->new(
      io => $package::_rdb_io
    );
  }
  else {
    $self = __PACKAGE__->new;
  }

  return $self;
}

sub connect {
  my $self     = shift;
  my $host     = shift || 'localhost';
  my $port     = shift || 28015;
  my $db       = shift || 'test';
  my $auth_key = shift || '';
  my $timeout  = shift || 20;

  my $io = Rethinkdb::IO->new(
    rdb      => $self,
    host     => $host,
    port     => $port,
    db       => $db,
    auth_key => $auth_key,
    timeout  => $timeout
  );

  weaken $io->{rdb};
  return $io->connect;
}

sub db_create {
  my $self = shift;
  my $name = shift;

  my $db = Rethinkdb::Database->new(
    rdb  => $self,
    args => $name,
  );

  weaken $db->{rdb};
  return $db->create;
}

sub db_drop {
  my $self = shift;
  my $name = shift;

  my $db = Rethinkdb::Database->new(
    rdb  => $self,
    args => $name,
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
    rdb  => $self,
    type => Term::TermType::DB,
    args => $name
  );

  weaken $db->{rdb};
  return $db;
}

sub table_create {
  my $self   = shift;
  my $name   = shift;
  my @params = @_;

  my $t = Rethinkdb::Table->new(
    rdb  => $self,
    args => $name,
  )->create(@params);

  weaken $t->{rdb};
  return $t;
}

sub table_drop {
  my $self = shift;
  my $name = shift;

  my $t = Rethinkdb::Table->new(
    rdb  => $self,
    args => $name,
  )->drop;

  weaken $t->{rdb};
  return $t;
}

sub table_list {
  my $self = shift;

  my $t = Rethinkdb::Table->new(
    rdb  => $self,
  )->list;

  weaken $t->{rdb};
  return $t;
}

sub table {
  my $self = shift;
  my $name = shift;

  my $t = Rethinkdb::Table->new(
    rdb  => $self,
    type => Term::TermType::TABLE,
    args => $name,
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

  return Rethinkdb::Util->to_datum($value);
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
