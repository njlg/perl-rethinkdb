package Rethinkdb;
use Rethinkdb::Base -base;

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
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => Term::TermType::DB_CREATE,
    args => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub db_drop {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => Term::TermType::DB_DROP,
    args => $args
  );

  weaken $q->{rdb};
  return $q;
}

sub db_list {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => Term::TermType::DB_LIST,
  );

  weaken $q->{rdb};
  return $q;
}

sub db {
  my $self = shift;
  my $name = shift;

  my $db = Rethinkdb::Database->new(
    rdb  => $self,
    type => Term::TermType::DB,
    name => $name,
    args => $name,
  );

  weaken $db->{rdb};
  return $db;
}

sub table_create {
  my $self   = shift;
  my $args   = shift;
  my $optargs = ref $_[0] ? $_[0] : {@_};

  my $q = Rethinkdb::Query->new(
    rdb     => $self,
    type    => Term::TermType::TABLE_CREATE,
    args    => $args,
    optargs => $optargs,
  );

  weaken $q->{rdb};
  return $q;
}

sub table_drop {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => Term::TermType::TABLE_DROP,
    args => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub table_list {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => Term::TermType::TABLE_LIST,
  );

  weaken $q->{rdb};
  return $q;
}

sub table {
  my $self = shift;
  my $name = shift;

  my $t = Rethinkdb::Table->new(
    rdb     => $self,
    type    => Term::TermType::TABLE,
    name    => $name,
    args    => $name,
  );

  weaken $t->{rdb};
  return $t;
}

sub row {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => Term::TermType::IMPLICIT_VAR,
  );

  weaken $q->{rdb};
  return $q;
}

sub asc {
  my $self = shift;
  my $name = shift;

  my $q = Rethinkdb::Query->new(
    type => Term::TermType::ASC,
    args => $name,
  );

  return $q;
}

sub desc {
  my $self = shift;
  my $name = shift;

  my $q = Rethinkdb::Query->new(
    type => Term::TermType::DESC,
    args => $name,
  );

  return $q;
}

sub js {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => Term::TermType::JAVASCRIPT,
    args => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub expr {
  my $self  = shift;
  my $value = shift;

  return Rethinkdb::Util->expr($value);
}

sub json {
  my $self  = shift;
  my $value = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => Term::TermType::JSON,
    args => $value,
  );

  weaken $q->{rdb};
  return $q;
}

sub count {
  my $self  = shift;

  return { COUNT => 1 };
}

sub sum {
  my $self = shift;
  my $attr = shift;

  return { SUM => $attr };
}

sub avg {
  my $self = shift;
  my $attr = shift;

  return { AVG => $attr };
}

# TODO: figure out why the arguments have to be reversed here
sub do {
  my $self = shift;
  my ($one, $two) = @_;

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => Term::TermType::FUNCALL,
    args => [$two, $one],
  );

  weaken $q->{rdb};
  return $q;
}

sub branch {
  my $self = shift;
  my ($predicate, $true, $false) = @_;

  $predicate = Rethinkdb::Util->wrap_func($predicate);
  $true = Rethinkdb::Util->wrap_func($true);
  $false = Rethinkdb::Util->wrap_func($false);

  my $q = Rethinkdb::Query->new(
    rdb  => $self,
    type => Term::TermType::BRANCH,
    args => [$predicate, $true, $false],
  );

  weaken $q->{rdb};
  return $q;
}

sub error {
  my $self = shift;
  my ($message) = @_;

  my $q = Rethinkdb::Query->new(
    type => Term::TermType::ERROR,
    args => $message,
  );

  return $q;
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
