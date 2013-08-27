package Rethinkdb::Database;
use Rethinkdb::Base 'Rethinkdb::Query';

use Scalar::Util 'weaken';

has [qw{ rdb name }];

sub create {
  my $self = shift;
  my $name = shift || $self->name;

  my $q = Rethinkdb::Query->new(
    rdb  => $self->rdb,
    type => Term::TermType::DB_CREATE,
    args => $name,
  );

  weaken $q->{rdb};
  return $q;
}

sub drop {
  my $self = shift;
  my $name = shift || $self->name;

  my $q = Rethinkdb::Query->new(
    rdb  => $self->rdb,
    type => Term::TermType::DB_DROP,
    args => $name,
  );

  weaken $q->{rdb};
  return $q;
}

sub list {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb  => $self->rdb,
    type => Term::TermType::DB_LIST,
  );

  weaken $q->{rdb};
  return $q;
}

sub table_create {
  my $self   = shift;
  my $args   = shift;
  my $optargs = ref $_[0] ? $_[0] : {@_};

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::TABLE_CREATE,
    args    => $args,
    optargs => $optargs,
  );

  return $q;
}

sub table_drop {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::TABLE_DROP,
    args    => $args,
  );

  return $q;
}

sub table_list {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    _parent => $self,
    type    => Term::TermType::TABLE_LIST,
  );

  return $q;
}

sub table {
  my $self = shift;
  my $name = shift;

  my $t = Rethinkdb::Table->new(
    _parent => $self,
    type    => Term::TermType::TABLE,
    name    => $name,
    args    => $name,
  );

  return $t;
}

1;
