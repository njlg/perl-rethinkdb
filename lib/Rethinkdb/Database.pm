package Rethinkdb::Database;
use Rethinkdb::Base -base;

use Scalar::Util 'weaken';
use Rethinkdb::Query;

has [qw{rdb name}];

sub create {
  my $self = shift;
  my $name = shift || $self->name;

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::META,
        token      => Rethinkdb::Util::token(),
        meta_query => {
          type    => MetaQuery::MetaQueryType::CREATE_DB,
          db_name => $name
        } } ) );

  weaken $q->{rdb};
  return $q;
}

sub drop {
  my $self = shift;
  my $name = shift || $self->name;

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::META,
        token      => Rethinkdb::Util::token(),
        meta_query => {
          type    => MetaQuery::MetaQueryType::DROP_DB,
          db_name => $name
        } } ) );

  weaken $q->{rdb};
  return $q;
}

sub list {
  my $self = shift;

  # token needs to be random? unique per connection
  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::META,
        token      => Rethinkdb::Util::token(),
        meta_query => { type => MetaQuery::MetaQueryType::LIST_DBS } } ) );

  weaken $q->{rdb};
  return $q;
}

sub table_create {
  my $self   = shift;
  my $name   = shift;
  my @params = @_;

  my $t = Rethinkdb::Table->new(
    rdb  => $self->rdb,
    db   => $self->name,
    name => $name,
  );

  weaken $t->{rdb};
  return $t->create(@params);
}

sub table_drop {
  my $self = shift;
  my $name = shift;

  my $t = Rethinkdb::Table->new(
    rdb  => $self->rdb,
    db   => $self->name,
    name => $name,
  );

  weaken $t->{rdb};
  return $t->drop;
}

sub table_list {
  my $self = shift;

  my $t = Rethinkdb::Table->new(
    rdb => $self->rdb,
    db  => $self->name,
  );

  weaken $t->{rdb};
  return $t->list;
}

sub table {
  my $self = shift;
  my $name = shift;

  my $t = Rethinkdb::Table->new(
    rdb  => $self->rdb,
    db   => $self->name,
    name => $name,
  );

  weaken $t->{rdb};
  return $t;
}

1;
