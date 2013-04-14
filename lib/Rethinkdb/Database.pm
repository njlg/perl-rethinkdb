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
        type  => Query::QueryType::START,
        token => Rethinkdb::Util::token(),
        query => {
          type  => Term::TermType::DB_CREATE,
          args => {
            type => Term::TermType::DATUM,
            datum => Rethinkdb::Util->to_datum($name)
          }
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

sub drop {
  my $self = shift;
  my $name = shift || $self->name;

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type  => Query::QueryType::START,
        token => Rethinkdb::Util::token(),
        query => {
          type  => Term::TermType::DB_DROP,
          args => {
            type => Term::TermType::DATUM,
            datum => Rethinkdb::Util->to_datum($name)
          }
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

sub list {
  my $self = shift;

  # token needs to be random? unique per connection
  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type  => Query::QueryType::START,
        token => Rethinkdb::Util::token(),
        query => {
          type => Term::TermType::DB_LIST
        }
      }
    )
  );

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
