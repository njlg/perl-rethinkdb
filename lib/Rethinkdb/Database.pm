package Rethinkdb::Database;
use Rethinkdb::Base 'Rethinkdb::Query';

use Scalar::Util 'weaken';

has [qw{rdb name}];

sub create {
  my $self = shift;
  my $name = shift || $self->name;

  $self->type(Term::TermType::DB_CREATE);

  if( $name ) {
    $self->_args($name);
  }

  return $self;
}

sub drop {
  my $self = shift;
  my $name = shift || $self->name;

  $self->type(Term::TermType::DB_DROP);

  if( $name ) {
    $self->_args($name);
  }

  return $self;
}

sub list {
  my $self = shift;

  $self->type(Term::TermType::DB_LIST);

  return $self;
}

sub table_create {
  my $self   = shift;
  my $name   = shift;
  my @params = @_;

  my $t = Rethinkdb::Table->new(
    rdb     => $self->rdb,
    args    => $name,
    _parent => $self,
  );

  weaken $t->{rdb};
  return $t->create(@params);
}

sub table_drop {
  my $self = shift;
  my $name = shift;

  my $t = Rethinkdb::Table->new(
    rdb     => $self->rdb,
    _parent => $self,
    args    => $name,
  );

  weaken $t->{rdb};
  return $t->drop;
}

sub table_list {
  my $self = shift;

  my $t = Rethinkdb::Table->new(
    rdb     => $self->rdb,
    _parent => $self,
  );

  weaken $t->{rdb};
  return $t->list;
}

sub table {
  my $self = shift;
  my $name = shift;

  my $t = Rethinkdb::Table->new(
    rdb     => $self->rdb,
    type    => Term::TermType::TABLE,
    args    => $name,
    _parent => $self,
  );

  weaken $t->{rdb};
  return $t;
}

1;
