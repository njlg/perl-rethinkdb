package Rethinkdb::Query;
use Rethinkdb::Base -base;

use Carp 'croak';
use Scalar::Util 'weaken';

use Rethinkdb;
use Rethinkdb::Protocol;

has [qw{rdb args optargs type _parent}];

sub new {
  my $class = shift;
  my $self = bless @_ ? @_ > 1 ? {@_} : { %{ $_[0] } } : {}, ref $class || $class;

  # process args and optargs
  $self->_args;
  $self->_optargs;

  return $self;
}

sub build {
  my $self = shift;

  my $q = {
    type => $self->type
  };

  if( $self->_parent && $self->_parent->can('build') ) {
    push @{$q->{args}}, $self->_parent->build;
  }

   if( $self->args ) {
    foreach( @{$self->args} ) {
      if( ref $_ && UNIVERSAL::can($_,'can') && $_->can('build') ) {
        push @{$q->{args}}, $_->build;
      }
      else {
        push @{$q->{args}}, $_;
      }
    }
  }

  if( $self->optargs ) {
    foreach( keys %{$self->optargs} ) {
      my $value = $self->{optargs}->{$_};
      if( ref $value && UNIVERSAL::can($value,'can') && $value->can('build') ) {
        push @{$q->{optargs}}, {
          key => $_,
          val => $value->build
        };
      }
      else {
        push @{$q->{optargs}}, {
          key => $_,
          val => $value
        }
      }
    }
  }

  return $q;
}

sub _args {
  my $self = shift;
  my $args = $self->args;
  delete $self->{args};

  if( $args ) {
    if( ref $args ne 'ARRAY' ) {
      $args = [$args];
    }

    my $expr_args = [];

    foreach( @{$args} ) {
      push @{$expr_args}, Rethinkdb::Util->expr($_);
    }

    $self->args($expr_args);
  }

  return;
}

sub _optargs {
  my $self = shift;
  my $optargs = $self->optargs;
  delete $self->{optargs};

  if( $optargs ) {
    if( ref $optargs ) {
      my $expr_optargs = {};

      foreach( keys %{$optargs} ) {
        $expr_optargs->{$_} = Rethinkdb::Util->expr($optargs->{$_});
      }

      $self->optargs($expr_optargs);
    }
  }

  return;
}

sub run {
  my $self       = shift;
  my ($connection, $args) = @_;

  if( ref $connection ne 'Rethinkdb::IO' ) {
    $args = $connection;
    if( $self->rdb->io ) {
      $connection = $self->rdb->io;
    }
    else {
      croak 'ERROR: run() was not given a connection';
    }
  }

  return $connection->_start($self, $args);
}

sub update {
  my $self    = shift;
  my $args    = shift;
  my $optargs = @_ ? @_ > 1 ? {@_} : { %{ $_[0] } } : {};

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::UPDATE,
    args    => $args,
    optargs => $optargs,
  );

  weaken $q->{rdb};
  return $q;
}

sub replace {
  my $self = shift;
  my $args = shift;
  my $optargs = @_ ? @_ > 1 ? {@_} : { %{ $_[0] } } : {};

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::REPLACE,
    args    => $args,
    optargs => $optargs,
  );

  weaken $q->{rdb};
  return $q;
}

sub merge {
  my $self = shift;
  my $doc  = shift;

  if ( ref $doc ne __PACKAGE__ ) {
    croak 'merge requires a Rethinkdb::Query';
  }

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::MERGE,
    args    => [$self, $doc]
  );

  weaken $q->{rdb};
  return $q;
}

sub has_fields {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::HAS_FIELDS,
    args    => $args
  );

  weaken $q->{rdb};
  return $q;
}

# TODO: replace this with AUTOLOAD or overload %{}
# to get something like r->table->get()->{attr}->run;
# or like r->table->get()->attr->run;
sub attr {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::GET_FIELD,
    args    => $args
  );

  weaken $q->{rdb};
  return $q;
}

sub append {
  my $self = shift;
  # my $args = @_ ? @_ > 1 ? [@_] : [ @{ $_[0] } ] : [];
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::APPEND,
    args    => $args
  );

  weaken $q->{rdb};
  return $q;
}

sub prepend {
  my $self = shift;
  # my $args = @_ ? @_ > 1 ? [@_] : [ @{ $_[0] } ] : [];
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::PREPEND,
    args    => $args
  );

  weaken $q->{rdb};
  return $q;
}

sub difference {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::DIFFERENCE,
    args    => [$args],
  );

  weaken $q->{rdb};
  return $q;
}

sub set_insert {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::SET_INSERT,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub set_union {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::SET_UNION,
    args    => [$args],
  );

  weaken $q->{rdb};
  return $q;
}

sub set_intersection {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::SET_INTERSECTION,
    args    => [$args],
  );

  weaken $q->{rdb};
  return $q;
}

sub set_difference {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::SET_DIFFERENCE,
    args    => [$args],
  );

  weaken $q->{rdb};
  return $q;
}

sub pluck {
  my $self = shift;
  my $args = @_ ? @_ > 1 ? [@_] : [ @{ $_[0] } ] : [];

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::PLUCK,
    args    => $args
  );

  weaken $q->{rdb};
  return $q;
}

sub without {
  my $self = shift;
  my $args = @_ ? @_ > 1 ? [@_] : [ @{ $_[0] } ] : [];

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::WITHOUT,
    args    => $args
  );

  weaken $q->{rdb};
  return $q;
}

sub delete {
  my $self = shift;
  my $optargs = @_ ? @_ > 1 ? {@_} : { %{ $_[0] } } : {};

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::DELETE,
    optargs => $optargs,
  );

  weaken $q->{rdb};
  return $q;
}

sub insert_at {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::INSERT_AT,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub splice_at {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::SPLICE_AT,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub delete_at {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::DELETE_AT,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub change_at {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::CHANGE_AT,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub keys {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::KEYS,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub with_fields {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    type => Term::TermType::WITH_FIELDS,
    args => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub slice {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::SLICE,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub indexes_of {
  my $self = shift;
  my ($args) = @_;

  if( ref $args ) {
    croak 'Unsupported argument to indexes_of';
  }

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::INDEXES_OF,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub is_empty {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::IS_EMPTY,
  );

  weaken $q->{rdb};
  return $q;
}

sub sample {
  my $self = shift;
  my $args = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::SAMPLE,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub zip {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::ZIP,
  );

  weaken $q->{rdb};
  return $q;
}

sub distinct {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::DISTINCT,
  );

  weaken $q->{rdb};
  return $q;
}

sub contains {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::CONTAINS,
    args    => $args
  );

  weaken $q->{rdb};
  return $q;
}

sub match {
  my $self = shift;
  my ($expr) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::MATCH,
    args    => $expr
  );

  weaken $q->{rdb};
  return $q;
}

sub nth {
  my $self   = shift;
  my $number = [@_];

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::NTH,
    args    => $number,
  );

  weaken $q->{rdb};
  return $q;
}

sub add {
  my $self = shift;
  my $args = [@_];

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::ADD,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub sub {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::SUB,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub mul {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::MUL,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub div {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::DIV,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub mod {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::MOD,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub and {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    type    => Term::TermType::ALL,
    args    => [$self, $args],
  );

  weaken $q->{rdb};
  return $q;
}

sub or {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    type    => Term::TermType::ANY,
    args    => [$self, $args],
  );

  weaken $q->{rdb};
  return $q;
}

sub eq {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::EQ,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub ne {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::NE,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub gt {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::GT,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub ge {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::GE,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub lt {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::LT,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub le {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::LE,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub not {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::NOT,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

sub map {
  my $self = shift;
  my ($args) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::MAP,
    args    => $args,
  );

  weaken $q->{rdb};
  return $q;
}

1;
