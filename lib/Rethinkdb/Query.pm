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

  if( $self->args ) {
    my $_args = $self->args;
    delete $self->{args};
    $self->_args($_args);
  }


  if( $self->optargs ) {
    my $_optargs = $self->optargs;
    delete $self->{optargs};
    $self->_optargs($_optargs);
  }

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
  my $args = [@_];

  # getter:
  if( ! $args ) {
    return $self->{args};
  }

  # setter:
  foreach( @{$args} ) {
    if( ref $_ eq 'HASH' && $_->{type} ) {
      push @{$self->{args}}, $_;
    }
    elsif( $_ ) {
      push @{$self->{args}}, Rethinkdb::Util->to_term($_);
    }
  }

  return $self;
}

sub _optargs {
  my $self = shift;
  my $args = @_ ? @_ > 1 ? {@_} : { %{ $_[0] } } : {};

  # getter:
  if( ! $args ) {
    return $self->{optargs};
  }

  # setter:
  foreach( keys %{$args} ) {
    $self->{optargs}->{$_} = Rethinkdb::Util->to_term($args->{$_});
  }

  return $self;
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

1;
