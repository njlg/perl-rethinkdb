package Rethinkdb::Table;
use Rethinkdb::Base 'Rethinkdb::Query';

use Carp 'croak';
use Scalar::Util 'weaken';

use Rethinkdb::Document;
use Rethinkdb::Protocol;
use Rethinkdb::Util;

has [qw{rdb name}];

# primary_key = None
# datacenter = None
# durability = hard|soft
# cache_size = '1024MB'
sub create {
  my $self = shift;
  my $params = ref $_[0] ? $_[0] : {@_};

  $self->type(Term::TermType::TABLE_CREATE);
  $self->_args($self->name);
  $self->_optargs($params);

  return $self;
}

sub drop {
  my $self = shift;

  $self->type(Term::TermType::TABLE_DROP);
  $self->_args($self->name);

  return $self;
}

sub list {
  my $self = shift;

  $self->type(Term::TermType::TABLE_LIST);

  return $self;
}

sub insert {
  my $self   = shift;
  my $data   = shift;
  my $params = shift;

  my $values = Rethinkdb::Util->to_json($data);
  my $args = {
    type => Term::TermType::JSON,
    args => Rethinkdb::Util->to_term($values),
  };

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::INSERT,
    args    => $args,
    optargs => $params,
  );

  weaken $q->{rdb};
  return $q;
}

sub delete {
  my $self = shift;

  # r.table('marvel').delete().run
  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode({
      type  => Query::QueryType::START,
      token => Rethinkdb::Util::token(),
      query => {
        type   => Term::TermType::DELETE,
        args => [
          {
            type  => Term::TermType::TABLE,
            args => [
              # {
              #   type => Term::TermType::DATUM,
              #   datum => Rethinkdb::Util->to_datum($self->db),
              # },
              {
                type  => Term::TermType::DATUM,
                datum => Rethinkdb::Util->to_datum($self->name),
              },
            ]
          }
        ]
      }
    })
  );

  weaken $q->{rdb};
  return $q;
}


# get a document by primary key
# TODO: key can be other things besides string
sub get {
  my $self = shift;
  my ( $key ) = @_;

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::GET,
    args    => $key,
  );

  weaken $q->{rdb};
  return $q;
}

# Get all documents where the given value matches the value of the requested index
sub get_all {
  my $self = shift;

  # extract values
  my $values = [@_];
  my $params = {};
  if( ref $values->[0] eq 'ARRAY' ) {
    ($values, $params) = @{$values};
  }

  if( ref $values->[$#{$values}] eq 'HASH' ) {
    $params = pop @{$values};
  }

  my $index = $params->{index} || 'id';

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::GET_ALL,
    args    => $values,
    optargs => $params->{index},
  );

  weaken $q->{rdb};
  return $q;

}

sub between {
  my $self = shift;
  my ( $lower, $upper, $attr ) = @_;

  $attr ||= 'id';

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::BETWEEN,
    args    => [$lower, $upper],
    optargs => {index => $attr},
  );

  weaken $q->{rdb};
  return $q;
}


# predicate = JSON, expr, or sub
# TODO: fix predicate for expr or sub
sub filter {
  my $self      = shift;
  my $predicate = shift;
  # my $predicate = @_ ? @_ > 1 ? {@_} : {%{$_[0]}} : {};

  if ( !ref $predicate ) {
    $predicate = { $predicate, @_ };
  }

  if ( ref $predicate ne 'HASH' ) {
    croak 'Unsupported predicated passed to filter.';
  }

  my $q = Rethinkdb::Query->new(
    rdb     => $self->rdb,
    _parent => $self,
    type    => Term::TermType::FILTER,
    args    => $predicate,
  );

  weaken $q->{rdb};
  return $q;
}


# TODO
sub inner_join {
  my $self = shift;
  my ( $table, $predicate ) = @_;

  croak 'inner_join is not implemented';

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::START,
        token      => Rethinkdb::Util::token(),
        query => {
          term => {
            type => Term::TermType::FUNCALL,
            call => {
              builtin => {
                type   => Term::TermType::FILTER,
                filter => {
                  predicate => {
                    arg  => '',
                    body => {
                      type => '',
                    }
                  }
                }
              },
            }
          }
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

# TODO
sub outer_join {
  my $self = shift;
  my ( $table, $predicate ) = @_;

  croak 'outer_join is not implemented';
}

# TODO
sub eq_join {
  my $self = shift;
  my ( $table, $predicate ) = @_;

  croak 'eq_join is not implemented';
}

# TODO
sub map {
  my $self = shift;
  my ( $table, $predicate ) = @_;

  croak 'map is not implemented';
}

# TODO
sub concat_map {
  my $self = shift;
  my ( $table, $predicate ) = @_;

  croak 'concat_map is not implemented';
}

sub order_by {
  my $self     = shift;
  my @keys     = @_;
  my $order_by = [];

  foreach (@keys) {
    if ( ref $_ ) {
      push @{$order_by}, $_;
    }
    else {
      push @{$order_by}, $self->rdb->asc($_);
    }
  }

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::START,
        token      => Rethinkdb::Util::token(),
        query => {
          term => {
            type => Term::TermType::FUNCALL,
            call => {
              builtin => {
                type     => Term::TermType::ORDERBY,
                order_by => $order_by
              },
              args => {
                type  => Term::TermType::TABLE,
                table => {
                  table_ref => {
                    db_name    => $self->db,
                    table_name => $self->name,
                  }
                }
              }
            }
          }
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

sub skip {
  my $self   = shift;
  my $number = shift;

  croak 'skip is not implemented';

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::START,
        token      => Rethinkdb::Util::token(),
        query => {
          term => {
            type => Term::TermType::FUNCALL,
            call => {
              builtin => {
                type => Term::TermType::SLICE,
              },
              args => Rethinkdb::Util->to_datum($number)
            },
            table => {
              table_ref => {
                db_name    => $self->db,
                table_name => $self->name,
              }
            }
          }
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

# TODO fix this
sub limit {
  my $self   = shift;
  my $number = shift;

  croak 'limit is not implemented';

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::START,
        token      => Rethinkdb::Util::token(),
        query => {
          term => {
            type => Term::TermType::FUNCALL,
            call => {
              builtin => {
                type => Term::TermType::SLICE,
              },
              args => Rethinkdb::Util->to_datum($number)
            },
            table => {
              table_ref => {
                db_name    => $self->db,
                table_name => $self->name,
              }
            }
          }
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

sub slice {
  my $self = shift;
  my ( $lower, $upper ) = @_;

  croak 'slice is not implemented';

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::START,
        token      => Rethinkdb::Util::token(),
        query => {
          term => {
            type => Term::TermType::FUNCALL,
            call => {
              builtin => {
                type => Term::TermType::SLICE,
              },
              args => [
                Rethinkdb::Util->to_datum($lower),
                Rethinkdb::Util->to_datum($upper),
              ]
            },
            table => {
              table_ref => {
                db_name    => $self->db,
                table_name => $self->name,
              }
            }
          }
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

sub nth {
  my $self   = shift;
  my $number = shift;

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::START,
        token      => Rethinkdb::Util::token(),
        query => {
          term => {
            type => Term::TermType::FUNCALL,
            call => {
              builtin => {
                type => Term::TermType::NTH,
              },
              args => Rethinkdb::Util->to_datum($number)
            },
            table => {
              table_ref => {
                db_name    => $self->db,
                table_name => $self->name,
              }
            }
          }
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

# TODO: fix this
sub pluck {
  my $self  = shift;
  my @attrs = @_;

  croak 'pluck is not implemented';

  # get termtype for each attr
  my $args = [];
  foreach (@attrs) {
    push @{$args}, Rethinkdb::Util::to_term($_);
  }

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::START,
        token      => Rethinkdb::Util::token(),
        query => {
          term => {
            type => Term::TermType::FUNCALL,
            call => {
              builtin => {
                type  => Term::TermType::PLUCK,
                attrs => $args
              },
            },
            table => {
              table_ref => {
                db_name    => $self->db,
                table_name => $self->name,
              }
            }
          }
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

sub without {
  my $self  = shift;
  my @attrs = @_;

  croak 'without is not implemented';

  # get termtype for each attr
  my $args = [];
  foreach (@attrs) {
    push @{$args}, Rethinkdb::Util::to_term($_);
  }

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::START,
        token      => Rethinkdb::Util::token(),
        query => {
          term => {
            type => Term::TermType::FUNCALL,
            call => {
              builtin => {
                type  => Term::TermType::WITHOUT,
                attrs => $args
              },
            },
            table => {
              table_ref => {
                db_name    => $self->db,
                table_name => $self->name,
              }
            }
          }
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

sub count {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::START,
        token      => Rethinkdb::Util::token(),
        query => {
          term => {
            type => Term::TermType::FUNCALL,
            call => {
              builtin => {
                type => Term::TermType::COUNT,
              },
            },
            table => {
              table_ref => {
                db_name    => $self->db,
                table_name => $self->name,
              }
            }
          }
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

sub distinct {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::START,
        token      => Rethinkdb::Util::token(),
        query => {
          term => {
            type => Term::TermType::FUNCALL,
            call => {
              builtin => {
                type => Term::TermType::DISTINCT,
              },
            },
            table => {
              table_ref => {
                db_name    => $self->db,
                table_name => $self->name,
              }
            }
          }
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

sub union {
  my $self = shift;
  croak 'union is not implemented';
}

sub grouped_map_reduce {
  my $self = shift;
  croak 'grouped_map_reduce is not implemented';
}

sub stream_to_array {
  my $self = shift;
  croak 'stream_to_array is not implemented';
}

1;
