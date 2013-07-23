package Rethinkdb::Table;
use Rethinkdb::Base -base;

use Carp 'croak';
use Scalar::Util 'weaken';

use Rethinkdb::Document;
use Rethinkdb::Protocol;
use Rethinkdb::Util;

has [qw{rdb db name}];

# primary_key = None
# datacenter = None
# durability = hard|soft
# cache_size = '1024MB'
sub create {
  my $self = shift;
  my $params = ref $_[0] ? $_[0] : {@_};

  my $args = {};
  if( $params ) {
    my $values = Rethinkdb::Util->to_datum($params);
    $args->{optargs} = $values->{r_object};
  }

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::START,
        token      => Rethinkdb::Util::token(),
        query => {
          type => Term::TermType::TABLE_CREATE,
          # db_name => $self->db,
          args => [
            {
              type  => Term::TermType::DB,
              args => {
                type => Term::TermType::DATUM,
                datum => Rethinkdb::Util->to_datum($self->db),
              }
            },
            {
              type  => Term::TermType::DATUM,
              datum => Rethinkdb::Util->to_datum($self->name),
            }
          ],
          %{$args}
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

sub drop {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type  => Query::QueryType::START,
        token => Rethinkdb::Util::token(),
        query => {
          type => Term::TermType::TABLE_DROP,
          args => [
            {
              type  => Term::TermType::DB,
              args => {
                type => Term::TermType::DATUM,
                datum => Rethinkdb::Util->to_datum($self->db),
              }
            },
            {
              type  => Term::TermType::DATUM,
              datum => Rethinkdb::Util->to_datum($self->name),
            }
          ]
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

sub list {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::START,
        token      => Rethinkdb::Util::token(),
        query => {
          type    => Term::TermType::TABLE_LIST,
          args => [
            {
              type  => Term::TermType::DB,
              args => {
                type => Term::TermType::DATUM,
                datum => Rethinkdb::Util->to_datum($self->db),
              }
            }
          ]
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

sub insert {
  my $self   = shift;
  my $data   = shift;
  my $params = shift;

  my $values = $self->rdb->expr($data);

  my $args = {};
  if( $values->{r_object} ) {
    $args = {
      type => Term::TermType::MAKE_OBJ,
      optargs => $values->{r_object}
    };
  }
  elsif( $values->{r_array} ) {
    foreach( @{$values->{r_array}} ) {
      push @{$args->{args}}, {
        type => Term::TermType::MAKE_OBJ,
        optargs => $_->{r_object}
      };
    }

    $args->{type} = Term::TermType::MAKE_ARRAY;
  }
  # elsif( $values->{r_array} ) {
  #   $args = {
  #     type => Term::TermType::MAKE_ARRAY,
  #     optargs => $values->{r_array}
  #   };
  # }

  my $optargs = {};
  if( $params ) {
    $values = $self->rdb->expr($params);
    if( $values->{r_object} ) {
      $optargs->{optargs} = $values->{r_object};
    }
    elsif( $values->{r_array} ) {
       $optargs->{optargs} = $values->{r_array};
    }
  }

  # r.table('marvel').insert({ 'superhero': 'Iron Man', 'superpower': 'Arc Reactor' }).run
  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
      type  => Query::QueryType::START,
      token => Rethinkdb::Util::token(),
      query => {
        type => Term::TermType::INSERT,
        args => [
          # {
          #   type  => Term::TermType::DB,
          #   args => {
          #     type => Term::TermType::DATUM,
          #     datum => Rethinkdb::Util->to_datum($self->db),
          #   }
          # },
          {
            type  => Term::TermType::TABLE,
            args => {
              type => Term::TermType::DATUM,
              datum => Rethinkdb::Util->to_datum($self->name),
            }
          },
          $args
        ],
        %{$optargs}
      },
      # global_optargs => [
      #   Rethinkdb::Util->to_datum({ db => $self->db })->{r_object},
      # ],
    })
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

sub run {
  my $self = shift;

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode({
      type   => Query::QueryType::START,
      token  => Rethinkdb::Util::token(),
      query => {
        type => Term::TermType::TABLE,
        args => [
          {
            type  => Term::TermType::DB,
            args => {
              type => Term::TermType::DATUM,
              datum => Rethinkdb::Util->to_datum($self->db),
            }
          },
          {
            type  => Term::TermType::DATUM,
            datum => Rethinkdb::Util->to_datum($self->name),
          }
        ],
        # optargs => [
        #   Rethinkdb::Util->to_datum({ use_outdated => Rethinkdb->false }),
        # ],
      }
    })
  );

  weaken $q->{rdb};
  return $q->run;
}

# get a document
# TODO: key can be other things besides string
sub get {
  my $self = shift;
  my ( $key, $attr ) = @_;

  $attr ||= 'id';

  my $d = Rethinkdb::Document->new(
    rdb   => $self->rdb,
    db    => $self->db,
    table => $self->name,
    key   => $attr,
    value => $key,
  );

  weaken $d->{rdb};
  return $d;
}

sub between {
  my $self = shift;
  my ( $lower, $upper, $attr ) = @_;

  $attr ||= 'id';

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
                type  => 'Term::TermType::RANGE',
                range => {
                  attrname   => $attr,
                  lowerbound => Rethinkdb::Util::to_term($lower),
                  upperbound => Rethinkdb::Util::to_term($upper),
                }
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

# predicate = JSON, expr, or sub
# TODO: fix predicate for expr or sub
sub filter {
  my $self      = shift;
  my $predicate = shift;
  # my $predicate = @_ ? @_ > 1 ? {@_} : {%{$_[0]}} : {};

  if ( !ref $predicate ) {
    $predicate = { $predicate, @_ };
  }

  my $filters = [];
  if ( ref $predicate eq 'HASH' ) {
    $filters = $self->_get_filters($predicate);
  }
  else {
    croak 'Unsupported predicated passed to filter.';
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
                type   => Term::TermType::FILTER,
                filter => {
                  predicate => {
                    arg  => 'row',
                    body => {
                      type => Term::TermType::FUNCALL,
                      call => {
                        builtin => {
                          type => Term::TermType::ALL,
                        },
                        # args could be an array
                        args => $filters
                      }
                    }
                  }
                }
              },
              args => {
                type  => Term::TermType::TABLE,
                table => {
                  table_ref => {
                    db_name    => $self->db,
                    table_name => $self->name,
                    # use_outdated => 0
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

sub _get_filters {
  my $self    = shift;
  my $filters = shift;

  my $retval = [];
  my $val;
  foreach ( keys %{$filters} ) {
    $val = Rethinkdb::Util::to_term( $filters->{$_} );
    push @{$retval}, {
      type => Term::TermType::FUNCALL,
      call => {
        builtin => {
          type       => 'Term::TermType::COMPARE',
          comparison => Term::TermType::EQ
        },
        args => [ {
            type => Term::TermType::FUNCALL,
            call => {
              builtin => {
                type => Term::TermType::GET_FIELD,
                attr => $_
              },
              args => {
                type => Term::TermType::IMPLICIT_VAR,
              } }
          },
          $val
        ]
      }
    };
  }

  return $retval;
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
