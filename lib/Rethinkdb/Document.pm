package Rethinkdb::Document;
use Rethinkdb::Base -base;

use Carp 'croak';
use Scalar::Util 'weaken';

use Rethinkdb::Protocol;
use Rethinkdb::Util;

has [qw{rdb db table key value getattr}];

sub run {
  my $self = shift;

  my $get_by_key = {
    type       => Term::TermType::GET,
    get_by_key => {
      table_ref => {
        db_name    => $self->db,
        table_name => $self->table,
      },
      attrname => $self->key,
      key      => Rethinkdb::Util::to_term( $self->value )
    }
  };

  my $term = $get_by_key;
  if ( $self->getattr ) {
    $term = {
      type => Term::TermType::FUNCALL,
      call => {
        builtin => {
          type => Term::TermType::GET_FIELD,
          attr => $self->getattr
        },
        args => $get_by_key
      }
    };
  }

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode({
      type  => Query::QueryType::START,
      token => Rethinkdb::Util::token(),
      query => {
        type => Term::TermType::GET,
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
                datum => Rethinkdb::Util->to_datum($self->table),
              },
            ]
          },
          {
            type  => Term::TermType::DATUM,
            datum => Rethinkdb::Util->to_datum($self->value),
          }
        ],
        # optargs => [
        #   Rethinkdb::Util->to_datum({ use_outdated => r->false }),
        # ],
      }
    })
  );

  weaken $q->{rdb};
  return $q->run;
}

# TODO: replace this with AUTOLOAD or overload %{}
# to get something like r->table->get()->{attr}->run;
# or like r->table->get()->attr->run;
sub attr {
  my $self = shift;
  my $attr = shift;
  $self->getattr($attr);
  return $self;
}

sub pick {
  my $self  = shift;
  my @attrs = @_;

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode(
      {
        type       => Query::QueryType::START,
        token      => Rethinkdb::Util::token(),
        read_query => {
          term => {
            type => Term::TermType::FUNCALL,
            call => {
              builtin => {
                type  => Term::TermType::PLUCK,
                attrs => \@attrs
              },
              args => {
                type       => Term::TermType::GET,
                get_by_key => {
                  table_ref => {
                    db_name    => $self->db,
                    table_name => $self->table,
                  },
                  attrname => $self->key,
                  key      => Rethinkdb::Util::to_term( $self->value )
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

sub unpick {
  my $self  = shift;
  my @attrs = @_;

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::START,
        token      => Rethinkdb::Util::token(),
        read_query => {
          term => {
            type => Term::TermType::FUNCALL,
            call => {
              builtin => {
                type  => Term::TermType::WITHOUT,
                attrs => \@attrs
              },
              args => {
                type       => Term::TermType::GET,
                get_by_key => {
                  table_ref => {
                    db_name    => $self->db,
                    table_name => $self->table,
                  },
                  attrname => $self->key,
                  key      => Rethinkdb::Util::to_term( $self->value )
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

sub merge {
  my $self = shift;
  my $doc  = shift;

  if ( ref $doc ne __PACKAGE__ ) {
    croak 'merge requires a Rethinkdb::Document';
  }

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::START,
        token      => Rethinkdb::Util::token(),
        read_query => {
          term => {
            type => Term::TermType::FUNCALL,
            call => {
              builtin => { type => Term::TermType::MERGE },
              args    => [ {
                  type       => Term::TermType::GET,
                  get_by_key => {
                    table_ref => {
                      db_name    => $self->db,
                      table_name => $self->table,
                    },
                    attrname => $self->key,
                    key      => Rethinkdb::Util::to_term( $self->value ) }
                }, {
                  type       => Term::TermType::GET,
                  get_by_key => {
                    table_ref => {
                      db_name    => $doc->db,
                      table_name => $doc->table,
                    },
                    attrname => $doc->key,
                    key      => Rethinkdb::Util::to_term( $doc->value )
                  }
                }
              ]
            }
          }
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

sub append {
  my $self = shift;
  my $attr = shift;

  my $term = $self->rdb->expr($attr);

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::START,
        token      => Rethinkdb::Util::token(),
        read_query => {
          term => {
            type => Term::TermType::FUNCALL,
            call => {
              builtin => { type => Term::TermType::APPEND },
              args    => [ {
                  type => Term::TermType::FUNCALL,
                  call => {
                    builtin => {
                      type => Term::TermType::GET_FIELD,
                      attr => $self->getattr,
                    },
                    args => {
                      type       => Term::TermType::GET,
                      get_by_key => {
                        table_ref => {
                          db_name    => $self->db,
                          table_name => $self->table,
                        },
                        attrname => $self->key,
                        key      => Rethinkdb::Util::to_term( $self->value )
                      }
                    }
                  }
                },
                $term
              ]
            }
          }
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

sub contains {
  my $self = shift;
  my $attr = shift;

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
        type       => Query::QueryType::START,
        token      => Rethinkdb::Util::token(),
        read_query => {
          term => {
            type => Term::TermType::FUNCALL,
            call => {
              builtin => {
                type => Term::TermType::CONTAINS,
                attr => $attr
              },
              args => [
                {
                  type       => Term::TermType::GET,
                  get_by_key => {
                    table_ref => {
                      db_name    => $self->db,
                      table_name => $self->table,
                    },
                    attrname => $self->key,
                    key      => Rethinkdb::Util::to_term( $self->value )
                  }
                }
              ]
            }
          }
        }
      }
    )
  );

  weaken $q->{rdb};
  return $q;
}

sub update {
  my $self = shift;
  my $attr = shift;

  my $term = $self->rdb->expr($attr);

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode({
      type  => Query::QueryType::START,
      token => Rethinkdb::Util::token(),
      query => {
        type   => Term::TermType::UPDATE,
        args => [
          {
            type  => Term::TermType::GET,
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
                    datum => Rethinkdb::Util->to_datum($self->table),
                  },
                ]
              },
              {
                type  => Term::TermType::DATUM,
                datum => Rethinkdb::Util->to_datum($self->value),
              }
            ]
          },
          {
            type    => Term::TermType::MAKE_OBJ,
            optargs => $term->{r_object}
          }
        ],
      }
    })
  );

  weaken $q->{rdb};
  return $q;
}

sub replace {
  my $self = shift;
  my $attr = shift;

  my $term = $self->rdb->expr($attr);

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode({
      type  => Query::QueryType::START,
      token => Rethinkdb::Util::token(),
      query => {
        type   => Term::TermType::REPLACE,
        args => [
          {
            type  => Term::TermType::GET,
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
                    datum => Rethinkdb::Util->to_datum($self->table),
                  },
                ]
              },
              {
                type  => Term::TermType::DATUM,
                datum => Rethinkdb::Util->to_datum($self->value),
              }
            ]
          },
          {
            type    => Term::TermType::MAKE_OBJ,
            optargs => $term->{r_object}
          }
        ],
      }
    })
  );

  weaken $q->{rdb};
  return $q;
}

sub delete {
  my $self = shift;
  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode({
      type  => Query::QueryType::START,
      token => Rethinkdb::Util::token(),
      query => {
        type   => Term::TermType::DELETE,
        args => [
          {
            type  => Term::TermType::GET,
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
                    datum => Rethinkdb::Util->to_datum($self->table),
                  },
                ]
              },
              {
                type  => Term::TermType::DATUM,
                datum => Rethinkdb::Util->to_datum($self->value),
              }
            ]
          }
        ]
      }
    })
  );

  weaken $q->{rdb};
  return $q;
}

sub del { return (shift)->delete(); }

1;
