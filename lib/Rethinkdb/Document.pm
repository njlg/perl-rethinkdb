package Rethinkdb::Document;
use Rethinkdb::Base -base;

use Carp 'croak';
use Scalar::Util 'weaken';

use Rethinkdb::Protocol;
use Rethinkdb::Util;

has [qw{rdb db table key value getattr}];

sub run {
  my $self = shift;

  my $query = {
    type => Term::TermType::GET,
    args => [
      {
        type  => Term::TermType::TABLE,
        args => Rethinkdb::Util->to_term($self->table)
      },
      Rethinkdb::Util->to_term($self->value)
    ]
  };

  if ( $self->getattr ) {
    $query = {
      type => Term::TermType::GET_FIELD,
      args => [
        $query,
        Rethinkdb::Util->to_term($self->getattr)
      ]
    };
  }

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode({
      type  => Query::QueryType::START,
      token => Rethinkdb::Util::token(),
      query => $query
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
    query => Query->encode({
      type  => Query::QueryType::START,
      token => Rethinkdb::Util::token(),
      query => {
        type => Term::TermType::MERGE,
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
            type  => Term::TermType::GET,
            args => [
              {
                type  => Term::TermType::TABLE,
                args => [
                  {
                    type  => Term::TermType::DATUM,
                    datum => Rethinkdb::Util->to_datum($doc->table),
                  },
                ]
              },
              {
                type  => Term::TermType::DATUM,
                datum => Rethinkdb::Util->to_datum($doc->value),
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

sub append {
  my $self = shift;
  my $attr = shift;

  my $term = $self->rdb->expr($attr);
  my $args = $term->{r_object};

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
      type  => Query::QueryType::START,
      token => Rethinkdb::Util::token(),
      query => {
        type => Term::TermType::CONTAINS,
        args => [
          {
            type  => Term::TermType::GET,
            args => [
              {
                type  => Term::TermType::TABLE,
                args => Rethinkdb::Util->to_term($self->table),
              },
              Rethinkdb::Util->to_term($self->value)
            ]
          },
          $args
        ]
      }
    })
  );

  weaken $q->{rdb};
  return $q;
}

sub has_fields {
  my $self = shift;
  my $attr = shift;

  my $term = Rethinkdb::Util->to_term($attr);

  my $q = Rethinkdb::Query->new(
    rdb   => $self->rdb,
    query => Query->encode( {
      type  => Query::QueryType::START,
      token => Rethinkdb::Util::token(),
      query => {
        type => Term::TermType::HAS_FIELDS,
        args => [
          {
            type  => Term::TermType::GET,
            args => [
              {
                type  => Term::TermType::TABLE,
                args => [
                  Rethinkdb::Util->to_term($self->table),
                ]
              },
              Rethinkdb::Util->to_term($self->value),
            ]
          },
          $term
        ]
      }
    })
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
