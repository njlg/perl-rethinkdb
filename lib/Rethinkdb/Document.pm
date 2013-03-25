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
    type => Term::TermType::GETBYKEY,
    get_by_key => {
      table_ref => {
        db_name => $self->db,
        table_name => $self->table,
      },
      attrname => $self->key,
      key => Rethinkdb::Util::to_term($self->value)
    }
  };

  my $term = $get_by_key;
  if( $self->getattr ) {
    $term = {
      type => Term::TermType::CALL,
      call => {
        builtin => {
          type => Builtin::BuiltinType::GETATTR,
          attr => $self->getattr
        },
        args => $get_by_key
      }
    };
  }

  my $q = Rethinkdb::Query->new(
    rdb => $self->rdb,
    query => Query->encode({
      type => Query::QueryType::READ,
      token => Rethinkdb::Util::token(),
      read_query => {
        term => $term
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
  my $self = shift;
  my @attrs = @_;

  my $q = Rethinkdb::Query->new(
    rdb => $self->rdb,
    query => Query->encode({
      type => Query::QueryType::READ,
      token => Rethinkdb::Util::token(),
      read_query => {
        term => {
          type => Term::TermType::CALL,
          call => {
            builtin => {
              type => Builtin::BuiltinType::PICKATTRS,
              attrs => \@attrs
            },
            args => {
              type => Term::TermType::GETBYKEY,
              get_by_key => {
                table_ref => {
                  db_name => $self->db,
                  table_name => $self->table,
                },
                attrname => $self->key,
                key => Rethinkdb::Util::to_term($self->value)
              }
            }
          }
        }
      }
    })
  );

  weaken $q->{rdb};
  return $q;
}

sub unpick {
  my $self = shift;
  my @attrs = @_;

  my $q = Rethinkdb::Query->new(
    rdb => $self->rdb,
    query => Query->encode({
      type => Query::QueryType::READ,
      token => Rethinkdb::Util::token(),
      read_query => {
        term => {
          type => Term::TermType::CALL,
          call => {
            builtin => {
              type => Builtin::BuiltinType::WITHOUT,
              attrs => \@attrs
            },
            args => {
              type => Term::TermType::GETBYKEY,
              get_by_key => {
                table_ref => {
                  db_name => $self->db,
                  table_name => $self->table,
                },
                attrname => $self->key,
                key => Rethinkdb::Util::to_term($self->value)
              }
            }
          }
        }
      }
    })
  );

  weaken $q->{rdb};
  return $q;
}

sub merge {
  my $self = shift;
  my $doc = shift;

  if( ref $doc ne __PACKAGE__ ) {
    croak 'merge requires a Rethinkdb::Document';
  }

  my $q = Rethinkdb::Query->new(
    rdb => $self->rdb,
    query => Query->encode({
      type => Query::QueryType::READ,
      token => Rethinkdb::Util::token(),
      read_query => {
        term => {
          type => Term::TermType::CALL,
          call => {
            builtin => {
              type => Builtin::BuiltinType::MAPMERGE
            },
            args => [
              {
                type => Term::TermType::GETBYKEY,
                get_by_key => {
                  table_ref => {
                    db_name => $self->db,
                    table_name => $self->table,
                  },
                  attrname => $self->key,
                  key => Rethinkdb::Util::to_term($self->value)
                }
              },
              {
                type => Term::TermType::GETBYKEY,
                get_by_key => {
                  table_ref => {
                    db_name => $doc->db,
                    table_name => $doc->table,
                  },
                  attrname => $doc->key,
                  key => Rethinkdb::Util::to_term($doc->value)
                }
              }
            ]
          }
        }
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

  my $q = Rethinkdb::Query->new(
    rdb => $self->rdb,
    query => Query->encode({
      type => Query::QueryType::READ,
      token => Rethinkdb::Util::token(),
      read_query => {
        term => {
          type => Term::TermType::CALL,
          call => {
            builtin => {
              type => Builtin::BuiltinType::ARRAYAPPEND
            },
            args => [
              {
                type => Term::TermType::CALL,
                call => {
                  builtin => {
                    type => Builtin::BuiltinType::GETATTR,
                    attr => $self->getattr,
                  },
                  args => {
                    type => Term::TermType::GETBYKEY,
                    get_by_key => {
                      table_ref => {
                        db_name => $self->db,
                        table_name => $self->table,
                      },
                      attrname => $self->key,
                      key => Rethinkdb::Util::to_term($self->value)
                    }
                  }
                }
              },
             $term
            ]
          }
        }
      }
    })
  );

  weaken $q->{rdb};
  return $q;
}

sub contains {
  my $self = shift;
  my $attr = shift;

  my $q = Rethinkdb::Query->new(
    rdb => $self->rdb,
    query => Query->encode({
      type => Query::QueryType::READ,
      token => Rethinkdb::Util::token(),
      read_query => {
        term => {
          type => Term::TermType::CALL,
          call => {
            builtin => {
              type => Builtin::BuiltinType::HASATTR,
              attr => $attr
            },
            args => [
              {
                type => Term::TermType::GETBYKEY,
                get_by_key => {
                  table_ref => {
                    db_name => $self->db,
                    table_name => $self->table,
                  },
                  attrname => $self->key,
                  key => Rethinkdb::Util::to_term($self->value)
                }
              }
            ]
          }
        }
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
    rdb => $self->rdb,
    query => Query->encode({
      type => Query::QueryType::WRITE,
      token => Rethinkdb::Util::token(),
      write_query => {
        type => WriteQuery::WriteQueryType::POINTUPDATE,
        atomic => 1,
        point_update => {
          table_ref => {
            db_name => $self->db,
            table_name => $self->table,
          },
          attrname => $self->key,
          key => Rethinkdb::Util::to_term($self->value),
          mapping => {
            arg => 'row',
            body => $term,
          }
        }
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
    rdb => $self->rdb,
    query => Query->encode({
      type => Query::QueryType::WRITE,
      token => Rethinkdb::Util::token(),
      write_query => {
        type => WriteQuery::WriteQueryType::POINTMUTATE,
        atomic => 1,
        point_mutate => {
          table_ref => {
            db_name => $self->db,
            table_name => $self->table,
          },
          attrname => $self->key,
          key => Rethinkdb::Util::to_term($self->value),
          mapping => {
            arg => 'row',
            body => $term,
          }
        }
      }
    })
  );

  weaken $q->{rdb};
  return $q;
}

sub delete {
  my $self = shift;
  my $q = Rethinkdb::Query->new(
    rdb => $self->rdb,
    query => Query->encode({
      type => Query::QueryType::WRITE,
      token => Rethinkdb::Util::token(),
      write_query => {
        type => WriteQuery::WriteQueryType::POINTDELETE,
        atomic => 1,
        point_delete => {
          table_ref => {
            db_name => $self->db,
            table_name => $self->table,
          },
          attrname => $self->key,
          key => Rethinkdb::Util::to_term($self->value)
        }
      }
    })
  );

  weaken $q->{rdb};
  return $q;
}

sub del { return (shift)->delete(); };

1;
