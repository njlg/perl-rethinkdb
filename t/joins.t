use Test::More;

use Rethinkdb;


# setup
r->connect;

# inner_join
eval {
  r->table('marvel')->inner_join(r->table('dc'), sub {
    my ($marvelRow, $dcRow) = @_;
    return $marvelRow->{strength} < $dcRow->{strength};
  })->run;
};
unlike $@, qr/is not implemented/;

# outer_join
eval {
  r->table('marvel')->outer_join(r->table('dc'), sub {
    my ($marvelRow, $dcRow) = @_;
    return $marvelRow->{strength} < $dcRow->{strength};
  })->run;
};
unlike $@, qr/is not implemented/;

# eq_join
eval {
  r->table('marvel')->eq_join('main_dc_collaborator', r->table('dc'))->run;
  r->table('marvel')->eq_join('main_dc_collaborator', r->table('dc'), 'hero_name')->run;
};
unlike $@, qr/is not implemented/;

eval {
  r->table('marvel')->inner_join(r->table('dc'), sub {
    my ($left, $right) = @_;
    return $left->{main_dc_collaborator}->eq($right->{hero_name});
  })->run;
};
unlike $@, qr/is not implemented/;

# zip
# my $join_result->zip()->run;

done_testing();