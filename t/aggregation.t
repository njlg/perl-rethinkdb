use Test::More;

use Rethinkdb;

# setup
r->connect;

eval {
  r->table('marvel')->map(sub {
    my $hero = shift;
    return $hero->{monstersKilled}.add($hero->{superVillainsVanquished});
  })->reduce(0, sub {
    my ($acc, $val) = @_;
    return $acc->add($val);
  })->run;
};
unlike $@, qr/is not implemented/;

eval {
  #(r->table('marvel')->count() + r->table('dc')->count())->run;
  r->table('marvel')->count().add(r->table('dc')->count())->run;
};
unlike $@, qr/is not implemented/;

eval {
  r->table('marvel')->concat_map(sub {
    my $hero = shift;
    return $hero->{villainList};
  })->distinct()->count()->run;
};
unlike $@, qr/is not implemented/;

eval {
  r->table('marvel')->grouped_map_reduce(sub {
      my $hero = shift;
      return $hero->{weightClass}
    },
    sub {
      my $hero = shift;
      return $hero->pick('name', 'strength');
    },
    { name => 'none', strength => 0},
    sub {
      my $acc = shift;
      my $hero = shift;
      return $r->branch($acc->{strength}->lt($hero->{strength}), $hero, $acc);
  })->run;
};
unlike $@, qr/is not implemented/;

eval {
  r->table('marvel')->group_by('weightClass', r->avg('strength'))->run;
};

done_testing();