use Test::More;

use Rethinkdb;

# setup
my $conn = r->connect->repl;
r->db('test')->drop->run;
r->db('test')->create->run;
r->db('test')->table('marvel')->create( primary_key => 'superhero' )->run;
r->table('marvel')->insert(
  [
    {
      user_id    => 1,
      superhero  => 'Iron Man',
      superpower => 'Arc Reactor',
      active     => 1,
      age        => 35,
      villians   => { count => 5 },
      birthdate  => r->iso8601('1986-11-03T10:01:00-08:00'),
    },
    {
      user_id    => 2,
      superhero  => 'Hulk',
      superpower => 'Smash',
      active     => 1,
      age        => 35,
      villians   => { count => 6 },
      birthdate  => r->iso8601('1986-12-01T18:05:00-08:00'),
    },
    {
      user_id    => 3,
      superhero  => 'Captain America',
      superpower => 'Super Strength',
      active     => 1,
      age        => 135,
      villians   => { count => 7 },
      birthdate  => r->iso8601('1986-11-03T06:08:00-08:00'),
    },
    {
      user_id    => 4,
      superhero  => 'Thor',
      superpower => 'God-like powers',
      active     => 1,
      age        => 1035,
      villians   => { count => 8 },
      birthdate  => r->iso8601('1986-12-09T08:10:00-08:00'),
    },
    {
      user_id    => 5,
      superhero  => 'Hawk-Eye',
      superpower => 'Bow-n-arrow',
      active     => 0,
      age        => 35,
      villians   => { count => 9 },
      birthdate  => r->iso8601('1986-11-03T08:12:00-08:00'),
    },
    {
      user_id    => 6,
      superhero  => 'Wasp',
      superpower => 'Bio-lasers',
      active     => 0,
      age        => 35,
      villians   => { count => 5 },
      birthdate  => r->iso8601('1986-11-03T08:30:00-08:00'),
    },
    {
      user_id    => 7,
      superhero  => 'Ant-Man',
      superpower => 'Size',
      active     => 1,
      age        => 35,
      villians   => { count => 10 },
      birthdate  => r->iso8601('1986-11-03T07:17:00-08:00'),
    },
    {
      user_id    => 8,
      superhero  => 'Wolverine',
      superpower => 'Adamantium',
      active     => 0,
      age        => 35,
      villians   => { count => 1 },
      birthdate  => r->iso8601('1986-11-04T08:35:04-08:00'),
    },
    {
      user_id    => 9,
      superhero  => 'Spider-Man',
      superpower => 'Spidy Sense',
      active     => 0,
      age        => 20,
      villians   => { count => 2 },
      birthdate  => r->iso8601('1986-11-04T08:55:03-08:00'),
    },
  ]
)->run;

my $res;

# now
$res = r->table('marvel')->insert( { superhero => 'Bob', joined => r->now, } )
  ->run;

is $res->type, 1, 'Correct response type';
is $res->response->{inserted}, 1, 'Correct response';

# time
$res = r->table('marvel')->get('Bob')
  ->update( { birthdate1 => r->time( 1986, 11, 3, 'Z' ) } )->run;

is $res->type, 1, 'Correct response type';
is $res->response->{replaced}, 1, 'Correct response';

# epoch_time
$res = r->table('marvel')->get('Bob')
  ->update( { birthdate2 => r->epoch_time(531360000) } )->run;

is $res->type, 1, 'Correct response type';
is $res->response->{replaced}, 1, 'Correct response';

# iso8601
$res = r->table('marvel')->get('Bob')
  ->update( { birthdate => r->iso8601('1986-11-03T08:30:00-07:00') } )->run;

is $res->type, 1, 'Correct response type';
is $res->response->{replaced}, 1, 'Correct response';

# in_timezone
$res = r->iso8601('1986-11-03T08:30:00-07:00')->in_timezone('-08:00')
  ->hours->run($conn);

is $res->type,     1, 'Correct response type';
is $res->response, 7, 'Correct response';

# timezone
$res = r->table("marvel")->filter(
  sub {
    my $hero = shift;
    $hero->attr('birthdate')->timezone->eq('-07:00');
  }
)->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 1, 'Correct response';
is $res->response->[0]->{superhero}, 'Bob', 'Correct response';

# during
$res
  = r->table('marvel')
  ->filter( r->row->attr('birthdate')
    ->during( r->time( 1986, 12, 1, 'Z' ), r->time( 1986, 12, 10, 'Z' ) ) )
  ->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 2, 'Correct response';

$res = r->table('marvel')->filter(
  r->row->attr('birthdate')->during(
    r->time( 1986, 12, 1,  'Z' ),
    r->time( 1986, 12, 10, 'Z' ),
    { left_bound => "open", right_bound => "closed" }
  )
)->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 2, 'Correct response';

# date
$res = r->table('marvel')->filter(
  sub {
    my $hero = shift;
    $hero->attr('birthdate')->date->eq( r->now->date );
  }
)->run;

is $res->type, 2, 'Correct response type';
is_deeply $res->response, [], 'Correct response';

# time_of_day
$res = r->table('marvel')->filter(
  sub {
    my $hero = shift;
    $hero->attr('birthdate')->time_of_day->le( 12 * 60 * 60 );
  }
)->run;

is $res->type, 2, 'Correct response type';
is_deeply $res->response, [], 'Correct response';

# year
$res = r->table('marvel')->filter(
  sub {
    my $hero = shift;
    $hero->attr('birthdate')->year->eq(1986);
  }
)->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 10, 'Correct response';

# month
$res = r->table('marvel')->filter(
  sub {
    my $hero = shift;
    $hero->attr('birthdate')->month->eq(12);
  }
)->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 2, 'Correct response';

# month with December constant
$res = r->table('marvel')->filter(
  sub {
    my $hero = shift;
    $hero->attr('birthdate')->month->eq( r->december );
  }
)->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 2, 'Correct response';

# day
$res = r->table('marvel')->filter(
  sub {
    my $hero = shift;
    $hero->attr('birthdate')->day->eq(4);
  }
)->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 2, 'Correct response';

# day_of_week
$res = r->table('marvel')->filter(
  sub {
    my $hero = shift;
    $hero->attr('birthdate')->day_of_week->eq(2);
  }
)->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 3, 'Correct response';

# day_of_week with Tuesday constant
$res = r->table('marvel')->filter(
  sub {
    my $hero = shift;
    $hero->attr('birthdate')->day_of_week->eq( r->tuesday );
  }
)->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 3, 'Correct response';

# day_of_year
$res = r->table('marvel')
  ->filter( r->row->attr('birthdate')->day_of_year->eq(308) )->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 2, 'Correct response';

# hours
$res
  = r->table('marvel')->filter( r->row->attr('birthdate')->hours->lt(7) )->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 1, 'Correct response';

# minutes
$res = r->table('marvel')->filter( r->row->attr('birthdate')->minutes->lt(10) )
  ->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 3, 'Correct response';

# seconds
$res = r->table('marvel')->filter( r->row->attr('birthdate')->seconds->gt(1) )
  ->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 2, 'Correct response';

# to_iso8601
$res = r->time( 1986, 11, 3, 'Z' )->to_iso8601->run($conn);

is $res->type, 1, 'Correct response type';
is $res->response, '1986-11-03T00:00:00+00:00', 'Correct response';

# to_epoch_time
$res = r->time( 1986, 11, 3, 'Z' )->to_epoch_time->run($conn);

is $res->type,     1,           'Correct response type';
is $res->response, '531360000', 'Correct response';

# clean up
r->db('test')->drop->run;

done_testing();
