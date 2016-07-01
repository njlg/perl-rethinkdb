use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

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
      strength   => 1000,
      dc_buddies => [ 'Superman', 'Batman' ],
    },
    {
      user_id    => 2,
      superhero  => 'Hulk',
      superpower => 'Smash',
      active     => 1,
      age        => 35,
      strength   => 2012,
      dc_buddies => [ 'Superman', 'Flash' ],
    },
    {
      user_id    => 3,
      superhero  => 'Captain America',
      superpower => 'Super Strength',
      active     => 1,
      age        => 135,
      strength   => 1035,
      dc_buddies => [ 'Superman', 'Green Lantern' ],
    },
    {
      user_id    => 4,
      superhero  => 'Thor',
      superpower => 'God-like powers',
      active     => 1,
      age        => 1035,
      strength   => 2035,
      dc_buddies => [ 'Flash', 'Batman' ],
    },
    {
      user_id    => 5,
      superhero  => 'Hawk-Eye',
      superpower => 'Bow-n-arrow',
      active     => 0,
      age        => 35,
      strength   => 10,
      dc_buddies => [ 'Aquaman', 'Wonder Women' ],
    },
    {
      user_id    => 6,
      superhero  => 'Wasp',
      superpower => 'Bio-lasers',
      active     => 0,
      age        => 35,
      strength   => 52,
      dc_buddies => [ 'Superman', 'Batman' ],
    },
    {
      user_id    => 7,
      superhero  => 'Ant-Man',
      superpower => 'Size',
      active     => 1,
      age        => 35,
      strength   => 50,
      dc_buddies => [ 'Green Lantern', 'Aquaman' ],
      extra      => 1,
    },
    {
      user_id    => 8,
      superhero  => 'Wolverine',
      superpower => 'Adamantium',
      active     => 0,
      age        => 35,
      strength   => 135,
      dc_buddies => [ 'Hawkman', 'Batman' ],
      extra      => 1,
    },
    {
      user_id    => 9,
      superhero  => 'Spider-Man',
      superpower => 'Spidy Sense',
      active     => 0,
      age        => 20,
      strength   => 200,
      dc_buddies => [ 'Wonder Women', 'Martian Manhunter' ],
      extra      => 1,
    }
  ]
)->run;

r->db('test')->table('dc')->create( primary_key => 'superhero' )->run;
r->table('dc')->insert(
  [
    {
      user_id    => 10,
      superhero  => 'Superman',
      superpower => 'Alien',
      active     => 1,
      age        => 35
    },
    {
      user_id    => 11,
      superhero  => 'Batman',
      superpower => 'Cunning',
      active     => 1,
      age        => 35
    },
    {
      user_id    => 12,
      superhero  => 'Flash',
      superpower => 'Super Speed',
      active     => 1,
      age        => 135
    },
    {
      user_id    => 13,
      superhero  => 'Wonder Women',
      superpower => 'Super Stregth',
      active     => 1,
      age        => 1035
    },
    {
      user_id    => 14,
      superhero  => 'Green Lantern',
      superpower => 'Ring',
      active     => 0,
      age        => 35
    },
    {
      user_id    => 15,
      superhero  => 'Aquaman',
      superpower => 'Hydrokinesis',
      active     => 0,
      age        => 35
    },
    {
      user_id    => 16,
      superhero  => 'Hawkman',
      superpower => 'Ninth Metal',
      active     => 1,
      age        => 35
    },
    {
      user_id    => 17,
      superhero  => 'Martian Manhunter',
      superpower => 'Shapeshifting',
      active     => 0,
      age        => 35
    },
  ]
)->run;

# group
$res = r->table('marvel')->group('age')->avg('strength')->run;

is $res->type, 1, 'Correct response type';
is_deeply $res->response,
  {
  '1035' => '2035',
  '35'   => '543.166666666667',
  '135'  => '1035',
  '20'   => '200',
  },
  'Correct response';

# group by more than one field (we have to use `group_format=>'raw'`)
$res = r->table('marvel')->group( 'age', 'active' )
  ->run( { group_format => 'raw' } );

is_deeply $res->response->{data}->[0][0], [ 20, 0 ];
is $res->response->{data}->[0][1][0]->{superhero}, 'Spider-Man';

# group using a function
$res = r->table('marvel')->group(
  sub {
    my $row = shift;
    return $row->pluck( 'age', 'active' );
  }
)->run( { group_format => 'raw' } );

is_deeply $res->response->{data}->[0][0], { age => 20, active => 0 };
is $res->response->{data}->[0][1][0]->{superhero}, 'Spider-Man';

# group `multi=true`
# r.table('games2').group(r.row['matches'].keys(), multi=True).run()
$res = r->table('marvel')
  ->group( r->row->bracket('dc_buddies'), { multi => r->true } )->run;

is $#{ $res->response->{Batman} }, 3, 'Correct `multi=true` response';

# ungroup
$res = r->table('marvel')->group('age')->avg('strength')->ungroup->run;

is_deeply [ sort keys %{ $res->response->[0] } ], [ 'group', 'reduction' ];

# reduce
$res = r->table('marvel')->map( r->row->bracket('age') )->reduce(
  sub ($$) {
    my ( $acc, $val ) = @_;
    $acc->add($val);
  }
)->default(0)->run;

is $res->type,     1,      'Correct response type';
is $res->response, '1400', 'Correct response';

# fold
$res = r->table('marvel')->fold(
  0,
  sub ($$) {
    my ( $acc, $row ) = @_;
    return $acc->add( $row->attr('age') );
  }
)->run;

is $res->type,     1,      'Correct response type';
is $res->response, '1400', 'Correct response';

$res = r->table('marvel')->fold(
  [],
  sub ($$) {
    my ( $acc, $row ) = @_;
    return $acc->append( $row->attr('age') );
  }
)->run;

is $res->type, 1, 'Correct response type';
is_deeply $res->response, [ 135, 35, 35, 35, 35, 1035, 35, 20, 35 ],
  'Correct response';

$res = r->table('marvel')->fold(
  0,
  sub ($$) {
    my ( $acc, $row ) = @_;
    return $acc->add(1);
  },
  sub ($$$) {
    my ( $acc, $row, $newAcc ) = @_;
    return r->branch( $acc->mod(2)->eq(0), [$row], [] );
  }
)->run;

is $res->type, 2, 'Correct response type';
is_deeply [ map { $_->{superhero} } @{ $res->response } ],
  [ 'Captain America', 'Ant-Man', 'Hawk-Eye', 'Wasp', 'Iron Man' ],
  'Correct response';

# count
$res = r->table('marvel')->count->run;

is $res->type,     1,   'Correct response type';
is $res->response, '9', 'Correct response';

# count (with parameter)
$res = r->table('marvel')->concat_map(
  sub {
    my $row = shift;
    $row->bracket('dc_buddies');
  }
)->count('Batman')->run;

is $res->type,     1,   'Correct response type';
is $res->response, '4', 'Correct response';

$res = r->table('marvel')->count(
  sub {
    my $hero = shift;
    $hero->bracket('dc_buddies')->contains('Batman');
  }
)->run;

is $res->type,     1,   'Correct response type';
is $res->response, '4', 'Correct response';

# sum
$res = r->expr( [ 3, 5, 7 ] )->sum->run($conn);

is $res->response, 15, 'Correct response';

# sum - document attributes
$res = r->table('marvel')->sum('age')->run;

is $res->response, 1400, 'Correct response';

# sum - documents based on function
$res = r->table('marvel')->sum(
  sub {
    my $row = shift;
    return $row->bracket('strength')->mul( $row->bracket('active') );
  }
)->run;

is $res->response, 6132, 'Correct response';

# avg
$res = r->expr( [ 3, 5, 7 ] )->avg->run($conn);

is $res->response, 5, 'Correct response';

# avg - document attributes
$res = r->table('marvel')->avg('age')->run;

is substr( $res->response, 0, 7 ), '155.555', 'Correct response';

# avg - documents based on function
$res = r->table('marvel')->avg(
  sub {
    my $row = shift;
    return $row->bracket('strength')->mul( $row->bracket('active') );
  }
)->run;

is substr( $res->response, 0, 7 ), '681.333', 'Correct response';

# min
$res = r->expr( [ 3, 5, 7 ] )->min->run($conn);

is $res->response, 3, 'Correct response';

# min - document attributes
$res = r->table('marvel')->min('age')->run;
is $res->response->{age}, 20, 'Correct response';

# min - documents based on function
$res = r->table('marvel')->min(
  sub {
    my $row = shift;
    return $row->bracket('strength')->mul( $row->bracket('active') );
  }
)->run;

is $res->response->{age}, 20, 'Correct response';

# max
$res = r->expr( [ 3, 5, 7 ] )->max->run($conn);

is $res->response, 7, 'Correct response';

# max - document attributes
$res = r->table('marvel')->max('age')->run;
is $res->response->{age}, 1035, 'Correct response';

# max - documents based on function
$res = r->table('marvel')->max(
  sub {
    my $row = shift;
    return $row->bracket('strength')->mul( $row->bracket('active') );
  }
)->run;

is $res->response->{age}, 1035, 'Correct response';

# distinct (on table)
$res = r->table('marvel')->distinct->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 9, 'Correct response';

# distinct (on query)
$res = r->expr( [ 1, 1, 1, 1, 1, 2, 3 ] )->distinct->run($conn);

is $res->type, 1, 'Correct response type';
is scalar @{ $res->response }, 3, 'Correct response';

# contains
$res = r->table('marvel')->get('Iron Man')->bracket('dc_buddies')
  ->contains('Superman')->run;

is $res->type, 1, 'Correct response type';
is $res->response, r->true, 'Correct response value';

$res = r->table('marvel')->filter(
  sub {
    my $hero = shift;
    return r->expr( [ 'Smash', 'Size' ] )
      ->contains( $hero->bracket('superpower') );
  }
)->bracket('superhero')->run;

is_deeply sort $res->response, [ 'Ant-Man', 'Hulk' ],
  'Correct filter & contains response';

# clean up
r->db('test')->drop->run;

done_testing();
