use Test::More;

use Rethinkdb;


# setup
r->connect->repl;
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
      strength   => 35,
      dc_partner => 10,
      dc_buddy   => 'Superman',
    },
    {
      user_id    => 2,
      superhero  => 'Hulk',
      superpower => 'Smash',
      active     => 1,
      age        => 35,
      strength   => 1000,
      dc_partner => 11,
      dc_buddy   => 'Martian Manhunter',
    },
    {
      user_id    => 3,
      superhero  => 'Captain America',
      superpower => 'Super Strength',
      active     => 1,
      age        => 135,
      strength   => 135,
      dc_partner => 12,
      dc_buddy   => 'Batman',
    },
    {
      user_id    => 4,
      superhero  => 'Thor',
      superpower => 'God-like powers',
      active     => 1,
      age        => 1035,
      strength   => 1000,
      dc_partner => 13,
      dc_buddy   => '',
    },
    {
      user_id    => 5,
      superhero  => 'Hawk-Eye',
      superpower => 'Bow-n-arrow',
      active     => 0,
      age        => 35,
      strength   => 35,
      dc_partner => 14,
      dc_buddy   => '',
    },
    {
      user_id    => 6,
      superhero  => 'Wasp',
      superpower => 'Bio-lasers',
      active     => 0,
      age        => 35,
      strength   => 35,
      dc_partner => 15,
      dc_buddy   => '',
    },
    {
      user_id    => 7,
      superhero  => 'Ant-Man',
      superpower => 'Size',
      active     => 1,
      age        => 35,
      strength   => 45,
      dc_partner => 16,
      dc_buddy   => '',
    },
    {
      user_id    => 8,
      superhero  => 'Wolverine',
      superpower => 'Adamantium',
      active     => 0,
      age        => 35,
      strength   => 135,
      dc_partner => 17,
      dc_buddy   => '',
    },
    {
      user_id    => 9,
      superhero  => 'Spider-Man',
      superpower => 'Spidy Sense',
      active     => 0,
      age        => 20,
      strength   => 20,
      dc_partner => 18,
      dc_buddy   => '',
    },
  ]
)->run;

r->db('test')->table('dc')->create( primary_key => 'user_id' )->run;
r->db('test')->table('dc')->index_create('name')->run;
r->table('dc')->insert(
  [
    {
      user_id    => 10,
      name       => 'Superman',
      superpower => 'Alien',
      active     => 1,
      age        => 35,
      strength   => 350,
    },
    {
      user_id    => 11,
      name       => 'Batman',
      superpower => 'Cunning',
      active     => 1,
      age        => 35,
      strength   => 35,
    },
    {
      user_id    => 12,
      name       => 'Flash',
      superpower => 'Super Speed',
      active     => 1,
      age        => 135,
      strength   => 15,
    },
    {
      user_id    => 13,
      name       => 'Wonder Women',
      superpower => 'Super Stregth',
      active     => 1,
      age        => 1035,
      strength   => 25,
    },
    {
      user_id    => 14,
      name       => 'Green Lantern',
      superpower => 'Ring',
      active     => 0,
      age        => 35,
      strength   => 35,
    },
    {
      user_id    => 15,
      name       => 'Aquaman',
      superpower => 'Hydrokinesis',
      active     => 0,
      age        => 35,
      strength   => 20,
    },
    {
      user_id    => 16,
      name       => 'Hawkman',
      superpower => 'Ninth Metal',
      active     => 1,
      age        => 35,
      strength   => 50,
    },
    {
      user_id    => 17,
      name       => 'Martian Manhunter',
      superpower => 'Shapeshifting',
      active     => 0,
      age        => 35,
      strength   => 75,
    },
  ]
)->run;

my $res;

# inner_join
$res = r->table('marvel')->inner_join(
  r->table('dc'),
  sub ($$) {
    my ( $marvel_row, $dc_row ) = @_;
    return $marvel_row->attr('strength')->lt( $dc_row->attr('strength') );
  }
)->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 20, 'Correct response';
is $res->response->[0]->{left}->{superhero}, 'Captain America',
  'Correct response';
is $res->response->[0]->{right}->{name}, 'Superman', 'Correct response';

# outer_join
$res = r->table('marvel')->outer_join(
  r->table('dc'),
  sub ($$) {
    my ( $marvel_row, $dc_row ) = @_;
    return $marvel_row->attr('strength')->lt( $dc_row->attr('strength') );
  }
)->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 22, 'Correct response';
is $res->response->[0]->{left}->{superhero}, 'Captain America',
  'Correct response';
is $res->response->[0]->{right}->{name}, 'Superman', 'Correct response';

# eq_join
$res = r->table('marvel')->eq_join( 'dc_partner', r->table('dc') )->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 8, 'Correct number of documents returned';

# eq_join with secondary index
$res = r->table('marvel')
  ->eq_join( 'dc_buddy', r->table('dc'), { index => 'name' } )->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 3, 'Correct number of documents returned';

# zip
$res = r->table('marvel')->eq_join( 'dc_partner', r->table('dc') )->zip->run;

is $res->type, 2, 'Correct response type';
is scalar @{ $res->response }, 8, 'Correct number of documents returned';

# clean up
r->db('test')->drop->run;

done_testing();
