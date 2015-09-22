use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use Rethinkdb;

# setup
my $conn = r->connect->repl;
r->db('test')->drop->run;
r->db('test')->create->run;
r->db('test')->table_create('geo')->run;
r->db('test')->table('geo')->index_create( 'location', { geo => r->true } )
  ->run;

# circle
my $res = r->table('geo')->insert(
  {
    'id'           => 300,
    'name'         => 'Hayes Valley',
    'neighborhood' => r->circle( [ -122.423246, 37.779388 ], 1000 )
  }
)->run;

is $res->type, 1, 'Correct response type';
is $res->response->{inserted}, 1, 'Correct response';

# distance
my $point1 = r->point( -122.423246, 37.779388 );
my $point2 = r->point( -117.220406, 32.719464 );
$res = r->distance( $point1, $point2, { unit => 'km' } )->run($conn);

is $res->type, 1, 'Correct response type';
like $res->response, qr/734.125/, 'Correct response';

# fill
r->table('geo')->insert(
  {
    'id'        => 201,
    'rectangle' => r->line(
      [ -122.423246, 37.779388 ],
      [ -122.423246, 37.329898 ],
      [ -121.886420, 37.329898 ],
      [ -121.886420, 37.779388 ]
    )
  }
)->run;

$res
  = r->table('geo')->get(201)
  ->update( { 'rectangle' => r->row->bracket('rectangle')->fill },
  { non_atomic => r->true } )->run;

is $res->type, 1, 'Correct response type';
is $res->response->{replaced}, 1, 'Correct response';

# geojson
my $geo_json
  = { 'type' => 'Point', 'coordinates' => [ -122.423246, 37.779388 ] };

$res = r->table('geo')->insert(
  {
    'id'       => 'sfo',
    'name'     => 'San Francisco',
    'location' => r->geojson($geo_json)
  }
)->run;

is $res->type, 1, 'Correct response type';
is $res->response->{inserted}, 1, 'Correct response';

# to_geojson
$res = r->table('geo')->get('sfo')->bracket('location')->to_geojson->run;

is $res->type, 1, 'Correct response type';
is_deeply $res->response, $geo_json, 'Correct response';

# wait on the index since these next few tests require the index to be ready
r->table('geo')->index_wait('location')->run;

# get_intersecting
my $circle1 = r->circle( [ -122.423246, 37.770378359 ], 10, { unit => 'mi' } );
$res = r->table('geo')->get_intersecting( $circle1, { index => 'location' } )
  ->run;

is $res->type, 2, 'Correct response type';
is $res->response->[0]->{id}, 'sfo', 'Correct response';

# get_nearest
my $secret_base = r->point( -122.422876, 37.777128 );
$res
  = r->table('geo')
  ->get_nearest( $secret_base, { index => 'location', max_dist => 5000 } )
  ->run;

is $res->type, 1, 'Correct response type';
is $res->response->[0]->{doc}->{id}, 'sfo', 'Correct response';
is $res->response->[0]->{dist}, '252.951509509011', 'Correct response';

# includes
$point1 = r->point( -117.220406, 32.719464 );
$point2 = r->point( -117.206201, 32.725186 );
$res = r->circle( $point1, 2000 )->includes($point2)->run($conn);

is $res->type, 1, 'Correct response type';
is $res->response, r->true, 'Correct response';

# intersects
$point1 = r->point( -117.220406, 32.719464 );
$point2 = r->point( -117.206201, 32.725186 );
$res = r->circle( $point1, 2000 )->intersects($point2)->run($conn);

is $res->type, 1, 'Correct response type';
is $res->response, r->true, 'Correct response';

# line
$res = r->table('geo')->insert(
  {
    id    => 101,
    route => r->line( [ -122.423246, 37.779388 ], [ -121.886420, 37.329898 ] )
  }
)->run;

is $res->type, 1, 'Correct response type';
is $res->response->{inserted}, 1, 'Correct response';

# point
$res = r->table('geo')->insert(
  {
    id       => 1,
    name     => 'San Francisco',
    location => r->point( -122.423246, 37.779388 )
  }
)->run;

is $res->type, 1, 'Correct response type';
is $res->response->{inserted}, 1, 'Correct response';

# polygon
$res = r->table('geo')->insert(
  {
    id        => 102,
    rectangle => r->polygon(
      [ -122.423246, 37.779388 ],
      [ -122.423246, 37.329898 ],
      [ -121.886420, 37.329898 ],
      [ -121.886420, 37.779388 ]
    )
  }
)->run;

is $res->type, 1, 'Correct response type';
is $res->response->{inserted}, 1, 'Correct response';

# polygon_sub
my $outer_polygon = r->polygon(
  [ -122.4, 37.7 ],
  [ -122.4, 37.3 ],
  [ -121.8, 37.3 ],
  [ -121.8, 37.7 ]
);
my $inner_polygon = r->polygon(
  [ -122.3, 37.4 ],
  [ -122.3, 37.6 ],
  [ -122.0, 37.6 ],
  [ -122.0, 37.4 ]
);
$res = $outer_polygon->polygon_sub($inner_polygon)->run($conn);

is $res->type, 1, 'Correct response type';
isa_ok $res->response->{coordinates}, 'ARRAY',   'Correct response';
is $res->response->{type},            'Polygon', 'Correct response';
is $res->response->{'$reql_type$'}, 'GEOMETRY', 'Correct response';

# clean up
r->db('test')->drop->run;

done_testing();
