#!/usr/bin/perl

use feature 'say';
use lib qw'../google-protocolbuffers-perl/lib lib';
use Google::ProtocolBuffers;

use strict;
use warnings;

Google::ProtocolBuffers->parsefile('external/ql2.proto', {
  generate_code => 'lib/Rethinkdb/Protocol.pm',
  create_accessors => 1
});

