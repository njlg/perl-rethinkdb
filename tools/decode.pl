#!/bin/env perl

use feature ':5.10';
use utf8;
use strict;
use warnings;

use Data::Dumper;
$Data::Dumper::Indent = 1;

use lib qw'../google-protocolbuffers-perl/lib lib';
use Rethinkdb::Protocol;

my @i = <>;
my $s = join '', @i;
chomp $s;
my $q = Query->decode($s);

my $out = Dumper $q;

$out =~ s/\$VAR1 = //g;
$out =~ s/bless\( //g;
$out =~ s/, '[^']+' \)//g;

say $out;
