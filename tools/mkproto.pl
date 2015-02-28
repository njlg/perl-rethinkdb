#!/usr/bin/perl

use feature ':5.16';

use strict;
use warnings;

use Data::Dumper;

sub convert {
  my $lines = shift;

  my $package = [];
  my $sub_package = [];
  my $new_lines = [];
  my $lc = q{};

  foreach my $line ( @{$lines} ) {
    if ( $line =~ /^(\s*)(message) (?P<name>\w+) \{/ ) {
      # if ( defined $1 && $2 && $3 ) {
      if ( defined $1 && $2 && $3 && $3 ne 'AssocPair' ) {
        $lc = lcfirst $3;
        push @{$new_lines}, @{$sub_package}, '';
        push @{$new_lines}, "package Rethinkdb::Protocol::$3;";
        push @{$new_lines}, 'use Rethinkdb::Base -base;';
        push @{$package}, "has '$lc' => sub { Rethinkdb::Protocol::$3->new; };";
        $sub_package = [];
      }
    }
    elsif ( $line =~ /^(\s*)(message|enum) (?P<name>\w+) \{/ ) {
      if ( defined $1 && $2 && $3 ) {
        $lc = lcfirst $3;
        push @{$new_lines}, "has '$lc' => sub { Rethinkdb::Protocol::$3->new; };";
        push @{$sub_package}, '', "package Rethinkdb::Protocol::$3;", 'use Rethinkdb::Base -base;';
        # push @{$sub_package}, '1;', '', "package $3;", 'use Rethinkdb::Base -base;';
      }
    }
    elsif ( $line =~ /^(\s*)(?P<name>\w+)\s*=\s*(?P<value>\w+)/ ) {
      if ( defined $1 && $2 && $3 ) {
        $lc = lc $2;
        push @{$sub_package}, "has '$lc' => $3;";
      }
    }
  }

  if( @{$sub_package} ) {
    push @{$new_lines}, @{$sub_package};
  }

  my $template = join '', <DATA>;
  my $content = join "\n", @{$package};
  $content .= join "\n", @{$new_lines};

  $template =~ s/{{CONTENT}}/$content/;

  return $template;
}

sub convert_write {
  my $input  = shift;
  my $output = shift;

  open my $file, '<', $input or die "Could not open `$input`";
  my @lines = <$file>;
  close $file;

  my $content = convert \@lines;

  open $file, '>', $output or die "Could not open `$output`";
  say $file $content;
  close $file;
}

convert_write( 'external/ql2.proto', 'lib/Rethinkdb/Protocol.pm' );
say 'Done.';

__DATA__

# DO NOT EDIT
# Autogenerated by mkproto.pl

package Rethinkdb::Protocol;
use Rethinkdb::Base -base;

{{CONTENT}}

1;

=encoding utf8

=head1 NAME

Rethinkdb::Protocol - Rethinkdb Protocol

=head1 SYNOPSIS

  my $p = Rethinkdb::Protocol->new;
  $p->term->termType->get_all;

=head1 DESCRIPTION

This file is automatically generated to enable this driver to serialize &
deserialize RethinkDB Query Langauge messages.

=head1 ATTRIBUTES

L<Rethinkdb::Protocol> implements the following attributes.

=head2 backtrace

Quick access to the C<backtrace> section of the protocol.

=head2 datum

Quick access to the C<datum> section of the protocol.

=head2 frame

Quick access to the C<frame> section of the protocol.

=head2 query

Quick access to the C<query> section of the protocol.

=head2 response

Quick access to the C<response> section of the protocol.

=head2 term

Quick access to the C<term> section of the protocol.

=head2 versionDummy

Quick access to the C<versionDummy> section of the protocol.

=head1 SEE ALSO

L<Rethinkdb>, L<http://rethinkdb.com>

=cut
