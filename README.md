# perl-rethinkdb

[![Build Status](https://travis-ci.org/njlg/perl-rethinkdb.svg?branch=master)](https://travis-ci.org/njlg/perl-rethinkdb)
[![Coverage Status](https://coveralls.io/repos/njlg/perl-rethinkdb/badge.svg?branch=master)](https://coveralls.io/r/njlg/perl-rethinkdb?branch=master)
[![CPAN version](https://badge.fury.io/pl/Rethinkdb.svg)](https://metacpan.org/pod/Rethinkdb)

A Pure-Perl RethinkDB Driver

```perl
package MyApp;
use Rethinkdb;

r->connect->repl;
r->table('agents')->get('007')->update(
  r->branch(
    r->row->attr('in_centrifuge'),
    {'expectation': 'death'},
    {}
  )
)->run;
```

## Documentation
See http://njlg.info/perl-rethinkdb/

## Notes

* This version is compatible with RethinkDB 2.0.4
* This is still in beta stage
* For examples see the tests in `t/*.t` or see the documentation (link above)

## Todo

* Add sugar syntax for `attr` (e.g. `$doc->{attr}`), `slice` (e.g. `$doc->[3..6]`), and `nth` (e.g. `$doc->[3]`)
* Add sugar syntax for as many operators as possible (e.g. `+`, `-`, `/`, `*`)
* Performance testing and fixes
* Look into non-blocking IO
