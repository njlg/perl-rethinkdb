# perl-rethinkdb

[![Build Status](https://travis-ci.org/njlg/perl-rethinkdb.svg?branch=master)](https://travis-ci.org/njlg/perl-rethinkdb)
[![Coverage Status](https://coveralls.io/repos/njlg/perl-rethinkdb/badge.svg?branch=coveralls-init)](https://coveralls.io/r/njlg/perl-rethinkdb?branch=coveralls-init)

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

* This version is compatible with RethinkDB 1.16.2-1
* This is still in beta stage
* For examples see the tests in `t/*.t`

## Todo

* Add sugar syntax for `attr` (e.g. `$doc->{attr}`), `slice` (e.g. `$doc->[3..6]`), and `nth` (e.g. `$doc->[3]`)
* Add sugar syntax for as many operators as possible (e.g. `+`, `-`, `/`, `*`)
* Performance testing and fixes
* Submit to [CPAN](http://www.cpan.org/) &mdash; Coming soon!
* Look into non-blocking IO
