# perl-rethinkdb

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

* This is still in alpha stage
* This version is compatible with RethinkDB 1.16.2-1
* The implementation is close to 100% complete
* For now, see tests in `t/*.t` for examples

## Todo

* Double check all method parameters match the official drivers
* Add sugar syntax for `attr` (e.g. `$doc->{attr}`), `slice` (e.g. `$doc->[3..6]`), and `nth` (e.g. `$doc->[3]`)
* Add sugar syntax for as many operators as possible (e.g. `+`, `-`, `/`, `*`)
* Performance testing and fixes
* Submit to [CPAN](http://www.cpan.org/)
* Look into non-blocking IO
* Organize code
* Integrate with Travis-CI
