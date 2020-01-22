Thank you for your interest in helping.

## There is no code of conduct

I'd write "just don't be a jerk", but that only works if you've
internalized that others do not perceive you or your behavior in the
same way that you do.  So, just think about that for a bit. Beyond
that, if we could enumerate all inappropriate behaviors, philosophers
would've made more progress on ethics in the last 2000 years than
they've managed.

Apropos of some of the outrages that have occurred in open source:

* I may not accept your PR.
* If you don't like what I'm doing, that's ok! You can fork the code,
  subject to the license terms. I won't be offended, that's why it is
  open. If you do a better job of running your fork than I do of
  running mine, then that's great, I can go do something else.

## I rebase the repo

While I'm the only person working on it, I'm willing to rebase more or
less arbitrarily. If I become aware that others are working on it,
I'll calm that down. But for now you should assume the most recent
commit, and any others made in the past ~24 hours are candidates to be
squashed/whatevered.

## Productive contributions

### Bug reports

There are a lot of moving parts. While I'm sympathetic to one off bug
reports, anything that you can't write down a test case for is
unactionable. I'll leave it open for a little bit in case anyone else
can chime in with more useful information, but it'll get closed sooner
or later.

It's ok if the test case only triggers your bug 1 time in 1000; I'll
gladly run it 10,000 times. But "I saw a weird thing happen once" is
currently far too likely and has way too many possible causes.

### Pull requests / patches

It is trendy at the moment to suggest that open source projects should
accept all pull requests, to be inclusive. I believe the assumption is
that if you accept enough of them, then they'll sort of average out
and quality will improve. I don't think that is the case.

Any pull request that doesn't at least appear to be a "pareto
improvement" on the current state of the code is not going to be
merged. For instance, a patch which adds new functionality, but causes
old functionality to fail its tests will not be merged. Code which
fixes a bug but makes a mess of the code style will not be accepted.

Please match the existing style, ensure all existing tests are passed.

#### Git style

In all likelihood I will squash your pull request into a single
commit, and rewrite the message. If your PR represents a significant
amount of work, best represented as multiple commits and/or you
want me to not squash it, then:

* Every commit message needs to have at least a sentence worth of coherent text.
* Every commit should represent some meaningful unit of work.
* No commits for fixing a typo in a previous commit. Squash that into
  the relevant commit before pushing.
* Don't reformat code you're not making semantic changes to. Intentionally or
  otherwise.

This is to ensure that the git history can be consulted to determine
when, and why a given line of code was changed. I make heavy use of
git blame, and you should too.
