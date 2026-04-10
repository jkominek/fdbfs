Much of the test suite was coughed up by Codex with minimal
supervision. Filesystems are sufficiently common that it can string
together some basic sanity checks without too much problem.

Biggest issue is that there tends to be a lot of duplication in the
helper code; it also tends to produce way more of the stuff than seems
necessary.

So if you think something in here needs to be cleaned up, you're right.
