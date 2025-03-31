# `absorb-repo.sh`

This script absorbs a repository into the monorepo in a way that (mostly) preserves its history. History will be rewritten so that the repository's contents will be, or "will have always been", in a subdirectory, but otherwise the authors and dates of each commit will be preserved. Operations like `git blame` can continue past the point where the repository was absorbed.

Example usage:
```
$ git checkout -b absorb-worker
$ ./absorb-repo.sh worker git@github.com:codecov/worker.git apps/worker
$ git push origin absorb-worker
```

The above invocation will create one or two commits on the `absorb-worker` branch:
- if `apps/worker` already exists (like as a submodule), a commit will be added to delete it
- a merge commit that merges the `absorb-worker` branch with the rewritten default branch of `git@github.com:codecov/worker.git`

You can run it multiple times to absorb multiple repositories in a single branch.
