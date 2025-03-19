# livegrep

[Livegrep](https://github.com/livegrep/livegrep) is a fast multi-repo code search tool. This folder contains a livegrep configuration that indexes our repositories.

### Setup

It should work out of the box, but if you want to index any private repositories you'll need to do two things:
- [Generate a GitHub PAT](https://github.com/settings/tokens) with full "repo" access
- Create a `.env` file in this directory that looks something like:
  ```
  GITHUB_KEY=<your PAT, which probably starts with ghp_>
  EXTRA_REPOS="-repo=codecov/one-private-repo -repo=codecov/two-private-repo"
  ```

When you restart the service, it should add `codecov/one-private-repo` and `codecov/two-private-repo` to the index.

### Usage

The following command will re-run the indexer and start the service:
```
$ docker compose -fdocker-compose.{,indexer.}yml run --remove-orphans livegrep-indexer && docker compose up
```

Navigate to http://localhost:8910 and search away. Restart the service every few days to re-index everything.
