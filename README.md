# GHES Traffic Simulator

Standalone utilities for generating realistic, randomized traffic against a
GitHub Enterprise Server (GHES) appliance. The toolkit can provision simulator
users, create a deliberately difficult Git repository, run concurrent Git and
REST API activity, and import public or private repositories from github.com.

## Requirements

- Bash 3.2 or newer
- Git, curl, jq, openssl, and Python 3
- A GHES administrator token for appliance preparation
- A GHES user token for seeding, importing, and traffic generation

The scripts are designed to work on macOS Bash 3.2 and avoid `mapfile`, `flock`,
and platform-specific millisecond timestamp commands.

The searchable sample corpus, including the Project Gutenberg texts of *Pride
and Prejudice* and *Romeo and Juliet*, is a Git submodule. Clone with
`git clone --recurse-submodules`, or initialize it after cloning with
`git submodule update --init --recursive`. The corpus repository is private, so
you need access to `harricross/ghes-traffic-sim-testcontent`.

## Configure GHES

```bash
cp .gh-api-examples.conf.example .gh-api-examples.conf
chmod 600 .gh-api-examples.conf
${EDITOR:-vi} .gh-api-examples.conf
```

Set `hostname` to the appliance host without `/api/v3`, then set `org`, `repo`,
`GITHUB_TOKEN`, and `GITHUB_API_BASE_URL`. The token is sourced by the scripts;
never commit the resulting `.gh-api-examples.conf`.

## Prepare and seed

Preparation creates the organization, repository, simulator users, repository
permissions, impersonation tokens, and `tmp/sim-users.json`:

```bash
./prepare-appliance-for-traffic-sim.sh -n 16 -r search-corpus
./seed-messy-repo.sh -r search-corpus -c 200 -b 25 -k 256 -t 2000
```

Use the same repository name for preparation and seeding. The preparation
script grants simulator users push access to its target repository; when
seeding an alternate repo with `-r`, pass that repo to preparation as well.
The users manifest can be regenerated for existing simulator users.

Increase `-c`, `-b`, and `-k` to create a larger and more expensive repository.
`-t` controls how many tags are created (default 2000), mixing lightweight and
annotated tags across nested namespaces (`v1.*`, `release/*`, `build/ci-*`,
`sim/nightly/*`) so `refs/tags/` is wide and ref lookups do real work.
The generated token manifest is secret-bearing and is ignored by Git.

To add text-heavy, searchable content through the normal Git ingestion path,
use `-T` to set the size of one synthetic text file added to each main-history
commit:

```bash
./seed-messy-repo.sh -r search-corpus -c 500 -b 0 -t 0 -T 64
```

This adds 500 committed text files (about 32 MB total) under `searchable/`,
one to each of the 500 main-history commits. Each file pulls varied samples
from the synthetic records and book excerpts in `test-data/`, with stable
document IDs such as `main-1`. `-T` accepts 0-1024 KB per file, with 0
disabling the feature. The existing Git push sends these commits to GHES; the
seeder does not write directly to GHES-managed Elasticsearch indices.

For a large corpus, use bulk mode to generate files concurrently and commit
them in batches. Set `-c 0` to skip the randomized commit history, `-N` for the
file count, `-P` for generator workers, and `-B` for files per commit:

```bash
./prepare-appliance-for-traffic-sim.sh -n 30 -r search-large
./seed-messy-repo.sh -r search-large -c 0 -b 0 -k 1 -t 0 \
  -T 1024 -N 8000 -P 4 -B 250
```

This generates 8,000 1 MiB files using four workers, then adds them in 32 Git
commits before the normal push. Bulk mode still uses Git commits and does not
write directly to Elasticsearch. As with the other seeder modes, it force-pushes
the generated history, so use a new or disposable repository.

### Generate a standalone searchable sample

`generate-searchable-text.py` creates one local UTF-8 text file from the sample
records and book excerpts in `test-data/`. It takes an output path, size in KB,
and document ID:

```bash
python3 generate-searchable-text.py /tmp/sample.md 64 experiment-001
```

The size must be between 1 and 1024 KB. The document ID is included in each
generated record and also seeds the sample selection, so using the same ID and
corpus produces repeatable output. Use this script directly to inspect content
or create a local fixture. It only writes the file; it does not commit, push, or
index it. For GHES indexing and replication load, use `seed-messy-repo.sh -T`,
which adds generated files to ordinary Git commits and pushes the repository.

## Run traffic

```bash
./simulate-day-of-traffic.sh \
  -r messy-repo \
  -u tmp/sim-users.json \
  -p 16 \
  -d 3600 \
  -j 15 \
  -v
```

`-p` supports up to 16 concurrent workers. Operations are randomized per
worker, and the report includes success/error counts plus latency trends.

Use `-w` to provide an explicit traffic mix whose percentages total 100%:

```bash
./simulate-day-of-traffic.sh -u tmp/sim-users.json -p 16 -d 3600 \
  -w 'git_clone=35,git_fetch=25,api_read=20,api_commit=10,api_issue=10'
```

Use `-x` to disable operations; omitted operations are rebalanced among the
remaining enabled operations:

```bash
./simulate-day-of-traffic.sh -u tmp/sim-users.json -p 8 -d 3600 \
  -x git_clone,git_fetch,git_push,git_branch
```

## Add persistent tags or generate tag churn

Two operations drive `/repositories/:repository_id/git/refs/*` traffic:

- `api_refs_read` lists, paginates, and prefix-matches refs, plus single-ref
  lookups and the repository tags listing.
- `api_tag_ref` creates a batch of tag refs against random commits, then
  deletes roughly half of them so the ref store churns instead of only growing.

For an existing repository, use `seed-repo-tags.sh` to add permanent
lightweight tags without cloning the repository or force-pushing its branches.
It targets random commits from the latest 100 commits returned by GHES. The
configured `GITHUB_TOKEN` must have write access:

```bash
./seed-repo-tags.sh -r search-small -n 2000 -p load-test
```

Do not rerun `seed-messy-repo.sh -t` to add tags to an existing repository.
That script builds a fresh history and force-pushes its refs.

To create tag churn after adding the baseline refs, weight traffic towards
`api_tag_ref`. Each operation creates 3-10 tags and deletes roughly half of
those it created, so the tag set grows while the API and ref store are exercised:

```bash
./simulate-day-of-traffic.sh -r search-small -u tmp/sim-users.json -p 4 -d 300 \
  -w 'api_tag_ref=100'
```

For mixed ref reads and tag churn:

```bash
./simulate-day-of-traffic.sh -r search-small -u tmp/sim-users.json -p 16 -d 3600 \
  -w 'api_refs_read=70,api_tag_ref=25,git_fetch=5'
```

## Import github.com repositories

Import repositories into the configured GHES organization:

```bash
./import-github-repos-into-org.sh -o traffic-sim -p src- \
  cli/cli hashicorp/terraform
```

Use `-f repos.txt` for one source repository per line. Public repositories do
not require a github.com token; set `GITHUB_DOTCOM_TOKEN` when importing
private repositories.

## Security

Keep `.gh-api-examples.conf`, `tmp/sim-users.json`, and logs out of source
control. Use disposable test accounts and tokens on non-production appliances.
These scripts intentionally create users, repositories, forks, issues, pull
requests, and commits.
