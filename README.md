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
./prepare-appliance-for-traffic-sim.sh -n 16
./seed-messy-repo.sh -r messy-repo -c 200 -b 25 -k 256
```

Increase `-c`, `-b`, and `-k` to create a larger and more expensive repository.
The generated token manifest is secret-bearing and is ignored by Git.

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
