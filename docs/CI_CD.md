# CI/CD

GitHub Actions, defined in `.github/workflows/`. Every job calls the same
commands as the Makefile, so a failing job reproduces locally with `make`.

## Workflows

**`ci.yml`** runs on pull requests and on pushes to `main` (superseded runs on
the same ref are cancelled):

| Job | What it runs | Reproduce locally |
| --- | --- | --- |
| `python` | `uv sync --frozen`, ruff check, ruff format check, mypy `--strict`, unit tests with coverage (`coverage.xml` uploaded) | `make check` |
| `frontend` | `npm ci`, typecheck, ESLint, Prettier check, Vitest, production build | `make frontend-check frontend-build` |
| `observability` | `promtool` check and rule unit tests (pinned release, SHA-256 verified), dashboards-in-sync check, benchmark-tables-in-sync check, `actionlint` on the workflows | `make prometheus-test` |
| `terraform` | `fmt -check`, `init -backend=false -lockfile=readonly`, `validate` | `make terraform-check` |
| `compose` | `docker compose config` against `.env.example` | `make compose-config` |
| `images` | Builds all nine service images (matrix, buildx layer cache, nothing pushed) | `docker compose build` |
| `integration` | Brings up Kafka, PostgreSQL, Redis and the rest of the infrastructure with `make infra-up`, runs `make test-integration`, tears down | `make infra-up test-integration` |

`integration` waits for `python` and `compose`; the rest run in parallel.

**`security.yml`** runs weekly, on pushes to `main`, and on pull requests that
touch a lockfile, a `pyproject.toml` or a workflow (new advisories appear
without any code change, hence the schedule):

| Job | Tool |
| --- | --- |
| `python-audit` | `pip-audit` on the exported `uv.lock` |
| `npm-audit` | `npm audit --audit-level=high` for the dashboard |
| `secrets` | `gitleaks` over the full git history (pinned release, SHA-256 verified); the same hook runs locally through pre-commit |

**`dependabot.yml`** opens weekly update pull requests for uv, npm, Docker
and GitHub Actions.

## What was verified, and what was not

Run before committing, in a sandbox without Docker or GitHub:

* The Python and frontend jobs' commands on a clean `git clone` (fresh
  `uv sync --frozen`, `npm ci`): all green. Data files are git-ignored, and the
  unit tests do not need them.
* `actionlint` over all workflows: no findings.
* `pip-audit`, `npm audit` and `gitleaks` against the current tree and
  history: no findings.
* `promtool` rule tests and Terraform `fmt` and `validate`.
* The integration tests against a real Kafka 4.0.0 broker, PostgreSQL 16 and
  Redis 7 started as local processes: 36 passed (they found one bug, fixed in
  the same phase).

Not run, so expect to look at the first run:

* The workflows themselves on GitHub: a first run can still fail on something
  only the hosted runners show (action inputs, cache behaviour, runner
  resources).
* `images`, `compose` and `integration`, which need Docker. The integration job
  uses the project's own `make infra-up`, which builds and starts about a
  dozen containers; if it is too heavy for a hosted runner, replace it with
  GitHub `services:` containers for Kafka, PostgreSQL and Redis.
* No image is pushed anywhere, no image is scanned, and nothing deploys:
  there is no release workflow. That would be the next step together with the
  Terraform in `infrastructure/terraform`, which is likewise validated only.

Third-party actions are pinned to major versions, not commit SHAs, and
Dependabot keeps them current; pinning to SHAs is the stricter option.
