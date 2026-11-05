# MariaDB MCP Server Tests

## Running the tests

From the repository root:

```bash
# Unit tests only (no database needed) — integration tests skip automatically
python -m unittest discover -s src/tests -t .

# Everything, including integration tests
docker compose -f docker-compose.test.yml up -d --wait
python -m unittest discover -s src/tests -t .
docker compose -f docker-compose.test.yml down
```

Run the suite from the repository root, not from `src/`. `src/tests/__init__.py`
puts `src/` on `sys.path` so that `server.py`'s flat imports (`from config
import ...`) resolve while the tests import `src.server`.

## Continuous integration

`.github/workflows/tests.yml` runs on every push to `main` and on every pull
request. The `pull_request` trigger is deliberately unfiltered, so stacked pull
requests that target a feature branch instead of `main` still get checked.

Two jobs:

-   **Unit tests (no database)** — runs the suite with no container and no `REQUIRE_TEST_DATABASE`, asserting the integration tests skip cleanly. This is the documented experience for a contributor without Docker, so it is checked rather than assumed.
-   **Full suite** — brings up `docker-compose.test.yml` and runs everything with `REQUIRE_TEST_DATABASE=1`.

CI uses the same compose file you run locally, so the two cannot drift.

### `REQUIRE_TEST_DATABASE`

Because integration tests skip when the database is unreachable, a container
that failed to start would otherwise leave CI **green while verifying almost
nothing**. Setting `REQUIRE_TEST_DATABASE=1` makes that condition raise instead
of skip:

| Database | `REQUIRE_TEST_DATABASE` | Exit code |
| --- | --- | --- |
| healthy | `1` | 0 |
| unavailable | `1` | 1 — hard failure |
| unavailable | unset | 0 — skips, for local work without Docker |

The workflow waits with `docker compose up -d --wait`, which blocks on the
healthcheck. Plain `up -d` returns before MariaDB accepts connections and would
trip the guard.

Both jobs install with `uv sync --frozen`, so they use exactly the versions
pinned in the committed `uv.lock` and fail if that lockfile has drifted from
`pyproject.toml`. An unrelated upstream release therefore cannot break an
unrelated pull request.

## Layout

| File | Needs a database | What it covers |
| --- | --- | --- |
| `test_table_schema_comments.py` | No (mocked) | Schema-tool logic in isolation: comment handling, validation, error paths, field mapping |
| `test_list_databases_unittest.py` | No (mocked) | `list_databases` |
| `test_fastmcp_contract.py` | No (mocked) | The fastmcp/mcp contract: tool registration and input schemas, result serialization, error mapping to `ToolError`, the read-only prefix gate, and the stateless streamable-HTTP transport production runs |
| `test_integration_schema_comments.py` | Yes | Table/column comments against a real schema (DL-5840) |
| `test_mcp_server.py` | Yes | All six tools through a fastmcp client, plus read-only enforcement |
| `test_mariadb_mcp_tools.py` | Yes | Tool behaviour called directly: SQL execution, joins, aggregation, parameter edge cases |
| `smoke_test.py` | Yes | Standalone sanity check; run directly, not collected by `unittest` |
| `support.py` | — | Shared integration fixtures, the skip-if-unavailable probe, and the `REQUIRE_TEST_DATABASE` guard |
| `fixtures/01-test-schema.sql` | — | Schema loaded into the test container |

## The test database

`docker-compose.test.yml` runs MariaDB 11 on host port **3307**, so it never
collides with a local server on 3306. Storage is `tmpfs`, so every `up` starts
from an empty data directory and re-applies `fixtures/01-test-schema.sql`. The
healthcheck waits for the fixture tables, so `--wait` does not return early.

Connection details default to that container and can be overridden with
`TEST_DB_HOST`, `TEST_DB_PORT`, `TEST_DB_USER`, `TEST_DB_PASSWORD`,
`TEST_DB_NAME` and `TEST_DB_OTHER_NAME`. Note that the integration tests do
**not** read `.env`; they patch the settings that `server.py` binds at import
time, so they never touch a real database by accident.

When the container is not reachable, integration tests skip with a message
telling you how to start it, rather than failing. Set `REQUIRE_TEST_DATABASE=1`
to turn that skip into a hard failure — see below.

## What the fixture schema is for

Each object exists to pin down a specific behaviour:

-   `documented` — every column commented, plus one deliberately uncommented column, a `DEFAULT`, and a foreign key
-   `undocumented` — no comments anywhere; everything must come back as `''`
-   `column_defaults` — every shape of column default, because `INFORMATION_SCHEMA.COLUMN_DEFAULT` returns SQL literals (`'active'`) and the string `NULL` for "no default", all of which get decoded back to plain values
-   `with_comment_column` — has a column literally named `comment`, which is why `get_table_schema` nests columns under `columns` instead of returning them flat
-   `documented_view` — MariaDB reports `TABLE_COMMENT` as the literal string `'VIEW'` for views, which must be suppressed rather than surfaced as documentation
-   `parents` — foreign-key target, and a table with no FKs of its own
-   `mcp_test_other.documented` — same table and column names as `mcp_test.documented` but different comments, so a lookup that forgot `TABLE_SCHEMA` returns visibly wrong text

## Known behaviour pinned by tests

`execute_sql` passes `params or ()` to the driver. An empty tuple is not
`None`, so the driver applies `%`-formatting even when no parameters were
supplied, and a literal `%` must be doubled: `SHOW VARIABLES LIKE 'version%%'`
works, `'version%'` raises. `test_step_12*` pins both halves of this. Changing
it would silently alter what `%%` means for callers already escaping it.
