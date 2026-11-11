# HTTP API

`lt serve` exposes the `TaskStore` operations (`src/store.rs`) over HTTP so agents on
other hosts share one SQLite database. `lt` becomes a client of this API when `LT_URL`
is set; every command behaves the same as in local mode.

## What it must do

- [ ] `lt serve --addr <ip:port> --token-file <path> [--db <path>]` binds the given address (default `127.0.0.1:8095`). TLS is out of scope; publish through a TLS-terminating proxy (Cloudflare tunnel).
- [x] Token file: one accepted bearer token per line; blank lines ignored. Startup fails when the file is missing or holds no token. The file is read at startup.
- [x] Every route except `GET /healthz` requires `Authorization: Bearer <token>`; a missing or unknown token returns 401 before any database access. Token comparison is constant-time.
- [x] The actor for the audit trail comes from the `X-LT-Actor` header (default `unknown`). It is caller-asserted, not authenticated.
- [x] Concurrent claims of one task over HTTP produce exactly one 204; every other claimer gets 409.
- [x] A claim is one conditional `UPDATE` in an immediate transaction; events are recorded only for the winning claim.
- [ ] Client mode: `LT_URL` (base URL, no trailing slash needed) + `LT_TOKEN` (required when `LT_URL` is set). Responses 404 and 409 map back to `db::NotFound` / `db::Conflict` with the server's message, so CLI output matches local mode. Other non-2xx statuses fail with the status and the server's `error` message.

## Routes

All bodies are JSON. Errors are `{"error": "<message>"}`.

| Method | Path | Body | Success | `TaskStore` method |
|---|---|---|---|---|
| GET | `/healthz` | — | 200 `"ok"` (no auth) | — |
| POST | `/tasks` | `{"title", "description"?, "priority"?}` | 201 `Task` | `create_task` |
| GET | `/tasks?status=&assignee=` | — | 200 `[Task]` | `list_tasks` |
| GET | `/tasks/ready` | — | 200 `[Task]` | `ready_tasks` |
| GET | `/tasks/{id}` | — | 200 `Task` | `get_task` |
| PATCH | `/tasks/{id}` | `{"status"?, "priority"?, "title"?, "description"?, "assignee"?, "target_branch"?}` | 204 | `update_task` |
| POST | `/tasks/{id}/claim` | — | 204 | `claim_task` |
| POST | `/tasks/{id}/close` | — | 204 | `close_task` |
| GET | `/tasks/{id}/dependencies` | — | 200 `[Dependency]` | `get_dependencies` |
| POST | `/tasks/{id}/dependencies` | `{"depends_on", "dep_type"?}` (default `blocks`) | 204 | `add_dependency` |
| DELETE | `/tasks/{id}/dependencies/{depends_on}` | — | 204 | `remove_dependency` |
| GET | `/tasks/{id}/comments` | — | 200 `[Comment]` | `get_comments` |
| POST | `/tasks/{id}/comments` | `{"content"}` | 201 `Comment` | `add_comment` |
| GET | `/tasks/{id}/events` | — | 200 `[Event]` | `get_events` |

`Task`, `Dependency`, `Comment` and `Event` serialize exactly as the structs in `src/db.rs`.

## Error mapping

| Store error | HTTP |
|---|---|
| `db::NotFound` | 404 |
| `db::Conflict` | 409 |
| malformed JSON / missing field | 400 (axum default rejection is fine) |
| anything else | 500 |

## Tests asserting this spec

- `tests/db_test.rs`: atomic claim (concurrent claimers, one winner, one event pair).
- `tests/server_test.rs`: auth, routes, error mapping, concurrent HTTP claims.
- `tests/remote_test.rs`: `HttpStore` against a real `lt serve` router, including 404/409 mapping.

## Out of scope

- TLS, per-agent identities, token rotation without restart, MCP server.
