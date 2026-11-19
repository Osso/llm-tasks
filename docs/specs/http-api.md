# HTTP API

`lt serve` exposes the `TaskStore` operations (`src/store.rs`) over HTTP so agents on
other hosts share one SQLite database. `lt` becomes a client of this API when `LT_URL`
is set; every command behaves the same as in local mode.

## What it must do

- [x] `lt serve --addr <ip:port> --token-file <path> [--db <path>]` binds the given address (default `127.0.0.1:8095`). TLS is out of scope; publish through a TLS-terminating proxy (Cloudflare tunnel).
- [x] Token file: one accepted bearer token per line; blank lines ignored. Startup fails when the file is missing or holds no token. The file is read at startup.
- [x] `--rules-file <path>` (optional): `GET /rules` returns that file as `text/markdown`, read on every request so edits apply without a restart. Without the flag it returns 404. Agents that cannot read host rule files (OpenAI dots) fetch their coordination rules here.
- [x] `GET /events?after=<id>` returns events on all tasks with `id > after` (default 0), oldest first, so a watcher can follow the tracker with one cursor. Adding a comment records a `commented` event whose `new_value` is the comment id.
- [x] `--ui-addr <ip:port>` (optional) binds a second listener on the same database. It serves the web dashboard at `GET /` (`ui/index.html`, compiled in) and every API route WITHOUT bearer auth, so it must only be bound to a network-restricted address (production: the droplet's home-WireGuard address). The main listener does not serve `/`. The dashboard sends `X-LT-Actor: web`. It lists tasks grouped by status, plus a derived Scheduled group (open, not in progress or blocked, `due_at` in the future; it moves to Pending once due, re-evaluated every 30 s), with text, assignee and ready filters, and edits status, priority, assignee, due time, branch, title and description. It also creates and closes tasks, adds and removes blockers, comments, uploads and downloads attachments, and answers question sets. It refreshes when `/events` reports new events (polled every 3 s).
- [x] Every route on the main listener except `GET /healthz` requires `Authorization: Bearer <token>`; a missing or unknown token returns 401 before any database access. Token comparison is constant-time.
- [x] The actor for the audit trail comes from the `X-LT-Actor` header (default `unknown`). It is caller-asserted, not authenticated.
- [x] Concurrent claims by ID of one task over HTTP produce exactly one 204; every other claimer gets 409. A claim by ID requires a non-blank `{"project"}` and returns 400 unless it equals the task's project; a claim by ID uses one conditional `UPDATE` in an immediate transaction, and events are recorded only for the winning claim.
- [x] `POST /tasks/claim` with a non-blank `{"project"}` selects and claims one ready task of that project in one immediate SQLite transaction, ordered by priority DESC, created_at ASC, id ASC. It returns 200 with the claimed `Task`, 204 when nothing is ready, and 400 when project is missing, null, empty or blank. It does not validate the supplied project slug. Concurrent callers never receive the same task. `lt claim` requires `--project` or `LT_PROJECT` with or without an ID, and without an ID it needs no prior `lt ready`.
- [x] Reminders are tasks with a due time. `due_at` and `delivered_at` are RFC 3339 UTC (`...Z`) or null. Input due times need an explicit offset and are normalized to UTC; any other value returns 400. Changing `due_at`, or setting status back to `pending`, clears `delivered_at`.
- [x] `GET /tasks/due` returns tasks whose status is not `completed`/`done`, whose `due_at` is at or before the server's current time, and whose `delivered_at` is null, ordered by `due_at`. `POST /tasks/{id}/delivered` with `{"due_at"}` sets `delivered_at` only when that due time is still current, undelivered and open (one conditional `UPDATE`); otherwise 409. It records a `delivered` event. The Pi Supervisor polls this route and delivers each due task into its conversation.
- [x] Every task belongs to a project: a lowercase slug matching `^[a-z0-9][a-z0-9-]{0,39}$` (e.g. `wow-ui-sim`), stored in `tasks.project TEXT NOT NULL`. Projects need no registration. `POST /tasks` without `project` or with an invalid one returns 400; `PATCH` may move a task to another valid project (recorded as an `updated` event on field `project`). `GET /tasks?project=` filters. CLI: `lt create --project <slug>` (default `LT_PROJECT`; required), `lt list --project`, `lt update --project`; list and show output include the project. The dashboard shows a project column, a project selector kept in `?project=`, and a required project in the create dialog.
- [x] Migration of a database from before projects: opening adds a nullable `project` column and fails, naming the number of tasks without a project, until the operator sets one on every task. The next open rebuilds `tasks` with `project NOT NULL` (SQLite's table-rebuild procedure: foreign keys off, `foreign_key_check` before commit), keeping every row, dependency and event.
- [x] CLI: `lt create --due <time>`, `lt update <id> --due <time>` (`""` clears it), `lt due`.
- [x] `GET /tasks/ready` (and `lt ready`) lists open, unassigned tasks with no open blocker, no unanswered question set, and no due time in the future.
- [x] Attachments: `POST /tasks/{id}/attachments?name=<name>` stores the raw request body (Content-Type kept as the MIME type, default `application/octet-stream`) in tasks.db, so the database backup covers it. A file is at most 10 MiB (413 above that); the name must be non-empty and contain no `/` (400). It records an `attached` event (`new_value` = `<id> <name>`). `GET /attachments/{id}` returns the bytes with `Content-Type`, `Content-Disposition` (RFC 6266: ASCII `filename` with `_` for other characters, exact UTF-8 name in `filename*`) and the metadata as ASCII JSON in `X-LT-Attachment`. CLI: `lt attach <task> <file> [--name] [--mime]`, `lt attachments <task>`, `lt attachment <id> [-o file]`.
- [x] Questions: `POST /tasks/{id}/questions` with `{"questions": [...]}` in Pi's `ask_questions` shape (1-4 questions with distinct texts; each has `question`, optional `header`, `options` that are empty or 2-4 distinct non-empty `{label, description?}`, and `multiSelect`). Invalid sets return 400. It records an `asked` event, so `lt-watch` wakes the orchestrator, which answers or forwards the questions to the Supervisor. `POST /questions/{id}/answer` with `{"answers": {"<question text>": "<answer>"}}` must answer every question and nothing else (400); a second answer returns 409. It records an `answered` event. CLI: `lt ask <task> <json|->`, `lt questions <task>`, `lt answer <id> <json|->`; `lt show` lists attachments and question sets.
- [x] Client mode: `LT_URL` (base URL, no trailing slash needed) + `LT_TOKEN` (required when `LT_URL` is set). Responses 400, 404 and 409 map to `db::Invalid`, `db::NotFound` and `db::Conflict` with the server's message. Other non-2xx statuses fail with the status and the server's error message or response body.

## Routes

Request bodies are JSON except for attachment uploads. Store errors are `{"error": "<message>"}`; axum rejections may use plain text.

| Method | Path | Body | Success | `TaskStore` method |
|---|---|---|---|---|
| GET | `/healthz` | — | 200 `"ok"` (no auth) | — |
| GET | `/events?after=` | — | 200 `[Event]` | — (`Database::events_after`) |
| GET | `/rules` | — | 200 markdown text (404 when unconfigured) | — |
| POST | `/tasks` | `{"project", "title", "description"?, "priority"?}` | 201 `Task` | `create_task` |
| GET | `/tasks?status=&assignee=&project=` | — | 200 `[Task]` | `list_tasks` |
| GET | `/tasks/ready` | — | 200 `[Task]` | `ready_tasks` |
| GET | `/tasks/{id}` | — | 200 `Task` | `get_task` |
| PATCH | `/tasks/{id}` | `{"status"?, "priority"?, "title"?, "description"?, "assignee"?, "target_branch"?, "due_at"?, "project"?}` | 204 | `update_task` |
| GET | `/tasks/due` | — | 200 `[Task]` | `due_tasks` |
| POST | `/tasks/{id}/attachments?name=` | raw bytes | 201 `Attachment` | `add_attachment` |
| GET | `/tasks/{id}/attachments` | — | 200 `[Attachment]` | `list_attachments` |
| GET | `/attachments/{id}` | — | 200 bytes | `get_attachment` |
| POST | `/tasks/{id}/questions` | `{"questions"}` | 201 `QuestionSet` | `ask_questions` |
| GET | `/tasks/{id}/questions` | — | 200 `[QuestionSet]` | `list_questions` |
| POST | `/questions/{id}/answer` | `{"answers"}` | 204 | `answer_questions` |
| POST | `/tasks/{id}/delivered` | `{"due_at"}` | 204 | `mark_delivered` |
| POST | `/tasks/claim` | `{"project"}` (required) | 200 `Task`, or 204 when nothing is ready | `claim_next` |
| POST | `/tasks/{id}/claim` | `{"project"}` (required) | 204, or 400 for a missing or foreign project | `claim_task` |
| POST | `/tasks/{id}/close` | — | 204 | `close_task` |
| GET | `/tasks/{id}/dependencies` | — | 200 `[Dependency]` | `get_dependencies` |
| POST | `/tasks/{id}/dependencies` | `{"depends_on", "dep_type"?}` (default `blocks`) | 204 | `add_dependency` |
| DELETE | `/tasks/{id}/dependencies/{depends_on}` | — | 204 | `remove_dependency` |
| GET | `/tasks/{id}/comments` | — | 200 `[Comment]` | `get_comments` |
| POST | `/tasks/{id}/comments` | `{"content"}` | 201 `Comment` | `add_comment` |
| GET | `/tasks/{id}/events` | — | 200 `[Event]` | `get_events` |

`Task`, `Dependency`, `Comment` and `Event` serialize exactly as the structs in `src/db.rs`; `Attachment` (metadata, no bytes) as in `src/attachments.rs`; `QuestionSet` as in `src/questions.rs`.

## Error mapping

| Store error | HTTP |
|---|---|
| `db::NotFound` | 404 |
| `db::Conflict` | 409 |
| `db::Invalid` (missing or invalid project, bad due time, question set, answers, attachment name or size) | 400 |
| request body over 10 MiB | 413 |
| missing, null, empty or blank `project` in `/tasks/claim` or `/tasks/{id}/claim` | 400 |
| task does not belong to the claimed project | 400 |
| malformed JSON | 400 (axum default rejection) |
| invalid JSON data / missing required field | 422 (axum default rejection) |
| anything else | 500 |

## Tests asserting this spec

- `tests/db_test.rs`: atomic claim (concurrent claimers, one winner, one event pair); cross-project claim rejection; one-command claim (priority, project filter, concurrent `claim_next` never double-claims); due listing, due-time validation, delivery reset on reschedule/reopen, stale or repeated delivery conflicts; future reminders not ready; question validation, ready blocking until answered, single answer; attachment round trip, size cap.
- `tests/db_test.rs` (projects): validation, `?project` filter, moving a task with its event; legacy database with tasks refuses to open until backfilled, then rebuilds NOT NULL keeping tasks, due times and dependencies.
- `tests/server_test.rs`: missing/invalid project on creation and missing project on claim return 400, `?project=` filter; the dashboard listener (page, tokenless writes visible through the token API with actor `web`, main listener 401 without token and 404 for `/`); auth, routes, error mapping, concurrent HTTP claims (by ID and `POST /tasks/claim`), `/rules` auth, live file edits and 404 when unconfigured, `/events` cursor feed, `/tasks/due` and delivery acknowledgement; `HttpStore` attachments (non-ASCII name, 413) and questions (400, 409, ready blocking) against the real router.
- `tests/remote_test.rs`: `HttpStore` request shapes, including the project claim body, and error mapping against a stub server.
- `tests/cli_remote_test.rs`: a real `lt serve` process and `lt` clients in `LT_URL` mode: round trip, single claim winner, one-command `lt claim`, required project for both claim forms, foreign-project rejection, empty claim exit status, 401, 404.

## Watcher

`pi-extension/lt-watch.ts` is a Pi extension that polls `/events` and wakes a session only when another actor changes the tracker. It is enabled per Pi session in `~/.config/llm-tasks/watch.json`; the file header documents the format. Install it by copying it to `~/.config/pi/agent/extensions/`.

## Out of scope

- TLS, per-agent identities, token rotation without restart, MCP server.
