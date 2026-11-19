# llm-tasks

Persistent task store for LLM agents: `lt`, a CLI over SQLite with dependencies,
atomic claims, comments and an audit trail. One `lt serve` instance lets agents on
different hosts share the same tasks.

## Local

```sh
export LT_PROJECT=sakuin        # every task needs a project (or pass --project)
lt create "Fix login" -p 2      # prints the new id, e.g. lt-a1b2
lt ready                        # unblocked, unassigned tasks
lt create "Recheck advisories" --due 2026-11-15T09:00:00-06:00   # a reminder
lt due                          # reminders past due, not yet delivered
lt attach lt-a1b2 build.log     # file stored with the task (max 10 MiB)
lt ask lt-a1b2 '[{"question":"Which DB?","options":[{"label":"SQLite"},{"label":"Postgres"}]}]'
lt answer 1 '{"Which DB?":"SQLite"}'   # the task is not ready until answered
lt claim lt-a1b2 --actor me     # atomic: exactly one claimer wins
lt claim                        # claim the best ready task from LT_PROJECT
lt claim --project sakuin       # ... or from an explicit project
lt close lt-a1b2
```

Database: `--db`, else `LT_DB`, else `~/.local/share/llm-tasks/tasks.db`.
Actor: `--actor`, else `LT_ACTOR`.

## Shared server

```sh
lt serve --addr 127.0.0.1:8095 --token-file tokens   # one bearer token per line
LT_URL=https://tasks.example LT_TOKEN=... lt ready    # same commands, remote store
```

`lt serve` speaks plain HTTP; put a TLS-terminating proxy in front of it. The API
contract is [docs/specs/http-api.md](docs/specs/http-api.md). Production runs on
the sakuin droplet as `tasks.sakuin.org` (`sakuin/ops/nixos/llm-tasks.nix`).

## Dashboard

`lt serve --ui-addr <ip:port>` adds a second listener with a web dashboard
(`ui/index.html`, compiled into the binary) and the same API without bearer auth.
Bind it only to an address the network already restricts. Production binds it to
the droplet's home-WireGuard address, `http://192.168.5.4:8096/`; the home router
allows only aso to reach it. Writes from the page are recorded with actor `web`.

## Install

`./deploy.sh` installs `lt` into `~/.local/bin`.
