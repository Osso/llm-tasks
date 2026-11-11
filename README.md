# llm-tasks

Persistent task store for LLM agents: `lt`, a CLI over SQLite with dependencies,
atomic claims, comments and an audit trail. One `lt serve` instance lets agents on
different hosts share the same tasks.

## Local

```sh
lt create "Fix login" -p 2      # prints the new id, e.g. lt-a1b2
lt ready                        # unblocked, unassigned tasks
lt claim lt-a1b2 --actor me     # atomic: exactly one claimer wins
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

## Install

`./deploy.sh` installs `lt` into `~/.local/bin`.
