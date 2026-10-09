# shardio

Sharded ZIP archives plus a Parquet index for many-small-blob workloads. Design summary: `docs/design.md`; usage: `README.md`.

## Commands

Use the make targets (`make help` lists them): `make install`, `make all` (format-check, lint, typecheck, test), `make test-slow`, `make build`, `make publish`.

## Rules

- The library stays domain-agnostic. It stores opaque bytes with caller-supplied metadata; no example, name, or test may assume what the blobs are.
- Strict typing: `mypy --strict` is the gate. Annotate everything; a suppression needs a one-line justification.
- No skipped tests. Slow tests are deselected by marker (`make test-slow`), never skipped.
- `publish` reads the PyPI token from the environment via `uv publish`; never store credentials in the tree.
