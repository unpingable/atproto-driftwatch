# Diagrams

Mermaid diagrams supplementing the architecture docs. GitHub renders `mermaid` code blocks in `.md` files inline.

## Files

| File | Purpose |
|------|---------|
| `system-overview.md` | External systems, single-process organs, storage, outputs |
| `dataflow.md` | Pipeline stages with fingerprint precedence and stage gates |
| `publication-boundary.md` | Decision tree: aggregate / per-cluster / per-DID × stage 0–1 / 2+ |
| `signal-model.md` | Claims-anchored vs structural-only fork; what each can/cannot see |

## Editing

For interactive editing, paste the mermaid block into the [Mermaid Live Editor](https://mermaid.live).

For local rendering, use `mmdc` from `@mermaid-js/mermaid-cli`.

## Cross-reference

| Diagram | Companion doc |
|---------|---------------|
| `system-overview.md` | `../OVERVIEW.md` |
| `dataflow.md` | `../DATAFLOW.md` |
| `publication-boundary.md` | `../PUBLIC_SURFACES.md` |
| `signal-model.md` | `../SIGNAL_MODEL.md` |
