# driftwatch — Specs

Normative, authoritative documents. The decision rule is borrowed from agent_gov's `DOC_TAXONOMY`:

> **Could implementation be judged wrong by this document?**
>
> If yes → here. If no → `docs/`.

## Layout

```
specs/
  core/         shipped canonical specs (architecture, protocol, invariant)
  gaps/         explicit backlog — things we know are missing
  research/     non-committed lines of inquiry — empty for now
```

## core/

| File | What it specifies |
|------|-------------------|
| `ADMISSIBILITY-PROTOCOL.md` | Procedural spec: required steps for taking observed cross-population skews from "interesting pattern" to "publishable claim or honestly retired" |
| `DW-SPEC-PDS-ENRICHMENT-001.md` | PDS-side enrichment behavior — resolver scope, identity provenance, not-a-crawler discipline |

## gaps/

| File | Gap |
|------|-----|
| `gap-spec-storage-layout-labelwatch-driftwatch.md` | Cross-project storage layout decisions |
| `gap-spec-off-host-backup-labelwatch-driftwatch.md` | Off-host backup posture (depends on storage layout being settled first) |

## Adding a new spec

1. Apply the rule. If the doc could be a basis for "the implementation is wrong against this," it's a spec.
2. Place it: `core/` if shipped, `gaps/` if explicit backlog, `research/` if speculative.
3. Update this README's table.

## Architecture vs specs

`docs/architecture/` is the orientation surface — overviews, dataflow, signal model, public surfaces, failure modes. It explains how the system is shaped and why.

`specs/` is the binding contract. Implementation can be judged wrong against a spec; it cannot be "judged wrong" against an explanation.

Both refer to each other. Neither replaces the other.

## Adapted from

agent_gov's `docs/DOC_TAXONOMY.md` (north-star, partially adhered to in source).
