# Driftwatch consumer policy — external advisory caveats

This directory holds Driftwatch's documented consumer-policy artifact
for the `external_advisory_caveats` policy. It exists to give the
Labelwatch specimens track (Bundle G) a real named non-default
consumer to cite, with a real receipt-emitting effect.

## What it does

Driftwatch consumes Labelwatch exporter outputs. When Labelwatch
exports an `emitter_declared` candidate for a labeler/label pair that
appears in this policy's allowlist, the policy:

1. **Admits** the pair into a local roster:
   `data/consumer_policy/state/external_advisory_caveats.json`.
2. **Emits a receipt** documenting the admission:
   `data/consumer_policy/receipts/<timestamp>-<sha>.json`.

The roster is intended to be consulted by Driftwatch's cluster report
generator (future work) to annotate / route / exclude claims involving
the labeled targets. v1 produces the state file; live wiring of the
report-side consumer is parked.

## What it does NOT do

- It does NOT promote `emitter_declared` to `global_platform`. The
  receipt's `discipline_note` explicitly states this.
- It does NOT erase caveats. `non_global_provenance` and other
  caveats from the input candidate are copied into the roster entry
  and receipt as `inherited_caveats`.
- It does NOT admit blocked candidates. If Labelwatch refused to
  export, Driftwatch refuses to admit.
- It does NOT generalize across consumers. The receipt is
  `consumer_id: driftwatch`; nothing in the artifact speaks to other
  consumers' behavior.

## Allowlist (v1)

| labeler | label values |
|---------|--------------|
| `skywatch.blue` (`did:plc:e4elbtctnfqocyfcml6h2lf7`) | `fringe-media`, `fundraising-link` |
| `xblock.aendra.dev` (`did:plc:newitj5jo3uel7o4mnf3vj2o`) | `twitter-screenshot` |
| `label.haus` (`did:plc:6ebfnuunfngxfw3rth3ewojw`) | `fucked-up-replyref` |

Rationale per entry lives in `policy.py:ADMITTED`.

## Refusal vocabulary

| reason | when it fires |
|--------|---------------|
| `input_is_blocked_candidate` | input is `schema_kind=blocked_candidate` (not `specimen_candidate`) |
| `input_scope_not_emitter_declared` | input's `consumer_scope_effective` ≠ `emitter_declared` |
| `labeler_label_pair_not_in_allowlist` | `(labeler_did, label_value)` not in the table above |

## Receipt shape

```json
{
  "consumer_id": "driftwatch",
  "policy_version": "external_advisory_caveats-v1.0.0",
  "policy_artifact_ref": "driftwatch/scripts/consumer_policy/policy.py",
  "action_taken": "advisory_caveat_roster_admission",
  "emitted_at": "<ISO8601>",
  "input_packet": {
    "evidence_source": "<filename or url-ish>",
    "labeler_did": "did:plc:...",
    "labeler_handle": "...",
    "label_value": "...",
    "input_hash": "<sha256 of canonical candidate JSON>"
  },
  "inherited_caveats": ["non_global_provenance", ...],
  "roster_entry": { ... },
  "discipline_note": "consumer_scope is opt_in:driftwatch; ..."
}
```

## Run

```bash
python3 scripts/consumer_policy/policy.py \
    --candidate <path/to/labelwatch-exporter-candidate.json> \
    [--state-dir data/consumer_policy/state] \
    [--receipt-dir data/consumer_policy/receipts] \
    [--print-receipt]
```

Idempotent: re-applying the same input candidate (same `input_hash`)
does not double-admit into the roster, but DOES emit a new receipt
(the receipt records each apply attempt).

## Versioning

`POLICY_VERSION` in `policy.py` is the source of truth. Bumping the
allowlist, refusal logic, or receipt shape REQUIRES a version bump.
Receipts are pinned to the version that emitted them; downstream
consumers can check which policy revision produced a roster entry.
