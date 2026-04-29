# driftwatch — Publication boundary

The decision tree for whether a proposed surface may be published.

```mermaid
flowchart TD
    Q["New surface proposal?"]
    Q --> Q1{"Aggregate, per-cluster,<br/>or per-DID?"}
    Q1 -->|Aggregate| OK1["✓ Default-permitted"]
    Q1 -->|Per-cluster| OK2["✓ Permitted<br/>(author lists collapsed<br/>to counts)"]
    Q1 -->|Per-DID| FORBID["✗ FORBIDDEN<br/>Dossier shape"]

    OK1 --> Q2{"Stage 0–1<br/>or Stage 2+?"}
    OK2 --> Q2
    Q2 -->|Stage 0–1<br/>current| ALLOW["✓ Cluster reports<br/>Drift findings<br/>facts.sqlite sidecar"]
    Q2 -->|Stage 2+| GATE["⚠ Requires explicit<br/>LABELER_EMIT_MODE<br/>confirm step"]
    GATE --> ATPROTO["ATProto label emission"]

    FORBID --> FORBA["Forbidden shapes:<br/>GET /poster/{did}/clusters<br/>GET /poster/{did}/weather<br/>GET /poster/{did}/automation_score<br/><br/>Rule:<br/>if the tables can answer it,<br/>the API still must not."]

    classDef ok fill:#d4edda,stroke:#155724,color:#155724
    classDef no fill:#f8d7da,stroke:#721c24,color:#721c24
    classDef warn fill:#fff3cd,stroke:#856404,color:#856404
    classDef detail fill:#e2e3e5,stroke:#383d41,color:#383d41

    class OK1,OK2,ALLOW ok
    class FORBID no
    class GATE warn
    class FORBA detail
```

## The two axes

Driftwatch's publication boundary has two orthogonal axes:

1. **Aggregation level** — aggregate / per-cluster / per-DID. Per-DID is forbidden as a surface, full stop.
2. **Stage** — 0–1 (sealed lab, current) / 2+ (public emission, gated by explicit confirm step).

Both axes must be cleared independently. A Stage 0 aggregate is permitted; a Stage 0 per-DID is not. A Stage 2 aggregate requires the emit-mode confirm step; a Stage 2 per-DID is still forbidden.

## Stage gating in detail

`LABELER_EMIT_MODE` defaults to `detect-only`. All decisions land in `label_decisions` (the receipt ledger) regardless of mode. Flipping the gate requires an explicit confirm step in `emit_mode.py`. The gate is not a default that drifts on by accident.

See `../PUBLIC_SURFACES.md` for the full surfaces inventory and `../driftwatch/SCOPE.md` for stage descriptions.
