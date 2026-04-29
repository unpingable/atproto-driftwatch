# driftwatch — Signal model fork

The architectural fork between **claims-anchored** (driftwatch's choice) and **structural-only** observatories.

```mermaid
flowchart LR
    subgraph claims["Claims-anchored (driftwatch)"]
        direction TB
        DT["post text"]
        DFP["fingerprint"]
        DCL["cluster"]
        DSIG["author entropy<br/>variant entropy<br/>half-life<br/>burst<br/>synchrony"]
        DT --> DFP --> DCL --> DSIG
    end

    subgraph other["Structural-only (different observatory)"]
        direction TB
        OT["posts + edges"]
        OG["propagation graph"]
        OSIG["cascade shape<br/>topology<br/>posting rhythm<br/>network position"]
        OT --> OG --> OSIG
    end

    DSIG -->|CAN| CAN1["✓ half-life<br/>✓ correction resistance<br/>✓ cluster persistence<br/>✓ claim mutation tracking"]
    DSIG -->|CANNOT| CNT1["✗ network topology<br/>✗ paraphrase-evading<br/>  campaigns"]

    OSIG -->|CAN| CAN2["✓ cascade shape<br/>✓ synchrony of arrival<br/>✓ network position<br/>✓ coordinated action"]
    OSIG -->|CANNOT| CNT2["✗ claim identity<br/>✗ what spread<br/>  (only how)"]

    classDef can fill:#d4edda,stroke:#155724,color:#155724
    classDef cannot fill:#f8d7da,stroke:#721c24,color:#721c24

    class CAN1,CAN2 can
    class CNT1,CNT2 cannot
```

## What each architecture sees

A **coordinated copy-paste campaign** produces both a low-author-entropy claim cluster (claims-anchored sees it) and a tight cascade with synchronous arrival (structural-only sees it). Either approach catches it.

A **paraphrase-evading campaign** is invisible to claims-anchored (no fingerprint match), visible to structural-only (the cascade is still there).

A **viral idea spreading through independent rephrasing** is visible to claims-anchored (variant entropy is high but cluster persists), partially visible to structural-only (shape may look like organic spread).

## Why driftwatch picked claims-anchored

The original question was "do claims persist, mutate, resist correction." That question requires claim identity. Structural-only is a different observatory, not driftwatch's. They are complementary, not interchangeable.

See `../SIGNAL_MODEL.md` for full doctrine on what the structural signals claim and don't claim.
