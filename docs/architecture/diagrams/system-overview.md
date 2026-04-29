# driftwatch — System overview

```mermaid
flowchart TD
    JS["Jetstream WSS<br/>(posts + reposts)"]
    PLC["PLC Directory"]
    DW["did:web hosts"]

    subgraph driftwatch["driftwatch (single process)"]
        CONS["Consumer + Queue<br/>(drop-aware)"]
        FP["Fingerprint pipeline<br/>entity/quantity/<br/>domain/span/text"]
        SENS["Sensor array"]
        MAINT["Maintenance +<br/>Retention loops"]
        RES["Resolver"]
    end

    DB[("SQLite WAL")]
    ARCH[("Archives<br/>JSONL + gzip")]
    FACTS[("facts.sqlite<br/>(read-only export)")]

    LW["labelwatch<br/>(separate project)"]

    JS --> CONS
    CONS --> FP
    FP --> DB
    DB --> SENS
    DB --> MAINT
    MAINT --> ARCH
    PLC --> RES
    DW --> RES
    RES --> DB

    SENS --> REP["Cluster reports"]
    SENS --> DRIFT["Drift findings<br/>(detect-only)"]
    DB --> FACTS

    FACTS -.ATTACH read-only.-> LW
```

## Notes

- Single process, single SQLite DB. The architectural simplicity is the point — every subsystem operates on the shared journal.
- `claim_history` is the central append-only journal. `observed_at` is the trusted timestamp; `createdAt` from the firehose may be wrong.
- facts-bridge is **unidirectional** (driftwatch → labelwatch). Schema changes are data contract changes. See `../PUBLIC_SURFACES.md`.
- See `../OVERVIEW.md` for invariants and population definitions.
