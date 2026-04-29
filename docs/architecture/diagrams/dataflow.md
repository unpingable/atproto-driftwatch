# driftwatch — Dataflow

```mermaid
flowchart LR
    JS["Jetstream"]

    subgraph fp["Fingerprint"]
        FP1["Extract:<br/>spans, entities,<br/>quantities, URLs"]
        FP2["Apply precedence"]
        FP3["fp_kind + hash"]
        FP1 --> FP2 --> FP3
    end

    CH[("claim_history<br/>append-only,<br/>observed_at")]

    subgraph sensors["Sensor array"]
        S1["author/post ratio"]
        S2["variant entropy"]
        S3["half-life"]
        S4["burst score"]
        S5["synchrony"]
    end

    subgraph drift["Drift detection"]
        D1["Δ across versions"]
        D2["assertiveness, laundering"]
        D3["recheck queue<br/>(rolling hotset, 10k)"]
    end

    subgraph maint["Retention"]
        M1["NULL events.raw @ 24h"]
        M2["archive claim_history<br/>@ 14d → JSONL+gzip"]
        M3["disk pressure brake<br/>(85% / 92%)"]
    end

    OUT["Reports + facts.sqlite"]

    JS --> FP1
    FP3 --> CH
    CH --> sensors
    CH --> drift
    CH --> maint
    sensors --> OUT
    drift --> OUT
```

## Fingerprint precedence

The order matters. Higher-precedence signals win:

```
multi-word entity > quantity+context > URL domain >
single-word entity > spans > normalized text
```

Year exclusion strips bare 19xx/20xx from quantity extraction (treadmill avoidance). Complexity gate (`MIN_CLAIM_ALPHA_TOKENS=3`) skips single-word noise.

## Stage gates

| Stage | Gates |
|-------|-------|
| Ingest | Bounded queue with explicit drop counter; drop_frac surfaced in STATS |
| Fingerprint | Complexity gate; year exclusion; entity stopwords |
| Sensors | Platform health (WARMING_UP/OK/DEGRADED) hard-gates rechecks |
| Drift | Singleton gate (multi-author OR link OR reply-thread); cooldown filter |
| Retention | Disk pressure brake pauses ingest at 92% |

See `../DATAFLOW.md` for stage detail and `../FAILURE_MODES.md` for what each gate prevents.
