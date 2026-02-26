# ATProto Seams: Patterns from PCAR → Driftwatch + Labelwatch

> What to extract as **specs/patterns**, what to keep as **parallel evolution**,
> and what is **not portable**.

This document maps governance concepts from the PCAR family (agent_gov) to
the ATProto observatory projects. It borrows structure, not code.

---

## A. PCAR → ATProto Governance Spec Template

A minimal spec outline for an **ATProto Labeler Governance Spec**, borrowing
PCAR structure but staying domain-specific.

### A.1 Typed Claims

What is being asserted, and by whom.

| PCAR Claim Type | ATProto Analog | Status |
|-----------------|---------------|--------|
| OBSERVED | Fingerprint match (claim_history row) | Implemented |
| INFERRED | Burst score, half-life, regime shift detection | Implemented |
| ASSUMED | Complexity gate pass (post is "claim-like enough") | Implemented |
| REQUESTED_ACTION | Label emission request | Implemented (emit mode gating) |
| POLICY_INTERPRETATION | — | Not applicable (no policy reasoning layer) |

**Gap**: driftwatch claims are implicit (a row in claim_history *is* the claim).
Making the claim type explicit in the decision ledger would improve auditability
but isn't urgent while in sealed-lab mode.

### A.2 Proof Objects

Evidence pointers, hashes, observation windows.

| Component | Driftwatch | Labelwatch |
|-----------|-----------|------------|
| Evidence hash | `evidence_hash` (per claim_history row) | `evidence_hashes_json` (per alert) |
| Config snapshot | `fingerprint_config_hash()` → 16-char SHA | `config_hash()` → full SHA-256 |
| Observation window | `createdAt` range in queries | Window param in rule evaluation |
| Sampling method | Complexity gate + singleton gate | Polling cadence + warmup gating |

**Spec requirement**: proof objects MUST bind to an immutable state reference.
Both projects already do this via content hashing. Neither chains proofs yet
(see B.1).

### A.3 Constraint Decisions

What rules were applied, with versioning.

Driftwatch's `label_decisions` table already captures:
- `rule_id` — which rule fired
- `fingerprint_version` — FP_VERSION at decision time
- `config_hash` — full knob snapshot
- `inputs_json` — decision inputs
- `decision_trace` — human-readable audit trail
- `status` — committed | expired

Labelwatch's `alerts` table captures the same core:
- `rule_id`, `config_hash`, `evidence_hashes_json`, `receipt_hash`

**Spec requirement**: decisions MUST be deterministic given identical inputs.
Both projects satisfy this — no RNG, no model consultation, no network calls
in the decision path.

### A.4 Canonical Receipt Encoding

Tamper-evident audit trail.

**PCAR-D canonicalization profile** (directly adoptable):
- `json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=True)`
- SHA-256, lowercase hex
- Timestamps: RFC 3339 UTC, truncated to seconds
- Null handling: omit optional fields, don't set to null
- Domain separation tags (e.g., `driftwatch.receipt\x00` prefix before hashing)

**Current state**:
- Labelwatch already computes `receipt_hash` over canonical JSON — close to PCAR-D
- Driftwatch stores `config_hash` and `evidence_hash` but doesn't chain them
  into a single receipt hash per decision

**Near-term spec work**:
1. Align both projects on one canonicalization profile (PCAR-D's is fine)
2. Add `receipt_hash` to driftwatch's `label_decisions`
3. Add `prev_receipt_hash` to both for chain integrity (hash chain)
4. Domain separation tags to prevent cross-project hash collision

### A.5 Effect Taxonomy

| PCAR Effect | ATProto Effect | Semantics |
|-------------|---------------|-----------|
| PASS | detect-only (logged, no emission) | Observation recorded |
| WARN | — (could add) | Soft signal, no action |
| ESCALATE | quarantine | Suppressed, human review needed |
| BLOCK | — | Not applicable (observatory, not enforcer) |

Driftwatch-specific additions:
- **SUGGEST_META_LABEL** — recommend a label for human review (future)
- **EMIT** — actually publish a label to ATProto (requires `LABELER_EMIT_CONFIRM`)

**Key difference from PCAR**: PCAR effects gate execution. ATProto effects gate
*publication*. The blast radius is different — a bad label is embarrassing and
correctable, not catastrophic.

### A.6 Glossary

| Term | Definition (in this context) |
|------|------------------------------|
| **Fingerprint** | Content-addressed hash of a post's claim signal (entity/quantity/domain/span/text) |
| **Claim history** | Append-only record of (author, fingerprint, timestamp, evidence) tuples |
| **Decision ledger** | Append-only record of label decisions with inputs, evidence, and config snapshots |
| **Receipt** | Content-addressed hash over a decision's canonical serialization |
| **Receipt chain** | Linked list of receipts via `prev_receipt_hash` (not yet implemented) |
| **Regime** | Operational state derived from aggregate signal dynamics (burst, drift, shift) |
| **Config hash** | SHA-256 of all tunable knobs at decision time — proves which version of the rules produced this output |
| **Sealed lab** | `LABELER_EMIT_MODE=detect-only` — observe and record, never publish |
| **Complexity gate** | Filter that rejects posts too simple to fingerprint meaningfully |
| **Singleton gate** | Filter requiring >=2 distinct authors before a fingerprint enters the recheck queue |

### A.7 Normative vs. Informative

**Normative** (MUST implement for spec compliance):
- Canonical JSON serialization profile
- Receipt hash computation (content-addressed)
- Config hash at decision time
- Evidence hash binding
- Append-only decision ledger (no UPDATE/DELETE on committed rows)
- Deterministic decision path (no RNG, no model, no network)

**Informative** (SHOULD consider, not required):
- Hash chain (prev_receipt_hash)
- Domain separation tags
- Typed claim envelopes
- Effect taxonomy alignment
- Epoch boundaries for segment verification

---

## B. Long-term Seams (Patterns, Not Shared Code)

### B.1 Receipt Kernel Seam

Transferable invariants from PCAR-D:

1. **Canonicalization** — one profile, everywhere. Both projects should produce
   identical bytes for identical logical content. PCAR-D's profile is simple
   and well-specified.

2. **Content-addressing** — receipt identity = hash of canonical bytes.
   `receipt_id = SHA256(domain_tag + canonical_json)`. Already partially
   implemented in labelwatch.

3. **Hash chain** — each receipt includes `prev_receipt_hash`. Chain integrity
   is verifiable offline. Gap detection is trivial (missing link = tampering
   or data loss).

4. **Replay** — given the chain and the evidence store, you can reconstruct
   the decision path. Both projects already store enough in `inputs_json` +
   `evidence_hashes_json` to support this.

**Not transferable**: PCAR-D's epoch boundaries and evidence retention tiers
(PUBLIC/SEALED) assume a single-host trust model. ATProto evidence is public
by nature — retention policy is "the network remembers."

### B.2 Regime Detection Seam

**Abstract pattern** (from PCAR-C, applicable to both projects):

```
signals → classifier → regime → posture → constraints
```

| Component | agent_gov | Driftwatch | Labelwatch |
|-----------|-----------|-----------|------------|
| Signals | hysteresis, tool_gain, anisotropy, provenance_deficit | burst_score, synchrony, half_life, author_growth, fp_kind mix | rate_spike, flip_flop, concentration, churn |
| Classifier | threshold-based, priority-ordered | JSD divergence + z-score | rule activation counts |
| Regime | ELASTIC/WARM/DUCTILE/UNSTABLE | not yet formalized | not yet formalized |
| Posture | constraint tightening per regime | — | — |
| Constraints | action class restrictions | budget caps (per-rule) | — |

**Requirements that transfer**:
- **Hysteresis** — regime transitions must be sticky (don't flap between states).
  Require N consecutive bins above threshold before promoting.
- **Cooldown** — after a regime change, suppress further transitions for a
  minimum window.
- **Budgets** — hard caps on actions per regime. Driftwatch already has
  per-rule budgets; labelwatch should add them.
- **Tests as laws** — regime transition logic must be covered by deterministic
  tests with known inputs/outputs. No "it feels right."

**Thresholds and semantics differ completely**. agent_gov's `tool_gain >= 1.0`
(positive feedback in agent execution) has no analog in ATProto. Driftwatch's
`burst_score` (claim velocity z-score) has no analog in agent_gov. The *shape*
of the state machine is the same; the *content* is domain-specific.

**Proposed ATProto regime taxonomy** (not yet implemented):

| Regime | Driftwatch Meaning | Labelwatch Meaning |
|--------|-------------------|-------------------|
| QUIET | Normal churn, no dominant clusters | Labelers behaving within baseline |
| ACTIVE | Elevated burst activity, clusters forming | Rate or concentration anomalies |
| SURGE | Dominant clusters with high synchrony | Coordinated labeling detected |
| ALARM | Regime shift detected (JSD divergence) | Multiple rules firing simultaneously |

### B.3 Composition Governance Seam

Cross-labeler coordination detectors that labelwatch could add:

1. **Lead/lag** — labeler A consistently labels subjects before labeler B
   (within minutes). Possible: A is B's source, or both follow the same feed.

2. **Mirroring** — two labelers produce near-identical label distributions
   over a window. Suspicious if they claim independence.

3. **Coordinated drift** — multiple labelers shift label distributions in the
   same direction during the same window. Natural (news event) or suspicious
   (coordinated campaign).

4. **Contradiction networks** — labeler A labels X as "misinformation" while
   labeler B labels X as "verified." Map the contradiction graph.

5. **Silence correlation** — labelers that go silent at the same time, then
   resume at the same time. Infrastructure sharing or coordination signal.

6. **Target overlap** — labelers that disproportionately label the same
   subjects despite different stated scopes.

7. **Rate synchrony** — cross-correlation of labeling rate time series.
   High correlation = shared trigger source.

8. **Cascade detection** — subject labeled by A, then rapidly labeled by
   B, C, D in sequence. Natural virality or pile-on?

**Important**: these are *detectors*, not *verdicts*. Labelwatch describes
geometry, not intent. A high lead/lag score between two labelers is an
observation. Whether it's collusion, inspiration, or coincidence is for
humans to decide.

### B.4 Trust Model Mismatch

**Why the enforcement kernel is NOT portable**:

| Property | agent_gov | ATProto |
|----------|-----------|---------|
| Trust anchor | Local host (Governor runs on your machine) | No single trusted host |
| Agent trust | Untrusted (can hallucinate, drift, loop) | N/A (labelers are autonomous actors) |
| Enforcement | Gate execution before it happens | Observe and report after the fact |
| Blast radius | Prevent file writes, API calls, tool use | Flag labels, never block publication |
| Authority | Governor has root; agents have none | Every labeler is sovereign |

**Consequence**: PCAR-E (actuator contract) and PCAR's enforcement kernel
have no ATProto analog. You cannot prevent a labeler from publishing. You
can only observe what they published and surface patterns.

**What IS portable**: the *audit* side. Receipt chains, evidence hashing,
config snapshots, decision ledgers — all the "proof that we observed
correctly" machinery transfers directly. The "and therefore we blocked it"
part does not.

---

## C. Timeline

### Now (parallel with current work)
- Align canonicalization profile (PCAR-D) across driftwatch + labelwatch
- Add `receipt_hash` to driftwatch `label_decisions`
- Draft effect taxonomy (detect / warn / quarantine / suggest / emit)

### Next (after sealed lab stabilizes)
- Receipt chain (`prev_receipt_hash`) in both projects
- Formalize regime taxonomy for driftwatch (QUIET/ACTIVE/SURGE/ALARM)
- Add per-rule budgets to labelwatch
- First 2-3 composition detectors in labelwatch (lead/lag, mirroring, rate synchrony)

### Later (months, not weeks)
- Typed claim envelopes (explicit claim_type in decision ledger)
- Cross-project receipt verification tool (replay from chain)
- Casefile / annotation ledger for human review notes
- Domain separation tags for hash namespacing

### Never
- Shared enforcement kernel (trust models are incompatible)
- Importing agent_gov's regime signals (wrong domain)
- LLM-in-the-loop governance decisions
- Blocking labeler publication from the outside
