# Codex architecture audit — 2026-07-17

**Status:** captured, NOT triaged, NOT acted on. External audit (codex) of the
driftwatch codebase. No files were changed. Recorded so the findings survive;
each still needs local verification before any fix — treat as **testimony**
(a detector's output), not ratified defects. Line anchors are codex's; verify
they still point where claimed before acting.

**Why parked:** filed mid-flight during the cold-path facts-snapshot campaign
(`specs/gaps/gap-spec-facts-snapshot-scale-containment.md`). Pick up once the
cold-path attempt-2 work settles. Companion audit for labelwatch:
`labelwatch/docs/findings/2026-07-17-codex-architecture-audit.md`.

Codex's own verdict: *"targeted refactor soon, not a rewrite. The domain
architecture is good, but several correctness contracts are currently split
across the wrong boundaries."* Explicitly called **strong** (preserve, refactor
incrementally behind failure-focused tests): detection envelopes, sensor
interface, **facts-snapshot pipeline**, documentation, test breadth.

---

## Top priorities (codex ordering)

### 1. Ingest transaction + cursor ownership — REPRO'D DATA LOSS
- When longitudinal enqueueing is enabled, `db.py:497` calls a queue helper
  that commits the batch connection in `recheck_queue.py:14`.
- **Codex forced a two-event batch failure: the consumer reported both rolled
  back, but both remained persisted.** (A concrete repro — verify it first,
  but this is the load-bearing finding.)
- Also: the received cursor is persisted **ahead of** committed work in
  `consumer.py:450` → risks skipped events after restart.
- **Proposed:** transaction-safe enqueueing / outbox semantics; a committed
  cursor attached to each batch.
- *Note:* same failure class as the labelwatch audit's #1 (cursor-before-commit).
  Cross-observatory pattern worth fixing consistently.

### 2. Resolver work blocks queue draining
- The drain loop awaits resolver processing in `consumer.py:568`. Resolution
  can process 20 DIDs sequentially with 10s timeouts → potentially pauses
  ingestion ~200s.
- **Proposed:** independent scheduled resolver worker, off the drain path.

### 3. Labels + audit receipts are not atomic
- `db.py:598` commits the label, then best-effort writes its receipt
  separately. Longitudinal writes another receipt and sometimes uses the last
  loop's post for earlier subjects in `longitudinal.py:267`.
- **Proposed:** one transactional `commit_label_with_receipt` path — removes
  missing / duplicate / incorrect receipts.

### 4. Public-policy contradiction — INTERSECTS RATIFIED DOCTRINE
- `docs/architecture/PUBLIC_SURFACES.md:39` forbids per-DID surfaces, but the
  app exposes `/exposure/{did}` and unauthenticated top authors in
  `main.py:315`.
- **Proposed:** remove, internalize, or deliberately re-authorize.
- *This is not just a code bug.* It intersects the observatory's ratified
  constraints — aggregate-first, detect-only, no per-account dossiers,
  "recomposition changes the ethics" (per-DID/intersectional surfaces are
  target-discovery surfaces). Treat as a doctrine-compliance finding, higher
  priority than its position implies. Decide deliberately, don't quietly patch.

### 5. Duplicated / dead implementations
- Production vs debug fingerprinting use different algorithms in
  `claims.py:225`; **sampled hashes disagreed.** (Verify — divergent
  fingerprinting would corrupt dedup/claim identity.)
- `retention.py` is ~1,539 lines and keeps an explicitly unwired historical
  implementation at `retention.py:747`. (Note: retention was touched
  2026-07-16/17 — the test-isolation + parquet-capture work. Confirm the
  dead-code claim against current state.)
- `db.py` needs versioned migrations + an explicitly SQLite-only hot-store
  boundary.

---

## Validation (codex)
- 62 targeted tests passed; source compilation succeeded.
- **Full local suite is not a dependable one-command gate:** a test-package
  import failure + an HTTP test exceeding 15s. *(Worth fixing early — a
  trustworthy `pytest` gate is the safety net for every other refactor here.
  NB: the maintainer's local run was 424 passed on 2026-07-17, so the
  "import failure" may be environment/path-specific — reconcile.)*
- Ruff: 55 source issues; CI ignores lint failures.

---

## Triage guidance (when picked up)
- **Verify before fixing.** Codex's claims, codex's line numbers, against a
  tree that moved tonight.
- **Load-bearing correctness:** #1 (repro'd batch-rollback data loss) and #5's
  fingerprint-divergence (sampled hashes disagreed) — both have concrete
  repros to reproduce first; both are data-integrity, not hygiene.
- **Doctrine call:** #4 needs a deliberate decision (aligns with the
  observatory's own PUBLIC_SURFACES policy), not a silent patch.
- **Don't hand correctness fixes to codex-that-found-them** — a wrong claim
  yields a confident wrong fix on transaction/cursor/migration code. Mechanical
  refactors (dead-code removal, lint) are safer to farm with a review gate.
- Keep this off the cold-path critical path — separate track, separate session.
