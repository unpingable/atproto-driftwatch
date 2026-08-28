# Murex prerequisites — Driftwatch

Campaign: **Murex** / `public-readonly-instrument-fact-api`
Track: public-observation-surface
Decision: **B — SHARED-SURFACE-DESIGN-ONLY**. Driftwatch is **excluded from the
shared fact surface at this time.** Nothing here is authorised or built.

Full campaign record, contract, and privacy analysis:
`atproto-weatherwatch/docs/MUREX-PUBLIC-FACT-SURFACE.md`.

## Status

Item 6 (unauthenticated identity-bearing routes) was **repaired by the
follow-up campaign Breakwater** (`observation-adequacy-and-public-boundary-
hardening`, 2026-08-28). Items 1–5 and 7 are open, and **Driftwatch remains
excluded from the Murex public fact surface.** Hardening an internal
diagnostic boundary is not the same act as earning a public one.

## Why Driftwatch is excluded

> **Correction (Breakwater).** The first version of this file said Driftwatch
> "has no privacy or publication gate module anywhere in the repository" and
> implied it had no publication boundary at all. The second half was wrong.
> Driftwatch carries `docs/architecture/PUBLIC_SURFACES.md` and
> `docs/architecture/diagrams/publication-boundary.md`: a surfaces inventory,
> stage gating, an aggregate / per-cluster / per-DID classification with
> per-DID forbidden outright, a forbidden-shape list, and an add-a-surface
> checklist — closing with *"If the tables can answer dossier-shaped
> questions, the API still must not."* The doctrine is real and well argued.
> What remains true, and is the actual finding, is the narrower claim below.

Driftwatch's publication boundary is **written but not enforced in code**.
There is no gate module comparable to Weatherwatch's `publication.py`, and the
gap has already produced a live divergence: three per-DID HTTP routes existed
in contradiction of this repository's own rule and were absent from its own
surfaces inventory (see item 6). The repository's 2026-07-17 codex audit had
already filed exactly this as finding #4, flagged **"INTERSECTS RATIFIED
DOCTRINE"** and "decide deliberately, don't quietly patch."

So a Murex surface on Driftwatch would not be *exposing* a settled boundary. It
would be publishing across one the code does not yet hold.

The obstruction is custody, not ontology. Driftwatch's aggregate shape — claims
per hour, distinct clusters per hour — fits the shared fact envelope without
distortion. Everything below is about earning the right to emit it.

## Blocking repairs

### 1. There is no decision about what Driftwatch may publish *now* — open

`PUBLIC_SURFACES.md` records the *shape* rule (aggregate and per-cluster
permitted, per-DID forbidden) and the stage gating, which is more than Murex
originally credited. What it does not record is a current determination that
any specific artifact is ready to publish, computed over what window, with what
coverage attached. That determination precedes every item below.

### 2. There is no privacy gate — open

The doctrine exists (`PUBLIC_SURFACES.md`); the enforcement does not.
Weatherwatch has `publication.py`: a deterministic gate that walks a rendered
candidate, refuses symlinks, and scans every byte for DID, `at://`, CID,
handle, and actor-token shapes, returning labels rather than the matching
bytes. Driftwatch has no analogue. Nothing should be published from this
repository before one exists and is tested against a fixture corpus.

### 3. `claim_fingerprint` can never be published — open

`claims.fingerprint_text` is `sha256(normalized_claim_text)` truncated to 16
hex characters (`src/labeler/claims.py:313`, `:353`). Verified directly: the
function is pure, deterministic, **unsalted**, holds no secret, and is stable
across surface mutation — differently punctuated, differently capitalised,
emoji-bearing renderings of the same sentence yield an identical fingerprint.

The hazard is not that SHA-256 can be reversed — it cannot, and this document
does not claim otherwise. The hazard is **candidate enumeration**: the
normaliser is open source, the corpus it addresses — public Bluesky posts — is
public and enumerable, and the function is unsalted and deterministic. Anyone
holding candidate posts can normalise and hash them and compare digests. A
published fingerprint is therefore practically re-identifiable. It is not a
pseudonym for a claim; it is a content address of specific posts, and by
extension of the accounts that wrote them.

It compounds. Cluster entries carry `latest_authors`, and the detector sets
`single_author_heavy` when `latest_authors <= 1 and total_posts >= 10`
(`src/labeler/driftmetrics.py:437-439`). A published cluster fact with that
flag is a practically re-identifiable pointer to one identifiable account
carrying an automation-shaped label — simultaneously an identity disclosure and the
accusation-shaped output this repository's own `CLAUDE.md` forbids.

**No salted or truncated derivative rescues it.**

- Hashing does not make the fact safely anonymous — the digest stands in for
  the text, and against an enumerable corpus that is not anonymity.
- Truncation alone does not solve candidate enumeration; it raises the
  collision rate, adding noise without removing the matching capability.
- Salting changes linkability, not publishability. A rotating salt destroys
  cross-window comparison, which is the only reason to publish a cluster key
  at all; a fixed salt stays enumerable to anyone who learns it. Either way a
  *single-author* semantic fact still describes one account.

The correct publication decision at this granularity may simply be refusal.
Designing an anonymisation mechanism is out of scope.

The estate's shared reduction boundary already refuses these values
independently — a real fingerprint offered as a dimension is rejected as
`UNBOUNDED_DIMENSION` or, when token-legal, as `IDENTITY_SHAPED_VALUE` under
the "opaque stable hashes are not a privacy escape hatch" rule. That is a
backstop, not a licence to try.

### 4. Observation health is not historically attributable — open

`platform_health` is documented as ephemeral runtime state that resets on
restart (`src/labeler/platform_health.py:8-11`), and there is no persisted
per-window health table in the schema. `cluster_report` calls
`get_health_snapshot()` and attaches that *current* snapshot to an
`hours`-long *historical* window (`src/labeler/driftmetrics.py:452-466`).

Every published fact would therefore carry coverage describing the wrong span
of time. Weatherwatch's `window_health` table — one durable row per window,
written in the same transaction as the counters — is the working model.

### 5. `coverage_pct` is not a coverage fraction — open

It is a ratio of observed throughput against a *learned EWMA baseline*
(`platform_health.py`), not a fraction of a known denominator. Publishing it as
the shared contract's `coverage_fraction` under any `coverage_profile` would
repeat the "no canonical denominator" error Weatherwatch explicitly refuses for
network totals. It is a gate input. It is not a coverage measure.

### 6. Unauthenticated identity-bearing routes — **REPAIRED**

`GET /exposure/{did}`, `GET /strain/top` ("top authors by event count",
returning raw author identifiers) and `GET /labels/{subject_uri}` carried no
`admin_auth` dependency, while `/recent-decisions` and `/quarantine/recent`
did — an internal inconsistency, since `/labels/{subject_uri}` and
`/recent-decisions` return the same data class.

All three are per-DID or per-subject surfaces, i.e. the shape
`PUBLIC_SURFACES.md` forbids, and none appeared in its inventory.

**Repair (Breakwater).** All three now depend on the pre-existing `admin_auth`.
No new authentication system was introduced. A bounded audit of the remaining
routes found `/health*`, `/metrics` and the `/admin/*` pair carry no
per-account material and need no change. A constellation-wide consumer search
found **no caller of any kind** — production, test, or historical — for the
three routes; the only other definitions are in the ancestor
`reference-labeler` repository. Pinned by
`tests/test_identity_route_boundary.py`.

**Two limits are recorded rather than quietly fixed.**

- `admin_auth` is a **no-op when `ADMIN_API_TOKEN` is unset**. That is
  pre-existing and shared with the already-protected routes; changing it would
  alter their behaviour too. Setting the token is a deployment prerequisite,
  asserted by a test rather than assumed.
- `/labels/{subject_uri}` captures a single path segment, so a real `at://`
  URI never matches it and 404s at routing. That leaks nothing, but the route
  cannot serve the subject shape its name implies. Recorded, not repaired:
  changing the path shape is an API change, out of scope for boundary
  hardening.

Route hardening does **not** make Driftwatch eligible for the Murex surface.
Items 1–5 and 7 stand.

### 7. History is out of scope — open

Retention is 7 days for events and 14 days for edges and claims
(`src/labeler/retention.py:87-89`). The campaign forbids broadening retention
to make an API attractive, and the Parquet archive rail is a separate protocol
that this campaign did not evaluate for publication.

## What a future Driftwatch contract could honestly carry

Once 1–5 are discharged: bounded aggregate counts over closed windows with
persisted per-window coverage — claims observed per hour, distinct clusters per
hour, cluster-size distribution above a stated k-threshold — carrying no
cluster key, no author count below the threshold, and no fingerprint in any
field at any truncation.
