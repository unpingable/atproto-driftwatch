# Admissibility protocol — controlling observed skews before publishing

**Status**: short procedural artifact, not a doctrine essay.
**Scope**: observatory claims built on observed cross-population skews.
**Parent instance**: `../../docs/VINTAGE-ADMISSIBILITY.md` (hosting-locus / vintage).

## Purpose

Observed skews often turn out to be cohort or confound artifacts rather
than the observable they appear to describe. This protocol fixes the
sequence for taking such a skew from "interesting pattern" to
"publishable claim, or honestly retired." It generalizes the sequence
used for the 2026-04 hosting-locus → vintage pass.

The goal is not rigor theater. It is to make it cheap to kill a claim
early, and hard to sneak a claim past its own confounds.

## When to invoke

Invoke when an unexpected cross-population skew is observed and
publication is being considered. Especially when the field underlying
the skew may be collapsing two semantics:

- current state vs historical state
- mutable vs immutable facts
- cohort / vintage / activity differences
- "same column, two jobs"

Do not invoke for internal diagnostic observations that will never be
published. This is a gate on external claims.

## Required steps

Executed in order. Each step produces an artifact; do not skip artifacts
even when the answer looks obvious.

1. **State the claim in plain language.** One sentence, with the
   population, the comparison, and the direction. If the claim needs
   more than one sentence it is not yet a claim.
2. **Identify field semantics and mutability.** For each field the
   claim depends on: what does it record, when, and does it change over
   time. Flag any field where the answer is "it depends."
3. **Enumerate plausible confounds.** At minimum: cohort / vintage,
   activity bias, resolver or coverage tail, staleness. Add
   domain-specific confounds as they come up.
4. **Define exclusion rules before rerun.** Eligibility filters, sample
   floors, effect floors, robustness checks. Written down *before* the
   rerun produces numbers. Post-hoc exclusions are disallowed.
5. **Add the missing covariate or control.** Whatever confound step 3
   surfaced as most plausible gets a controlled rerun. If multiple
   confounds are plausible, control for the most tractable first and
   flag the others.
6. **Recompute under control.** Run the rerun against the pre-registered
   rules. Do not adjust the rules based on what the numbers show.
7. **Classify outcome.** One of:
   - **admissible** — effect clears all pre-registered gates
   - **narrowed** — effect survives in a strict subset; publish with
     that scope, not the original one
   - **dissolved** — effect does not survive control; retire the claim
   - **deferred** — necessary substrate (covariate, coverage, sample)
     is missing; no publishable claim yet, document what is missing

## Publication rule

No publishable claim survives on uncontrolled skew alone. If the
controlled rerun does not clear the pre-registered gates, the claim is
retired or narrowed. Uncontrolled numbers may appear in internal notes;
they do not appear in published artifacts.

## Artifact rule

Each invocation of this protocol produces, at minimum:

- **pre-registration doc** — the pre-run rules (parent of this protocol
  is the canonical example)
- **primary tables** — the controlled rerun's per-stratum numbers
- **decisive drilldown** — the one table that most directly answers the
  admissibility question
- **repro script** — the script that produced the numbers, committed
- **dated findings note** — short, under `out/admissibility/YYYY-MM-DD_*.md`,
  explicitly stating the verdict from step 7

Artifacts are committed. A claim without committed artifacts is not a
published claim.

## Anti-patterns

- **One column doing two jobs.** A field that silently conflates two
  semantics (e.g. current vs historical) underneath a claim that
  depends on only one of them.
- **Current state masquerading as historical state.** Reading
  present-tense observables and narrating them as if they were a
  history.
- **Post-hoc exclusion rules.** Adjusting eligibility filters after
  seeing the rerun's numbers. If a rule needs to change, that is a new
  pre-registration, not an amendment.
- **Confound discovered, then waved away in prose.** A named confound
  in the writeup that was not actually controlled for. If it was
  important enough to name, it is important enough to control for or
  scope out explicitly.
- **Bucket-granularity shopping.** Re-binning strata until an effect
  appears. Bucket rules are fixed before the rerun.

## Worked example — hosting-locus / vintage (2026-04)

The 2026-04-19 check observed a seed-vs-live non-major host share skew
and framed it as "hosting-locus concentration." Invoked under this
protocol:

1. Claim: "Labeled-target DIDs concentrate on non-major PDSes relative
   to the live population."
2. Field semantics: `pds_host` records *current* PDS. Historical host
   is not stored.
3. Confounds: account-creation vintage (cohort), activity bias, resolver
   coverage tail, staleness.
4. Pre-registration: `../../docs/VINTAGE-ADMISSIBILITY.md` — eligibility
   filters, 100-DID sample floor, 5pp + 1.5× ratio-of-ratios effect
   floor, ≥2-bucket replication requirement.
5. Control added: account-creation vintage bucket, via PLC /export
   backfill.
6. Rerun: `scripts/vintage_admissibility.py` against the pre-registered
   rules.
7. Verdict: **dissolved**. No non-major host family cleared the floors
   in ≥2 vintage buckets. `pds_host` remains operationally correct; the
   claim built on it did not survive control.

Artifacts: `../../docs/VINTAGE-ADMISSIBILITY.md`, `out/admissibility/2026-04-21_findings.md`,
`out/admissibility/2026-04-21_1847Z_vintage_admissibility.json`,
`scripts/vintage_admissibility.py`, `src/labeler/vintage_buckets.py`.

The claim was retired in under 48 hours of protocol time, before
additional substrate (e.g. `did_pds_observation` historical log) was
built around it. That is the protocol working as intended.
