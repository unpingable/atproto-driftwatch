# driftwatch — Signal Model

**Status**: v0 starter.
**Last updated**: 2026-04-28

## The job

This doc names what driftwatch's structural signals (burst, synchrony, half-life, variant entropy, single-author concentration) actually claim to measure — and what they don't. It also names the architectural fork between **claims-anchored** and **structural-only** observatories, since this fork is load-bearing for understanding what driftwatch can and cannot see.

## The fork

There are two architecturally distinct ways to do "discourse measurement" on ATProto:

### Claims-anchored (driftwatch's choice)

```
post text → fingerprint → cluster → structural signals over cluster
```

The unit of analysis is the **claim cluster** — posts that share a fingerprint (multi-word entity, quantity+context, URL domain, single-word entity, span, or normalized text). Structural signals (author entropy, variant entropy, half-life) are computed *over the cluster*.

What this can do: half-life estimation, correction resistance, cluster persistence, claim mutation tracking, "is this a claim that's spreading or a one-off." All require knowing *that two posts say the same thing.*

What this needs: a working fingerprint pipeline. Fingerprint quality bounds everything downstream. `FP_VERSION` is a contract; migrations require dual-emit periods (audit only).

### Structural-only (the other observatory)

```
posts + edges → topology + temporal shape signals
```

The unit of analysis is the **propagation graph**. Repost cascades, reply trees, follow-graph activity, posting-rate patterns. Computed without ever asking "is this the same claim."

What this can do: cascade shape, network position, synchrony, posting rhythm, structural signature of coordinated action.

What this can't do: anything keyed on claim identity. It can see that a thing spread; it can't see *what* spread.

### The two see overlapping but non-identical phenomena

A coordinated copy-paste campaign produces both a low-author-entropy claim cluster (claims-anchored sees it) and a tight cascade with synchronous arrival (structural-only sees it). Either approach catches it.

A campaign that uses paraphrase variation to evade fingerprinting is invisible to claims-anchored, visible to structural-only (the cascade is still there). Conversely, a viral *idea* that spreads through independent rephrasing is visible to claims-anchored (variant entropy is high but cluster persists), partially visible to structural-only (shape may look like organic spread).

Driftwatch picked claims-anchored because the original question was "do claims persist, mutate, resist correction." That question requires claim identity. Structural-only is a different observatory; not driftwatch's. They are complementary, not interchangeable.

## What burst/synchrony measurements claim

For a given claim cluster, driftwatch computes:

- **Author/post ratio** — N authors / N posts. Low ratio (e.g., 1000 posts from 10 authors) approximates coordinated behavior or automation. High ratio (e.g., 1000 posts from 800 authors) approximates organic spread.
- **Variant entropy** — within the cluster, how much do post variants differ? Low entropy = copy-paste / template behavior. High entropy = people rephrasing in their own words.
- **Half-life** — peak → decay curve. Short half-life = viral spike. Long half-life = sustained discourse. Requires ≥8 hourly bins to be meaningful.
- **Burst score** — z-score of cluster volume against a rolling baseline.
- **Synchrony score** — simultaneity of arrival. High synchrony + low author entropy = coordination signature.
- **Single-author detection** — clusters with 1 author + ≥10 posts flagged `likely_automation`. Aggregate-only — the *prevalence* is published, not the cluster's author identity.

All of these are computed *over the cluster*, not over individual accounts. The unit of report is the cluster.

## What these measurements do NOT claim

The structural signature of conversation is not the same as discourse itself. Structural signals can separate *kinds* of virality from each other (bot synchrony, coordinated amplification, copy-paste mimicry, organic spread) but do not, by themselves, decide whether any of those is "actual discourse."

Driftwatch's implicit working approximation: *high author entropy + high variant entropy + long half-life* approximates discourse-shape. *Low on all three* approximates virality-shape. This is a working approximation, not a measurement of discourse. Discourse-vs-virality is a downstream judgment that requires semantic content interpretation, which driftwatch does not do.

If a system claims "this cluster is discourse" or "this cluster is bot activity" based purely on driftwatch's signals, it has gone past what the measurements support. The signals are evidence; they are not verdicts.

## Output language: signatures, not categories

Driftwatch outputs describe *signatures*, not categories:

- "concentration anomaly" not "bot farm"
- "low variant entropy in cluster X" not "copy-paste campaign"
- "synchrony spike" not "coordinated attack"
- "single-author cluster (likely_automation)" not "automated account"

The output's job is to surface signatures that warrant operator attention. The verdict — "this is automation, this is organic, this is laundering" — sits with whoever reads the report and applies judgment. The architecture is deliberately built to *not* render that verdict.

## What this means for new signals

Any new structural signal added to driftwatch should answer:

1. **What unit does it operate on?** Cluster (default), host family, time window. Not account.
2. **What does it claim?** A specific structural property — concentration, entropy, decay shape — not a category like "bot" or "discourse."
3. **What's the failure mode?** Under what data condition does it produce loss-conditioned output? (See `FAILURE_MODES.md` TODO.)
4. **What population is it computed over?** Live-observed, labeled-seed, overlap. Coverage must specify denominator.
5. **What does it not say?** Name the verdict it stops short of, so downstream consumers don't read it as the verdict.

## Cross-reference

- The claims-anchored / structural-only fork was named in conversation 2026-04-28; this doc is its first written form.
- The "language may propose; only evidence commits state" invariant in `OVERVIEW.md` is the same principle one level down: rules propose, evidence commits. Signals describe, judgments commit.
- Aggregate-first / no per-account scoring lives in `PUBLIC_SURFACES.md` (TODO) and `../driftwatch/SCOPE.md`.
- `../../THEORY_TO_CODE.md` maps these concepts to code locations (`fingerprint.py`, `detection.py`, `platform_health.py`, etc.).
