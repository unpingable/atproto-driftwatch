import json
from pathlib import Path
from labeler.claims import fingerprint_text

# Budgets: change only with explicit justification (audit + PR)
CHURN_BUDGET = 0.25  # at most 25% of minor edits should change fingerprint
NEAR_MISS_SEPARATION_MIN = 0.9  # at least 90% of near-miss pairs should be distinct


def test_fingerprint_stability_budgets():
    p = Path("fixtures/fp_stability_transforms.jsonl")
    items = [json.loads(l) for l in p.read_text().splitlines() if l.strip()]

    # Compute churn across all base->transform variants
    total_variants = 0
    total_changed = 0
    for it in items:
        if "base_text" not in it:
            continue
        base = it["base_text"]
        base_fp = fingerprint_text(base)
        variants = it.get("transforms", [])
        for v in variants:
            total_variants += 1
            if fingerprint_text(v) != base_fp:
                total_changed += 1

    churn_rate = total_changed / total_variants if total_variants else 0.0

    # Compute near-miss separation
    pairs = []
    for it in items:
        if "near_miss_pairs" in it:
            pairs.extend(it["near_miss_pairs"])

    total_pairs = len(pairs)
    distinct_count = 0
    collisions = []
    for a, b in pairs:
        fa = fingerprint_text(a)
        fb = fingerprint_text(b)
        if fa != fb:
            distinct_count += 1
        else:
            collisions.append((a, b, fa))

    distinct_ratio = distinct_count / total_pairs if total_pairs else 1.0

    # Fail loudly with useful diagnostics
    assert churn_rate <= CHURN_BUDGET, (
        f"Fingerprint churn rate too high: {churn_rate:.2%} > {CHURN_BUDGET:.2%} (changed {total_changed}/{total_variants}).\n" 
        + "Sample variants that changed relative to base might indicate brittle normalization."
    )

    assert distinct_ratio >= NEAR_MISS_SEPARATION_MIN, (
        f"Near-miss separation too low: {distinct_ratio:.2%} < {NEAR_MISS_SEPARATION_MIN:.2%} (distinct {distinct_count}/{total_pairs}).\n"
        + f"Collisions: {len(collisions)} sample collisions: {collisions[:5]}"
    )
