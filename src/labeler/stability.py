import json
import re
import math
import datetime
from typing import Dict, List, Tuple

from .claims import fingerprint_text, FP_VERSION, fingerprint_config_hash


def _percentile(values: List[int], p: float) -> int:
    if not values:
        return 0
    vals = sorted(values)
    k = max(0, min(len(vals) - 1, int(math.ceil(p * len(vals))) - 1))
    return vals[k]


def _mutate_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())


def _mutate_punctuation(text: str) -> str:
    return re.sub(r"[^\w\s]", "", text)


def _mutate_casefold(text: str) -> str:
    return text.lower()


def _mutate_url_param(text: str) -> str:
    if "http" in text:
        if "?" in text:
            return text + "&utm=1"
        return text + "?utm=1"
    return text + " https://example.com/?utm=1"


def _mutate_emoji(text: str) -> str:
    return text + " 🙂"


def _mutate_small_edit(text: str) -> str:
    for i, ch in enumerate(text):
        if ch.isalpha():
            repl = "b" if ch.lower() != "b" else "c"
            return text[:i] + repl + text[i + 1 :]
    return text + "a"


MUTATIONS = {
    "whitespace": _mutate_whitespace,
    "punctuation": _mutate_punctuation,
    "casefold": _mutate_casefold,
    "url_param": _mutate_url_param,
    "emoji": _mutate_emoji,
    "small_edit": _mutate_small_edit,
}


def load_items(path: str, limit: int = None) -> List[dict]:
    items = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            items.append(json.loads(line))
            if limit and len(items) >= limit:
                break
    return items


def compute_stability_report(items: List[dict]) -> dict:
    texts = [i.get("text") or i.get("post") or i.get("body") or "" for i in items]
    uniq_texts = list(dict.fromkeys(texts))

    fps = [fingerprint_text(t) for t in uniq_texts]
    unique_inputs = len(uniq_texts)
    unique_fps = len(set(fps))
    collision_count = max(0, unique_inputs - unique_fps)
    collision_rate = (collision_count / unique_inputs) if unique_inputs else 0.0

    buckets: Dict[str, int] = {}
    for fp in fps:
        buckets[fp] = buckets.get(fp, 0) + 1
    bucket_sizes = list(buckets.values())
    max_bucket = max(bucket_sizes) if bucket_sizes else 0
    p95_bucket = _percentile(bucket_sizes, 0.95)

    anchors: Dict[str, set] = {}
    for it in items:
        anchor = it.get("group") or it.get("anchor") or it.get("uri") or it.get("id")
        if not anchor:
            continue
        fp = fingerprint_text(it.get("text") or "")
        anchors.setdefault(anchor, set()).add(fp)
    anchors_seen = len(anchors)
    anchors_with_churn = sum(1 for s in anchors.values() if len(s) > 1)
    churn_events_total = sum(max(0, len(s) - 1) for s in anchors.values())
    churn_rate_per_anchor = (anchors_with_churn / anchors_seen) if anchors_seen else 0.0

    drift = {}
    for name, fn in MUTATIONS.items():
        flips = 0
        total = 0
        for t in uniq_texts:
            base = fingerprint_text(t)
            mut = fingerprint_text(fn(t))
            total += 1
            if base != mut:
                flips += 1
        drift[name] = {
            "total": total,
            "flips": flips,
            "flip_rate": (flips / total) if total else 0.0,
        }

    return {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "fp_version": FP_VERSION,
        "config_hash": fingerprint_config_hash(),
        "interpretation": {
            "collision_rate": "lower_bound_if_eviction",
            "churn_rate_per_anchor": "lower_bound_if_eviction",
        },
        "drift_definition": {
            "per_class": "1 mutation per input",
            "aggregation": "flip_rate = flips/total",
            "seed": None,
        },
        "collision": {
            "unique_inputs": unique_inputs,
            "unique_fingerprints": unique_fps,
            "collision_count": collision_count,
            "collision_rate": collision_rate,
            "max_bucket": max_bucket,
            "p95_bucket": p95_bucket,
        },
        "churn": {
            "anchors_seen": anchors_seen,
            "anchors_with_churn": anchors_with_churn,
            "churn_events_total": churn_events_total,
            "churn_rate_per_anchor": churn_rate_per_anchor,
        },
        "drift": drift,
    }


def stability_thresholds_from_env() -> dict:
    def _f(key: str, default: float) -> float:
        try:
            return float(os.getenv(key, str(default)))
        except Exception:
            return default
    import os
    return {
        "collision_rate": _f("STABILITY_COLLISION_RATE_MAX", 0.0),
        "churn_rate_per_anchor": _f("STABILITY_CHURN_RATE_MAX", 0.1),
        "drift_max": {
            "whitespace": _f("STABILITY_DRIFT_WHITESPACE_MAX", 0.05),
            "punctuation": _f("STABILITY_DRIFT_PUNCT_MAX", 0.05),
            "casefold": _f("STABILITY_DRIFT_CASEFOLD_MAX", 0.05),
            "url_param": _f("STABILITY_DRIFT_URL_MAX", 0.1),
            "emoji": _f("STABILITY_DRIFT_EMOJI_MAX", 0.1),
            "small_edit": _f("STABILITY_DRIFT_SMALL_EDIT_MAX", 0.5),
        },
    }


def evaluate_stability(report: dict, thresholds: dict) -> Tuple[bool, dict]:
    drift = report.get("drift", {})
    drift_max = thresholds.get("drift_max", {})
    checks = {
        "collision_rate_ok": report["collision"]["collision_rate"] <= thresholds["collision_rate"],
        "churn_rate_ok": report["churn"]["churn_rate_per_anchor"] <= thresholds["churn_rate_per_anchor"],
        "drift_whitespace_ok": drift.get("whitespace", {}).get("flip_rate", 0.0) <= drift_max.get("whitespace", 0.0),
        "drift_punctuation_ok": drift.get("punctuation", {}).get("flip_rate", 0.0) <= drift_max.get("punctuation", 0.0),
        "drift_casefold_ok": drift.get("casefold", {}).get("flip_rate", 0.0) <= drift_max.get("casefold", 0.0),
        "drift_url_param_ok": drift.get("url_param", {}).get("flip_rate", 0.0) <= drift_max.get("url_param", 0.0),
        "drift_emoji_ok": drift.get("emoji", {}).get("flip_rate", 0.0) <= drift_max.get("emoji", 0.0),
        "drift_small_edit_ok": drift.get("small_edit", {}).get("flip_rate", 0.0) <= drift_max.get("small_edit", 0.0),
    }
    ok = all(checks.values())
    return ok, checks
