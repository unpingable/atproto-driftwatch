from typing import List, Dict, Any
import json
from .models import Post, LabelRecord
from .extract import extract_claim_signals
from .diff import detect_assertiveness_increase, comparable_claim_texts

ATTRIBUTION_TOKENS = ["reportedly", "according to", "source says", "reported by", "sources say"]
QUOTE_MARK_RE = '"'


def rule_provenance_laundering(post: Post, thread: List[Post]) -> List[LabelRecord]:
    labels = []
    # find prior post in thread by same author
    priors = [p for p in thread if p.authorDid == post.authorDid and p.uri != post.uri]

    post_cs = extract_claim_signals(post.text)

    # Helper to evaluate a prior post object-like (can be Post or raw dict)
    def _check_prior_text(prior_text: str, prior_uri: str) -> bool:
        prior_text_l = prior_text.lower()
        post_text = post.text.lower()
        prior_has_attr = any(tok in prior_text_l for tok in ATTRIBUTION_TOKENS)
        post_has_attr = any(tok in post_text for tok in ATTRIBUTION_TOKENS)
        if not prior_has_attr or post_has_attr:
            return False
        prior_cs = extract_claim_signals(prior_text)
        strong_text = comparable_claim_texts(prior_text, post.text)
        signal_overlap = bool(set(prior_cs.dates) & set(post_cs.dates)) or bool(set(prior_cs.quantities) & set(post_cs.quantities)) or bool(set(prior_cs.entities) & set(post_cs.entities))
        if strong_text or signal_overlap:
            labels.append(LabelRecord(subject_uri=post.uri, label="provenance_laundering_possible", score=0.9, reasons=["attribution removed compared to prior post"], evidence=[{"prior": prior_uri, "post": post.uri}], rule_id="provenance_laundering"))
            return True
        return False

    # First check thread-local priors
    for prior in reversed(priors):
        if _check_prior_text(prior.text, prior.uri):
            return labels

    # Fallback: check claim_history for prior claims by fingerprint for this author
    try:
        from ..claims import fingerprint_text, get_claim_history
        from ..db import get_conn
        fp = fingerprint_text(post.text)
        history = get_claim_history(post.authorDid, fp)
        # look for earlier prior posts in history
        for h in reversed(history):
            if h["createdAt"] < post.createdAt:
                conn = get_conn()
                rows = conn.execute("SELECT raw FROM events WHERE event_uri = ?", (h["post_uri"],)).fetchall()
                conn.close()
                if not rows:
                    continue
                raw = json.loads(rows[0][0])
                if _check_prior_text(raw.get("text", ""), h["post_uri"]):
                    return labels
    except Exception:
        # be conservative
        pass

    return labels


def rule_repeat_claim_no_new_evidence(post: Post, thread: List[Post]) -> List[LabelRecord]:
    labels = []
    priors = [p for p in thread if p.authorDid == post.authorDid and p.uri != post.uri]
    if not priors:
        return labels

    post_claim = extract_claim_signals(post.text)
    for prior in reversed(priors):
        prior_claim = extract_claim_signals(prior.text)
        if comparable_claim_texts(prior.text, post.text):
            # consider new evidence as presence of link/embed in current vs prior
            prior_evidence = bool(prior.externalLinks or prior.embeds)
            post_evidence = bool(post.externalLinks or post.embeds)
            if post_evidence == prior_evidence:
                labels.append(LabelRecord(subject_uri=post.uri, label="repeat_claim_no_new_evidence", score=0.6, reasons=["claim repeated without new evidence"], evidence=[{"prior": prior.uri, "post": post.uri}], rule_id="repeat_claim_no_new_evidence"))
                break
    return labels


def rule_quote_mismatch(post: Post, thread: List[Post]) -> List[LabelRecord]:
    labels = []
    text = post.text
    if '"' in text or 'according to' in text.lower():
        if not post.externalLinks and not post.facets:
            labels.append(LabelRecord(subject_uri=post.uri, label="quote_mismatch", score=0.55, reasons=["quote or attribution present but no link/facet found"], evidence=[{"post": post.uri}], rule_id="quote_mismatch"))
    return labels


def rule_time_inconsistency(post: Post, thread: List[Post]) -> List[LabelRecord]:
    # detect if post mentions a future date in YYYY-MM-DD format
    labels = []
    import re
    from datetime import datetime
    m = re.search(r"(\d{4}-\d{2}-\d{2})", post.text)
    if m:
        try:
            d = datetime.fromisoformat(m.group(1))
            created = datetime.fromisoformat(post.createdAt)
            if d > created:
                labels.append(LabelRecord(subject_uri=post.uri, label="time_inconsistency", score=0.4, reasons=["mentioned date appears in future compared to createdAt"], evidence=[{"post": post.uri, "date_mentioned": m.group(1)}], rule_id="time_inconsistency"))
        except Exception:
            pass
    return labels


def rule_assertiveness_increase(post: Post, thread: List[Post]) -> List[LabelRecord]:
    """Detects if the author has increased assertiveness for the same claim fingerprint without new evidence."""
    labels = []
    try:
        from ..claims import fingerprint_text, get_claim_history, compute_claim_state_from_post, compare_claim_states, evidence_hash_from_raw
        from ..db import get_conn
        # compute fingerprint for this post
        fp = fingerprint_text(post.text)
        # fetch history for this author+fingerprint
        history = get_claim_history(post.authorDid, fp)
        if not history:
            return labels
        # find the most recent prior claim before this post (by createdAt)
        prior = None
        for h in history:
            if h["createdAt"] < post.createdAt:
                prior = h
        if not prior:
            return labels
        # fetch raw of the prior post to compute state
        conn = get_conn()
        rows = conn.execute("SELECT raw FROM events WHERE event_uri = ?", (prior["post_uri"],)).fetchall()
        conn.close()
        if not rows:
            return labels
        prior_raw = json.loads(rows[0][0])
        # compute states
        prior_state = compute_claim_state_from_post(prior_raw)
        current_state = compute_claim_state_from_post({"text": post.text, "externalLinks": post.externalLinks, "embeds": post.embeds, "facets": post.facets})
        deltas = compare_claim_states(prior_state, current_state)
        # use a heuristic: confidence increase >= ASSERTIVENESS_DELTA and evidence unchanged
        import os
        try:
            threshold = float(os.getenv("ASSERTIVENESS_DELTA", "0.2"))
        except Exception:
            threshold = 0.2
        if deltas["confidence_delta"] >= threshold and not deltas["evidence_changed"]:
            labels.append(LabelRecord(subject_uri=post.uri, label="assertiveness_increase_possible", score=0.7, reasons=["assertiveness/confidence increased without new evidence"], evidence=[{"prior": prior["post_uri"], "post": post.uri}], rule_id="assertiveness_increase"))
    except Exception:
        # be conservative on errors
        pass
    return labels


def apply_all_rules(post: Post, thread: List[Post]) -> List[LabelRecord]:
    labels = []
    labels.extend(rule_provenance_laundering(post, thread))
    labels.extend(rule_repeat_claim_no_new_evidence(post, thread))
    labels.extend(rule_assertiveness_increase(post, thread))
    labels.extend(rule_quote_mismatch(post, thread))
    labels.extend(rule_time_inconsistency(post, thread))
    # filter by score threshold
    return [l for l in labels if l.score >= 0.4]
