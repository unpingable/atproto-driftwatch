import hashlib
import json
import os
import re
import unicodedata
from typing import Optional, List
from .db import get_conn
from . import timeutil

# --- Config knobs via env vars (CLI can set these) ---
FP_VERSION = "v1"
FP_QUOTE_KEEP = os.getenv("FINGERPRINT_QUOTE_KEEP", "false").lower() in ("1","true","yes")
FP_NUMBER_MODE = os.getenv("FINGERPRINT_NUMBER_MODE", "bucket")  # exact|bucket|redact
FP_ENTITY_CANON = os.getenv("FINGERPRINT_ENTITY_CANON", "none")  # none|domain|handles
FP_MODAL_FILTER = set([s.strip().lower() for s in os.getenv("FINGERPRINT_MODAL_FILTER", "confirmed,reported,according,report,said,says").split(",") if s.strip()])
FP_ENTITY_STOPWORDS = set([s.strip().lower() for s in os.getenv("FINGERPRINT_ENTITY_STOPWORDS", "about,per,some,see,screenshot,report,reporting,source,approximately,approx").split(",") if s.strip()])
# context stopwords used to pick a concise predicate token when quantities present
FP_CONTEXT_STOPWORDS = set([s.strip().lower() for s in os.getenv("FINGERPRINT_CONTEXT_STOPWORDS", "people,were,in,the,a,of,per,some,about,see,report,screenshot,reported,according,source").split(",") if s.strip()])
# hedging tokens to avoid using as fingerprint context; these should not separate identity
FP_HEDGE_FILTER = set([s.strip().lower() for s in os.getenv("FINGERPRINT_HEDGE_FILTER", "think,maybe,might,could,possibly,suggests,about,approximately").split(",") if s.strip()])

# Minimum complexity gate for claim_history insertion.
# Posts that don't meet this bar still get ingested as events but don't create claim rows.
MIN_ALPHA_TOKENS = int(os.getenv("MIN_CLAIM_ALPHA_TOKENS", "3"))

_EMOJI_RE = re.compile(
    r"[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF"
    r"\U0001F900-\U0001F9FF\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF"
    r"\U00002702-\U000027B0\U0000FE00-\U0000FE0F\U0000200D]+", re.UNICODE
)


def passes_complexity_gate(text: str, external_links: list = None, embeds: list = None, facets: list = None) -> bool:
    """Return True if the text is complex enough to warrant a claim_history row.

    Passes if:
      (a) text has >= MIN_ALPHA_TOKENS alphabetic tokens (3+ chars each), OR
      (b) text has a surviving entity after stopword filtering, OR
      (c) post has an external link, embed, or facet.
    """
    if not text or not text.strip():
        return False

    # (c) structural evidence passes immediately
    if external_links or embeds or facets:
        return True

    # strip emoji to avoid counting them as content
    cleaned = _EMOJI_RE.sub("", text).strip()
    if not cleaned:
        return False

    # (a) count alphabetic tokens with 3+ chars
    alpha_tokens = [t for t in re.findall(r"\b[a-zA-Z]{3,}\b", cleaned)]
    if len(alpha_tokens) >= MIN_ALPHA_TOKENS:
        return True

    # (b) check for a surviving entity
    from .drift.extract import extract_claim_signals
    cs = extract_claim_signals(text)
    if cs.entities:
        return True

    return False


def fingerprint_config_hash() -> str:
    cfg = {
        "fp_version": FP_VERSION,
        "quote_keep": FP_QUOTE_KEEP,
        "number_mode": FP_NUMBER_MODE,
        "entity_canon": FP_ENTITY_CANON,
        "modal_filter": sorted(list(FP_MODAL_FILTER)),
        "entity_stopwords": sorted(list(FP_ENTITY_STOPWORDS)),
        "context_stopwords": sorted(list(FP_CONTEXT_STOPWORDS)),
        "hedge_filter": sorted(list(FP_HEDGE_FILTER)),
        "assertiveness_delta": os.getenv("ASSERTIVENESS_DELTA", "0.2"),
    }
    j = json.dumps(cfg, sort_keys=True)
    return hashlib.sha256(j.encode("utf-8")).hexdigest()[:16]


def _normalize_whitespace_and_unicode(t: str) -> str:
    # unicode normalization and whitespace collapse
    s = unicodedata.normalize("NFKC", t or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _remove_quotes(s: str) -> str:
    if FP_QUOTE_KEEP:
        return s
    # remove double-quote bounded content and single-quote bounded content
    s = re.sub(r'".*?"', "", s)
    s = re.sub(r"'.*?'", "", s)
    return s


def _normalize_number(tok: str) -> str:
    s = tok.strip().lower()
    # handle k suffix (1.2k, 2k)
    if s.endswith("k"):
        try:
            v = float(s[:-1].replace(",", ""))
            n = int(round(v * 1000))
        except Exception:
            return tok
    else:
        # handle European style 1.234,56 vs US 1,234.56
        try:
            if "." in s and "," in s:
                if s.find(".") < s.find(","):
                    # likely European: 1.234,56 -> 1234.56
                    s2 = s.replace(".", "").replace(",", ".")
                else:
                    # likely US: 1,234.56 -> 1234.56
                    s2 = s.replace(",", "")
                nfloat = float(s2)
                n = int(round(nfloat))
            elif "," in s and "." not in s:
                # thousands separator: 1,234 -> 1234
                n = int(s.replace(",", ""))
            else:
                # plain int or float
                if "." in s:
                    n = int(round(float(s)))
                else:
                    n = int(s)
        except Exception:
            return tok
    if FP_NUMBER_MODE == "exact":
        return str(n)
    if FP_NUMBER_MODE == "redact":
        return "<NUM>"
    # bucket mode: coarse-grain to nearest magnitude
    if n < 10:
        return str(n)
    if n < 100:
        return str(int(round(n, -1)))
    if n < 1000:
        return str(int(round(n, -2)))
    k = int(round(n / 1000.0))
    return f"{k}k"


def _canonicalize_entity(ent: str) -> str:
    # basic heuristics: strip tracking params for urls, lower domain, preserve handles
    ent = ent.strip()
    if FP_ENTITY_CANON == "none":
        return ent
    if FP_ENTITY_CANON == "handles":
        # preserve @handles and lowercase
        if ent.startswith("@"):
            return ent.lower()
        return ent
    # domain canonicalization for urls
    if FP_ENTITY_CANON == "domain":
        m = re.match(r"https?://([^/]+)", ent)
        if m:
            host = m.group(1).lower()
            # strip common tracking params not stored in domain view
            host = host.split(":")[0]
            return host
        return ent
    return ent


def _quantity_magnitude(tok: str):
    # Return approximate integer magnitude for a quantity token or None
    s = tok.strip().lower()
    if s.endswith("k"):
        try:
            v = float(s[:-1].replace(",", ""))
            return int(round(v * 1000))
        except Exception:
            return None
    try:
        # handle EU/US formats
        if "." in s and "," in s:
            if s.find(".") < s.find(","):
                s2 = s.replace(".", "").replace(",", ".")
            else:
                s2 = s.replace(",", "")
            return int(round(float(s2)))
        if "," in s and "." not in s:
            return int(s.replace(",", ""))
        if "." in s:
            return int(round(float(s)))
        return int(s)
    except Exception:
        return None


def _normalize_text_for_fingerprint(t: str) -> str:
    s = _normalize_whitespace_and_unicode(t or "")
    s = _remove_quotes(s)
    # tokenize and normalize numbers and drop punctuation
    toks = re.findall(r"\b\w+\b", s)
    out = []
    for tok in toks:
        if tok.isdigit() or re.match(r"^\d[\d,]*$", tok):
            out.append(_normalize_number(tok))
        else:
            out.append(tok.lower())
    return " ".join(out)


def _extract_link_domains(text: str) -> list:
    """Extract normalized domains from URLs in text."""
    from urllib.parse import urlparse
    domains = []
    for url in re.findall(r"https?://\S+", text or ""):
        try:
            host = urlparse(url).netloc.lower().split(":")[0]
            # strip www. prefix
            if host.startswith("www."):
                host = host[4:]
            if host:
                domains.append(host)
        except Exception:
            pass
    return sorted(set(domains))


def fingerprint_text_with_kind(text: str) -> tuple:
    """Derive a stable fingerprint with configurable heuristics.

    Returns (fingerprint_hash, kind) where kind is one of:
      "entity", "quantity", "domain", "span", "text"

    Kind rule: first winning source in precedence order. A post with both
    a multi-word entity and a quantity gets kind="entity" because entity
    wins the precedence race. This is stable across runs — changing
    precedence order changes kind assignment, which is the point.

    Source precedence (strongest signal wins):
      1. Multi-word named entity — E:New York,Jd Vance
      2. Quantity + context — Q:10k|C:killed
      3. URL domain (when no entity/quantity) — D:nytimes.com
      4. Single-word named entity (when nothing stronger) — E:Vance
      5. Claim spans (sentences with numbers/dates/modals)
      6. Fallback: normalized text hash

    Domains are a strong clustering signal but are NOT additive — a claim
    with a quantity should fingerprint the same whether or not a URL is present.
    """
    from .drift.extract import extract_claim_signals

    cs = extract_claim_signals(text or "")
    parts = []
    kind = "text"  # default fallback

    # Split entities into multi-word (strong) and single-word (weak)
    all_ents = [e for e in cs.entities if e and e.lower().strip(":") not in FP_MODAL_FILTER and e.lower().strip(":") not in FP_ENTITY_STOPWORDS]
    all_ents = [_canonicalize_entity(e) for e in all_ents]
    multi_word_ents = [e for e in all_ents if " " in e]
    single_word_ents = [e for e in all_ents if " " not in e]

    # --- 1. Multi-word named entities ---
    if multi_word_ents:
        parts.append("E:" + ",".join(sorted(multi_word_ents)))
        kind = "entity"

    # --- 2. Quantities + context ---
    if cs.quantities:
        qn = [str(_normalize_number(q)) for q in sorted(cs.quantities)]
        parts.append("Q:" + ",".join(qn))
        if kind == "text":
            kind = "quantity"
        # add predicate context to distinguish "10k killed" from "10k donated"
        if cs.spans:
            added_prep = False
            span_raw = cs.spans[0]
            m = re.search(r"\b(in|by)\s+([A-Za-z0-9_\-]{2,})", span_raw, re.I)
            if m:
                prep = m.group(1).lower()
                obj = m.group(2).lower()
                if obj not in FP_CONTEXT_STOPWORDS and obj not in FP_ENTITY_STOPWORDS:
                    parts.append(f"P:{prep}:{obj}")
                    added_prep = True
            if not added_prep:
                s0 = _normalize_text_for_fingerprint(cs.spans[0])
                s0_n = re.sub(r"\b\d[\d\.,]*k?\b", "", s0)
                toks = [t for t in s0_n.split() if len(t) > 3 and t not in FP_CONTEXT_STOPWORDS]
                token = toks[0] if toks else ""
                if token and token.lower() not in FP_HEDGE_FILTER and token.lower() not in FP_MODAL_FILTER and token.lower() not in FP_ENTITY_STOPWORDS:
                    parts.append("C:" + token)

    # --- 3. URL domains (only if no entity/quantity signal) ---
    if not parts:
        domains = _extract_link_domains(text or "")
        if domains:
            parts.append("D:" + ",".join(domains))
            kind = "domain"

    # --- 4. Single-word entities (only if nothing stronger exists) ---
    if not parts and single_word_ents:
        parts.append("E:" + ",".join(sorted(single_word_ents)))
        kind = "entity"

    # --- 5. Claim spans ---
    if cs.spans and not parts:
        spans_norm = [_normalize_text_for_fingerprint(s) for s in cs.spans]
        parts.append("S:" + ",".join(sorted(spans_norm)))
        kind = "span"

    # --- 6. Fallback: normalized text ---
    if not parts:
        parts.append("T:" + _normalize_text_for_fingerprint(text or ""))
        # kind remains "text"

    fingerprint_source = "|".join(parts)
    h = hashlib.sha256(fingerprint_source.encode("utf-8")).hexdigest()
    return h[:16], kind


def fingerprint_text(text: str) -> str:
    """Derive a stable fingerprint hash (convenience wrapper)."""
    fp, _kind = fingerprint_text_with_kind(text)
    return fp


def fingerprint_debug(text: str) -> dict:
    """Return debug information for fingerprinting: source, fingerprint, version, and knobs."""
    from .drift.extract import extract_claim_signals
    cs = extract_claim_signals(text or "")

    # reuse fingerprint construction so debug output matches production
    qn = [str(_normalize_number(q)) for q in sorted(cs.quantities)] if cs.quantities else []
    ents = [e for e in cs.entities if e and e.lower().strip(":") not in FP_MODAL_FILTER and e.lower().strip(":") not in FP_ENTITY_STOPWORDS]
    ents = [_canonicalize_entity(e) for e in ents] if ents else []
    spans_norm = [ _normalize_text_for_fingerprint(s) for s in cs.spans ] if cs.spans else []

    parts = []
    if qn:
        parts.append("Q:" + ",".join(qn))
        # include short normalized span context when quantities present
        magnitude = _quantity_magnitude(sorted(cs.quantities, key=lambda x: len(x))[0]) if cs.quantities else None
        if cs.spans and (magnitude is None or magnitude >= 1000):
            s0 = _normalize_text_for_fingerprint(cs.spans[0])
            s0_n = re.sub(r"\b\d[\d\.,]*k?\b", "", s0)
            s0_tok = " ".join(s0_n.split()[:6])
            if s0_tok:
                parts.append("C:" + s0_tok)
    if ents:
        parts.append("E:" + ",".join(sorted(ents)))
    if spans_norm and not parts:
        parts.append("S:" + ",".join(sorted(spans_norm)))
    if not parts:
        parts.append("T:" + _normalize_text_for_fingerprint(text or ""))

    fingerprint_source = "|".join(parts)
    fp = hashlib.sha256(fingerprint_source.encode("utf-8")).hexdigest()[:16]
    return {
        "fingerprint_version": FP_VERSION,
        "fingerprint": fp,
        "source": fingerprint_source,
        "config": {
            "quote_keep": FP_QUOTE_KEEP,
            "number_mode": FP_NUMBER_MODE,
            "entity_canon": FP_ENTITY_CANON,
            "modal_filter": sorted(list(FP_MODAL_FILTER)),
            "entity_stopwords": sorted(list(FP_ENTITY_STOPWORDS)),
        },
    }


def evidence_hash_from_raw(raw: dict) -> str:
    return evidence_hash_from_signals(
        raw.get("text", ""),
        raw.get("externalLinks") or [],
        raw.get("embeds") or [],
        raw.get("facets") or [],
    )


def evidence_hash_from_signals(text: str, external_links: list, embeds: list, facets: list) -> str:
    from urllib.parse import urlparse, urlunparse

    def _normalize_link(u: str) -> str:
        try:
            p = urlparse(u)
            return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
        except Exception:
            return u

    ent = {
        "links": sorted(_normalize_link(u) for u in (external_links or [])),
        "embeds": sorted(json.dumps(e, sort_keys=True) for e in (embeds or [])),
        "facets": sorted(json.dumps(f, sort_keys=True) for f in (facets or [])),
    }
    j = json.dumps(ent, sort_keys=True)
    return hashlib.sha256(j.encode("utf-8")).hexdigest()[:16]


def classify_evidence(external_links: list = None, embeds: list = None, facets: list = None) -> str:
    """Classify a post's evidence into a coarse bucket for entropy calculations.

    Returns one of: none, link, embed, facet, mixed
    """
    has_link = bool(external_links)
    has_embed = bool(embeds)
    has_facet = bool(facets)
    count = sum([has_link, has_embed, has_facet])
    if count == 0:
        return "none"
    if count > 1:
        return "mixed"
    if has_link:
        return "link"
    if has_embed:
        return "embed"
    return "facet"


def add_claim_history_txn(conn, authorDid: str, text: str, createdAt: str, post_uri: str, post_cid: Optional[str] = None, confidence: Optional[float] = None, provenance: Optional[str] = None, evidence_hash: Optional[str] = None, evidence_class: Optional[str] = None):
    """Transaction-scoped insert: uses the passed conn, does not commit or close."""
    fp, fp_kind = fingerprint_text_with_kind(text)
    createdAt = timeutil.to_utc_iso(createdAt)
    observed_at = timeutil.now_utc().isoformat()
    conn.execute(
        "INSERT INTO claim_history (authorDid, claim_fingerprint, createdAt, confidence, provenance, evidence_hash, post_uri, post_cid, fingerprint_version, evidence_class, fp_kind, observed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (authorDid, fp, createdAt, confidence, provenance or "", evidence_hash or "", post_uri, post_cid or "", FP_VERSION, evidence_class or "none", fp_kind, observed_at),
    )
    return fp, fp_kind


def add_claim_history(authorDid: str, text: str, createdAt: str, post_uri: str, post_cid: Optional[str] = None, confidence: Optional[float] = None, provenance: Optional[str] = None, evidence_hash: Optional[str] = None, evidence_class: Optional[str] = None):
    conn = get_conn()
    try:
        result = add_claim_history_txn(conn, authorDid, text, createdAt, post_uri, post_cid, confidence, provenance, evidence_hash, evidence_class)
        conn.commit()
        return result
    finally:
        conn.close()


def get_claim_history(authorDid: str, fingerprint: str) -> List[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT authorDid, claim_fingerprint, createdAt, confidence, provenance, evidence_hash, post_uri, post_cid, fingerprint_version FROM claim_history WHERE authorDid = ? AND claim_fingerprint = ? ORDER BY createdAt ASC",
        (authorDid, fingerprint),
    ).fetchall()
    conn.close()
    return [
        {
            "authorDid": r[0],
            "claim_fingerprint": r[1],
            "createdAt": r[2],
            "confidence": r[3],
            "provenance": r[4],
            "evidence_hash": r[5],
            "post_uri": r[6],
            "post_cid": r[7],
            "fingerprint_version": r[8],
        }
        for r in rows
    ]


# ------ Delta & claim state utilities ------
ATTRIBUTION_TOKENS = ["reportedly", "according to", "source says", "reported by", "sources say"]


def compute_claim_state_from_post(raw: dict) -> dict:
    """Compute a small claim state dict from a post raw JSON.

    Returns: {"confidence": float, "evidence_hash": str, "attribution_present": bool}
    """
    from .drift.extract import extract_claim_signals
    from .drift.diff import assertiveness_score

    text = raw.get("text", "")
    cs = extract_claim_signals(text)
    confidence = assertiveness_score(cs)
    evidence_hash = evidence_hash_from_signals(
        raw.get("text", ""),
        raw.get("externalLinks") or [],
        raw.get("embeds") or [],
        raw.get("facets") or [],
    )
    text_l = text.lower() if text else ""
    attribution_present = any(tok in text_l for tok in ATTRIBUTION_TOKENS)
    return {"confidence": confidence, "evidence_hash": evidence_hash, "attribution_present": attribution_present}


def compare_claim_states(prior_state: dict, current_state: dict) -> dict:
    """Return computed deltas between two claim state dicts."""
    return {
        "confidence_delta": current_state.get("confidence", 0.0) - prior_state.get("confidence", 0.0),
        "evidence_changed": prior_state.get("evidence_hash") != current_state.get("evidence_hash"),
        "attribution_removed": bool(prior_state.get("attribution_present") and not current_state.get("attribution_present")),
    }
