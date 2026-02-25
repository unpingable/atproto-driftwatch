from typing import List
from .models import Post, ClaimSignal


def assertiveness_score(claim: ClaimSignal) -> float:
    # crude score: number of modal/assertive tokens
    return min(1.0, len(claim.modal) / 3.0)


def detect_assertiveness_increase(prior: ClaimSignal, current: ClaimSignal) -> bool:
    return assertiveness_score(current) > assertiveness_score(prior) + 0.1


def comparable_claim_texts(prior_text: str, current_text: str) -> bool:
    # simple normalization and substring check for demo
    def norm(s: str) -> str:
        return s.lower().replace("\n", " ").strip()

    p = norm(prior_text)
    c = norm(current_text)
    # check if significant overlap
    return p in c or c in p or any(tok in c for tok in p.split() if len(tok) > 4)
