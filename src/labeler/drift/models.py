from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class Post:
    uri: str
    cid: Optional[str]
    text: str
    createdAt: str
    authorDid: str
    replyParentUri: Optional[str] = None
    replyRootUri: Optional[str] = None
    facets: Optional[List[Any]] = field(default_factory=list)
    embeds: Optional[List[Any]] = field(default_factory=list)
    externalLinks: Optional[List[str]] = field(default_factory=list)


@dataclass
class ClaimSignal:
    spans: List[str]
    dates: List[str]
    quantities: List[str]
    entities: List[str]
    modal: List[str]


@dataclass
class LabelRecord:
    subject_uri: str
    label: str
    score: float
    reasons: List[str]
    evidence: List[Dict[str, Any]]
    rule_id: str = ""
