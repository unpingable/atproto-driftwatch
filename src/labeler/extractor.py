"""Extract social graph edges from events.

Works with the canonical event dict produced by the Jetstream consumer.
"""

from typing import List, Tuple, Dict, Any
from . import timeutil


def extract_edges_from_event(event: Dict[str, Any]) -> List[Tuple[str, str, str, str]]:
    """Return list of edges as tuples (src_did, dst_did, type, ctime_iso).

    Handles both Jetstream-normalized events (top-level authorDid, replyParentUri, etc.)
    and legacy event formats.
    """
    edges = []
    ctime = event.get("createdAt") or event.get("time")
    if isinstance(ctime, (int, float)):
        ctime = timeutil.to_utc_iso(ctime)
    if not ctime:
        ctime = timeutil.now_utc().isoformat()
    else:
        ctime = timeutil.to_utc_iso(ctime)

    author = event.get("authorDid") or event.get("author") or event.get("did")
    if not author:
        return edges

    record = event.get("record", {})
    if not isinstance(record, dict):
        record = {}

    # --- Reply edges ---
    # Jetstream-normalized: top-level replyParentUri
    reply_parent_uri = event.get("replyParentUri")
    if reply_parent_uri:
        # Extract DID from AT URI: at://did:plc:xxx/collection/rkey
        parent_did = _did_from_uri(reply_parent_uri)
        if parent_did and parent_did != author:
            edges.append((author, parent_did, "reply", ctime))
    else:
        # Legacy format: record.reply.parent
        reply = record.get("reply") if isinstance(record, dict) else None
        if reply and isinstance(reply, dict):
            parent = reply.get("parent", {})
            if isinstance(parent, dict):
                parent_uri = parent.get("uri", "")
                parent_did = _did_from_uri(parent_uri) or parent.get("author")
                if parent_did and parent_did != author:
                    edges.append((author, parent_did, "reply", ctime))

    # --- Repost edges ---
    if event.get("type") == "repost" or event.get("_collection") == "app.bsky.feed.repost":
        subject = event.get("subject") or record.get("subject", {})
        if isinstance(subject, dict):
            subj_uri = subject.get("uri", "")
            subj_did = _did_from_uri(subj_uri) or subject.get("author")
            if subj_did and subj_did != author:
                edges.append((author, subj_did, "repost", ctime))

    # --- Quote-post edges ---
    embed = record.get("embed", {})
    if isinstance(embed, dict):
        # Direct record embed
        emb_record = embed.get("record") or {}
        if isinstance(emb_record, dict):
            quote_uri = emb_record.get("uri", "")
            quote_did = _did_from_uri(quote_uri) or emb_record.get("author")
            if quote_did and quote_did != author:
                edges.append((author, quote_did, "quote", ctime))

    return edges


def _did_from_uri(uri: str) -> str:
    """Extract the DID from an AT URI like at://did:plc:xxx/collection/rkey."""
    if not uri or not uri.startswith("at://"):
        return ""
    parts = uri[5:].split("/", 1)
    did = parts[0] if parts else ""
    return did if did.startswith("did:") else ""
