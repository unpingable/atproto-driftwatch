"""Driftwatch external advisory caveats — consumer policy v1.

Driftwatch as a NAMED non-default consumer of Labelwatch's
emitter_declared exports. This module is the policy artifact: it
declares which labeler/label pairs are admitted into Driftwatch's
external_advisory_caveats roster, applies the policy when invoked,
and emits a receipt for each application.

The conversion is real but narrow:
  Input  - a Labelwatch exporter candidate (the JSON shape produced
           by docs/specimens/specimen_exporter.py in atproto-labelwatch)
  Action - if admitted, append a tuple to
           data/consumer_policy/state/external_advisory_caveats.json
           AND emit a receipt to
           data/consumer_policy/receipts/<timestamp>-<sha>.json
  Effect - the roster is consultable by subsequent Driftwatch
           operations (cluster report annotation, export filters,
           claim-routing tagging). Live wiring of those consumers is
           future work — v1 produces the state, doesn't yet consume it.

DISCIPLINE (matches Bundle G invariants in labelwatch's specimens
track):
  - consumer adoption NEVER promotes to global_platform
  - emitter_declared provenance is inherited into the roster entry
  - non_global_provenance and other caveats from the input are
    copied into the receipt + roster entry
  - the receipt contains: input packet hash, consumer_id, policy
    version, action taken, timestamp, inherited caveats
  - if the input candidate is BLOCKED (schema_kind=blocked_candidate),
    the policy REFUSES it. Blocked is not adoptable.

This is a standalone script, not imported by Driftwatch's live
service. Future work would wire the roster into cluster report
generation; v1's effect is the roster file + receipts only.

CLI:
  python3 policy.py --candidate <exporter-candidate.json>
                    [--state-dir data/consumer_policy/state]
                    [--receipt-dir data/consumer_policy/receipts]
                    [--print-receipt]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


CONSUMER_ID = "driftwatch"
POLICY_VERSION = "external_advisory_caveats-v1.0.0"
POLICY_ARTIFACT_REF = "driftwatch/scripts/consumer_policy/policy.py"
POLICY_RULE_SUMMARY = (
    "Admits emitter_declared labels from a hand-curated allowlist of "
    "labeler/label pairs into Driftwatch's external_advisory_caveats "
    "roster. Each admission preserves the input's non_global_provenance "
    "caveat and is scoped to driftwatch's local state only. Roster is "
    "consulted by future cluster-report annotation; v1 produces state, "
    "does not yet wire downstream."
)

# Allow-list of admitted (labeler_did, label_value) pairs. v1 covers
# only the three third-party labelers Labelwatch's Bundle B/C/F runs
# verified as emitter_declared. Adding labelers/labels here is the
# explicit way to extend Driftwatch's documented adoption.
ADMITTED = [
    {
        "labeler_did": "did:plc:e4elbtctnfqocyfcml6h2lf7",
        "labeler_handle": "skywatch.blue",
        "label_values": ["fringe-media", "fundraising-link"],
        "rationale": (
            "skywatch.blue publishes labelValueDefinitions for these "
            "labels; admitting them as advisory caveats lets Driftwatch "
            "cluster reports flag claims involving the labeled actors "
            "without treating the labels as platform-wide truth."
        ),
    },
    {
        "labeler_did": "did:plc:newitj5jo3uel7o4mnf3vj2o",
        "labeler_handle": "xblock.aendra.dev",
        "label_values": ["twitter-screenshot"],
        "rationale": (
            "xblock's twitter-screenshot label is operationally useful "
            "for caveating cross-platform image provenance in cluster "
            "reports; declared in xblock's service record."
        ),
    },
    {
        "labeler_did": "did:plc:6ebfnuunfngxfw3rth3ewojw",
        "labeler_handle": "label.haus",
        "label_values": ["fucked-up-replyref"],
        "rationale": (
            "label.haus's replyref labels are a structural-quality "
            "signal; admitting them lets Driftwatch caveat malformed "
            "thread structure in reports."
        ),
    },
]

# Refusal reasons (typed strings; the inverse of admission)
REFUSAL_BLOCKED_CANDIDATE = "input_is_blocked_candidate"
REFUSAL_NOT_EMITTER_DECLARED = "input_scope_not_emitter_declared"
REFUSAL_NOT_IN_ALLOWLIST = "labeler_label_pair_not_in_allowlist"


def policy_admits(labeler_did: str, label_value: str) -> Optional[Dict[str, Any]]:
    """Return the matching allowlist entry, or None."""
    for entry in ADMITTED:
        if entry["labeler_did"] == labeler_did and label_value in entry["label_values"]:
            return entry
    return None


def apply_to_exporter_candidate(
    candidate: Dict[str, Any],
    *,
    state_dir: str,
    receipt_dir: str,
    now_iso: Optional[str] = None,
) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    """Apply this policy to one exporter candidate.

    Returns (admitted, receipt_or_None, reason).
    On admitted=True, the roster file is updated and a receipt is
    written; receipt is the dict that was persisted. On admitted=False,
    no files are written and reason is a typed string from the
    REFUSAL_* constants.
    """
    if now_iso is None:
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1. Blocked candidates: refuse outright.
    if candidate.get("schema_kind") != "specimen_candidate":
        return False, None, REFUSAL_BLOCKED_CANDIDATE

    # 2. Scope check: only emitter_declared. global_platform doesn't need
    # us; opt_in_consumer_observed would be another consumer's adoption.
    scope = candidate.get("consumer_scope_effective")
    if scope != "emitter_declared":
        return False, None, REFUSAL_NOT_EMITTER_DECLARED

    # 3. Allowlist check.
    labeler_did = (candidate.get("labeler") or {}).get("did")
    label_value = (candidate.get("label") or {}).get("value")
    entry = policy_admits(labeler_did, label_value)
    if entry is None:
        return False, None, REFUSAL_NOT_IN_ALLOWLIST

    # 4. Compute input packet hash for receipt provenance.
    canonical = json.dumps(candidate, sort_keys=True, separators=(",", ":"))
    input_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    # 5. Build the roster entry. Inherit caveats from the candidate.
    inherited_caveats = list(candidate.get("export_caveats") or [])
    roster_entry = {
        "labeler_did": labeler_did,
        "labeler_handle": (candidate.get("labeler") or {}).get("handle"),
        "label_value": label_value,
        "target_uri": (candidate.get("target") or {}).get("uri"),
        "target_did": (candidate.get("target") or {}).get("target_did"),
        "admitted_at": now_iso,
        "input_packet_hash": input_hash,
        "input_evidence_source": candidate.get("evidence_source"),
        "inherited_caveats": inherited_caveats,
        "policy_version": POLICY_VERSION,
    }

    # 6. Build the receipt.
    receipt = {
        "consumer_id": CONSUMER_ID,
        "policy_version": POLICY_VERSION,
        "policy_artifact_ref": POLICY_ARTIFACT_REF,
        "action_taken": "advisory_caveat_roster_admission",
        "emitted_at": now_iso,
        "input_packet": {
            "evidence_source": candidate.get("evidence_source"),
            "labeler_did": labeler_did,
            "labeler_handle": (candidate.get("labeler") or {}).get("handle"),
            "label_value": label_value,
            "input_hash": input_hash,
        },
        "inherited_caveats": inherited_caveats,
        "roster_entry": roster_entry,
        "discipline_note": (
            "consumer_scope is opt_in:driftwatch; this admission does NOT "
            "promote the underlying emitter_declared finding to "
            "global_platform. non_global_provenance is inherited and must "
            "be preserved by any downstream consumer that consults this "
            "roster."
        ),
    }

    # 7. Persist roster + receipt.
    os.makedirs(state_dir, exist_ok=True)
    os.makedirs(receipt_dir, exist_ok=True)
    roster_path = os.path.join(state_dir, "external_advisory_caveats.json")
    roster: List[Dict[str, Any]] = []
    if os.path.exists(roster_path):
        try:
            with open(roster_path) as f:
                roster = json.load(f)
        except (json.JSONDecodeError, OSError):
            roster = []
    # Idempotent: don't double-admit the same input hash
    existing_hashes = {r.get("input_packet_hash") for r in roster}
    if input_hash not in existing_hashes:
        roster.append(roster_entry)
        with open(roster_path, "w") as f:
            json.dump(roster, f, indent=2)
            f.write("\n")

    receipt_sha = hashlib.sha256(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    safe_ts = now_iso.replace(":", "").replace("-", "").replace("T", "_")
    receipt_path = os.path.join(
        receipt_dir, f"{safe_ts}-{receipt_sha[:12]}.json"
    )
    receipt["receipt_sha256"] = receipt_sha
    receipt["receipt_path"] = receipt_path
    with open(receipt_path, "w") as f:
        json.dump(receipt, f, indent=2)
        f.write("\n")

    return True, receipt, "admitted"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--candidate", required=True, help="path to exporter candidate JSON")
    ap.add_argument(
        "--state-dir",
        default="data/consumer_policy/state",
        help="directory for the external_advisory_caveats.json roster",
    )
    ap.add_argument(
        "--receipt-dir",
        default="data/consumer_policy/receipts",
        help="directory for receipt JSONs",
    )
    ap.add_argument("--print-receipt", action="store_true")
    args = ap.parse_args()

    with open(args.candidate) as f:
        candidate = json.load(f)

    admitted, receipt, reason = apply_to_exporter_candidate(
        candidate, state_dir=args.state_dir, receipt_dir=args.receipt_dir,
    )

    if admitted:
        print(
            f"ADMITTED: {receipt['input_packet']['labeler_handle']}/"
            f"{receipt['input_packet']['label_value']} -> roster + "
            f"receipt {receipt['receipt_path']}"
        )
        if args.print_receipt:
            print(json.dumps(receipt, indent=2))
        return 0
    else:
        print(f"REFUSED: {reason}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
