import argparse
import json
from pathlib import Path
from .models import Post, LabelRecord
from .rules import apply_all_rules


def load_posts(path):
    posts = []
    for line in Path(path).read_text().strip().splitlines():
        if not line.strip():
            continue
        data = json.loads(line)
        posts.append(Post(**data))
    return posts


def run(input_path, out_path, live=False, confirm_live=False, max_emit=0):
    """Run the detector and write labels to `out_path`.

    Safe defaults:
    - live: False (do not emit labels externally by default)
    - confirm_live: must be True to allow live emissions
    - max_emit: hard cap on number of labels emitted when live (default 0)
    """
    posts = load_posts(input_path)
    # build simple thread mapping by root uri
    threads = {}
    for p in posts:
        root = p.replyRootUri or p.uri
        threads.setdefault(root, []).append(p)

    labels = []
    for thread_posts in threads.values():
        # sort by createdAt ascending
        thread_posts = sorted(thread_posts, key=lambda x: x.createdAt)
        for p in thread_posts:
            labs = apply_all_rules(p, thread_posts)
            for l in labs:
                labels.append(l)

    # Deterministic output: sort labels by (subject_uri, label, -score)
    labels = sorted(labels, key=lambda l: (l.subject_uri, l.label, -l.score))

    # safety checks for live emission
    if live and not confirm_live:
        raise RuntimeError("Live mode requires --confirm-live to proceed")
    if live and max_emit <= 0:
        raise RuntimeError("Live mode requires a positive --max-emit value; hard cap enforced by default")

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        for l in labels:
            rec = {
                "subject_uri": l.subject_uri,
                "label": l.label,
                # round scores to 3 decimal places for stability
                "score": round(float(l.score), 3),
                "reasons": l.reasons,
                "evidence": l.evidence,
            }
            # deterministic json: sort keys and compact separators
            f.write(json.dumps(rec, sort_keys=True, separators=(",", ":")) + "\n")
    # If live emission requested, simulate a controlled emit respecting max_emit
    if live:
        to_emit = labels[:max_emit]
        _emit_to_live(to_emit)
    print(f"Wrote {len(labels)} labels to {out_path}")


def _emit_to_live(labels):
    # Use the safe emitter to write a deterministically-formatted audit trail. The emitter
    # returns the path to the audit file written.
    from ..emitter import record_emit_decision
    from ..emit_mode import get_emit_mode

    # convert LabelRecord dataclasses to plain dicts for JSONL emission
    recs = [
        {
            "subject_uri": l.subject_uri,
            "label": l.label,
            "score": round(float(l.score), 3),
            "reasons": l.reasons,
            "evidence": l.evidence,
            "rule_id": l.rule_id,
        }
        for l in labels
    ]
    path, status = record_emit_decision(recs, get_emit_mode())
    if status == "emitted":
        print(f"[LIVE-EMIT] Wrote {len(recs)} labels to audit file {path}")
    else:
        print(f"[EMIT-SUPPRESSED] Wrote {len(recs)} would-emit labels to {path}")


def explain(input_path, uri):
    posts = load_posts(input_path)
    threads = {}
    for p in posts:
        root = p.replyRootUri or p.uri
        threads.setdefault(root, []).append(p)

    for thread_posts in threads.values():
        for p in thread_posts:
            if p.uri == uri:
                labs = apply_all_rules(p, thread_posts)
                print(json.dumps([{"label": l.label, "score": l.score, "reasons": l.reasons, "evidence": l.evidence} for l in labs], indent=2))
                return
    print("URI not found")


def main():
    parser = argparse.ArgumentParser(prog="labeler.drift")
    sub = parser.add_subparsers(dest="cmd")
    runp = sub.add_parser("run")
    runp.add_argument("--input", required=True)
    runp.add_argument("--out", required=True)
    runp.add_argument("--live", action="store_true", help="Enable live emission (audit-only by default)")
    runp.add_argument("--confirm-live", action="store_true", help="Required confirmation flag for live mode")
    runp.add_argument("--max-emit", type=int, default=0, help="Hard cap on number of labels emitted in live mode")

    fp = sub.add_parser("fingerprint")
    fp.add_argument("--text", help="Single text to fingerprint (mutually exclusive with --input)")
    fp.add_argument("--input", help="JSONL file with posts to fingerprint (line-delimited), prints each fingerprint_debug result")

    ex = sub.add_parser("explain")
    ex.add_argument("--input", required=True)
    ex.add_argument("--uri", required=True)

    args = parser.parse_args()
    if args.cmd == "run":
        run(args.input, args.out, live=args.live, confirm_live=args.confirm_live, max_emit=args.max_emit)
    elif args.cmd == "explain":
        explain(args.input, args.uri)
    elif args.cmd == "fingerprint":
        from ..claims import fingerprint_debug
        if args.text and args.input:
            raise RuntimeError("Provide either --text or --input, not both")
        if args.text:
            res = fingerprint_debug(args.text)
            print(json.dumps(res, sort_keys=True, indent=2))
        elif args.input:
            for line in Path(args.input).read_text().strip().splitlines():
                if not line.strip():
                    continue
                data = json.loads(line)
                text = data.get("text") or data.get("post") or data.get("body")
                res = fingerprint_debug(text or "")
                res_out = {"uri": data.get("uri"), **res}
                print(json.dumps(res_out, sort_keys=True, separators=(",",":")))
        else:
            raise RuntimeError("Provide --text or --input to fingerprint")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
