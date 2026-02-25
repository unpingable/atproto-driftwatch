"""Regenerate the golden labels file by running the drift CLI deterministically."""
import json
from pathlib import Path
from labeler.drift import cli


def main():
    in_path = Path("fixtures/posts.jsonl")
    out_path = Path("tests/golden/expected_labels.jsonl")
    # run CLI programmatically with deterministic defaults
    cli.run(str(in_path), str(out_path))
    print(f"Wrote golden file to {out_path}")


if __name__ == "__main__":
    main()
