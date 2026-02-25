import json
from pathlib import Path
from labeler.drift import cli


def _load_jsonl(path):
    lines = Path(path).read_text().strip().splitlines()
    return [json.loads(l) for l in lines if l.strip()]


def test_golden_labels(tmp_path):
    in_path = Path("fixtures/posts.jsonl")
    out_path = tmp_path / "labels_out.jsonl"
    cli.run(str(in_path), str(out_path))

    expected = Path("tests/golden/expected_labels.jsonl")

    got = _load_jsonl(out_path)
    want = _load_jsonl(expected)

    # exact list equality on parsed JSON (more robust than byte-for-byte)
    assert got == want, f"Output differs from golden file: {out_path} vs {expected}\nGot:\n{got}\nWant:\n{want}"
