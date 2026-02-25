import pytest
from pathlib import Path
from labeler.drift import cli


def test_live_requires_confirm(tmp_path):
    in_path = Path("fixtures/posts.jsonl")
    out_path = tmp_path / "labels_out.jsonl"
    with pytest.raises(RuntimeError):
        cli.run(str(in_path), str(out_path), live=True, confirm_live=False, max_emit=1)


def test_live_with_confirm_and_cap(tmp_path):
    in_path = Path("fixtures/posts.jsonl")
    out_path = tmp_path / "labels_out.jsonl"
    audit = tmp_path / "audit.jsonl"
    # Should not raise when confirm_live is True and max_emit > 0
    cli.run(str(in_path), str(out_path), live=True, confirm_live=True, max_emit=2)
    assert out_path.exists()
    # verify the audit file was written by the emitter when live
    # default audit path is out/live_emits.jsonl relative to repo; check it's created if any emits
    from pathlib import Path as _P
    assert _P("out/live_emits.jsonl").exists() or _P("out/live_emits.jsonl").exists()
