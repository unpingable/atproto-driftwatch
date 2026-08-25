"""Recovery tooling must refuse unsafe preconditions before touching anything."""

import importlib.util
import os
import pathlib
import sqlite3
import types

import pytest

_SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "recover_labeler_db.py"
_spec = importlib.util.spec_from_file_location("recover_labeler_db", _SCRIPT)
rec = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rec)

GIB = 1024 ** 3


def _make_source(path, mode_incremental=False, rows=2000, keep=200):
    c = sqlite3.connect(str(path))
    if mode_incremental:
        c.execute("PRAGMA auto_vacuum=INCREMENTAL")
    c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, blob TEXT)")
    c.executemany("INSERT INTO t (blob) VALUES (?)",
                  [("x" * 2000,) for _ in range(rows)])
    c.commit()
    c.execute("DELETE FROM t WHERE id > ?", (keep,))
    c.commit()
    c.close()
    return path


def _args(**kw):
    base = dict(db=None, scratch=None, container="driftwatch",
                compose_dir="/tmp", expect_page_count=None,
                accept_no_original_backup=False, execute=False, json=False)
    base.update(kw)
    return types.SimpleNamespace(**base)


@pytest.fixture
def src(tmp_path):
    return _make_source(tmp_path / "source.sqlite")


class TestPreflightRefusals:
    def test_missing_source_refused(self, tmp_path):
        with pytest.raises(rec.Refused, match="does not exist"):
            rec.preflight(_args(db=str(tmp_path / "definitely-absent.sqlite"),
                                scratch=str(tmp_path)))

    def test_scratch_not_a_directory_refused(self, src, tmp_path):
        with pytest.raises(rec.Refused, match="not a directory"):
            rec.preflight(_args(db=str(src), scratch=str(tmp_path / "nodir")))

    def test_scratch_on_same_filesystem_refused(self, src, tmp_path):
        """A full volume cannot stage its own rebuild."""
        scratch = tmp_path / "scratch"
        scratch.mkdir()
        with pytest.raises(rec.Refused, match="same filesystem"):
            rec.preflight(_args(db=str(src), scratch=str(scratch)))

    def test_scratch_too_small_refused(self, src, tmp_path, monkeypatch):
        scratch = tmp_path / "scratch"
        scratch.mkdir()
        monkeypatch.setattr(os, "stat", _stat_with_fake_dev(str(scratch)))
        monkeypatch.setattr(rec.shutil, "disk_usage",
                            lambda p: types.SimpleNamespace(total=1, used=1, free=1024))
        with pytest.raises(rec.Refused, match="rebuild needs"):
            rec.preflight(_args(db=str(src), scratch=str(scratch)))

    def test_no_room_for_original_backup_refused_without_optin(
            self, src, tmp_path, monkeypatch):
        """Enough for the rebuild, not enough for a rollback copy."""
        scratch = tmp_path / "scratch"
        scratch.mkdir()
        live = rec.geometry(str(src))["live_bytes"]
        monkeypatch.setattr(os, "stat", _stat_with_fake_dev(str(scratch)))
        monkeypatch.setattr(
            rec.shutil, "disk_usage",
            lambda p: types.SimpleNamespace(total=0, used=0,
                                            free=int(live * 1.3)))
        with pytest.raises(rec.Refused, match="accept-no-original-backup"):
            rec.preflight(_args(db=str(src), scratch=str(scratch)))

    def test_existing_destination_refused(self, src, tmp_path, monkeypatch):
        scratch = tmp_path / "scratch"
        scratch.mkdir()
        (scratch / "labeler.rebuilt.sqlite").write_bytes(b"x")
        monkeypatch.setattr(os, "stat", _stat_with_fake_dev(str(scratch)))
        monkeypatch.setattr(rec.shutil, "disk_usage",
                            lambda p: types.SimpleNamespace(total=0, used=0,
                                                           free=500 * GIB))
        with pytest.raises(rec.Refused, match="already exists"):
            rec.preflight(_args(db=str(src), scratch=str(scratch)))

    def test_already_incremental_source_refused(self, tmp_path, monkeypatch):
        """A mode-2 DB should use in-place reclaim, not a rebuild."""
        s = _make_source(tmp_path / "m2.sqlite", mode_incremental=True)
        scratch = tmp_path / "scratch"
        scratch.mkdir()
        monkeypatch.setattr(os, "stat", _stat_with_fake_dev(str(scratch)))
        monkeypatch.setattr(rec.shutil, "disk_usage",
                            lambda p: types.SimpleNamespace(total=0, used=0,
                                                           free=500 * GIB))
        with pytest.raises(rec.Refused, match="already auto_vacuum=INCREMENTAL"):
            rec.preflight(_args(db=str(s), scratch=str(scratch)))

    def test_material_geometry_drift_refused(self, src, tmp_path, monkeypatch):
        scratch = tmp_path / "scratch"
        scratch.mkdir()
        monkeypatch.setattr(os, "stat", _stat_with_fake_dev(str(scratch)))
        monkeypatch.setattr(rec.shutil, "disk_usage",
                            lambda p: types.SimpleNamespace(total=0, used=0,
                                                           free=500 * GIB))
        real = rec.geometry(str(src))["page_count"]
        with pytest.raises(rec.Refused, match="changed materially"):
            rec.preflight(_args(db=str(src), scratch=str(scratch),
                                expect_page_count=real * 10))

    def test_clean_preflight_passes(self, src, tmp_path, monkeypatch):
        scratch = tmp_path / "scratch"
        scratch.mkdir()
        monkeypatch.setattr(os, "stat", _stat_with_fake_dev(str(scratch)))
        monkeypatch.setattr(rec.shutil, "disk_usage",
                            lambda p: types.SimpleNamespace(total=0, used=0,
                                                           free=500 * GIB))
        monkeypatch.setattr(rec.shutil, "which", lambda n: "/usr/bin/docker")
        monkeypatch.setattr(rec, "container_running", lambda n: True)
        report = rec.preflight(_args(db=str(src), scratch=str(scratch)))
        assert report["preflight"] == "PASS"
        assert report["will_back_up_original"] is True


class TestRebuildAndVerify:
    def test_end_to_end_rebuild_produces_mode_two_and_matching_rows(
            self, src, tmp_path):
        """The rebuild itself, exercised without the docker/swap phases."""
        scratch = tmp_path / "scratch"
        scratch.mkdir()
        report = {"dest_path": str(scratch / "rebuilt.sqlite"),
                  "will_back_up_original": False,
                  "source_size_bytes": os.path.getsize(src)}
        args = _args(db=str(src), scratch=str(scratch))

        src_geo_before = rec.geometry(str(src))
        assert src_geo_before["auto_vacuum"] == 0
        assert src_geo_before["freelist_count"] > 0

        dest = rec.rebuild(args, report)
        rec.verify(args, report, dest)

        assert report["dest_geometry"]["auto_vacuum"] == rec.AUTO_VACUUM_INCREMENTAL
        assert report["integrity_check"] == "ok"
        assert report["row_counts_source"] == report["row_counts_dest"]
        # source untouched and still mode 0
        assert rec.geometry(str(src))["auto_vacuum"] == 0
        # and materially smaller
        assert report["dest_size_bytes"] < report["source_size_bytes"]

    def test_verify_refuses_row_count_mismatch(self, src, tmp_path):
        scratch = tmp_path / "scratch"
        scratch.mkdir()
        dest = scratch / "rebuilt.sqlite"
        report = {"dest_path": str(dest), "will_back_up_original": False,
                  "source_size_bytes": os.path.getsize(src)}
        args = _args(db=str(src), scratch=str(scratch))
        rec.rebuild(args, report)
        # corrupt the correspondence
        c = sqlite3.connect(str(dest))
        c.execute("DELETE FROM t WHERE id < 50")
        c.commit()
        c.close()
        with pytest.raises(rec.Refused, match="row counts differ"):
            rec.verify(args, report, str(dest))


def _stat_with_fake_dev(scratch_path):
    """os.stat wrapper that reports the scratch dir on a different device."""
    real_stat = os.stat

    def fake(path, *a, **kw):
        st = real_stat(path, *a, **kw)
        if str(path) == scratch_path:
            return types.SimpleNamespace(
                st_dev=st.st_dev + 1, st_uid=st.st_uid, st_gid=st.st_gid,
                st_mode=st.st_mode)
        return st
    return fake
