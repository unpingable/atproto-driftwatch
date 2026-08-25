"""auto_vacuum must be explicit policy for new DBs, not an inherited default."""

import sqlite3

import pytest

from labeler import maintenance as M


class TestAutoVacuumPolicy:
    def test_fresh_database_is_incremental(self, tmp_path, monkeypatch):
        """A newly created labeler DB must report auto_vacuum == 2."""
        from labeler import db as dbmod
        monkeypatch.setattr(dbmod, "DATA_DIR", tmp_path)
        dbmod.init_db()
        conn = sqlite3.connect(str(tmp_path / "labeler.sqlite"))
        try:
            assert conn.execute("PRAGMA auto_vacuum").fetchone()[0] == \
                M.AUTO_VACUUM_INCREMENTAL
        finally:
            conn.close()

    def test_existing_mode_zero_database_is_not_silently_rebuilt(self, tmp_path):
        """Compatibility: the pragma must be a no-op on an existing mode-0 DB.

        Production's 193 GiB labeler.sqlite is mode 0. Importing the app must
        never implicitly try to convert it — that would require a full VACUUM.
        """
        path = tmp_path / "legacy.sqlite"
        c = sqlite3.connect(str(path))
        c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        c.commit()
        assert c.execute("PRAGMA auto_vacuum").fetchone()[0] == M.AUTO_VACUUM_NONE
        c.execute("PRAGMA auto_vacuum=INCREMENTAL")
        assert c.execute("PRAGMA auto_vacuum").fetchone()[0] == M.AUTO_VACUUM_NONE
        c.close()


# ---------------------------------------------------------------------------
# Phase 4 — a database that cannot reclaim space is a fault, not an idle pass.
# ---------------------------------------------------------------------------

def _build_db(path, mode_incremental, rows=4000, keep=1000):
    c = sqlite3.connect(str(path))
    if mode_incremental:
        c.execute("PRAGMA auto_vacuum=INCREMENTAL")
    c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, blob TEXT)")
    c.executemany("INSERT INTO t (blob) VALUES (?)", [("x" * 2000,) for _ in range(rows)])
    c.commit()
    c.execute("DELETE FROM t WHERE id > ?", (keep,))
    c.commit()
    return c
