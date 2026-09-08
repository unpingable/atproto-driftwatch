"""Bounds remain exact without the production non-covering index scan."""
import sqlite3

from labeler.facts_export import _ensure_tables, _recompute_bounds


def test_bounds_sequential_scan_preserves_aggregate_and_replacement():
    conn = sqlite3.connect(":memory:")
    _ensure_tables(conn)
    # Fingerprint order deliberately differs from input order; ties and the
    # zero epoch retain the original MIN/MAX/COUNT semantics.
    conn.executemany("INSERT INTO uri_fingerprint VALUES (?,?,?,?)", [
        ("at://fixture/a", "z", 90, 1),
        ("at://fixture/b", "a", 30, 2),
        ("at://fixture/c", "z", 10, 3),
        ("at://fixture/d", "a", 30, 4),
        ("at://fixture/e", "n", 0, 5),
    ])
    expected = conn.execute(
        "SELECT fingerprint,MIN(created_epoch),MAX(created_epoch),COUNT(*) "
        "FROM uri_fingerprint INDEXED BY idx_uri_fp GROUP BY fingerprint"
    ).fetchall()
    conn.execute("INSERT INTO fingerprint_bounds VALUES ('obsolete',0,0,1)")
    statements = []
    conn.set_trace_callback(statements.append)
    _recompute_bounds(conn)
    conn.set_trace_callback(None)
    actual = conn.execute("SELECT * FROM fingerprint_bounds ORDER BY fingerprint").fetchall()
    assert actual == expected == [("a", 30, 30, 2), ("n", 0, 0, 1), ("z", 10, 90, 2)]
    query = next(s for s in statements if "INSERT INTO fingerprint_bounds" in s)
    plan = [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + query)]
    assert any("SCAN uri_fingerprint" in detail for detail in plan)
    assert not any("USING INDEX idx_uri_fp" in detail for detail in plan)
    conn.execute("DELETE FROM uri_fingerprint")
    _recompute_bounds(conn)
    assert conn.execute("SELECT count(*) FROM fingerprint_bounds").fetchone()[0] == 0
    conn.close()
