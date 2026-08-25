#!/usr/bin/env python3
"""Rebuild labeler.sqlite compactly onto external scratch, then swap it in.

This tool is deliberately boring. It refuses more often than it acts, it never
deletes the original before a verified replacement exists, and every phase is
preceded by a check that can abort the run.

    # preflight only — never mutates anything, safe to run in production
    ./scripts/recover_labeler_db.py --scratch /mnt/recovery

    # actually do it
    ./scripts/recover_labeler_db.py --scratch /mnt/recovery --execute

Why this exists
---------------
Production ran auto_vacuum=NONE, so retention's DELETEs parked freed pages on
an in-file freelist that could never be returned to the filesystem. By
2026-08-12 the file was 192.7 GiB of which 135.6 GiB was freelist, on a 196 GiB
volume at 100% full. In-place reclamation is impossible on a mode-0 database,
and converting the mode itself requires a rebuild. The only escape is to write
a compact copy elsewhere and swap it in.

Space arithmetic, which is the whole reason external scratch is required
---------------------------------------------------------------------
The data volume cannot stage the replacement itself: the original occupies
193 GiB of a 196 GiB volume, so there is nowhere to put a 57 GiB rebuild
alongside it. The new file therefore has to be built on scratch, the original
removed, and the new file copied back. That ordering has a window in which the
data volume holds no database.

Rollback is what closes that window:

  * ``--scratch`` >= live + original  (~250 GiB here): the original is copied
    to scratch first, so the run is fully reversible. This is the default and
    the recommended shape.
  * ``--scratch`` >= live * margin only (~70 GiB here): the verified compact
    copy on scratch is the only rollback artifact. If the copy-back fails it is
    re-copied from scratch; data is lost only if scratch is also lost. This
    narrower mode must be requested explicitly with
    ``--accept-no-original-backup``.

Nothing here removes the original until the replacement has passed
integrity_check, reports auto_vacuum=INCREMENTAL, and matches the source on
per-table row counts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time

AUTO_VACUUM_INCREMENTAL = 2
DEFAULT_DB = "/mnt/zonestorage/driftwatch/data/labeler.sqlite"
DEFAULT_CONTAINER = "driftwatch"
DEFAULT_COMPOSE_DIR = "/opt/driftwatch/deploy"
REBUILD_MARGIN = 1.2

# Geometry the operator signed off on. A materially different database means
# the situation changed since this plan was made, and the run should stop.
EXPECT_PAGE_SIZE = 4096
GEOMETRY_TOLERANCE = 0.25  # 25% drift in page/freelist counts aborts


class Refused(Exception):
    """A precondition failed. Nothing has been mutated."""


def log(msg):
    print("[%s] %s" % (time.strftime("%H:%M:%SZ", time.gmtime()), msg), flush=True)


def refuse(msg):
    raise Refused(msg)


# ---------------------------------------------------------------------------
# Inspection helpers.
# ---------------------------------------------------------------------------

def geometry(path, read_only=True):
    uri = "file:%s?mode=ro" % path if read_only else path
    conn = sqlite3.connect(uri, uri=read_only, timeout=30)
    try:
        g = {}
        for pragma in ("page_size", "page_count", "freelist_count",
                       "auto_vacuum", "journal_mode"):
            g[pragma] = conn.execute("PRAGMA %s" % pragma).fetchone()[0]
        g["total_bytes"] = g["page_size"] * g["page_count"]
        g["freelist_bytes"] = g["page_size"] * g["freelist_count"]
        g["live_bytes"] = g["total_bytes"] - g["freelist_bytes"]
        return g
    finally:
        conn.close()


def table_row_counts(path):
    """Per-table row counts, used to prove the rebuild preserved content."""
    conn = sqlite3.connect("file:%s?mode=ro" % path, uri=True, timeout=60)
    try:
        names = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()]
        return {n: conn.execute('SELECT COUNT(*) FROM "%s"' % n).fetchone()[0]
                for n in names}
    finally:
        conn.close()


def sha256(path, limit_bytes=None):
    h = hashlib.sha256()
    read = 0
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 22), b""):
            h.update(block)
            read += len(block)
            if limit_bytes and read >= limit_bytes:
                break
    return h.hexdigest()


def container_running(name):
    try:
        out = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", name],
            capture_output=True, text=True, timeout=30)
        return out.stdout.strip() == "true"
    except Exception:
        return False


def db_writers(db_path):
    """Processes holding the database open, best-effort."""
    try:
        out = subprocess.run(["lsof", "--", db_path], capture_output=True,
                             text=True, timeout=30)
        lines = [l for l in out.stdout.splitlines()[1:] if l.strip()]
        return lines
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Phases.
# ---------------------------------------------------------------------------

def preflight(args):
    """Every check that can be made without mutating anything."""
    report = {}

    if not os.path.exists(args.db):
        refuse("source database does not exist: %s" % args.db)
    src_size = os.path.getsize(args.db)
    report["source_path"] = args.db
    report["source_size_bytes"] = src_size

    src_geo = geometry(args.db)
    report["source_geometry"] = src_geo
    log("source: %.1f GiB total, %.1f GiB live, %.1f GiB freelist, auto_vacuum=%d"
        % (src_geo["total_bytes"] / 2**30, src_geo["live_bytes"] / 2**30,
           src_geo["freelist_bytes"] / 2**30, src_geo["auto_vacuum"]))

    if src_geo["page_size"] != EXPECT_PAGE_SIZE:
        refuse("unexpected page_size %d (expected %d)"
               % (src_geo["page_size"], EXPECT_PAGE_SIZE))

    if src_geo["auto_vacuum"] == AUTO_VACUUM_INCREMENTAL:
        refuse("source is already auto_vacuum=INCREMENTAL; in-place "
               "incremental_vacuum should be used instead of a rebuild")

    if args.expect_page_count:
        drift = abs(src_geo["page_count"] - args.expect_page_count) / \
            float(args.expect_page_count)
        report["page_count_drift"] = round(drift, 4)
        if drift > GEOMETRY_TOLERANCE:
            refuse("page_count %d differs from expected %d by %.1f%% (> %.0f%%); "
                   "the database changed materially since this run was planned"
                   % (src_geo["page_count"], args.expect_page_count,
                      drift * 100, GEOMETRY_TOLERANCE * 100))

    # --- scratch ---
    if not os.path.isdir(args.scratch):
        refuse("scratch is not a directory: %s" % args.scratch)
    if not os.access(args.scratch, os.W_OK):
        refuse("scratch is not writable: %s" % args.scratch)

    db_dev = os.stat(os.path.dirname(args.db)).st_dev
    if os.stat(args.scratch).st_dev == db_dev:
        refuse("scratch %s is on the same filesystem as the database; a full "
               "volume cannot stage its own rebuild" % args.scratch)

    scratch_free = shutil.disk_usage(args.scratch).free
    required_rebuild = int(src_geo["live_bytes"] * REBUILD_MARGIN)
    required_full = required_rebuild + src_size
    report["scratch_path"] = args.scratch
    report["scratch_free_bytes"] = scratch_free
    report["required_rebuild_bytes"] = required_rebuild
    report["required_with_original_backup_bytes"] = required_full

    log("scratch: %.1f GiB free; rebuild needs %.1f GiB; rebuild+rollback "
        "copy needs %.1f GiB"
        % (scratch_free / 2**30, required_rebuild / 2**30, required_full / 2**30))

    if scratch_free < required_rebuild:
        refuse("scratch has %.1f GiB free, rebuild needs %.1f GiB"
               % (scratch_free / 2**30, required_rebuild / 2**30))

    backup_original = scratch_free >= required_full
    if not backup_original and not args.accept_no_original_backup:
        refuse(
            "scratch has %.1f GiB free — enough for the %.1f GiB rebuild but not "
            "for a %.1f GiB copy of the original as well (%.1f GiB needed). "
            "Provide larger scratch, or re-run with --accept-no-original-backup "
            "to proceed with the verified compact copy as the only rollback "
            "artifact." % (scratch_free / 2**30, required_rebuild / 2**30,
                           src_size / 2**30, required_full / 2**30))
    report["will_back_up_original"] = backup_original

    # --- destinations must not already exist ---
    dest = os.path.join(args.scratch, "labeler.rebuilt.sqlite")
    orig_backup = os.path.join(args.scratch, "labeler.original.sqlite")
    for p in ([dest] + ([orig_backup] if backup_original else [])):
        if os.path.exists(p):
            refuse("destination already exists, refusing to overwrite: %s" % p)
    report["dest_path"] = dest
    report["original_backup_path"] = orig_backup if backup_original else None

    # --- quiesce feasibility ---
    running = container_running(args.container)
    report["container_running"] = running
    if not running:
        log("NOTE: container %s is not running" % args.container)
    if not shutil.which("docker"):
        refuse("docker not available; cannot quiesce %s" % args.container)

    report["preflight"] = "PASS"
    return report


def quiesce(args):
    log("stopping container %s" % args.container)
    subprocess.run(["docker", "compose", "-f",
                    os.path.join(args.compose_dir, "docker-compose.prod.yml"),
                    "-f", os.path.join(args.compose_dir,
                                       "docker-compose.override.yml"),
                    "stop", args.container],
                   cwd=args.compose_dir, check=True, timeout=300)
    for _ in range(30):
        if not container_running(args.container):
            break
        time.sleep(2)
    else:
        refuse("container %s did not stop" % args.container)

    writers = db_writers(args.db)
    if writers:
        refuse("database still has open writers after quiesce:\n%s"
               % "\n".join(writers))
    log("quiesced; no open writers")


def checkpoint(args):
    log("checkpointing WAL")
    conn = sqlite3.connect(args.db, timeout=120)
    try:
        row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        log("wal_checkpoint(TRUNCATE) -> %s" % (row,))
        if row and row[0] != 0:
            refuse("WAL checkpoint did not complete cleanly: %s" % (row,))
    finally:
        conn.close()


def rebuild(args, report):
    dest = report["dest_path"]
    if report["will_back_up_original"]:
        log("copying original to scratch for rollback (this is the slow part)")
        shutil.copy2(args.db, report["original_backup_path"])
        log("original backed up to %s" % report["original_backup_path"])

    log("VACUUM INTO %s" % dest)
    t0 = time.time()
    conn = sqlite3.connect(args.db, timeout=120)
    try:
        # The destination inherits auto_vacuum from this connection's pragma.
        # Verified on SQLite 3.37.2 and 3.46.1: the source is left byte-identical
        # and mode 0, while the destination is written as mode 2.
        conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
        conn.execute("VACUUM INTO '%s'" % dest)
    finally:
        conn.close()
    log("rebuild finished in %.1fs" % (time.time() - t0))
    return dest


def verify(args, report, dest):
    log("verifying replacement")
    dest_geo = geometry(dest)
    report["dest_geometry"] = dest_geo
    report["dest_size_bytes"] = os.path.getsize(dest)

    if dest_geo["auto_vacuum"] != AUTO_VACUUM_INCREMENTAL:
        refuse("replacement reports auto_vacuum=%d, expected INCREMENTAL(2); "
               "the whole point of the rebuild was to enable in-place reclaim"
               % dest_geo["auto_vacuum"])

    conn = sqlite3.connect("file:%s?mode=ro" % dest, uri=True, timeout=600)
    try:
        ic = conn.execute("PRAGMA integrity_check").fetchone()[0]
    finally:
        conn.close()
    report["integrity_check"] = ic
    if ic != "ok":
        refuse("integrity_check failed on replacement: %s" % ic)

    log("comparing per-table row counts")
    src_counts = table_row_counts(args.db)
    dst_counts = table_row_counts(dest)
    report["row_counts_source"] = src_counts
    report["row_counts_dest"] = dst_counts
    if src_counts != dst_counts:
        diff = {k: (src_counts.get(k), dst_counts.get(k))
                for k in set(src_counts) | set(dst_counts)
                if src_counts.get(k) != dst_counts.get(k)}
        refuse("row counts differ between source and replacement: %s" % diff)

    log("verified: integrity ok, %d tables match, %.1f GiB -> %.1f GiB"
        % (len(src_counts), report["source_size_bytes"] / 2**30,
           report["dest_size_bytes"] / 2**30))


def swap_in(args, report, dest):
    """Remove the original and move the replacement into place.

    This is the only destructive step, and it runs only after verify() passed.
    """
    if not report["will_back_up_original"] and not args.accept_no_original_backup:
        refuse("internal: refusing to swap without a rollback artifact")

    st = os.stat(args.db)
    log("original owner uid=%d gid=%d mode=%o" % (st.st_uid, st.st_gid,
                                                  st.st_mode & 0o777))

    side = args.db + ".pre-recovery"
    log("renaming original aside: %s" % side)
    os.replace(args.db, side)          # same filesystem, atomic
    for suffix in ("-wal", "-shm"):
        p = args.db + suffix
        if os.path.exists(p):
            os.replace(p, side + suffix)

    try:
        free_after = shutil.disk_usage(os.path.dirname(args.db)).free
        need = report["dest_size_bytes"]
        if free_after < need:
            # The original still occupies the volume; it must go before the
            # replacement will fit. This is the window the docstring describes.
            log("removing original to make room (%.1f GiB needed, %.1f GiB free)"
                % (need / 2**30, free_after / 2**30))
            os.remove(side)
            for suffix in ("-wal", "-shm"):
                if os.path.exists(side + suffix):
                    os.remove(side + suffix)
            report["original_removed_before_copy"] = True

        log("copying replacement into place")
        shutil.copy2(dest, args.db)
        os.chown(args.db, st.st_uid, st.st_gid)
        os.chmod(args.db, st.st_mode & 0o777)
        report["swapped"] = True
    except Exception:
        log("SWAP FAILED — rollback artifact is %s"
            % (report.get("original_backup_path") or dest))
        raise


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--scratch", required=True,
                    help="external scratch mount (must not be the DB's filesystem)")
    ap.add_argument("--container", default=DEFAULT_CONTAINER)
    ap.add_argument("--compose-dir", default=DEFAULT_COMPOSE_DIR)
    ap.add_argument("--expect-page-count", type=int, default=None,
                    help="abort if page_count drifted materially from this")
    ap.add_argument("--accept-no-original-backup", action="store_true",
                    help="proceed when scratch fits the rebuild but not a full "
                         "copy of the original")
    ap.add_argument("--execute", action="store_true",
                    help="without this, only preflight runs and nothing is mutated")
    ap.add_argument("--json", action="store_true", help="emit the report as JSON")
    args = ap.parse_args()

    report = {"started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    try:
        report.update(preflight(args))
        if not args.execute:
            log("PREFLIGHT ONLY — nothing mutated. Re-run with --execute to proceed.")
            report["result"] = "PREFLIGHT_PASS"
        else:
            quiesce(args)
            checkpoint(args)
            dest = rebuild(args, report)
            verify(args, report, dest)
            swap_in(args, report, dest)
            log("restarting %s" % args.container)
            subprocess.run(["docker", "compose", "start", args.container],
                           cwd=args.compose_dir, check=True, timeout=300)
            report["result"] = "RECOVERED"
            log("done — now qualify the pipeline (eps, coverage, backlog drain) "
                "before declaring recovery")
    except Refused as e:
        report["result"] = "REFUSED"
        report["refused_reason"] = str(e)
        log("REFUSED: %s" % e)
        if args.json:
            print(json.dumps(report, indent=2))
        return 2
    except Exception as e:
        report["result"] = "ERROR"
        report["error"] = str(e)
        log("ERROR: %s" % e)
        if args.json:
            print(json.dumps(report, indent=2))
        return 1

    if args.json:
        print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
