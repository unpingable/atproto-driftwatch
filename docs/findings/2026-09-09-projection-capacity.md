# Projection capacity: batch catch-up and publication age are different

This continues the [completed-prefix recovery](2026-09-08-completed-prefix-recovery.md)
and [facts-export scan incident](../INCIDENT-2026-09-08-facts-export-scan.md).
Production measurements use unchanged `v0.1.0-rc.6`, source
`9ec9da593b6a435de273396626ecaca3755268e7`, image
`sha256:70c60bb3403c5f2d57c5f9b789306d5933ec3dc751b837c9bde9830d3c88d80a`.
This is a bounded measurement record through 2026-09-09 19:05 UTC, not an
all-healthy declaration or qualification of a new production configuration.

## Natural production cycles

Six completed cycles were already available before starting bounded read-only
measurement. The exporter reported eligible batch rows, not distinct URI upserts.

| Completion (UTC) | Eligible rows | Batch seconds | Batch rows/s | Rows/s over completion spacing |
| --- | ---: | ---: | ---: | ---: |
| Sep 8 21:32:50 | 573,328 | 1,719.9 | 333.35 | initial boundary unavailable |
| Sep 9 00:31:26 | 396,839 | 626.6 | 633.32 | 37.03 |
| Sep 9 03:33:59 | 351,083 | 754.5 | 465.32 | 32.05 |
| Sep 9 06:31:07 | 300,303 | 560.7 | 535.59 | 28.26 |
| Sep 9 09:22:06 | 233,475 | 478.6 | 487.83 | 22.76 |
| Sep 9 12:14:33 | 231,794 | 405.1 | 572.19 | 22.40 |
| Sep 9 15:14:19 | 305,364 | 531.7 | 574.32 | 28.31 |
| Sep 9 18:35:39 | 460,824 | 1,130.9 | 407.48 | 38.15 |

The next snapshot completed at 15:14:19: 305,364 eligible rows, 531.7 seconds
batch time (574.32 rows/s), 8,986.1 seconds total work (33.98 rows/s), and
10,786.1 seconds since the prior completion (28.31 rows/s). Its copy phase took
3,206.2 seconds, hourly aggregation 1,325.9, bounds 1,728.9, identity 599.6,
and pruning 1,169.5. Those phase totals identify recurring projection cost;
they are not isolated before/after performance measurements.

Ten-minute source queries measured approximately 43–51 eligible arrivals/s
during the first hour, later declining to 34.34/s at 16:49. Counts had a
three-second execution bound. Timed-out backlog/processed-range counts remain
incomplete; rowid distance was not substituted for an eligible-row count.
Queries inspected retained rows in a bounded rowid interval, not a network-wide
arrival census.

Whole-cycle output was below those contemporary arrival samples. That alone
does not prove divergent backlog: the completed batch covered an earlier input
window, while its instantaneous processing rate had substantial margin. The
next natural batch demonstrated the distinction. Between the 15:48 and 15:58
samples, the work checkpoint advanced from 90,151,223 to 90,612,047 while the
source reached 90,648,983. Work lag contracted from 472,059 to 36,936 rowids
despite 25,701 eligible arrivals. The 460,824-rowid advance was not yet an exact
eligible-row count; its bounded count query timed out. Published state remained
at 90,151,223 while later arrivals accumulated during projection maintenance.

At `src/labeler/facts_export.py:293`, a batch SELECT reads up to 500,000 eligible
rows. After committing, the loop stops when that SELECT returned fewer than
the limit. Its checkpoint therefore represents the source extent at selection
time, not necessarily the later commit time. Batch catch-up near that boundary
does not establish continuously current publication. At 16:49 the new work
export marker had committed, but the second snapshot was not yet published.

The default 1,800-second sleep follows the entire serial export. The default
3,600-second snapshot interval is shorter than the measured cycle itself, so
each subsequent cycle qualifies for another full projection and snapshot. This
latency mechanism is separate from the repaired source admission/resume cursor.

The following exact publication completed at 18:35:39. It processed 460,824
eligible rows in a 1,130.9-second batch (407.48 rows/s), while complete serial
work took 10,280.0 seconds (44.83 rows/s). Completion-to-completion spacing was
12,080.168 seconds including the configured 1,800-second post-cycle sleep, or
38.15 rows/s. Phase times were pruning 1,644.0, identity 544.9, hourly 1,328.5,
bounds 1,862.5, and snapshot copy 3,226.3 seconds. The published selection
watermark was already 10,280.6 seconds old when the atomic publication completed.
At the 19:00 sample the source-to-published rowid distance was 379,006; that is
not an eligible-row or loss count.

This closes the second-publication observation but not freshness. The batch again
had substantial instantaneous catch-up capacity, and the separate stream-pressure
episode drained without known loss. Whole publication cadence, including the
required sleep, remained slower than work-only throughput and emitted an artifact
nearly three hours behind its selection point. Retained-row arrival samples later
became discontinuous (including one interval with only dozens of eligible rows)
and were not promoted into a claim that upstream traffic collapsed or capacity
passed; processing mix, deduplication and retention effects were not established.

## A measured pressure-to-recovery interval

A later interval made the distinction between processing throughput and fresh
coverage concrete. At the 17:15:37 capture, the queue held 5,000 admitted
envelopes and one pending envelope; all 5,001 unresolved attempts were accounted.
Stream lag was 509.3 seconds. At 18:23:16, only four admitted envelopes remained,
pending admission/retry were clear, and stream lag was zero. Reported drop,
rollback-loss, and batch-failure counters remained zero.

The captures were 4,059.114764 seconds apart. Their producer facts were stamped
17:15:33.759586 and 18:23:13.959919, 4,060.200333 seconds apart; counter rates use
that latter denominator. Received attempts increased by 487,952 (120.1793/s),
admitted attempts by 487,953 (120.1795/s), and completed attempts by 492,949
(121.4100/s). The 4,997 excess completions over newly received attempts exactly
match reduction of unresolved work: 4,996 already-admitted envelopes plus the
one pending envelope. There were 25,345 additional replay duplicates, 1,751
queue waits, and 51 reconnects. These are envelope-attempt accounting rates,
not unique upstream arrivals or eligible claim-history processing rates.

The durable cursor advanced 4,652.590095 seconds of source time across that
interval. This measures temporal catch-up, not a count of source events. The
final stream concern remained DEGRADED because two recalibration windows were
still required; zero current lag did not silently become an all-clear. Facts
publication was still pending separately.

A single function-only sample during pressure placed the consumer in event
insertion and the exporter in deletion of prior fingerprint bounds. A subsequent
30-second aggregate resource observation found the data device approximately
100% busy, with about 50.34 MB/s reads and 8.65 MB/s writes almost entirely
attributable to Driftwatch's cgroup. CPU user+system was about 15.44%, I/O wait
56.80%, and substantial available memory remained. These observations favor
storage pressure over CPU exhaustion, but do not establish a provider quota or
prove which SQL statement caused each I/O. No service was restarted to recover.

## Snapshot privacy negative qualification

A tiny synthetic experiment used the exact release runtime, SQLite 3.46.1,
and its existing table/prune functions. SQLite backup plus DELETE-journal
conversion preserved logical rows and schema, but also retained deleted
sentinel bytes when deletion had previously occurred with `secure_delete=OFF`.
Turning it ON afterward did not retroactively purge those bytes. `VACUUM INTO`
removed the sentinel in every case. The runtime default was ON; that setting
alone does not establish the history of an imported working database.

An unqualified backup replacement was therefore rejected before allocating a
full-data experiment. The existing README and SQLite production reference
specify compacted, atomically replaced snapshots. Publishing an unpurged current
copy separately from compaction is not established by that contract. See
[SQLite's documented VACUUM/backup distinction](https://www.sqlite.org/lang_vacuum.html)
and [secure_delete behavior](https://www.sqlite.org/pragma.html#pragma_secure_delete).

## Isolated recovery and cache experiment boundary

A separate full restored dataset passed six database checks, startup, restart,
cursor continuity, and the nine-finding demonstration before being offered as
read-only experimental input. That recovery result does not establish production
capacity. The restored working sidecar is bound to SHA-256
`8e4170636b5e58a053a967a0aa71708e56450850e6232c5a2da1c1d627c9e3ba`.

A bounded cache experiment uses the sealed restored dataset. It
keeps VACUUM/compaction, schema, retention, and atomic-publication semantics
unchanged. Planned cases are the existing 2,000-KiB cache default and bounded
128/256/512-MiB settings only as evidence warrants. Each case uses a sealed
read-only source, retains its output, checks full output integrity and all-table
semantic fingerprints, records peak RSS, and checks the full source hash/stat
before and after. Source-cache eviction is advisory and recorded; failure makes
timing comparison incomplete. Host/storage differ from production, so even a
successful result would be directional evidence requiring deployment review.

The isolated default-cache case completed at 17:56:54: VACUUM took 712.419
seconds with peak RSS 29,336 KiB. Full output integrity, all-table semantic
fingerprints, schema/journal/purge checks, and unchanged source hash/stat passed.
Its full output SHA-256 was
`0d8fd2fb9e08c8b888e5f0d658ce330b704ba63e590496818e37a83cebd35a19`.
The 128-MiB comparison passed the same gates. It produced the exact same
15,804,731,392-byte output digest, while VACUUM took 680.590 seconds and peak
RSS was 292,512 KiB. Against the baseline that is a 31.829-second (4.468%)
copy-only improvement for 263,176 KiB more peak RSS. It is too small to close
the measured end-to-end publication deficit and does not qualify relief of the
live bounds/prune pressure. No production cache setting was selected. The
256/512-MiB cases were not run because the first fully validated candidate gave
no evidence-driven path to the required margin; no acceptance component was
excluded to claim success.

No runtime cache setting, retention policy, admission/checkpoint invariant,
production database, or existing tag was changed by this investigation. The
smallest semantics-preserving next decision is measured data-volume performance
tier/cost, or an explicit review of projection output and freshness scope. Any
future cache change still requires full-export qualification under an explicit
memory limit.
