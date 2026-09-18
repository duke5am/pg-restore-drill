# pg-restore-drill

Prove your PostgreSQL backups can actually restore — including **point-in-time
recovery to a chosen moment** — and get a measured RPO/RTO report.

A backup you have never restored is a hypothesis. This runs the whole thing end
to end, unattended, and tells you whether it worked:

```bash
./drill/drill.sh
```

```
Verification - 13 checks, 13 passed, 0 failed, 0 skipped
verdict                  : PASS
MEASURED RPO                 : 2.241 s
MEASURED RTO (loss -> first query served) : 2.164 s
```

## What it actually does

1. `initdb` a throwaway cluster and enable WAL archiving — using this kit's own
   `wal_archive.sh` as the `archive_command`, so the atomicity guard below is the
   thing being exercised.
2. Take a base backup with `basebackup.sh` and check the artifact with
   `pg_verifybackup`.
3. Write **known data at known times** — markers and rows in batches with pauses,
   so every batch has a distinct recorded commit time.
4. Record a recovery target, then write **more** rows after it.
5. Destroy the cluster (stop it, move the data directory aside).
6. Restore the base backup, replay WAL to the target.
7. **Verify**: rows committed before the target must be present intact; rows
   committed after must be absent. Row counts **and a checksum** are compared, and
   a mismatch prints exactly what differs and **exits non-zero**.

Step 7 is the product. Without it you have a script that reports success whether
or not the restore worked.

## The test of the test

```bash
./drill/drill.sh --negative-control
```

This recovers to a target that is deliberately too early, and **exits 0 only if
the mismatch was detected**:

```
verdict                  : FAIL
Verification - 13 checks, 8 passed, 5 failed, 0 skipped
  orders_before_checksum       FAIL   5ee3ef4bffd9028dcbd965   30bcd4c12762c0845768181a
  orders_before_rows_missing   FAIL   0                          800
RESULT: FAIL
```

A drill that always passes is worthless, so this is the check that says the drill
is real.

## The RPO/RTO report

`drill/report.py` turns the run into a report — text or `--json`:

| | |
|---|---|
| **Measured RPO** | recovery target minus the newest surviving commit — how much committed data the last recoverable point sits behind |
| **Measured RTO** | wall-clock from simulated loss to a queryable cluster, with a breakdown of copy vs start+replay |
| WAL replay | the segment range actually replayed |
| verdict | PASS / FAIL / **NOT MEASURED** |

It reports **NOT MEASURED** rather than a confident number when the log is
truncated, empty or missing. A number built from nothing is worse than no number.

**Scope, stated in every report:** this measures correctness and wall-clock time
on *this* machine and data volume. It is not a production RTO estimate — offsite
fetch, real data volume, detection time and decision time are all excluded.

## Also included

- `backup/basebackup.sh` — base backups with sensible flags, retention that never
  deletes the newest, and checksum verification where available.
- `backup/logical_backup.sh` — custom-format `pg_dump` for single-object restore.
- `backup/wal_archive.sh` — a **safe** `archive_command`: it refuses to archive a
  partial WAL segment and is idempotent. The naive `cp %p /archive/%f` can archive
  a half-written segment and silently break recovery.
- `backup/verify_backup.sh` — restore into a throwaway cluster and query it, so
  you learn the backup is usable *before* you need it.
- `monitoring/check_archive.sh` — fails when the archive chain is unhealthy:
  `pg_stat_archiver` failures, a gap in the sequence, a stuck `last_archived_wal`,
  or an `archive_command` that can never report failure.

## Requirements

PostgreSQL 17 and Linux, plus a non-root user for the cluster (the scripts drop
privileges automatically when run as root).

**In containers without SysV shared memory** (some Android/PRoot sandboxes),
`initdb` fails with `could not create shared memory segment: Input/output error`.
Pass a preload shim and the mmap settings:

```bash
PGDR_LD_PRELOAD=/path/to/sysvshm_shim.so ./drill/drill.sh \
  --pg-option '-c shared_memory_type=mmap -c dynamic_shared_memory_type=mmap'
```

A normal Linux host needs neither.

## The full pack

The paid kit adds the restore runbook for 3am, the backup-strategy and RPO/RTO
reasoning, the common-failures catalogue, and the full documentation set.

→ More developer tooling like this: **[duke5am.gumroad.com](https://duke5am.gumroad.com)** <!-- GUMROAD-LINK -->
