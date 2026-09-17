#!/usr/bin/env bash
# ===========================================================================
# verify_backup.sh - prove a base backup is restorable WITHOUT touching prod
# Part of the PostgreSQL Backup & PITR Restore-Drill Kit
#
# A backup you have only listed with `ls -l` is a hypothesis. This script runs
# two independent checks and refuses to call the backup good unless they pass:
#
#   CHECK 1 - INTEGRITY  (pg_verifybackup)
#     Re-hashes every file in the backup against the checksums recorded in
#     backup_manifest at the moment the backup was taken, and checks the
#     structural sanity of the WAL files it contains. This catches silent bit
#     rot, a truncated transfer, and a file that changed after the backup.
#     Requires PostgreSQL 13+ (manifest) and 13+ for the pg_verifybackup tool.
#     If either is missing we say so and fall through to check 2 - we never
#     report "verified" on the strength of a check that did not run.
#
#   CHECK 2 - RESTORABILITY  (restore into a throwaway cluster and query it)
#     Copies the backup to a scratch directory, starts a REAL postgres on it as
#     a hot standby on a free port, and runs queries against it. This is the
#     only check that proves the bytes on disk can actually become a running
#     database. A backup can be bit-perfect and still be unrestorable (missing
#     tablespace mounts, a corrupted control file, wrong ownership).
#
# SAFETY - how this script avoids becoming the incident
#   * It NEVER writes to the backup directory. It copies first (cp -a).
#   * It NEVER starts on the production port: a free port is chosen and passed
#     on the command line, which overrides postgresql.conf.
#   * archive_mode=off is forced, so the throwaway cluster cannot push its own
#     WAL into your real archive and corrupt it.
#   * No primary_conninfo is set, so it cannot stream from production.
#   * standby.signal is created, so the cluster stays in recovery and is
#     READ-ONLY. It cannot be promoted by accident.
#   * The scratch cluster is stopped and deleted at the end unless --keep.
#
# Usage:
#   ./verify_backup.sh /var/backups/pgdrill/base/20260917T042500Z_nightly
#   ./verify_backup.sh --no-start --json /path/to/backup
#   ./verify_backup.sh --archive-dir /var/backups/pgdrill/wal /path/to/backup
# ===========================================================================

set -euo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SELF_DIR}/../lib/common.sh"

DO_START=1
KEEP=0
JSON=0
ARCHIVE_DIR="${ARCHIVE_DIR:-}"
PORT=""
WORKDIR=""
TIMEOUT=120
MAX_TABLES=10
BACKUP_DIR=""

usage() {
    cat >&2 <<'USAGE'
Usage: verify_backup.sh [OPTIONS] BACKUP_DIR

  BACKUP_DIR             the base backup directory (the one with PG_VERSION)

Options:
  --no-start             manifest/integrity checks only; do not start a cluster
  --archive-dir DIR      WAL archive. Needed when the backup is not
                         self-contained (it was taken without -X stream) so the
                         scratch cluster can replay to a consistent state.
  --port N               port for the scratch cluster (default: a free one)
  --workdir DIR          where to put the scratch cluster (default: mktemp -d)
  --keep                 leave the scratch cluster on disk for inspection
  --timeout N            seconds to wait for the scratch cluster (default 120)
  --max-tables N         how many user tables to actually count rows in (default 10)
  --json                 also print a machine-readable result block
  -h, --help             this help

Exit status is 0 only if every check that RAN passed. A check that could not
run is reported as SKIPPED, never as PASS.
USAGE
}

while [ $# -gt 0 ]; do
    case "$1" in
        --no-start) DO_START=0; shift ;;
        --keep) KEEP=1; shift ;;
        --json) JSON=1; shift ;;
        --archive-dir) ARCHIVE_DIR="$2"; shift 2 ;;
        --port) PORT="$2"; shift 2 ;;
        --workdir) WORKDIR="$2"; shift 2 ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        --max-tables) MAX_TABLES="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        -*) die "verify_backup.sh: unknown option '$1' (try --help)" ;;
        *) BACKUP_DIR="$1"; shift ;;
    esac
done

[ -n "$BACKUP_DIR" ] || { usage; exit 2; }
[ -d "$BACKUP_DIR" ] || die "verify_backup.sh: $BACKUP_DIR is not a directory"

PGBIN_DIR="$(pgbin)" || die "could not find the PostgreSQL binaries; set PGBIN"
export PATH="$PGBIN_DIR:$PATH"

BACKUP_DIR="$(cd "$BACKUP_DIR" && pwd)"
BACKUP_NAME="$(basename "$BACKUP_DIR")"

RC_INTEGRITY="SKIPPED"
RC_RESTORE="SKIPPED"
RC_QUERY="SKIPPED"
FAILURES=()

# ===========================================================================
# CHECK 0 - does this even look like a base backup?
# ===========================================================================
log "verifying base backup: $BACKUP_DIR"
[ -f "$BACKUP_DIR/PG_VERSION" ] || die "$BACKUP_DIR has no PG_VERSION - this is not a cluster directory (did you point at a .tar or a tar-format backup?)"
BK_PGVER="$(cat "$BACKUP_DIR/PG_VERSION" 2>/dev/null | tr -d '[:space:]')"
log "  backup PG_VERSION : $BK_PGVER"
[ -f "$BACKUP_DIR/backup_label" ] || warn "no backup_label found. A backup without backup_label is not a valid base backup to start from."

LOCAL_MAJOR="$(pg_major_version || echo '?')"
log "  local PG major    : $LOCAL_MAJOR"
if [ "$LOCAL_MAJOR" != "?" ] && [ "$BK_PGVER" != "$LOCAL_MAJOR" ]; then
    FAILURES+=("backup is PG $BK_PGVER but the local binaries are PG $LOCAL_MAJOR; you cannot start this cluster with these binaries")
    err "PG major version mismatch: backup=$BK_PGVER local=$LOCAL_MAJOR"
fi

# ===========================================================================
# CHECK 1 - INTEGRITY: pg_verifybackup against the manifest
# ===========================================================================
MANIFEST="$BACKUP_DIR/backup_manifest"
if [ ! -f "$MANIFEST" ]; then
    warn "no backup_manifest in this backup."
    warn "  Either the server is older than PostgreSQL 13, or the backup was taken"
    warn "  with --no-manifest. Without a manifest there are no recorded checksums,"
    warn "  so integrity can only be checked structurally. Reporting SKIPPED, not PASS."
elif ! command -v pg_verifybackup >/dev/null 2>&1; then
    warn "pg_verifybackup is not installed (it ships with PostgreSQL 13+)."
    warn "  Install the full client/server package, or verify on a host that has it."
else
    log "check 1: pg_verifybackup $(pg_verifybackup --version 2>/dev/null | awk '{print $NF}')"
    VB_LOG="$(mktemp)"
    set +e
    # --ignore=.pgdrill-meta: basebackup.sh writes that metadata file into the
    # backup AFTER pg_basebackup has finished, so it is deliberately not in
    # backup_manifest, and pg_verifybackup reports a file that is present on
    # disk but not in the manifest as an error. Without the --ignore every
    # backup this kit produces would fail its own integrity check - a false
    # alarm that trains people to ignore the check, which is worse than not
    # having it. Verified against PostgreSQL 17.11: `-i` tolerates a path that
    # is absent from the manifest, and a backup with no metadata file still
    # verifies clean.
    pg_verifybackup -P -i .pgdrill-meta "$BACKUP_DIR" > "$VB_LOG" 2>&1
    VB_RC=$?
    set -e
    # A --keep-forensics copy of the output is useful in an incident.
    sed 's/^/  pg_verifybackup: /' "$VB_LOG" >&2 || true
    if [ "$VB_RC" -eq 0 ]; then
        RC_INTEGRITY="PASS"
        log "check 1 PASS: every file matches its recorded checksum and the WAL parses"
    else
        RC_INTEGRITY="FAIL"
        FAILURES+=("pg_verifybackup reported a checksum or WAL-parse failure (exit $VB_RC); see output above")
        err "check 1 FAIL: the backup does not match its manifest."
        err "  Most common causes: bit rot on the storage, a truncated transfer, and"
        err "  a file that was edited after the backup. Do NOT rely on this backup."
    fi
    rm -f "$VB_LOG"
fi

# ===========================================================================
# CHECK 2 - RESTORABILITY: restore into a throwaway cluster
# ===========================================================================
SCRATCH=""
restore_cleanup() {
    if [ -n "$SCRATCH" ] && [ -d "$SCRATCH" ]; then
        if [ "$KEEP" = "1" ]; then
            warn "--keep: leaving the scratch cluster at $SCRATCH"
        else
            rm -rf -- "$SCRATCH"
        fi
    fi
}
trap restore_cleanup EXIT INT TERM

if [ "$DO_START" != "1" ]; then
    log "check 2 SKIPPED (--no-start)"
else
    [ "$RC_INTEGRITY" = "FAIL" ] && warn "integrity check failed; still attempting the restore check so you can see how it fails"

    if [ -z "$WORKDIR" ]; then
        SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/pgdrill-verify.XXXXXX")"
    else
        mkdir -p "$WORKDIR"
        SCRATCH="${WORKDIR}/pgdrill-verify.$$"
        mkdir -p "$SCRATCH"
    fi
    R_PGDATA="${SCRATCH}/data"
    R_LOG="${SCRATCH}/startup.log"
    R_SOCK="${SCRATCH}/sock"
    mkdir -p "$R_SOCK"

    if [ -z "$PORT" ]; then
        PORT="$(free_port 55200)" || die "could not find a free TCP port"
    fi
    log "check 2: restoring into $R_PGDATA on port $PORT"

    T_COPY_0="$(now_epoch)"
    # cp -a preserves ownership/timestamps. We NEVER recover inside the backup
    # itself: recovery writes to the cluster, and a "verify" that modifies the
    # artifact it is verifying destroys the evidence.
    if ! cp -a -- "$BACKUP_DIR" "$R_PGDATA" 2>"${SCRATCH}/cp.err"; then
        RC_RESTORE="FAIL"
        FAILURES+=("could not copy the backup to a scratch directory: $(head -1 "${SCRATCH}/cp.err" 2>/dev/null)")
        err "check 2 FAIL: copy failed"
    else
        T_COPY_1="$(now_epoch)"
        pg_owner_give "$SCRATCH"
        secure_pgdata "$R_PGDATA"

        # A standby starts read-only and stays in recovery: exactly what we want
        # for a verification we cannot afford to have go wrong.
        touch "$R_PGDATA/standby.signal"

        {
            echo "# written by verify_backup.sh - throwaway verification cluster"
            echo "archive_mode = off"
            echo "hot_standby = on"
            echo "primary_conninfo = ''"
            if [ -n "$ARCHIVE_DIR" ]; then
                echo "restore_command = 'cp ${ARCHIVE_DIR}/%f %p'"
            fi
        } > "$R_PGDATA/postgresql.auto.conf"
        chmod 600 "$R_PGDATA/postgresql.auto.conf" 2>/dev/null || true

        OPTS="-p $PORT -c listen_addresses=127.0.0.1 -c unix_socket_directories=$R_SOCK -c archive_mode=off -c hot_standby=on"
        # These two are here because SysV shared memory is unavailable in some
        # container/emulated environments. Harmless on a normal host.
        OPTS="$OPTS -c shared_memory_type=${PGDR_SHARED_MEMORY_TYPE:-mmap} -c dynamic_shared_memory_type=${PGDR_DYNAMIC_SHARED_MEMORY_TYPE:-mmap}"

        T_START_0="$(now_epoch)"
        set +e
        # shellcheck disable=SC2086
        "$PGBIN_DIR/pg_ctl" -D "$R_PGDATA" -l "$R_LOG" -o "$OPTS" -w -t "$TIMEOUT" start >&2
        START_RC=$?
        set -e
        T_START_1="$(now_epoch)"

        if [ "$START_RC" -ne 0 ]; then
            RC_RESTORE="FAIL"
            FAILURES+=("the restored cluster would not start (pg_ctl exit $START_RC)")
            err "check 2 FAIL: the restored cluster did not start. Last log lines:"
            tail -40 "$R_LOG" 2>/dev/null | sed 's/^/  | /' >&2 || true
        else
            RC_RESTORE="PASS"
            log "check 2: restored cluster started in $(fmt_dur "$(fsub "$T_START_1" "$T_START_0")") (copy took $(fmt_dur "$(fsub "$T_COPY_1" "$T_COPY_0")"))"

            # ---- CHECK 3: can we actually read data out of it? ---------------
            Q() {
                "$PGBIN_DIR/psql" -X -q -A -t -h 127.0.0.1 -p "$PORT" -U "${PGUSER:-postgres}" \
                    -d postgres -c "$1" 2>&1
            }
            log "check 3: sanity queries"
            IN_RECOVERY="$(Q 'SELECT pg_is_in_recovery()' || true)"
            REPLAY_LSN="$(Q 'SELECT COALESCE(pg_last_wal_replay_lsn()::text,$$none$$)' || true)"
            REPLAY_TS="$(Q "SELECT COALESCE(pg_last_xact_replay_timestamp()::text,'none')" || true)"
            N_DB="$(Q 'SELECT count(*) FROM pg_database' || true)"
            N_REL="$(Q 'SELECT count(*) FROM pg_class' || true)"
            log "  pg_is_in_recovery         : $IN_RECOVERY"
            log "  last replayed WAL LSN     : $REPLAY_LSN"
            log "  last replayed xact time   : $REPLAY_TS"
            log "  databases / relations     : $N_DB / $N_REL"

            if [ "$IN_RECOVERY" != "t" ]; then
                RC_QUERY="FAIL"
                FAILURES+=("the scratch cluster did not stay in recovery - unexpected for a standby")
            elif [ -z "$N_REL" ] || [ "$N_REL" -lt 100 ] 2>/dev/null; then
                RC_QUERY="FAIL"
                FAILURES+=("pg_class has only '$N_REL' rows; the restored catalog looks wrong")
            else
                RC_QUERY="PASS"
                # Count rows in real user tables. n_live_tup is NOT usable here:
                # statistics are not collected during recovery, so it reads 0.
                TABLES="$(Q "SELECT n.nspname||'.'||c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='r' AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast') ORDER BY pg_total_relation_size(c.oid) DESC LIMIT $MAX_TABLES" | grep -v '^$' || true)"
                if [ -z "$TABLES" ]; then
                    log "  (no user tables to count - empty cluster, which is itself informative)"
                else
                    log "  user table row counts:"
                    while IFS= read -r t; do
                        [ -n "$t" ] || continue
                        cnt="$("$PGBIN_DIR/psql" -X -q -A -t -h 127.0.0.1 -p "$PORT" -U "${PGUSER:-postgres}" -d postgres -c "SELECT count(*) FROM \"${t%%.*}\".\"${t##*.}\"" 2>&1 || echo ERR)"
                        if [ "$cnt" = "ERR" ] || ! printf '%s' "$cnt" | grep -qE '^[0-9]+$'; then
                            RC_QUERY="FAIL"
                            FAILURES+=("cannot count rows in $t: $cnt")
                            log "    $t : ERROR ($cnt)"
                        else
                            log "    $t : $cnt rows"
                        fi
                    done <<< "$TABLES"
                fi
            fi
        fi

        # ---- teardown --------------------------------------------------------
        if [ "$START_RC" -eq 0 ]; then
            set +e
            "$PGBIN_DIR/pg_ctl" -D "$R_PGDATA" -m immediate -w -t 30 stop >&2
            set -e
            log "check 2: scratch cluster stopped"
        fi
    fi
fi

# ===========================================================================
# VERDICT
# ===========================================================================
echo >&2
log "=================== verification result ==================="
log "  backup            : $BACKUP_NAME"
log "  integrity         : $RC_INTEGRITY"
log "  restore+start     : $RC_RESTORE"
log "  query restored db : $RC_QUERY"

VERDICT="PASS"
if [ "${#FAILURES[@]}" -gt 0 ]; then VERDICT="FAIL"; fi
# A skipped integrity check does not fail the run, but it does mean we cannot
# claim the backup is verified. Be explicit about that rather than quietly
# returning success.
if [ "$RC_INTEGRITY" = "SKIPPED" ] && [ "$VERDICT" = "PASS" ]; then
    VERDICT="PASS-WITH-CAVEAT"
fi

if [ "$JSON" = "1" ]; then
    printf 'PGDR_VERIFY_JSON {"backup":"%s","integrity":"%s","restore":"%s","query":"%s","verdict":"%s","failures":%d}\n' \
        "$BACKUP_NAME" "$RC_INTEGRITY" "$RC_RESTORE" "$RC_QUERY" "$VERDICT" "${#FAILURES[@]}" >&2
fi

if [ "$VERDICT" = "FAIL" ]; then
    err "VERDICT: FAIL"
    for f in "${FAILURES[@]}"; do err "  - $f"; done
    exit 1
fi

log "VERDICT: $VERDICT"
if [ "$VERDICT" = "PASS-WITH-CAVEAT" ]; then
    warn "the integrity check could not run, so this backup is proven RESTORABLE but"
    warn "not proven UNCHANGED. Enable manifests (PostgreSQL 13+) to close that gap."
fi
log "=========================================================="
exit 0
