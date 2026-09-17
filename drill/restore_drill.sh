#!/usr/bin/env bash
# ===========================================================================
# restore_drill.sh - take a backup, destroy the cluster, recover to a point in
#                    time, and PROVE the data came back
# Part of the PostgreSQL Backup & PITR Restore-Drill Kit
#
# WHAT IT DOES - one end-to-end run, no arguments required
#   1. initdb a throwaway cluster and turn on WAL archiving into a directory,
#      using ../backup/wal_archive.sh as archive_command, so the atomicity guard
#      this kit ships is the thing being exercised.
#   2. Build a small but realistic schema and take a base backup with
#      ../backup/basebackup.sh, then check the artifact with pg_verifybackup.
#   3. Write KNOWN DATA AT KNOWN TIMES: markers and rows in batches with pauses
#      between them, so each batch has a distinct, recorded commit time.
#   4. Record a recovery TARGET: a timestamp taken after the last "before"
#      batch. Force the WAL containing it into the archive and WAIT until the
#      archiver confirms it is there, because a target you cannot replay to is
#      not a target.
#   5. Write MORE rows after the target, and archive those segments as well.
#      This matters: the drill must show that the "after" rows are absent
#      because recovery STOPPED at the target, not because their WAL was
#      missing. A drill that never archives the after-WAL proves nothing.
#   6. SIMULATE LOSS: stop the cluster and move the data directory aside.
#   7. RESTORE: copy the base backup, configure restore_command (atomic copy +
#      rename, non-zero exit when a segment is absent), set recovery_target_time,
#      and start the cluster. Recovery replays WAL from the archive and stops at
#      the target.
#   8. VERIFY BY QUERYING: every row committed before the target must be present
#      AND intact (content checksum), every row committed after it must be
#      ABSENT, and every table's row count must match the number recorded before
#      the loss. On mismatch it prints exactly what differs - which ids are
#      missing, which unexpected rows came back, expected vs actual checksums -
#      and exits non-zero.
#
# The whole point is step 8. A backup kit that has never restored anything, and
# a drill that cannot fail, are both worthless - the second is worse, because it
# manufactures confidence. Run ./drill.sh --negative-control to watch this drill
# recover to a target BEFORE some rows were written and catch itself.
#
# WHAT IT MEASURES, AND WHAT IT ONLY EXERCISES
#   Measured, on your hardware, in this run: that the base backup is readable
#   and its manifest verifies; that the archive holds every segment recovery
#   asks for; that recovery reaches the target; that rows before the target
#   survive intact and rows after it do not; the wall-clock time from simulated
#   loss to a queryable cluster (RTO) broken into copy and replay; and the gap
#   between the requested target and the newest commit that actually survived
#   (measured RPO).
#   NOT measured: offsite fetch time, production data volume, replay distance on
#   your real storage, application cut-over, detection time or decision time. A
#   drill on a few hundred MiB locally is a correctness test and a regression
#   baseline, not a production RTO estimate. See docs/RPO-RTO.md section 7
#   before quoting any number it prints.
#
# SAFETY
#   * Everything it creates lives under one run directory; it refuses to stop,
#     move or delete anything outside its own work root.
#   * It never touches a server it did not start, and picks a free port.
#   * The restored cluster is started with archive_mode = off, so it can never
#     push its own WAL into the archive it is reading from - a mistake that has
#     destroyed real archives.
#
# Usage:
#   ./restore_drill.sh                          # defaults, throwaway, cleans up
#   ./restore_drill.sh --work-dir /srv/pgdrill  # put the scratch data elsewhere
#   ./restore_drill.sh --keep                   # keep everything for inspection
#   ./restore_drill.sh --negative-control       # expect and prove a FAILURE
#   ./restore_drill.sh --target-time '2026-09-17 05:00:00.5+00'
#
# Environment:
#   PGBIN               PostgreSQL bin directory (else resolved automatically)
#   PG_OS_USER          unprivileged user to run as when invoked as root
#                       (default postgres; PostgreSQL refuses to run as root)
#   PGDR_DRILL_ROOT     default for --work-dir (default /var/tmp/pgdrill-drill)
#   PGDR_SERVER_OPTS    extra postgresql.conf settings for BOTH clusters, as a
#                       space-separated list of KEY=VALUE pairs, e.g.
#                       'shared_memory_type=mmap dynamic_shared_memory_type=mmap'
#   PGDR_LD_PRELOAD     exported as LD_PRELOAD for every postgres process. Some
#                       containers (PRoot on Android, for example) have no SysV
#                       shared memory and need a shim library; a normal Linux
#                       host needs nothing here.
#   PGDR_RECOVERY_TARGET_INCLUSIVE   default for --inclusive
#   PGDR_QUIET          1 to suppress the human log, emitting only PGDR_EVENT
# ===========================================================================

set -euo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KIT_DIR="$(cd "${SELF_DIR}/.." && pwd)"
# shellcheck source=../lib/common.sh
. "${KIT_DIR}/lib/common.sh"

ORIG_ARGS=("$@")

# --- defaults ---------------------------------------------------------------
WORK_ROOT="${PGDR_DRILL_ROOT:-/var/tmp/pgdrill-drill}"
DRILL_PORT=""
KEEP=0
NEGATIVE_CONTROL=0
TARGET_TIME=""
INCLUSIVE="${PGDR_RECOVERY_TARGET_INCLUSIVE:-off}"
REPLAY_TIMEOUT=180
START_TIMEOUT=180
ARCHIVE_TIMEOUT_WAIT=180
PRE_ROWS=1200
PRE_BATCHES=3
POST_ROWS=300
CUSTOMERS=250
BATCH_GAP=1.0
PRE_TARGET_GAP=2.0
DO_VERIFYBACKUP=1
NO_FSYNC=0

usage() {
    cat >&2 <<'USAGE'
Usage: restore_drill.sh [OPTIONS]

Every option is optional. With none, the drill runs under /var/tmp/pgdrill-drill
on a free port and deletes everything it created when it finishes.

Where it runs:
  --work-dir DIR         root for this drill's throwaway data (default
                         $PGDR_DRILL_ROOT or /var/tmp/pgdrill-drill)
  --port N               port for both clusters (default: a free port)
  --keep                 keep the run directory, the backup, the archive and the
                         restored cluster running, for inspection
  --pg-option 'KEY=VALUE'  add a postgresql.conf setting to BOTH clusters, e.g.
                         --pg-option shared_memory_type=mmap. Repeatable. Use
                         this for container quirks (some sandboxes have no SysV
                         shared memory) or to test a non-default setting.

The timeline it builds:
  --pre-rows N           rows committed BEFORE the target (default 1200)
  --pre-batches N        how many batches to split them into (default 3); more
                         batches means more distinct commit times
  --post-rows N          rows committed AFTER the target (default 300)
  --customers N          static rows present in the base backup (default 250)
  --batch-gap S          pause between batches, in seconds (default 1.0)
  --pre-target-gap S     pause between the last "before" row and the target
                         timestamp (default 2.0) - this is what makes the
                         measured RPO non-zero and therefore meaningful

The recovery target:
  --target-time TS       an explicit recovery_target_time, e.g.
                         '2026-09-17 05:12:34.500000+00'
  --negative-control     choose the target BEFORE the rest of the "before" rows
                         were written. The drill is then EXPECTED to fail its
                         verification, which is the proof that the verification
                         can fail at all. Exits non-zero on purpose.
  --inclusive on|off     recovery_target_inclusive (default off). With 'on',
                         PostgreSQL stops just AFTER the first transaction at or
                         after the target, so that transaction IS included; with
                         'off', recovery stops just before it. 'off' is the
                         default because the drill's job is to show that
                         post-target rows are absent.

Behaviour:
  --no-verifybackup      skip pg_verifybackup on the base backup
  --no-fsync             initdb --no-sync and fsync=off. Faster, for CI only: it
                         then tests nothing about durability
  --start-timeout S      seconds to wait for a cluster to start (default 180)
  --replay-timeout S     seconds to wait for recovery to finish (default 180)
  --quiet                emit only PGDR_EVENT lines (for report.py)
  -h, --help             this help

Exit status:
  0  every check passed
  1  verification FAILED - one or more checks did not match expectation
     (with --negative-control, 1 is the CORRECT and expected outcome)
  2  bad usage or a missing prerequisite
  3  infrastructure failure: initdb, base backup, archiving or recovery could
     not be completed, so there was nothing to verify
USAGE
}

while [ $# -gt 0 ]; do
    case "$1" in
        --work-dir) WORK_ROOT="$2"; shift 2 ;;
        --port) DRILL_PORT="$2"; shift 2 ;;
        --keep) KEEP=1; shift ;;
        --pg-option) PGDR_SERVER_OPTS="${PGDR_SERVER_OPTS:-} $2"; shift 2 ;;
        --pre-rows) PRE_ROWS="$2"; shift 2 ;;
        --pre-batches) PRE_BATCHES="$2"; shift 2 ;;
        --post-rows) POST_ROWS="$2"; shift 2 ;;
        --customers) CUSTOMERS="$2"; shift 2 ;;
        --batch-gap) BATCH_GAP="$2"; shift 2 ;;
        --pre-target-gap) PRE_TARGET_GAP="$2"; shift 2 ;;
        --target-time) TARGET_TIME="$2"; shift 2 ;;
        --negative-control) NEGATIVE_CONTROL=1; shift ;;
        --inclusive) INCLUSIVE="$2"; shift 2 ;;
        --no-verifybackup) DO_VERIFYBACKUP=0; shift ;;
        --no-fsync) NO_FSYNC=1; shift ;;
        --start-timeout) START_TIMEOUT="$2"; shift 2 ;;
        --replay-timeout) REPLAY_TIMEOUT="$2"; shift 2 ;;
        --quiet|-q) PGDR_QUIET=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) usage; err "restore_drill.sh: unknown option '$1'"; exit 2 ;;
    esac
done

case "$INCLUSIVE" in on|off) ;; *) die "--inclusive takes on or off (got '$INCLUSIVE')" ;; esac

PGBIN_DIR="$(pgbin)" || die "could not find the PostgreSQL binaries; set PGBIN to the bin directory"
export PATH="${PGBIN_DIR}:${PATH}"
PG_VERSION_MAJOR="$(pg_major_version || echo '?')"

# Connection defaults, matching the rest of the kit. PGUSER in particular must
# be set: the drill runs with `set -u`, and an unset PGUSER would abort it in the
# middle of the base-backup step.
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-postgres}"

# ===========================================================================
# DROP PRIVILEGES
# ===========================================================================
# initdb and postgres both refuse to run as root, so when we are root we re-exec
# the whole script as an unprivileged user. Doing it once, at the top, means
# every later code path runs exactly the way it will on a real database host.
if [ "$(id -u)" = "0" ] && [ "${PGDR_DRILL_REEXEC:-0}" != "1" ]; then
    target_user="${PG_OS_USER:-postgres}"
    id -u "$target_user" >/dev/null 2>&1 || \
        die "running as root and PG_OS_USER=$target_user does not exist. Set PG_OS_USER to a user that can own a PostgreSQL cluster."

    mkdir -p "$WORK_ROOT" || die "cannot create --work-dir $WORK_ROOT"
    pg_owner_give "$WORK_ROOT"
    chmod 755 "$WORK_ROOT" 2>/dev/null || true
    WORK_ROOT="$(cd "$WORK_ROOT" && pwd)"

    if ! su -s /bin/bash -c "test -r $(printf '%q' "${KIT_DIR}/lib/common.sh")" "$target_user" 2>/dev/null; then
        die "$target_user cannot read ${KIT_DIR}/lib/common.sh. Make the path traversable (chmod o+x on each parent directory), or run this script as that user."
    fi

    # Rebuild the argument list, converting --work-dir to an absolute
    # PGDR_DRILL_ROOT so the relative-path meaning cannot change across the
    # privilege drop.
    INNER_ARGS=()
    _skip=0
    for _a in "${ORIG_ARGS[@]:-}"; do
        if [ "$_skip" = "1" ]; then _skip=0; continue; fi
        case "$_a" in
            --work-dir) _skip=1; continue ;;
            --work-dir=*) continue ;;
            # --pg-option is carried across the privilege drop in
            # PGDR_SERVER_OPTS instead, so it is not replayed as an argument.
            --pg-option) _skip=1; continue ;;
        esac
        INNER_ARGS+=("$_a")
    done

    _inner=""
    [ -n "${PGDR_LD_PRELOAD:-}" ] && _inner="export LD_PRELOAD=$(printf '%q' "$PGDR_LD_PRELOAD"); "
    _inner="${_inner}export PGDR_DRILL_REEXEC=1; "
    _inner="${_inner}export PGDR_DRILL_ROOT=$(printf '%q' "$WORK_ROOT"); "
    for _v in PGBIN PG_OS_USER PGDR_QUIET PGDR_SERVER_OPTS PGDR_RECOVERY_TARGET_INCLUSIVE TMPDIR LANG LC_ALL; do
        eval "_val=\${$_v:-}"
        [ -n "$_val" ] && _inner="${_inner}export ${_v}=$(printf '%q' "$_val"); "
    done
    _inner="${_inner}cd $(printf '%q' "$WORK_ROOT") && "
    _inner="${_inner}$(printf '%q' "${SELF_DIR}/restore_drill.sh")"
    for _a in "${INNER_ARGS[@]:-}"; do
        [ -n "$_a" ] && _inner="${_inner} $(printf '%q' "$_a")"
    done
    log "running as root: re-executing as '$target_user' (PostgreSQL refuses to run as root)"
    exec su -s /bin/bash -c "$_inner" "$target_user"
fi

# ===========================================================================
# PREFLIGHT
# ===========================================================================
for _c in initdb pg_ctl pg_basebackup psql pg_controldata; do
    [ -x "${PGBIN_DIR}/${_c}" ] || die "required command not found: ${PGBIN_DIR}/${_c} (set PGBIN)"
done
need_cmd awk
need_cmd sed

if [ "$PG_VERSION_MAJOR" != "17" ]; then
    warn "this kit is tested against PostgreSQL 17; found ${PG_VERSION_MAJOR}. Continuing, but recovery target semantics, pg_controldata output and pg_waldump output can differ between major versions."
fi

umask 077

# ===========================================================================
# RUN DIRECTORY
# ===========================================================================
STAMP="$(date -u '+%Y%m%dT%H%M%SZ')"
RUN_DIR="${WORK_ROOT}/run-${STAMP}-$$"
mkdir -p "$RUN_DIR" || die "cannot create run directory $RUN_DIR"
WORK_ROOT="$(cd "$WORK_ROOT" && pwd)"
RUN_DIR="$(cd "$RUN_DIR" && pwd)"

SRC_PGDATA="${RUN_DIR}/cluster"
SRC_LOG="${RUN_DIR}/logs/source.log"
LOST_DIR="${RUN_DIR}/lost"
ARCHIVE_DIR="${RUN_DIR}/wal_archive"
BACKUP_ROOT="${RUN_DIR}/backups"
RESTORE_PGDATA="${RUN_DIR}/restored"
RESTORE_LOG="${RUN_DIR}/logs/restored.log"
RESTORE_CMD="${RUN_DIR}/restore_command.sh"
RESTORE_CMD_LOG="${RUN_DIR}/logs/restore_command.log"
LOG_DIR="${RUN_DIR}/logs"
mkdir -p "$LOG_DIR" "$ARCHIVE_DIR" "$BACKUP_ROOT" "$LOST_DIR"

# --- safety net -------------------------------------------------------------
# Every destructive operation goes through these. A drill script that can move
# or delete an arbitrary directory is a bigger risk than the outage it rehearses.
assert_in_run() {
    local p="$1" why="$2"
    case "$p" in
        "${RUN_DIR}"/*) ;;
        *) die "REFUSING to ${why}: '$p' is outside this run's directory (${RUN_DIR})" ;;
    esac
}

safe_stop() {
    local pgdata="$1" logfile="$2"
    [ -d "$pgdata" ] || return 0
    case "$pgdata" in "${RUN_DIR}"/*) ;; *) return 0 ;; esac
    if [ -f "${pgdata}/postmaster.pid" ]; then
        LD_PRELOAD="${PGDR_LD_PRELOAD:-}" "${PGBIN_DIR}/pg_ctl" -D "$pgdata" -m fast -w -t 60 stop \
            >"$logfile" 2>&1 || warn "pg_ctl stop failed for $pgdata (see $logfile)"
    fi
    return 0
}

EXIT_RC=0
cleanup_run() {
    EXIT_RC=$?
    if [ "$KEEP" = "1" ]; then
        return 0
    fi
    safe_stop "$RESTORE_PGDATA" "${LOG_DIR}/stop-restored.log" || true
    safe_stop "$SRC_PGDATA" "${LOG_DIR}/stop-source.log" || true
    # Only ever remove a directory this script created, and only after checking
    # that its name matches the pattern we generate.
    case "$RUN_DIR" in
        "${WORK_ROOT}"/run-*) [ "$RUN_DIR" != "$WORK_ROOT" ] && rm -rf -- "$RUN_DIR" ;;
        *) warn "not removing $RUN_DIR: it does not look like a drill run directory" ;;
    esac
    return 0
}
trap cleanup_run EXIT

STARTED_EPOCH="$(now_epoch)"
STEP_START="$STARTED_EPOCH"
STEP_NAME=""
step_begin() { STEP_START="$(now_epoch)"; STEP_NAME="$1"; log "=== ${1}"; }
step_end() {
    event "EVENT=step" "STEP=${STEP_NAME}" "STATUS=ok" "DURATION_S=$(fsub "$(now_epoch)" "$STEP_START")"
}
fail_step() {
    event "EVENT=step" "STEP=${STEP_NAME}" "STATUS=failed" \
          "DURATION_S=$(fsub "$(now_epoch)" "$STEP_START")" \
          "DETAIL=\"$(printf '%s' "$1" | tr '"' "'")\""
}

MODE="normal"
[ "$NEGATIVE_CONTROL" = "1" ] && MODE="negative-control"

event "EVENT=drill" "DRILL_VERSION=1" "TOOL=restore_drill.sh" "MODE=${MODE}" \
      "STARTED_AT=\"$(now_iso)\"" "STARTED_EPOCH=${STARTED_EPOCH}" \
      "RUN_DIR=${RUN_DIR}" "WORK_ROOT=${WORK_ROOT}" "PG_MAJOR=${PG_VERSION_MAJOR}" \
      "PRE_ROWS=${PRE_ROWS}" "POST_ROWS=${POST_ROWS}" "CUSTOMERS=${CUSTOMERS}" \
      "RECOVERY_TARGET_INCLUSIVE=${INCLUSIVE}"

log "pgdrill restore drill"
log "  run directory : $RUN_DIR"
log "  mode          : $MODE"
log "  binaries      : $PGBIN_DIR (PostgreSQL $PG_VERSION_MAJOR)"
log "  running as    : $(id -un)"

# ---------------------------------------------------------------------------
# Disk space. A restore needs the backup plus the restored copy plus room for
# pg_wal. Running out at 90% is a much worse failure than not starting.
# ---------------------------------------------------------------------------
step_begin preflight
AVAIL_KB="$(df -Pk "$RUN_DIR" 2>/dev/null | awk 'NR==2 {print $4}' || true)"
AVAIL_KB="${AVAIL_KB:-0}"
NEED_KB=$(( 400 * 1024 ))
if [ "$AVAIL_KB" -lt "$NEED_KB" ]; then
    fail_step "only ${AVAIL_KB}KiB free under $WORK_ROOT, want at least ${NEED_KB}KiB"
    die "not enough free disk space under $WORK_ROOT (need roughly 400MiB)"
fi
log "  free space    : $(fmt_bytes $(( AVAIL_KB * 1024 )))"
step_end

# ---------------------------------------------------------------------------
# Port. Never 5432 or 5433: the drill must not be able to reach a production
# server by accident, and a deliberately non-default port surfaces port
# confusion in a drill rather than during an incident.
# ---------------------------------------------------------------------------
if [ -z "$DRILL_PORT" ]; then
    DRILL_PORT="$(free_port 55000)" || die "no free TCP port found in 55000-55200"
fi
case "$DRILL_PORT" in
    5432|5433) die "refusing to use port $DRILL_PORT - that is a conventional PostgreSQL port and could be a real server" ;;
esac
log "  cluster port  : $DRILL_PORT"

# ===========================================================================
# 1. INITDB
# ===========================================================================
step_begin initdb
INITDB_OPTS=( -D "$SRC_PGDATA" -A trust -U "$PGUSER" )
[ "$NO_FSYNC" = "1" ] && INITDB_OPTS+=( --no-sync )
if ! LD_PRELOAD="${PGDR_LD_PRELOAD:-}" "${PGBIN_DIR}/initdb" "${INITDB_OPTS[@]}" >"${LOG_DIR}/initdb.log" 2>&1; then
    fail_step "initdb failed; see ${LOG_DIR}/initdb.log"
    tail -20 "${LOG_DIR}/initdb.log" >&2 || true
    exit 3
fi
secure_pgdata "$SRC_PGDATA"
log "  initdb ok     : $SRC_PGDATA"
step_end

# ===========================================================================
# 2. CONFIGURE: WAL archiving through the kit's own wal_archive.sh
# ===========================================================================
step_begin configure
WAL_ARCHIVER="${KIT_DIR}/backup/wal_archive.sh"
[ -x "$WAL_ARCHIVER" ] || die "missing or not executable: $WAL_ARCHIVER"

# A separate restore_command script is generated rather than inlined, because
# inlining means quoting a shell command inside a postgresql.conf string - and
# the classic failure is quoting that survives review and dies at 3am. This one
# is readable, testable, and it LOGS every segment it serves, which is how the
# drill knows exactly what recovery replayed instead of inferring it.
{
    printf '#!/bin/sh\n'
    printf '# Generated by restore_drill.sh - the restore_command for the recovered cluster.\n'
    printf '#\n'
    printf '#   $1 = %%f   the segment file name recovery wants\n'
    printf '#   $2 = %%p   where PostgreSQL wants it (relative to PGDATA during recovery)\n'
    printf '#\n'
    printf '# Contract, and it is not optional:\n'
    printf '#   exit 0  the segment was put in place\n'
    printf '#   exit>0  the segment is NOT available, so recovery has run off the end of\n'
    printf '#           the archive and must stop. A restore_command that returns 0 for a\n'
    printf '#           missing segment tells PostgreSQL the archive is infinite; recovery\n'
    printf '#           then fails later with a confusing "invalid record length" or spins\n'
    printf '#           on the same segment forever.\n'
    printf '#\n'
    printf '# Writes to a temporary name in the same directory and renames, so a killed\n'
    printf '# restore cannot leave a zero-length file under a real segment name.\n'
    printf 'ARCHIVE_DIR=%s\n' "'${ARCHIVE_DIR}'"
    printf 'LOG=%s\n' "'${RESTORE_CMD_LOG}'"
    printf 'f="$1"\n'
    printf 'p="$2"\n'
    printf 'src="${ARCHIVE_DIR}/${f}"\n'
    printf 'if [ ! -f "$src" ]; then\n'
    printf '    printf "MISS %%s %%s\\n" "$f" "$(date -u +%%Y-%%m-%%dT%%H:%%M:%%SZ)" >> "$LOG" 2>/dev/null\n'
    printf '    exit 1\n'
    printf 'fi\n'
    printf 'if [ ! -s "$src" ]; then\n'
    printf '    printf "EMPTY %%s\\n" "$f" >> "$LOG" 2>/dev/null\n'
    printf '    exit 1\n'
    printf 'fi\n'
    printf 'cp "$src" "${p}.tmp" 2>/dev/null || exit 1\n'
    printf 'if ! mv "${p}.tmp" "$p" 2>/dev/null; then\n'
    printf '    rm -f "${p}.tmp" 2>/dev/null\n'
    printf '    exit 1\n'
    printf 'fi\n'
    printf 'printf "RESTORED %%s %%s\\n" "$f" "$(date -u +%%Y-%%m-%%dT%%H:%%M:%%SZ)" >> "$LOG" 2>/dev/null\n'
    printf 'exit 0\n'
} > "$RESTORE_CMD"
chmod 700 "$RESTORE_CMD"
: > "$RESTORE_CMD_LOG"

# The archive_command is the canonical `test ! -f ... && cp` idiom delegated to
# the kit's wal_archive.sh, which adds atomic publish and refuses to archive a
# segment whose header disagrees with its own name. ARCHIVE_DIR is set INLINE
# because the postmaster does not inherit a login shell's environment: a wrapper
# that relies on it fails on every segment with a confusing error.
ARCHIVE_COMMAND="ARCHIVE_DIR=${ARCHIVE_DIR} PGDR_QUIET=1 ${WAL_ARCHIVER} %p %f"

cat >> "$SRC_PGDATA/postgresql.conf" <<CONF_EOF

# ---------------------------------------------------------------------------
# pgdrill restore drill - appended by restore_drill.sh. Later settings win in
# postgresql.conf, so these override anything above.
# ---------------------------------------------------------------------------
listen_addresses = '127.0.0.1'
port = ${DRILL_PORT}
wal_level = replica
archive_mode = on
archive_timeout = 60
max_wal_senders = 4
max_replication_slots = 4
wal_keep_size = 128MB
archive_command = '${ARCHIVE_COMMAND}'
log_line_prefix = '%m [%p] %q%a '
CONF_EOF
if [ "$NO_FSYNC" = "1" ]; then
    printf 'fsync = off\nsynchronous_commit = off\nfull_page_writes = off\n' >> "$SRC_PGDATA/postgresql.conf"
    warn "--no-fsync: this run does NOT test durability; it is for CI only"
else
    printf 'synchronous_commit = on\n' >> "$SRC_PGDATA/postgresql.conf"
fi
if [ -n "${PGDR_SERVER_OPTS:-}" ]; then
    {
        printf '# extra settings from --pg-option / PGDR_SERVER_OPTS\n'
        for _o in $PGDR_SERVER_OPTS; do
            case "$_o" in
                -c) continue ;;                       # tolerate a stray "-c"
                -c\ *) _o="${_o#-c }" ;;              # tolerate "-c key=value"
            esac
            _k="${_o%%=*}"
            _v="${_o#*=}"
            if [ -z "$_k" ] || [ "$_k" = "$_o" ]; then
                warn "ignoring PGDR_SERVER_OPTS entry '$_o': expected KEY=VALUE"
                continue
            fi
            case "$_v" in
                ''|*[!A-Za-z0-9._-]*) printf "%s = '%s'\n" "$_k" "$(printf '%s' "$_v" | sed "s/'/''/g")" ;;
                *)                    printf '%s = %s\n' "$_k" "$_v" ;;
            esac
        done
    } >> "$SRC_PGDATA/postgresql.conf"
fi

log "  archive dir   : $ARCHIVE_DIR"
log "  archive cmd   : $ARCHIVE_COMMAND"
log "  restore cmd   : $RESTORE_CMD"
step_end

# ===========================================================================
# 3. START THE SOURCE CLUSTER
# ===========================================================================
start_cluster() {
    local pgdata="$1" logfile="$2"
    LD_PRELOAD="${PGDR_LD_PRELOAD:-}" "${PGBIN_DIR}/pg_ctl" -D "$pgdata" \
        -l "$logfile" -w -t "$START_TIMEOUT" start
}

step_begin start-source
if ! start_cluster "$SRC_PGDATA" "$SRC_LOG" >>"${LOG_DIR}/pgctl-source.log" 2>&1; then
    fail_step "the source cluster did not start; see $SRC_LOG"
    tail -30 "$SRC_LOG" >&2 || true
    exit 3
fi

PSQL_BASE=( -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$DRILL_PORT" -U "$PGUSER" -d postgres )
psql_do()  { LD_PRELOAD="${PGDR_LD_PRELOAD:-}" "${PGBIN_DIR}/psql" "${PSQL_BASE[@]}" -c "$1"; }
psql_val() { LD_PRELOAD="${PGDR_LD_PRELOAD:-}" "${PGBIN_DIR}/psql" "${PSQL_BASE[@]}" -A -t -c "$1"; }
# Tolerant variant for the verification phase: a failed query here must be
# REPORTED as a failed check, not abort the script before it can say what broke.
psql_try() { { psql_val "$1" 2>/dev/null || printf 'QUERY_FAILED'; } | tr -d '[:space:]'; }
# Same, but it KEEPS internal whitespace: a timestamptz value read back as text
# must not have its separating space squeezed out, or it will not parse as a
# timestamp when it is fed back into the next query. Learned the hard way: the
# measured-RPO calculation below returned QUERY_FAILED until this existed.
psql_try_raw() { { psql_val "$1" 2>/dev/null || printf 'QUERY_FAILED'; } | head -1; }

SRV_VERSION="$(psql_val 'SELECT version()' | head -1 || true)"
ARCHIVE_MODE_ACTUAL="$(psql_val 'SHOW archive_mode' | tr -d '[:space:]')"
[ "$ARCHIVE_MODE_ACTUAL" = "on" ] || { fail_step "archive_mode is '$ARCHIVE_MODE_ACTUAL', not on"; exit 3; }
log "  server        : $SRV_VERSION"
step_end

# ===========================================================================
# 4. SCHEMA (before the base backup, so the backup really contains it)
# ===========================================================================
step_begin schema
psql_do '
CREATE TABLE drill_customers (
    id     integer PRIMARY KEY,
    name   text NOT NULL,
    region text NOT NULL
);
CREATE TABLE drill_orders (
    id          integer PRIMARY KEY,
    customer_id integer NOT NULL REFERENCES drill_customers(id),
    amount      numeric(10,2) NOT NULL,
    note        text NOT NULL
);
CREATE INDEX drill_orders_customer_idx ON drill_orders (customer_id);
CREATE TABLE drill_marker (
    id          bigserial PRIMARY KEY,
    phase       text NOT NULL,
    note        text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
' >/dev/null

psql_do "INSERT INTO drill_customers (id, name, region)
         SELECT g, 'customer ' || g, 'region-' || (g % 7)
         FROM generate_series(1, ${CUSTOMERS}) g;" >/dev/null
CUSTOMER_ROWS="$(psql_val 'SELECT count(*) FROM drill_customers' | tr -d '[:space:]')"
CUSTOMER_CHECKSUM="$(psql_val "SELECT md5(string_agg(id || '|' || name || '|' || region, E'\n' ORDER BY id)) FROM drill_customers" | tr -d '[:space:]')"
[ "$CUSTOMER_ROWS" = "$CUSTOMERS" ] || die "seeded $CUSTOMER_ROWS customers, expected $CUSTOMERS"
log "  customers     : ${CUSTOMER_ROWS} rows, md5 ${CUSTOMER_CHECKSUM}"
step_end

# ===========================================================================
# 5. BASE BACKUP (through the kit's own basebackup.sh)
# ===========================================================================
step_begin basebackup
BASEBB_LOG="${LOG_DIR}/basebackup.log"
if ! "${KIT_DIR}/backup/basebackup.sh" \
        --host 127.0.0.1 --port "$DRILL_PORT" --user "$PGUSER" \
        --backup-root "$BACKUP_ROOT" --label drill --retries 1 --no-prune \
        >"$BASEBB_LOG" 2>&1; then
    fail_step "basebackup.sh failed; see $BASEBB_LOG"
    tail -30 "$BASEBB_LOG" >&2 || true
    exit 3
fi
BACKUP_DIR="$(sed -n 's/.*PGDR_EVENT .*BACKUP_DIR=\([^ ]*\).*/\1/p' "$BASEBB_LOG" | tail -1)"
if [ -z "$BACKUP_DIR" ] || [ ! -d "$BACKUP_DIR" ]; then
    fail_step "could not determine the backup directory from $BASEBB_LOG"
    exit 3
fi
BACKUP_BYTES="$(dir_bytes "$BACKUP_DIR")"
BACKUP_START_LSN="$(sed -n 's/.*PGDR_EVENT .*START_LSN=\([^ ]*\).*/\1/p' "$BASEBB_LOG" | tail -1)"
BACKUP_END_LSN="$(sed -n 's/.*PGDR_EVENT .*END_LSN=\([^ ]*\).*/\1/p' "$BASEBB_LOG" | tail -1)"
BACKUP_DURATION="$(sed -n 's/.*PGDR_EVENT .*DURATION_S=\([^ ]*\).*/\1/p' "$BASEBB_LOG" | tail -1)"

# The recovery point that matters most: nothing before this can be recovered, no
# matter how much WAL you hold, because WAL replay cannot run backwards.
CONTROLDATA="$(LD_PRELOAD="${PGDR_LD_PRELOAD:-}" "${PGBIN_DIR}/pg_controldata" -D "$BACKUP_DIR" 2>/dev/null || true)"
MIN_RECOVERY_LSN="$(printf '%s\n' "$CONTROLDATA" | sed -n 's/^Minimum recovery ending location:[[:space:]]*//p' | head -1 | tr -d ' ')"
CKPT_LSN="$(printf '%s\n' "$CONTROLDATA" | sed -n 's/^Latest checkpoint location:[[:space:]]*//p' | head -1 | tr -d ' ')"
SYSID="$(printf '%s\n' "$CONTROLDATA" | sed -n 's/^Database system identifier:[[:space:]]*//p' | head -1 | tr -d ' ')"

log "  backup        : $BACKUP_DIR ($(fmt_bytes "$BACKUP_BYTES"))"
log "  LSN range     : ${BACKUP_START_LSN:-?} .. ${BACKUP_END_LSN:-?}"
log "  checkpoint    : ${CKPT_LSN:-?}"
log "  min recov. pt : ${MIN_RECOVERY_LSN:-?}"
step_end

event "EVENT=backup" "KIND=base" "BACKUP_DIR=${BACKUP_DIR}" \
      "SIZE_BYTES=${BACKUP_BYTES}" "START_LSN=${BACKUP_START_LSN:-unknown}" \
      "END_LSN=${BACKUP_END_LSN:-unknown}" "MIN_RECOVERY_LSN=${MIN_RECOVERY_LSN:-unknown}" \
      "CKPT_LSN=${CKPT_LSN:-unknown}" "SYSID=${SYSID:-unknown}" \
      "DURATION_S=${BACKUP_DURATION:-0}"

# --- integrity of the artifact, before anything depends on it --------------
VERIFYBACKUP_STATUS="SKIPPED"
VERIFYBACKUP_DETAIL="pg_verifybackup not installed, or --no-verifybackup was given"
if [ "$DO_VERIFYBACKUP" = "1" ] && [ -x "${PGBIN_DIR}/pg_verifybackup" ]; then
    step_begin verifybackup
    # -i .pgdrill-meta: basebackup.sh writes that metadata file into the backup
    # after pg_basebackup completes, so it is not covered by backup_manifest and
    # pg_verifybackup would otherwise report it as an unexpected file. Verified
    # against PostgreSQL 17.11.
    if LD_PRELOAD="${PGDR_LD_PRELOAD:-}" "${PGBIN_DIR}/pg_verifybackup" -e -i .pgdrill-meta "$BACKUP_DIR" \
            >"${LOG_DIR}/pg_verifybackup.log" 2>&1; then
        VERIFYBACKUP_STATUS="PASS"
        VERIFYBACKUP_DETAIL="$(tail -1 "${LOG_DIR}/pg_verifybackup.log" | tr -d '"' | tr -s ' ')"
        log "  pg_verifybackup: PASS"
        sed 's/^/    /' "${LOG_DIR}/pg_verifybackup.log" >&2 || true
    else
        VERIFYBACKUP_STATUS="FAIL"
        VERIFYBACKUP_DETAIL="$(tail -3 "${LOG_DIR}/pg_verifybackup.log" | tr '\n' ';' | tr -d '"')"
        fail_step "pg_verifybackup FAILED on the backup we are about to restore"
        sed 's/^/    /' "${LOG_DIR}/pg_verifybackup.log" >&2 || true
    fi
    step_end
fi

# ===========================================================================
# 6. WRITE KNOWN DATA AT KNOWN TIMES
# ===========================================================================
# Rows go in in batches with pauses between them, so every batch has a distinct
# commit timestamp and the drill can name exactly which rows should and should
# not survive recovery.
step_begin data-pre
EARLY_TS=""
EARLY_LSN=""
PRE_MAX_ID=0
PRE_BATCH_ROWS=$(( PRE_ROWS / PRE_BATCHES ))
[ "$PRE_BATCH_ROWS" -lt 1 ] && PRE_BATCH_ROWS=1

_b=1
while [ "$_b" -le "$PRE_BATCHES" ]; do
    _first=$(( ( _b - 1 ) * PRE_BATCH_ROWS + 1 ))
    if [ "$_b" -eq "$PRE_BATCHES" ]; then _last=$PRE_ROWS; else _last=$(( _b * PRE_BATCH_ROWS )); fi
    [ "$_last" -lt "$_first" ] && break

    psql_do "BEGIN;
             INSERT INTO drill_marker (phase, note)
                VALUES ('pre', 'before-batch ${_b} of ${PRE_BATCHES}');
             INSERT INTO drill_orders (id, customer_id, amount, note)
                SELECT g, 1 + (g % ${CUSTOMERS}), round((g % 997)::numeric / 100, 2),
                       'pre-target row ' || g
                FROM generate_series(${_first}, ${_last}) g;
             COMMIT;" >/dev/null
    PRE_MAX_ID="$_last"
    _ts="$(psql_val 'SELECT clock_timestamp()')"
    log "  before batch ${_b}/${PRE_BATCHES}: orders ${_first}..${_last} committed at ${_ts}"
    if [ "$_b" -eq 1 ]; then
        sleep 0.4
        EARLY_TS="$(psql_val 'SELECT clock_timestamp()')"
        EARLY_LSN="$(psql_val 'SELECT pg_current_wal_flush_lsn()' | tr -d '[:space:]')"
    fi
    _b=$(( _b + 1 ))
    [ "$_b" -le "$PRE_BATCHES" ] && sleep "$BATCH_GAP"
done

PRE_FLUSH_LSN="$(psql_val 'SELECT pg_current_wal_flush_lsn()' | tr -d '[:space:]')"
PRE_ORDERS_CHECKSUM="$(psql_val "SELECT md5(string_agg(id || '|' || customer_id || '|' || amount || '|' || note, E'\n' ORDER BY id)) FROM drill_orders" | tr -d '[:space:]')"
PRE_MARKERS="$(psql_val "SELECT count(*) FROM drill_marker WHERE phase = 'pre'" | tr -d '[:space:]')"
PRE_ORDER_COUNT="$(psql_val 'SELECT count(*) FROM drill_orders' | tr -d '[:space:]')"
[ "$PRE_ORDER_COUNT" = "$PRE_ROWS" ] || die "wrote $PRE_ORDER_COUNT pre-target rows, expected $PRE_ROWS"
log "  before-data   : ${PRE_ORDER_COUNT} orders (ids 1..${PRE_MAX_ID}), ${PRE_MARKERS} markers"
log "  before md5    : ${PRE_ORDERS_CHECKSUM}"
log "  WAL flush LSN : ${PRE_FLUSH_LSN}"
step_end

event "EVENT=data" "PHASE=pre" "ORDER_ROWS=${PRE_ORDER_COUNT}" "PRE_MAX_ID=${PRE_MAX_ID}" \
      "MARKERS=${PRE_MARKERS}" "ORDERS_CHECKSUM=${PRE_ORDERS_CHECKSUM}" \
      "CUSTOMER_ROWS=${CUSTOMER_ROWS}" "CUSTOMER_CHECKSUM=${CUSTOMER_CHECKSUM}" \
      "LAST_PRE_LSN=${PRE_FLUSH_LSN}" "EARLY_TS=\"${EARLY_TS}\"" "EARLY_LSN=${EARLY_LSN}"

# ===========================================================================
# 7. CHOOSE THE RECOVERY TARGET
# ===========================================================================
step_begin target
if [ -n "$TARGET_TIME" ]; then
    TARGET_TS="$TARGET_TIME"
    TARGET_ORIGIN="--target-time"
    TARGET_LSN="$PRE_FLUSH_LSN"
elif [ "$NEGATIVE_CONTROL" = "1" ]; then
    # Deliberately too early: after the first batch, before the rest. Recovery
    # stops here, the later "before" rows are missing, and the verification MUST
    # notice. This is the drill's own test.
    sleep "$BATCH_GAP"
    TARGET_TS="$EARLY_TS"
    TARGET_ORIGIN="negative-control: deliberately before orders $(( PRE_MAX_ID - ( PRE_ROWS / PRE_BATCHES ) + 1 ))..${PRE_MAX_ID} were written"
    TARGET_LSN="$EARLY_LSN"
    warn "NEGATIVE CONTROL: the target is deliberately too early. This drill is EXPECTED to fail verification - that is the whole point of the run."
else
    sleep "$PRE_TARGET_GAP"
    TARGET_TS="$(psql_val 'SELECT clock_timestamp()')"
    TARGET_ORIGIN="now, ${PRE_TARGET_GAP}s after the last before-batch"
    TARGET_LSN="$PRE_FLUSH_LSN"
fi
TARGET_SEGMENT="$(lsn_to_segment "$TARGET_LSN" 1 2>/dev/null || printf 'UNKNOWN')"
log "  target time   : $TARGET_TS"
log "  target reason : $TARGET_ORIGIN"
log "  target LSN    : $TARGET_LSN (segment ${TARGET_SEGMENT})"

# A target you cannot replay to is not a target. Say so now rather than after a
# restore that was never going to work.
#
# Two floors matter, and they are not the same thing:
#   * the backup's START_LSN (the redo point): recovery begins here, and WAL
#     replay cannot run backwards, so no target before it is reachable;
#   * pg_control's "minimum recovery ending location", which pg_controldata
#     reports as 0/0 for a backup taken with pg_basebackup -X stream. 0/0 means
#     "no floor recorded here", NOT "any target is fine", so it is skipped and
#     START_LSN is used instead. The floor that matters for a base backup lives
#     in its backup_label, which is why this check uses the metadata too.
for _floor in "${BACKUP_START_LSN:-}" "${MIN_RECOVERY_LSN:-}"; do
    [ -n "$_floor" ] || continue
    [ "$_floor" = "0/0" ] && continue
    case "$_floor" in */*) ;; *) continue ;; esac
    if ! lsn_ge "$TARGET_LSN" "$_floor" 2>/dev/null; then
        fail_step "the recovery target ${TARGET_LSN} is BEFORE the backup's recovery floor ${_floor}: WAL replay cannot run backwards, so this target is unreachable from this backup"
        die "unreachable recovery target: ${TARGET_LSN} < backup recovery floor ${_floor}"
    fi
done
step_end

event "EVENT=target" "KIND=time" "MODE=${MODE}" "TARGET_TS=\"${TARGET_TS}\"" \
      "TARGET_LSN=${TARGET_LSN}" "TARGET_SEGMENT=${TARGET_SEGMENT}" \
      "ORIGIN=\"$(printf '%s' "$TARGET_ORIGIN" | tr '"' "'")\"" \
      "INCLUSIVE=${INCLUSIVE}"

# ===========================================================================
# 8. ARCHIVE BARRIER - the target's WAL must be in the archive
# ===========================================================================
# This is the step most backup scripts skip. A base backup plus a target is not
# a recovery capability: the WAL containing that target has to be OUTSIDE the
# host. Force a segment switch and wait for the archive to actually hold it.
#
# The test is file EXISTENCE in our own archive directory - the ground truth -
# with the archiver's counter as a secondary signal, because last_archived_wal
# can also name a timeline history file, which does not compare as a segment.
wait_for_archived_segment() {
    local want="$1" timeout="$2" t=0 last start
    start="$(now_epoch)"
    while [ "$t" -lt "$timeout" ]; do
        if [ -s "${ARCHIVE_DIR}/${want}" ]; then
            fsub "$(now_epoch)" "$start"
            return 0
        fi
        last="$(psql_val "SELECT COALESCE(last_archived_wal,'') FROM pg_stat_archiver" 2>/dev/null | tr -d '[:space:]' || true)"
        if [ "${#last}" -eq 24 ] && segment_ge "$last" "$want" 2>/dev/null; then
            fsub "$(now_epoch)" "$start"
            return 0
        fi
        sleep 0.5
        t=$(( t + 1 ))
    done
    return 1
}

step_begin archive-barrier
psql_do 'SELECT pg_switch_wal()' >/dev/null
if ! ARCHIVE_WAIT_S="$(wait_for_archived_segment "$TARGET_SEGMENT" "$ARCHIVE_TIMEOUT_WAIT")"; then
    STATS_FAIL="$(psql_val "SELECT failed_count || ' failures, last_failed_wal=' || COALESCE(last_failed_wal,'none') FROM pg_stat_archiver" 2>/dev/null || true)"
    fail_step "the segment containing the recovery target (${TARGET_SEGMENT}) never reached the archive within ${ARCHIVE_TIMEOUT_WAIT}s. Archiver state: ${STATS_FAIL}"
    err "  archiver state: ${STATS_FAIL}"
    err "  This is exactly the failure that makes a backup useless: the base backup is fine, and the WAL that contains the point you want is not."
    exit 3
fi
ARCHIVED_TO="$(psql_val 'SELECT last_archived_wal FROM pg_stat_archiver' | tr -d '[:space:]')"
ARCHIVED_COUNT_PRE="$(psql_val 'SELECT archived_count FROM pg_stat_archiver' | tr -d '[:space:]')"
FAILED_COUNT_PRE="$(psql_val 'SELECT failed_count FROM pg_stat_archiver' | tr -d '[:space:]')"
ARCHIVE_AGE_PRE="$(psql_val "SELECT COALESCE(EXTRACT(EPOCH FROM (now() - last_archived_time))::bigint, 0) FROM pg_stat_archiver" | tr -d '[:space:]')"
log "  archived to   : ${ARCHIVED_TO} (count ${ARCHIVED_COUNT_PRE}, failures ${FAILED_COUNT_PRE}, waited ${ARCHIVE_WAIT_S}s)"
step_end

event "EVENT=archive_barrier" "PHASE=target" "TARGET_SEGMENT=${TARGET_SEGMENT}" \
      "ARCHIVED_TO=${ARCHIVED_TO}" "WAITED_S=${ARCHIVE_WAIT_S}" \
      "ARCHIVED_COUNT=${ARCHIVED_COUNT_PRE}" "FAILED_COUNT=${FAILED_COUNT_PRE}" \
      "ARCHIVE_AGE_S=${ARCHIVE_AGE_PRE}"

# ===========================================================================
# 9. WRITE ROWS AFTER THE TARGET, AND ARCHIVE THEM TOO
# ===========================================================================
step_begin data-post
POST_FIRST_ID=$(( PRE_MAX_ID + 1 ))
POST_MAX_ID=$(( PRE_MAX_ID + POST_ROWS ))
POISON_ID=999999

psql_do "BEGIN;
         INSERT INTO drill_marker (phase, note)
            VALUES ('post', 'after the recovery target - must NOT survive');
         INSERT INTO drill_orders (id, customer_id, amount, note)
            SELECT g, 1 + (g % ${CUSTOMERS}), round((g % 997)::numeric / 100, 2),
                   'post-target row ' || g
            FROM generate_series(${POST_FIRST_ID}, ${POST_MAX_ID}) g;
         INSERT INTO drill_orders (id, customer_id, amount, note)
            VALUES (${POISON_ID}, 1, 0.01, 'POISON should-not-survive');
         INSERT INTO drill_marker (phase, note)
            VALUES ('post', 'second after-target marker - must NOT survive');
         COMMIT;" >/dev/null

POST_FLUSH_LSN="$(psql_val 'SELECT pg_current_wal_flush_lsn()' | tr -d '[:space:]')"
POST_ORDER_COUNT="$(psql_val 'SELECT count(*) FROM drill_orders' | tr -d '[:space:]')"
POST_MARKERS="$(psql_val "SELECT count(*) FROM drill_marker WHERE phase = 'post'" | tr -d '[:space:]')"
log "  after-data    : orders ${POST_FIRST_ID}..${POST_MAX_ID} plus poison id ${POISON_ID}"
log "                  totals: ${POST_ORDER_COUNT} orders, ${POST_MARKERS} after-markers"

# Archive the after-WAL as well. Without this, "after rows are absent" would be
# satisfied by missing WAL rather than by the recovery target, and the drill
# would be proving nothing at all.
sleep 0.5
psql_do 'SELECT pg_switch_wal()' >/dev/null
POST_SEGMENT="$(lsn_to_segment "$POST_FLUSH_LSN" 1 2>/dev/null || printf 'UNKNOWN')"
POST_ARCHIVE_OK=0
if POST_ARCHIVE_WAIT_S="$(wait_for_archived_segment "$POST_SEGMENT" "$ARCHIVE_TIMEOUT_WAIT")"; then
    POST_ARCHIVE_OK=1
    log "  after-WAL     : ${POST_SEGMENT} archived after ${POST_ARCHIVE_WAIT_S}s"
else
    warn "the segment holding the after-target commits (${POST_SEGMENT}) was not archived in time."
    warn "The 'after rows must be absent' check may then pass for the wrong reason; that will be recorded as UNPROVEN."
fi
step_end

event "EVENT=data" "PHASE=post" "ORDER_ROWS=${POST_ORDER_COUNT}" \
      "POST_FIRST_ID=${POST_FIRST_ID}" "POST_MAX_ID=${POST_MAX_ID}" "POISON_ID=${POISON_ID}" \
      "MARKERS=${POST_MARKERS}" "POST_FLUSH_LSN=${POST_FLUSH_LSN}" \
      "POST_SEGMENT=${POST_SEGMENT}" "POST_WAL_ARCHIVED=${POST_ARCHIVE_OK}"

event "EVENT=archive_barrier" "PHASE=post" "POST_SEGMENT=${POST_SEGMENT}" \
      "POST_WAL_ARCHIVED=${POST_ARCHIVE_OK}" "WAITED_S=${POST_ARCHIVE_WAIT_S:-0}"

# ===========================================================================
# 10. ARCHIVE HEALTH (the kit's own monitoring script, against a live cluster)
# ===========================================================================
step_begin archive-health
CHECK_ARCHIVE="${KIT_DIR}/monitoring/check_archive.sh"
ARCHIVE_HEALTH_STATUS="SKIPPED"
ARCHIVE_HEALTH_RC=2
if [ -x "$CHECK_ARCHIVE" ]; then
    set +e
    LD_PRELOAD="${PGDR_LD_PRELOAD:-}" "$CHECK_ARCHIVE" \
        --host 127.0.0.1 --port "$DRILL_PORT" --user "$PGUSER" \
        --archive-dir "$ARCHIVE_DIR" \
        >"${LOG_DIR}/check_archive.out" 2>&1
    ARCHIVE_HEALTH_RC=$?
    set -e
    case "$ARCHIVE_HEALTH_RC" in
        0) ARCHIVE_HEALTH_STATUS="HEALTHY" ;;
        1) ARCHIVE_HEALTH_STATUS="UNHEALTHY" ;;
        *) ARCHIVE_HEALTH_STATUS="UNKNOWN" ;;
    esac
    log "  check_archive : ${ARCHIVE_HEALTH_STATUS} (exit ${ARCHIVE_HEALTH_RC})"
    sed 's/^/    /' "${LOG_DIR}/check_archive.out" >&2 || true
fi
step_end

event "EVENT=archive_health" "HEALTH_STATUS=${ARCHIVE_HEALTH_STATUS}" \
      "HEALTH_EXIT=${ARCHIVE_HEALTH_RC}" "HEALTH_RC=${ARCHIVE_HEALTH_RC}"

# ===========================================================================
# 11. SIMULATE LOSS
# ===========================================================================
# Stop the cluster and move the data directory aside. This is what makes it a
# drill: from here the only thing that can bring the data back is the backup
# plus the archive.
step_begin loss
LOSS_EPOCH="$(now_epoch)"
if ! LD_PRELOAD="${PGDR_LD_PRELOAD:-}" "${PGBIN_DIR}/pg_ctl" -D "$SRC_PGDATA" -m fast -w -t 60 stop \
        >"${LOG_DIR}/stop-source.log" 2>&1; then
    fail_step "could not stop the source cluster"
    exit 3
fi
assert_in_run "$SRC_PGDATA" "simulate loss"
mv -- "$SRC_PGDATA" "$LOST_DIR/datadir"
log "  LOSS: cluster stopped; $SRC_PGDATA moved to $LOST_DIR/datadir"
log "  LOSS at       : $(epoch_to_pg_ts "$LOSS_EPOCH")"
step_end

event "EVENT=loss" "EPOCH=${LOSS_EPOCH}" "LOSS_AT=\"$(epoch_to_pg_ts "$LOSS_EPOCH")\"" \
      "PGDATA_MOVED_TO=${LOST_DIR}/datadir"

# ===========================================================================
# 12. RESTORE
# ===========================================================================
step_begin restore
RESTORE_START_EPOCH="$(now_epoch)"
RESTORE_START_AT="$(now_iso)"
assert_in_run "$RESTORE_PGDATA" "restore into"
rm -rf -- "$RESTORE_PGDATA"
if ! cp -a -- "$BACKUP_DIR" "$RESTORE_PGDATA" >"${LOG_DIR}/restore-copy.log" 2>&1; then
    fail_step "copying the base backup to $RESTORE_PGDATA failed; see ${LOG_DIR}/restore-copy.log"
    exit 3
fi
COPY_DONE_EPOCH="$(now_epoch)"
COPY_S="$(fsub "$COPY_DONE_EPOCH" "$RESTORE_START_EPOCH")"
RESTORED_BYTES="$(dir_bytes "$RESTORE_PGDATA")"

# The recorded recovery target. Explicit UTC offset: never let the local
# TimeZone setting decide where "14:05" was.
cat >> "${RESTORE_PGDATA}/postgresql.auto.conf" <<RECOVERY_EOF

# --- pgdrill restore drill: recovery settings -------------------------------
restore_command = '${RESTORE_CMD} %f %p'
recovery_target_time = '${TARGET_TS}'
recovery_target_inclusive = '${INCLUSIVE}'
recovery_target_action = 'promote'
RECOVERY_EOF

# The restored cluster reads the archive; it must never write to it. archive_mode
# is appended last in postgresql.conf so it wins over the value the backup
# carried, and it is decided BEFORE the first start because changing it needs a
# restart.
printf '\n# pgdrill restore drill: never archive from a restore target\narchive_mode = off\n' \
    >> "${RESTORE_PGDATA}/postgresql.conf"

# ---------------------------------------------------------------------------
# recovery.signal IS WHAT MAKES THIS A POINT-IN-TIME RECOVERY.
#
# A base backup taken with pg_basebackup -X stream carries its own WAL up to the
# end of the backup, and a data directory that contains backup_label and pg_wal
# but NO recovery.signal performs plain crash recovery: PostgreSQL replays the
# WAL it has locally, reaches the end of it, and promotes - silently ignoring
# restore_command AND recovery_target_time, because there is no archive recovery
# to target. The cluster starts, answers queries, and contains the data as of
# the backup. It looks like a successful restore.
#
# That is precisely the trap this drill exists to catch, and it caught it: an
# earlier run of this script reported "redo done at 0/2000120", zero segments
# served by restore_command, and a fully populated but WRONG database. The
# verification noticed and failed the run, which is the whole design working.
# recovery.signal (PostgreSQL 12+; recovery.conf before that) is the switch.
#
# Deliberately creation-safe: the file is created if absent, never truncated
# into something else, and it is checked before the cluster is started.
# ---------------------------------------------------------------------------
: > "${RESTORE_PGDATA}/recovery.signal"
[ -f "${RESTORE_PGDATA}/recovery.signal" ] || die "could not create recovery.signal in ${RESTORE_PGDATA}; without it this would be a crash recovery, not a PITR"
secure_pgdata "$RESTORE_PGDATA"

RESTORED_CONTROLDATA="$(LD_PRELOAD="${PGDR_LD_PRELOAD:-}" "${PGBIN_DIR}/pg_controldata" -D "$RESTORE_PGDATA" 2>/dev/null || true)"
REPLAY_REDO_LSN="$(printf '%s\n' "$RESTORED_CONTROLDATA" | sed -n "s/^Latest checkpoint.s REDO location:[[:space:]]*//p" | head -1 | tr -d ' ')"
[ -n "$REPLAY_REDO_LSN" ] || REPLAY_REDO_LSN="$(printf '%s' "${BACKUP_START_LSN:-0/0}" | tr -d ' ')"
REPLAY_START_SEGMENT="$(lsn_to_segment "$REPLAY_REDO_LSN" 1 2>/dev/null || printf 'UNKNOWN')"

log "  copied        : $BACKUP_DIR -> $RESTORE_PGDATA ($(fmt_bytes "$RESTORED_BYTES") in ${COPY_S}s)"
log "  restore_command: $RESTORE_CMD %f %p"
log "  recovery target: $TARGET_TS (inclusive=${INCLUSIVE}, action=promote)"
log "  replay from    : $REPLAY_REDO_LSN (segment ${REPLAY_START_SEGMENT})"
step_end

event "EVENT=restore" "RESTORE_PGDATA=${RESTORE_PGDATA}" \
      "COPIED_BYTES=${RESTORED_BYTES}" "COPY_S=${COPY_S}" "RESTORE_START_EPOCH=${RESTORE_START_EPOCH}" \
      "RESTORE_COMMAND=\"${RESTORE_CMD} %f %p\"" "REPLAY_REDO_LSN=${REPLAY_REDO_LSN}" \
      "REPLAY_START_SEGMENT=${REPLAY_START_SEGMENT}"

# ===========================================================================
# 13. START THE RESTORED CLUSTER AND WAIT FOR RECOVERY
# ===========================================================================
step_begin replay
RECOVERY_START_EPOCH="$(now_epoch)"
if ! start_cluster "$RESTORE_PGDATA" "$RESTORE_LOG" >>"${LOG_DIR}/pgctl-restored.log" 2>&1; then
    fail_step "the restored cluster did not start. See $RESTORE_LOG and ${LOG_DIR}/pgctl-restored.log"
    tail -30 "$RESTORE_LOG" >&2 || true
    exit 3
fi
START_DONE_EPOCH="$(now_epoch)"
START_S="$(fsub "$START_DONE_EPOCH" "$RECOVERY_START_EPOCH")"

# Queryable: the first moment the cluster answers a query at all. That is the
# earliest an application could do anything, so it is the RTO number.
QUERYABLE_EPOCH=""
_t=0
while [ "$_t" -lt "$REPLAY_TIMEOUT" ]; do
    if psql_val 'SELECT 1' >/dev/null 2>&1; then QUERYABLE_EPOCH="$(now_epoch)"; break; fi
    sleep 0.2
    _t=$(( _t + 1 ))
done
[ -n "$QUERYABLE_EPOCH" ] || { fail_step "the restored cluster never became queryable"; exit 3; }

# Recovery complete: no longer in recovery, so it accepts writes and could be
# cut over to.
_t=0
while [ "$_t" -lt "$REPLAY_TIMEOUT" ]; do
    if [ "$(psql_val 'SELECT pg_is_in_recovery()' 2>/dev/null | tr -d '[:space:]')" = "f" ]; then break; fi
    sleep 0.2
    _t=$(( _t + 1 ))
done
PROMOTED_EPOCH="$(now_epoch)"
IN_RECOVERY_FINAL="$(psql_val 'SELECT pg_is_in_recovery()' 2>/dev/null | tr -d '[:space:]' || printf 'unknown')"

REPLAYED_LSN="$(psql_val 'SELECT pg_last_wal_replay_lsn()' | tr -d '[:space:]')"
REPLAY_END_SEGMENT="$(lsn_to_segment "$REPLAYED_LSN" 1 2>/dev/null || printf 'UNKNOWN')"
# NOTE: `grep -c` prints 0 and exits 1 when nothing matches, so `|| printf 0`
# would append a SECOND 0 and inject a newline into the value. `|| true` keeps
# the single 0 that grep already printed.
SEGMENTS_RESTORED="$(grep -c '^RESTORED ' "$RESTORE_CMD_LOG" 2>/dev/null || true)"
SEGMENTS_MISSED="$(grep -c '^MISS ' "$RESTORE_CMD_LOG" 2>/dev/null || true)"
SEGMENTS_RESTORED="${SEGMENTS_RESTORED:-0}"
SEGMENTS_MISSED="${SEGMENTS_MISSED:-0}"
# The exact list of segments recovery asked for - real evidence, not an
# inference from an LSN range.
REPLAYED_SEGMENTS="$(awk '/^RESTORED / {print $2}' "$RESTORE_CMD_LOG" 2>/dev/null | sort -u | tr '\n' ',' | sed 's/,$//' || true)"
RECOVERY_ELAPSED_S="$(fsub "$PROMOTED_EPOCH" "$RECOVERY_START_EPOCH")"

log "  queryable at  : $(epoch_to_pg_ts "$QUERYABLE_EPOCH")"
log "  recovery done : $(epoch_to_pg_ts "$PROMOTED_EPOCH") (in_recovery=${IN_RECOVERY_FINAL})"
log "  replayed to   : $REPLAYED_LSN (segment ${REPLAY_END_SEGMENT})"
log "  segments      : ${SEGMENTS_RESTORED} served by restore_command, ${SEGMENTS_MISSED} requested-but-absent"
if [ "${SEGMENTS_RESTORED:-0}" = "0" ]; then
    warn "restore_command served NO segments. The base backup was self-contained"
    warn "(-X stream), so this can be legitimate - but it means this run did NOT"
    warn "exercise WAL replay from the archive, and the result is reported as such."
fi
step_end

event "EVENT=replay" "IN_RECOVERY_FINAL=${IN_RECOVERY_FINAL}" \
      "REPLAYED_LSN=${REPLAYED_LSN}" "REPLAY_END_SEGMENT=${REPLAY_END_SEGMENT}" \
      "SEGMENTS_RESTORED=${SEGMENTS_RESTORED}" "SEGMENTS_MISSED=${SEGMENTS_MISSED}" \
      "REPLAYED_SEGMENTS=\"${REPLAYED_SEGMENTS}\"" \
      "RECOVERY_ELAPSED_S=${RECOVERY_ELAPSED_S}" "START_S=${START_S}"

# ===========================================================================
# 14. VERIFY BY QUERYING - the only step that proves anything
# ===========================================================================
step_begin verify
CHECKS_RUN=0
CHECKS_FAILED=0
FAILURE_DETAILS=()

# record_check <name> <expected> <actual> <detail>
record_check() {
    local name="$1" expected="$2" actual="$3" detail="${4:-}"
    CHECKS_RUN=$(( CHECKS_RUN + 1 ))
    local status="PASS"
    if [ "$expected" != "$actual" ]; then
        status="FAIL"
        CHECKS_FAILED=$(( CHECKS_FAILED + 1 ))
        FAILURE_DETAILS+=("${name}: expected '${expected}', got '${actual}'. ${detail}")
    fi
    printf '  %-38s %-5s expected=%-16s actual=%s\n' "$name" "$status" "$expected" "$actual" >&2
    event "EVENT=verify" "CHECK=${name}" "STATUS=${status}" \
          "EXPECTED=\"$(printf '%s' "$expected" | tr '"' "'")\"" \
          "ACTUAL=\"$(printf '%s' "$actual" | tr '"' "'")\"" \
          "DETAIL=\"$(printf '%s' "$detail" | tr '"' "'")\""
}

printf '\n' >&2
log "verification: the recovered cluster against everything recorded before the loss"
printf '  %-38s %-5s %-24s %s\n' "CHECK" "STATE" "EXPECTED" "ACTUAL" >&2

# --- 1. the static table that was inside the base backup --------------------
_actual="$(psql_try 'SELECT count(*) FROM drill_customers')"
record_check "customers_row_count" "$CUSTOMER_ROWS" "$_actual" \
    "the static table seeded before the base backup; a short count means the backup itself is incomplete"
_actual="$(psql_try "SELECT md5(string_agg(id || '|' || name || '|' || region, E'\n' ORDER BY id)) FROM drill_customers")"
record_check "customers_checksum" "$CUSTOMER_CHECKSUM" "$_actual" \
    "row CONTENT, not just counts: a restore that returns the right number of wrong rows passes a count check"

# --- 2. the critical one: pre-target rows present and intact ----------------
_actual="$(psql_try 'SELECT count(*) FROM drill_orders')"
record_check "orders_total_rows" "$PRE_ORDER_COUNT" "$_actual" \
    "expected exactly the ${PRE_ORDER_COUNT} rows committed before the target (ids 1..${PRE_MAX_ID})"

_actual="$(psql_try "SELECT md5(string_agg(id || '|' || customer_id || '|' || amount || '|' || note, E'\n' ORDER BY id)) FROM drill_orders")"
record_check "orders_before_checksum" "$PRE_ORDERS_CHECKSUM" "$_actual" \
    "content of every surviving row in order; differs if rows are missing, duplicated, reverted or altered"

MISSING_ROWS="$(psql_try "SELECT count(*) FROM generate_series(1, ${PRE_MAX_ID}) g(id) LEFT JOIN drill_orders o ON o.id = g.id WHERE o.id IS NULL")"
record_check "orders_before_rows_missing" "0" "$MISSING_ROWS" \
    "ids in 1..${PRE_MAX_ID} that are NOT in the recovered table"
if [ "${MISSING_ROWS:-QUERY_FAILED}" != "0" ]; then
    _rng="$(psql_try "SELECT min(g.id) || '..' || max(g.id) FROM generate_series(1, ${PRE_MAX_ID}) g(id) LEFT JOIN drill_orders o ON o.id = g.id WHERE o.id IS NULL")"
    err "  rows committed BEFORE the target are MISSING after recovery: ${MISSING_ROWS} row(s), ids ${_rng}"
    err "  the recovery target landed earlier than intended, or the archive is missing the WAL for those commits"
fi

# --- 3. post-target rows must be ABSENT -------------------------------------
AFTER_ROWS="$(psql_try "SELECT count(*) FROM drill_orders WHERE id > ${PRE_MAX_ID}")"
record_check "orders_after_rows_present" "0" "$AFTER_ROWS" \
    "rows committed AFTER the target must not exist; a non-zero count means recovery overshot (check recovery_target_inclusive and recovery_target_action)"
if [ "${AFTER_ROWS:-QUERY_FAILED}" != "0" ]; then
    _ids="$(psql_try "SELECT string_agg(id::text, ',' ORDER BY id) FROM (SELECT id FROM drill_orders WHERE id > ${PRE_MAX_ID} ORDER BY id LIMIT 20) s")"
    err "  rows committed AFTER the target came back: ${AFTER_ROWS} row(s); first ids ${_ids}"
    err "  this is the failure people discover in production rather than in a drill:"
    err "  you asked for a point in time and received a later one."
fi

_actual="$(psql_try "SELECT count(*) FROM drill_orders WHERE id = ${POISON_ID}")"
record_check "poison_row_absent" "0" "$_actual" \
    "the deliberately named post-target row (id ${POISON_ID}); if it is present, recovery went past the target"

_actual="$(psql_try "SELECT count(*) FROM drill_marker WHERE phase = 'pre'")"
record_check "markers_before_present" "$PRE_MARKERS" "$_actual" \
    "every 'before' marker should be present in the recovered cluster"

_actual="$(psql_try "SELECT count(*) FROM drill_marker WHERE phase = 'post'")"
record_check "markers_after_absent" "0" "$_actual" \
    "no 'after' marker should exist in the recovered cluster"

# --- 4. did recovery actually reach the last pre-target commit? -------------
REPLAYED_DEC="$(lsn_to_dec "$REPLAYED_LSN" 2>/dev/null || printf -- '-1')"
PRE_DEC="$(lsn_to_dec "$PRE_FLUSH_LSN" 2>/dev/null || printf -- '-1')"
RECHECK_OK="no"
if [ "${REPLAYED_DEC:-0}" -ge 0 ] 2>/dev/null && [ "${PRE_DEC:-0}" -ge 0 ] 2>/dev/null; then
    [ "$REPLAYED_DEC" -ge "$PRE_DEC" ] && RECHECK_OK="yes"
fi
record_check "replay_reached_last_before_commit" "yes" "$RECHECK_OK" \
    "last replayed LSN ${REPLAYED_LSN} vs the last before-target commit at ${PRE_FLUSH_LSN}"
if [ "$RECHECK_OK" = "no" ]; then
    err "  recovery stopped at ${REPLAYED_LSN}, before the last commit that should have survived (${PRE_FLUSH_LSN})"
fi

# --- 5. was the after-WAL even there? otherwise absence proves nothing ------
if [ "${POST_ARCHIVE_OK:-0}" = "1" ]; then
    record_check "after_target_wal_was_archivable" "yes" "yes" \
        "the segments holding the after-target commits were archived before the simulated loss, so their absence is caused by the recovery target and not by missing WAL"
else
    record_check "after_target_wal_was_archivable" "yes" "no" \
        "UNPROVEN: the after-target WAL was never confirmed in the archive, so 'after rows are absent' could be caused by missing WAL rather than by the recovery target"
fi

# --- 6. the artifact and the archive ---------------------------------------
record_check "basebackup_integrity" "PASS" "$VERIFYBACKUP_STATUS" "$VERIFYBACKUP_DETAIL"
record_check "archive_health_at_backup_time" "HEALTHY" "$ARCHIVE_HEALTH_STATUS" \
    "monitoring/check_archive.sh against the live cluster, just before the simulated loss"

step_end

# ===========================================================================
# 15. RPO / RTO AND THE VERDICT
# ===========================================================================
NEWEST_MARKER_ROW="$(psql_try_raw "SELECT id || '|' || recorded_at FROM drill_marker WHERE phase = 'pre' ORDER BY recorded_at DESC, id DESC LIMIT 1")"
NEWEST_MARKER_ID="${NEWEST_MARKER_ROW%%|*}"
NEWEST_MARKER_TS="${NEWEST_MARKER_ROW#*|}"
if [ "$NEWEST_MARKER_TS" = "$NEWEST_MARKER_ROW" ] || [ "$NEWEST_MARKER_TS" = "QUERY_FAILED" ]; then
    NEWEST_MARKER_TS=""
fi

MEASURED_RPO_S="unknown"
if [ -n "$NEWEST_MARKER_TS" ] && [ "$TARGET_TS" != "$NEWEST_MARKER_TS" ]; then
    MEASURED_RPO_S="$(psql_try "SELECT round(EXTRACT(EPOCH FROM (TIMESTAMPTZ '${TARGET_TS}' - TIMESTAMPTZ '${NEWEST_MARKER_TS}'))::numeric, 3)")"
    [ -n "$MEASURED_RPO_S" ] || MEASURED_RPO_S="unknown"
fi

RTO_S="$(fsub "$QUERYABLE_EPOCH" "$LOSS_EPOCH")"
RTO_PROMOTED_S="$(fsub "$PROMOTED_EPOCH" "$LOSS_EPOCH")"
RESTORE_TO_QUERYABLE_S="$(fsub "$QUERYABLE_EPOCH" "$RESTORE_START_EPOCH")"

step_begin report
if [ "$CHECKS_FAILED" = "0" ]; then RESULT="PASS"; else RESULT="FAIL"; fi

printf '\n' >&2
log "---------------------------------------------"
log " drill summary"
log "---------------------------------------------"
log "  mode                    : $MODE"
log "  backup                  : $(fmt_bytes "$BACKUP_BYTES") in ${BACKUP_DURATION:-?}s"
log "  recovery target         : $TARGET_TS"
log "  newest surviving marker : ${NEWEST_MARKER_TS:-none} (id ${NEWEST_MARKER_ID:-none})"
if [ "$MEASURED_RPO_S" = "unknown" ]; then
    log "  measured RPO            : unknown"
else
    log "  measured RPO            : ${MEASURED_RPO_S}s"
fi
log "  RTO loss -> queryable   : ${RTO_S}s  (copy ${COPY_S}s + start/replay ${RECOVERY_ELAPSED_S}s)"
log "  RTO loss -> promoted    : ${RTO_PROMOTED_S}s"
log "  WAL replayed            : ${SEGMENTS_RESTORED} segment(s) served, ${REPLAY_START_SEGMENT} .. ${REPLAY_END_SEGMENT}"
log "  checks                  : $(( CHECKS_RUN - CHECKS_FAILED ))/${CHECKS_RUN} passed"
log "  RESULT                  : ${RESULT}"
if [ "$CHECKS_FAILED" != "0" ]; then
    log ""
    for _f in "${FAILURE_DETAILS[@]}"; do
        err "  MISMATCH ${_f}"
    done
fi
log "---------------------------------------------"

if [ "$KEEP" = "1" ]; then
    log ""
    log "kept for inspection (--keep): $RUN_DIR"
    log "  moved-aside source data dir    : $LOST_DIR/datadir"
    log "  base backup                    : $BACKUP_DIR"
    log "  WAL archive                    : $ARCHIVE_DIR"
    log "  restored cluster (left RUNNING): $RESTORE_PGDATA on port $DRILL_PORT"
    log "  restored cluster log           : $RESTORE_LOG"
    log "  every segment recovery wanted  : $RESTORE_CMD_LOG"
    log "  inspect it: $PGBIN_DIR/psql -h 127.0.0.1 -p $DRILL_PORT -U $PGUSER -d postgres"
    log "  stop it   : $PGBIN_DIR/pg_ctl -D $RESTORE_PGDATA -m fast stop"
fi

event "EVENT=rpo" "TARGET_TS=\"${TARGET_TS}\"" \
      "NEWEST_SURVIVING_MARKER_TS=\"${NEWEST_MARKER_TS:-none}\"" \
      "NEWEST_SURVIVING_MARKER_ID=${NEWEST_MARKER_ID:-none}" \
      "MEASURED_RPO_S=${MEASURED_RPO_S}" "ARCHIVE_AGE_AT_TARGET_S=${ARCHIVE_AGE_PRE:-unknown}"

event "EVENT=rto" "LOSS_EPOCH=${LOSS_EPOCH}" "RESTORE_START_EPOCH=${RESTORE_START_EPOCH}" \
      "QUERYABLE_EPOCH=${QUERYABLE_EPOCH}" "PROMOTED_EPOCH=${PROMOTED_EPOCH}" \
      "MEASURED_RTO_S=${RTO_S}" "MEASURED_RTO_PROMOTED_S=${RTO_PROMOTED_S}" \
      "RESTORE_TO_QUERYABLE_S=${RESTORE_TO_QUERYABLE_S}" \
      "COPY_S=${COPY_S}" "REPLAY_S=${RECOVERY_ELAPSED_S}" "START_S=${START_S}" \
      "EXCLUDES=\"offsite fetch, production data volume, detection time, decision time, application cut-over\""

TOTAL_S="$(fsub "$(now_epoch)" "$STARTED_EPOCH")"
event "EVENT=result" "STATUS=${RESULT}" "CHECKS=${CHECKS_RUN}" "FAILED=${CHECKS_FAILED}" \
      "MODE=${MODE}" "DURATION_S=${TOTAL_S}" "RUN_DIR=${RUN_DIR}"
step_end

# The exit code IS the alert condition. A drill that cannot fail is decoration.
if [ "$CHECKS_FAILED" != "0" ]; then
    if [ "$NEGATIVE_CONTROL" = "1" ]; then
        err "RESULT FAIL - which is the CORRECT outcome for --negative-control: the verification"
        err "detected that rows committed before the requested target were missing."
    fi
    exit 1
fi
exit 0
