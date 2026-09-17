#!/usr/bin/env bash
# ===========================================================================
# check_archive.sh - is the WAL archive chain actually healthy right now?
# Part of the PostgreSQL Backup & PITR Restore-Drill Kit
#
# Run this from your monitoring agent (cron, systemd timer, Nagios/Icinga,
# Zabbix, a Kubernetes CronJob). It exits non-zero when the archive is
# unhealthy, so a non-zero exit is all you need to alert on.
#
#   exit 0   HEALTHY    - the archive chain looks intact
#   exit 1   UNHEALTHY  - one or more specific, named problems (see the output)
#   exit 2   UNKNOWN    - could not determine health (server unreachable, bad
#                         arguments, psql missing). It alerts on purpose:
#                         "we cannot tell" must never be reported as healthy.
#
# ---------------------------------------------------------------------------
# WHY "THE BACKUP JOB EXITED ZERO" IS NOT THIS CHECK
# ---------------------------------------------------------------------------
# WAL archiving has no client, no error surface a human sees, and no retry
# anyone notices. Each failure below leaves the base backups looking perfectly
# healthy while the recovery window quietly shrinks:
#
#   1. THE ARCHIVER IS FAILING RIGHT NOW.  pg_stat_archiver.last_failed_wal is
#      later than last_archived_wal. PostgreSQL retries, so the cluster looks
#      busy and pg_wal slowly fills up un-archived. The archive has a hole at
#      its end, and the hole is not visible in a file listing.
#   2. THE ARCHIVE IS SILENTLY STALE.  Nothing has been archived for hours
#      because nothing has written enough WAL to fill a segment (an idle
#      cluster!) and archive_timeout was never set. Your real RPO is then
#      "whenever the last segment filled", not what the runbook claims.
#   3. archive_command CAN NEVER FAIL.  `... || true`, or `; exit 0`. Every
#      segment "succeeds", failed_count stays at 0, and segments that were
#      never written are counted as archived. This check looks for the pattern,
#      because no metric can: the counter it would use is being lied to.
#   4. A GAP IN THE MIDDLE.  Segment N is missing while N+1..N+40 are present -
#      a manual archive cleanup, a broken offsite sync, a partially deleted
#      directory. Costs you every recovery target after N.
#   5. THE ARCHIVER SAYS YES AND THE FILE IS NOT THERE.  last_archived_wal
#      names a segment that does not exist in --archive-dir (wrong path, an
#      archive_command writing elsewhere, an NFS mount that went away).
#   6. A ZERO-LENGTH OR TRUNCATED SEGMENT.  The signature of an interrupted
#      copy. PostgreSQL calls the archive complete; recovery fails months later.
#
# WHAT THIS SCRIPT CANNOT SEE - know the boundary before you trust it
#   * It knows about the ONE archive directory you point it at. If your offsite
#     copy (rsync/S3/restic) is broken, this still reports HEALTHY. Monitor the
#     offsite copy separately; only ../drill/restore_drill.sh proves that an
#     artifact can actually be restored.
#   * Without --archive-dir it cannot detect gaps or missing files, because it
#     is reading only the server's own counters - written by the same code
#     whose failure you are trying to detect. Use --archive-dir.
#   * It does not check that a usable base backup exists, nor that the archive
#     reaches back far enough to be useful on its own.
#   * --archive-dir expects the FLAT layout that ../backup/wal_archive.sh
#     produces (one directory, segments named %f). A date-partitioned archive
#     layout needs the gap check adapted.
#
# Usage:
#   ./check_archive.sh                                   # server counters only
#   ./check_archive.sh --archive-dir /var/backups/pgdrill/wal
#   ./check_archive.sh --json | --nagios | --quiet
#   ./check_archive.sh --state-file /var/lib/pgdrill/archive.state   # in cron
#
# Environment (each is overridden by the matching flag):
#   PGHOST PGPORT PGUSER PGDATABASE   connection (defaults: socket, 5432,
#                                     postgres, postgres)
#   ARCHIVE_DIR                       same as --archive-dir
#   MAX_ARCHIVE_AGE                   seconds since the last successful archive
#                                     before it counts as stale. Default:
#                                     3 x archive_timeout, minimum 900.
#   MAX_ARCHIVE_LAG_SEGMENTS          segments behind the current WAL position
#                                     before the archiver counts as behind
#                                     (default 2)
# ===========================================================================

set -uo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
if [ -r "${SELF_DIR}/../lib/common.sh" ]; then
    . "${SELF_DIR}/../lib/common.sh"
else
    # Standalone deployment: the kit's lib/ was not copied next to us. Only the
    # handful of helpers this script needs are repeated here so that a single
    # file can be dropped on a database host.
    log()   { printf '[INFO ] %s\n' "$*" >&2; }
    err()   { printf '[ERROR] %s\n' "$*" >&2; }
    event() { printf 'PGDR_EVENT %s\n' "$*" >&2; }
    _hex2dec() {
        [ -n "${1:-}" ] || return 1
        printf '%s\n' "$1" | awk '
            function hex2dec(s,   i,c,d,val,n) {
                n = length(s); val = 0
                for (i = 1; i <= n; i++) {
                    c = substr(s, i, 1)
                    if      (c >= "0" && c <= "9") d = index("0123456789", c) - 1
                    else if (c >= "a" && c <= "f") d = index("abcdef", c) + 9
                    else if (c >= "A" && c <= "F") d = index("ABCDEF", c) + 9
                    else return -1
                    val = val * 16 + d
                }
                return val
            }
            { v = hex2dec($1); if (v < 0) exit 1; printf "%.0f\n", v }'
    }
    segment_to_num() {
        local s="${1:-}" a b c
        [ "${#s}" -eq 24 ] || { printf 'INVALID\n'; return 1; }
        a="$(_hex2dec "${s:0:8}")"   || return 1
        b="$(_hex2dec "${s:8:8}")"   || return 1
        c="$(_hex2dec "${s:16:8}")"  || return 1
        awk -v a="$a" -v b="$b" -v c="$c" 'BEGIN{ printf "%010.0f%010.0f%010.0f\n", a, b, c }'
    }
fi

# --- small output helpers (defined before first use) ------------------------
json_escape() {
    printf '%s' "${1:-}" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e 's/\t/\\t/g' \
        | tr -d '\n\r' | tr -d '\000-\010\013\014\016-\037'
}
iso_from_epoch() {
    TZ=UTC date -d "@${1}" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || printf '%s' "$1"
}

# --- defaults ---------------------------------------------------------------
HOST="${PGHOST:-/var/run/postgresql}"
PORT="${PGPORT:-5432}"
USER="${PGUSER:-postgres}"
DB="${PGDATABASE:-postgres}"
ARCHIVE_DIR="${ARCHIVE_DIR:-}"
MAX_AGE="${MAX_ARCHIVE_AGE:-}"
MAX_LAG_SEGMENTS="${MAX_ARCHIVE_LAG_SEGMENTS:-2}"
STATE_FILE=""
JSON=0
NAGIOS=0
QUIET=0
SEG_SIZE=16777216
CHECK_FILES=0
CHECK_FILES_MAX=0

usage() {
    cat >&2 <<'USAGE'
Usage: check_archive.sh [OPTIONS]

Connection:
  --host HOST            server host or socket directory (default /var/run/postgresql)
  --port PORT            server port                     (default 5432)
  --user USER            role to connect as              (default postgres)
  --dbname DB            database to connect to          (default postgres)

Checks:
  --archive-dir DIR      also verify the archive ON DISK: that last_archived_wal
                         is really there, that the recent segments are non-zero
                         and not truncated, that no temp file was abandoned, and
                         that there is NO GAP in the segment sequence.
                         Strongly recommended - without it, checks 4 and 6 cannot
                         run at all, because the server's own counters are the
                         only evidence.
  --max-age SEC          seconds since the last successful archive before the
                         archive counts as stale. Default: 3 x archive_timeout,
                         but never less than 900.
  --max-lag-segments N   segments between last_archived_wal and the current WAL
                         position before the archiver counts as behind (default 2)
  --segment-size BYTES   WAL segment size (default 16777216); change it if the
                         cluster was created with --wal-segsize
  --files N              limit the gap scan to the NEWEST N segments. Default 0
                         checks the entire directory, because a hole anywhere in
                         the chain costs you every target after it.
  --state-file PATH      remember failed_count between runs, so a failure that
                         PostgreSQL has already retried successfully is still
                         reported. Use the same path in every run (cron).

Output:
  --json                 print one JSON object instead of the human report
  --nagios               print one Nagios/Icinga plugin line
  --quiet                print nothing when healthy; problems still print
  -h, --help             this help

Exit status:
  0 HEALTHY   1 UNHEALTHY   2 UNKNOWN (unreachable, bad arguments, no psql)

A non-zero exit is the alert condition. Exit 2 exists because "we could not
determine health" must never be reported as healthy.
USAGE
}

while [ $# -gt 0 ]; do
    case "$1" in
        --host) HOST="$2"; shift 2 ;;
        --port) PORT="$2"; shift 2 ;;
        --user) USER="$2"; shift 2 ;;
        --dbname) DB="$2"; shift 2 ;;
        --archive-dir) ARCHIVE_DIR="$2"; shift 2 ;;
        --max-age) MAX_AGE="$2"; shift 2 ;;
        --max-lag-segments) MAX_LAG_SEGMENTS="$2"; shift 2 ;;
        --segment-size) SEG_SIZE="$2"; shift 2 ;;
        --files) CHECK_FILES_MAX="$2"; shift 2 ;;
        --state-file) STATE_FILE="$2"; shift 2 ;;
        --json) JSON=1; shift ;;
        --nagios) NAGIOS=1; shift ;;
        --quiet|-q) QUIET=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) usage; err "check_archive.sh: unknown option '$1'"; exit 2 ;;
    esac
done

[ -n "$ARCHIVE_DIR" ] && CHECK_FILES=1

PROBLEMS=()
WARNINGS=()
OKS=()
NOTES=()
crit()  { PROBLEMS+=("$1"); }
warn_() { WARNINGS+=("$1"); }
ok()    { OKS+=("$1"); }

# --- connection --------------------------------------------------------------
PGBIN_DIR=""
if command -v pgbin >/dev/null 2>&1; then PGBIN_DIR="$(pgbin 2>/dev/null || true)"; fi
if [ -z "$PGBIN_DIR" ]; then
    if command -v psql >/dev/null 2>&1; then PGBIN_DIR="$(dirname "$(command -v psql)")"
    else
        err "check_archive.sh: no PostgreSQL client found (psql). Set PGBIN to the bin directory."
        printf 'ARCHIVE UNKNOWN - psql not found\n' 2>/dev/null || true
        exit 2
    fi
fi
PSQL="${PGBIN_DIR}/psql"
[ -x "$PSQL" ] || { err "check_archive.sh: ${PSQL} is not executable"; exit 2; }

q() {
    "$PSQL" -X -q -A -t -F '|' -v ON_ERROR_STOP=1 \
        -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -c "$1" 2>/dev/null
}

if ! q 'SELECT 1' >/dev/null; then
    err "check_archive.sh: cannot connect to PostgreSQL at ${HOST}:${PORT} as ${USER}"
    err "  an unreachable server is UNKNOWN, not healthy (exit 2)"
    if [ "$JSON" = "1" ]; then
        printf '{"produced_by":"check_archive.sh","status":"UNKNOWN","exit_code":2,'
        printf '"checked_at":"%s",' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        printf '"server":{"host":"%s","port":"%s","user":"%s"},' \
            "$(json_escape "$HOST")" "$PORT" "$(json_escape "$USER")"
        printf '"error":"cannot connect to this server as this user"}\n'
    elif [ "$NAGIOS" = "1" ]; then
        printf 'ARCHIVE UNKNOWN - cannot connect to %s:%s as %s\n' "$HOST" "$PORT" "$USER"
    fi
    exit 2
fi

IN_RECOVERY="$(q 'SELECT pg_is_in_recovery()')"

# ---------------------------------------------------------------------------
# Read the server's own view of archiving.
# ---------------------------------------------------------------------------
CONF="$(q "SELECT current_setting('archive_mode'),
                  current_setting('wal_level'),
                  EXTRACT(EPOCH FROM current_setting('archive_timeout')::interval)::bigint,
                  current_setting('archive_command')")"
IFS='|' read -r ARCHIVE_MODE WAL_LEVEL ARCHIVE_TIMEOUT ARCHIVE_COMMAND <<<"$CONF"

STATS="$(q "SELECT archived_count,
                    COALESCE(last_archived_wal,''),
                    COALESCE(EXTRACT(EPOCH FROM last_archived_time)::bigint::text,''),
                    failed_count,
                    COALESCE(last_failed_wal,''),
                    COALESCE(EXTRACT(EPOCH FROM last_failed_time)::bigint::text,''),
                    COALESCE(EXTRACT(EPOCH FROM stats_reset)::bigint::text,''),
                    COALESCE(EXTRACT(EPOCH FROM (now() - last_archived_time))::bigint::text,'')
              FROM pg_stat_archiver")"
IFS='|' read -r ARCHIVED_COUNT LAST_ARCHIVED_WAL LAST_ARCHIVED_EPOCH \
                FAILED_COUNT LAST_FAILED_WAL LAST_FAILED_EPOCH \
                STATS_RESET_EPOCH ARCHIVE_AGE <<<"$STATS"
ARCHIVED_COUNT="${ARCHIVED_COUNT:-0}"
FAILED_COUNT="${FAILED_COUNT:-0}"

if [ "$IN_RECOVERY" = "t" ]; then
    # A standby archives the WAL it receives; "current WAL" there is the replay
    # position, because pg_current_wal_lsn() raises an error during recovery.
    CUR_LSN="$(q 'SELECT pg_last_wal_replay_lsn()')"
    NOTES+=("this server is in recovery (a standby); the comparison uses the replayed WAL position")
else
    CUR_LSN="$(q 'SELECT pg_current_wal_lsn()')"
fi
CUR_SEGMENT="$(q "SELECT pg_walfile_name('${CUR_LSN}'::pg_lsn)")"
[ -n "$CUR_SEGMENT" ] || CUR_SEGMENT="$(lsn_to_segment "$CUR_LSN" 1 2>/dev/null || printf 'UNKNOWN')"
[ -n "$LAST_ARCHIVED_EPOCH" ] && [ -n "$STATS_RESET_EPOCH" ] && \
  [ "$STATS_RESET_EPOCH" -gt "$(( $(date +%s) - 3600 ))" ] && \
  NOTES+=("pg_stat_archiver was reset at $(iso_from_epoch "$STATS_RESET_EPOCH"), less than an hour ago: the counters below cover only the time since then")

# ---------------------------------------------------------------------------
# CHECK 1 - is archiving switched on, and can the command fail?
# ---------------------------------------------------------------------------
case "$ARCHIVE_MODE" in
    on|always)
        ok "archive_mode = ${ARCHIVE_MODE} (wal_level=${WAL_LEVEL})"
        [ "$ARCHIVE_MODE" = "always" ] && \
            NOTES+=("archive_mode=always: WAL is archived even while this server is in recovery")
        ;;
    *)
        crit "archive_mode = ${ARCHIVE_MODE}: WAL archiving is OFF. There is no point-in-time recovery window at all - a base backup only gets you back to the moment it was taken."
        ;;
esac

case "$ARCHIVE_COMMAND" in
    ""|"(disabled)")
        crit "archive_command is empty while archive_mode is on: nothing will ever be archived"
        ;;
    *'|| true'*|*'||true'*|*'; exit 0'*|*';exit 0'*|*'|| /bin/true'*|*'|| :'*)
        crit "archive_command contains '|| true' or 'exit 0': it can NEVER report failure. Every segment is marked archived whether or not it was written, so no counter on this server can be trusted. Fix this first - see docs/COMMON-FAILURES.md, 'archive_command that always succeeds'."
        ;;
    *)
        ok "archive_command is set and reports failure normally"
        ;;
esac

# ---------------------------------------------------------------------------
# CHECK 2 - has anything EVER been archived?
# ---------------------------------------------------------------------------
if [ "$ARCHIVED_COUNT" = "0" ] || [ -z "$LAST_ARCHIVED_WAL" ]; then
    crit "archived_count=${ARCHIVED_COUNT} and last_archived_wal='${LAST_ARCHIVED_WAL}': no WAL segment has ever reached the archive. The archive chain does not exist yet, so no point-in-time recovery is possible."
else
    ok "archived_count=${ARCHIVED_COUNT}  last_archived_wal=${LAST_ARCHIVED_WAL}"
fi

# ---------------------------------------------------------------------------
# CHECK 3 - did the MOST RECENT attempt fail?
#    last_failed_time >= last_archived_time means the newest event in the
#    archiver's history is a failure: the hole is at the end of the chain.
# ---------------------------------------------------------------------------
if [ -n "$LAST_FAILED_EPOCH" ]; then
    if [ -z "$LAST_ARCHIVED_EPOCH" ] || [ "$LAST_FAILED_EPOCH" -ge "$LAST_ARCHIVED_EPOCH" ]; then
        crit "the MOST RECENT archive attempt FAILED: last_failed_wal=${LAST_FAILED_WAL} at $(iso_from_epoch "$LAST_FAILED_EPOCH") (last success: ${LAST_ARCHIVED_WAL:-none}). Everything committed after the last good segment is outside your recovery window."
    else
        warn_ "a PAST archive attempt failed: last_failed_wal=${LAST_FAILED_WAL} at $(iso_from_epoch "$LAST_FAILED_EPOCH"); archiving succeeded later, at $(iso_from_epoch "$LAST_ARCHIVED_EPOCH"). Confirm the hole was filled - passing --archive-dir makes this script check it."
    fi
else
    ok "no archive failure has ever been recorded (last_failed_time is NULL)"
fi

# ---------------------------------------------------------------------------
# CHECK 4 - failed_count growth since the previous run (needs --state-file).
#    failed_count is cumulative and nothing compares it, so a failure that has
#    already been retried successfully leaves no trace anywhere.
# ---------------------------------------------------------------------------
if [ -n "$STATE_FILE" ]; then
    prev_failed=""
    if [ -r "$STATE_FILE" ]; then
        prev_failed="$(sed -n 's/^FAILED_COUNT=//p' "$STATE_FILE" | head -1)"
    fi
    if [ -n "$prev_failed" ]; then
        delta=$(( FAILED_COUNT - prev_failed ))
        if [ "$delta" -gt 0 ]; then
            warn_ "failed_count grew by ${delta} since the previous check (${prev_failed} -> ${FAILED_COUNT}): a segment had to be retried. Find out why even though it eventually succeeded."
        else
            ok "failed_count unchanged since the previous check (${FAILED_COUNT})"
        fi
    fi
    _sd="$(dirname "$STATE_FILE")"
    mkdir -p "$_sd" 2>/dev/null || true
    if {
        printf 'FAILED_COUNT=%s\n' "$FAILED_COUNT"
        printf 'ARCHIVED_COUNT=%s\n' "$ARCHIVED_COUNT"
        printf 'CHECKED_AT=%s\n' "$(date +%s)"
        printf 'LAST_ARCHIVED_WAL=%s\n' "${LAST_ARCHIVED_WAL:-}"
    } > "${STATE_FILE}.tmp.$$" 2>/dev/null; then
        mv -f "${STATE_FILE}.tmp.$$" "$STATE_FILE" 2>/dev/null || rm -f "${STATE_FILE}.tmp.$$" 2>/dev/null
    else
        rm -f "${STATE_FILE}.tmp.$$" 2>/dev/null
    fi
fi

# ---------------------------------------------------------------------------
# CHECK 5 - how far behind is the archive?
#    The recoverable point is the end of the last FULLY ARCHIVED segment, not
#    the current WAL position. This number is your real RPO.
# ---------------------------------------------------------------------------
LAG_SEGMENTS="unknown"
if [ -n "$LAST_ARCHIVED_WAL" ] && [ -n "$CUR_SEGMENT" ] && [ "$CUR_SEGMENT" != "UNKNOWN" ]; then
    ka="$(segment_to_num "$LAST_ARCHIVED_WAL" 2>/dev/null || true)"
    kb="$(segment_to_num "$CUR_SEGMENT" 2>/dev/null || true)"
    if [ -n "$ka" ] && [ -n "$kb" ]; then
        LAG_SEGMENTS="$(awk -v a="$ka" -v b="$kb" '
            function dec(s,   i,c,d,val) { val = 0
                for (i = 1; i <= length(s); i++) {
                    c = substr(s, i, 1); d = index("0123456789", c) - 1
                    val = val * 10 + d }
                return val }
            BEGIN{
                sa = dec(substr(a,11,10)) * 256 + dec(substr(a,21,10))
                sb = dec(substr(b,11,10)) * 256 + dec(substr(b,21,10))
                ta = dec(substr(a,1,10)); tb = dec(substr(b,1,10))
                printf "%.0f", (tb - ta) * 4294967296 + (sb - sa)
            }')"
        if [ "${LAG_SEGMENTS:-0}" -gt "$MAX_LAG_SEGMENTS" ]; then
            crit "the archiver is ${LAG_SEGMENTS} segments BEHIND the current WAL position (last archived ${LAST_ARCHIVED_WAL}, current ${CUR_SEGMENT}, limit ${MAX_LAG_SEGMENTS}). Every un-archived segment is outside the recovery window: if the host dies now, that WAL dies with it."
        else
            ok "archive lag ${LAG_SEGMENTS} segment(s) behind ${CUR_SEGMENT} (limit ${MAX_LAG_SEGMENTS})"
        fi
    fi
else
    NOTES+=("archive lag not computed (no last_archived_wal or no current WAL position)")
fi

# ---------------------------------------------------------------------------
# CHECK 6 - is the archive STALE?
# ---------------------------------------------------------------------------
if [ -z "$MAX_AGE" ]; then
    if [ -n "$ARCHIVE_TIMEOUT" ] && [ "$ARCHIVE_TIMEOUT" -gt 0 ] 2>/dev/null; then
        MAX_AGE=$(( ARCHIVE_TIMEOUT * 3 ))
        [ "$MAX_AGE" -lt 900 ] && MAX_AGE=900
    else
        MAX_AGE=900
    fi
fi
if [ -n "$ARCHIVE_AGE" ]; then
    if [ "$ARCHIVE_AGE" -gt "$MAX_AGE" ]; then
        crit "the last successful archive was ${ARCHIVE_AGE}s ago (limit ${MAX_AGE}s): the archive is STALE. Either archiving is stuck, or this cluster is idle and archive_timeout is not set - in which case your real RPO is this number, not the one in the runbook."
    else
        ok "last successful archive ${ARCHIVE_AGE}s ago (limit ${MAX_AGE}s)"
    fi
else
    crit "last_archived_time is NULL: no successful archive has a recorded time"
fi
if [ "${ARCHIVE_TIMEOUT:-0}" = "0" ]; then
    NOTES+=("archive_timeout = 0: on an idle cluster a segment is archived only once it fills. Set archive_timeout to bound the worst case - see docs/BACKUP-STRATEGY.md.")
fi

# ---------------------------------------------------------------------------
# CHECK 7 - verify the archive ON DISK (only with --archive-dir)
# ---------------------------------------------------------------------------
MISSING_LAST_WAL=""
GAP_SEGMENT=""
ZERO_FILES=0
WRONG_SIZE=0
TEMP_LEFTOVERS=0
ARCHIVE_FILE_COUNT=0
OLDEST_ARCHIVED=""
NEWEST_ON_DISK=""

if [ "$CHECK_FILES" = "1" ]; then
    if [ ! -d "$ARCHIVE_DIR" ]; then
        crit "--archive-dir ${ARCHIVE_DIR} is not a directory readable by $(id -un): the server says segments are archived, but this path has nothing in it"
    else
        # One listing pass. `ls -ln` is used (numeric uid/gid) so the field
        # positions are stable; column 5 is the size and the last field is the
        # name. WAL segment names never contain spaces.
        SEGROWS="$(ls -ln "$ARCHIVE_DIR" 2>/dev/null | awk 'NR>1 && NF>=9 { print $NF "|" $5 }')"
        SEGLIST=""
        while IFS='|' read -r fname fsize; do
            [ -n "$fname" ] || continue
            case "$fname" in
                *.pgdrill-tmp.*) TEMP_LEFTOVERS=$(( TEMP_LEFTOVERS + 1 )); continue ;;
                *.history|*.partial|*.backup|*.tmp) continue ;;
            esac
            case "$fname" in
                [0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]) ;;
                *) continue ;;
            esac
            [ "${fsize:-0}" -eq 0 ] && ZERO_FILES=$(( ZERO_FILES + 1 ))
            [ "${fsize:-0}" -gt 0 ] && [ "${fsize:-0}" -ne "$SEG_SIZE" ] && WRONG_SIZE=$(( WRONG_SIZE + 1 ))
            SEGLIST="${SEGLIST}${fname}"$'\n'
        done <<<"$SEGROWS"

        ARCHIVE_FILE_COUNT="$(printf '%s' "$SEGLIST" | grep -c . || true)"
        ARCHIVE_FILE_COUNT="${ARCHIVE_FILE_COUNT:-0}"

        if [ "$ARCHIVE_FILE_COUNT" -gt 0 ]; then
            SORTED="$(printf '%s' "$SEGLIST" | sort)"
            OLDEST_ARCHIVED="$(printf '%s\n' "$SORTED" | head -1)"
            NEWEST_ON_DISK="$(printf '%s\n' "$SORTED" | tail -1)"

            # ------------------------------------------------------------------
            # GAP DETECTION, and the trap that made the first version of this
            # check silently useless.
            #
            # The obvious implementation converts each 24-hex-digit segment name
            # to a number and looks for a jump. That does NOT work: a 24-digit
            # hex value is 64 bits being pushed through a double, whose mantissa
            # holds 53. Near 2^64 the spacing between representable doubles is
            # 4096, so ...0001, ...0002 and ...0003 all convert to the same
            # number, `v == prev + 1` is trivially true, and a DELETED SEGMENT IN
            # THE MIDDLE is reported as a healthy chain. This was caught by
            # deliberately deleting segment 2 from a copy of a real archive and
            # watching the check say "no gap".
            #
            # So: use segment_to_num(), which returns the name as three
            # zero-padded decimal fields. Each field is below 2^32, where
            # doubles are exact, and the fields combine into a segment index
            # (timeline, log id, segment number) that is still exact.
            # ------------------------------------------------------------------
            SEGINDEX_ROWS=""
            while IFS= read -r _seg; do
                [ -n "$_seg" ] || continue
                _key="$(segment_to_num "$_seg" 2>/dev/null || true)"
                [ -n "$_key" ] || continue
                # NOTE: the arithmetic is on ONE line on purpose. In awk a
                # newline inside the argument list of printf is treated as a
                # statement terminator by mawk, so
                #     printf "%.0f",\n  (a) * 256\n  + c
                # silently drops the "+ c" term. That is how the first version
                # of this fix computed the same index for every segment and
                # still reported a healthy chain.
                _idx="$(awk -v k="$_key" '
                    function dec(s,   i,c,d,v) { v = 0
                        for (i = 1; i <= length(s); i++) {
                            c = substr(s, i, 1); d = index("0123456789", c) - 1
                            v = v * 10 + d }
                        return v }
                    BEGIN { printf "%.0f", (dec(substr(k,1,10)) * 4294967296 + dec(substr(k,11,10))) * 256 + dec(substr(k,21,10)) }')"
                SEGINDEX_ROWS="${SEGINDEX_ROWS}${_idx} ${_seg}"$'\n'
            done <<<"$SORTED"

            # --files N with N > 0 narrows the scan to the NEWEST N segments,
            # which is where a fresh gap is most likely; 0 (the default) checks
            # the whole directory, because a hole anywhere costs you every
            # recovery target after it.
            if [ "${CHECK_FILES_MAX:-0}" -gt 0 ] 2>/dev/null; then
                CHECKED_ROWS="$(printf '%s' "$SEGINDEX_ROWS" | sort -k1,1n | tail -n "$CHECK_FILES_MAX")"
            else
                CHECKED_ROWS="$(printf '%s' "$SEGINDEX_ROWS" | sort -k1,1n)"
            fi

            # awk prints the name of the LAST segment before the hole; the shell
            # works out which name is missing, so the message can name it.
            GAP_AFTER="$(printf '%s\n' "$CHECKED_ROWS" | awk '
                NF < 2 { next }
                NR == 1 { prev = $1 + 0; next }
                { v = $1 + 0
                  if (v > prev + 1) { print prev; exit }
                  prev = v }' 2>/dev/null || true)"

            GAP_SEGMENT=""
            if [ -n "$GAP_AFTER" ]; then
                _prevname="$(printf '%s\n' "$CHECKED_ROWS" | awk -v p="$GAP_AFTER" '$1 + 0 == p + 0 { print $2 }' | head -1)"
                [ -n "$_prevname" ] && GAP_SEGMENT="$(segment_add "$_prevname" 1 2>/dev/null || printf '?')"
            fi

            # How many holes there are in total, so the message can say whether
            # this is one missing segment or a systematically truncated archive.
            GAP_COUNT="$(printf '%s\n' "$CHECKED_ROWS" | awk '
                NF < 2 { next }
                NR == 1 { prev = $1 + 0; next }
                { v = $1 + 0
                  if (v > prev + 1) n += (v - prev - 1)
                  prev = v }
                END { printf "%d", n + 0 }')"

            if [ -n "$GAP_SEGMENT" ]; then
                crit "GAP IN THE ARCHIVE: segment ${GAP_SEGMENT} is missing from ${ARCHIVE_DIR} while later segments are present (${GAP_COUNT} segment(s) missing in total, ${ARCHIVE_FILE_COUNT} present, ${OLDEST_ARCHIVED}..${NEWEST_ON_DISK}). Recovery stops at this hole, so every recovery target after it is unreachable no matter how much later WAL you hold."
            else
                ok "no gap in ${ARCHIVE_FILE_COUNT} archived segment(s) (${OLDEST_ARCHIVED}..${NEWEST_ON_DISK})"
            fi

            if [ -n "$LAST_ARCHIVED_WAL" ]; then
                if [ ! -f "${ARCHIVE_DIR}/${LAST_ARCHIVED_WAL}" ]; then
                    MISSING_LAST_WAL="yes"
                    crit "the server reports ${LAST_ARCHIVED_WAL} as archived, but ${ARCHIVE_DIR}/${LAST_ARCHIVED_WAL} does not exist. archive_command is writing somewhere other than the directory you are checking, or the mount went away."
                else
                    sz="$(wc -c < "${ARCHIVE_DIR}/${LAST_ARCHIVED_WAL}" 2>/dev/null | tr -d ' ')"
                    if [ "${sz:-0}" = "0" ]; then
                        crit "${LAST_ARCHIVED_WAL} in ${ARCHIVE_DIR} is ZERO BYTES. The server treats it as archived; recovery will fail on it. A zero-length segment is the classic signature of an interrupted copy."
                    elif [ -n "$sz" ] && [ "$sz" -ne "$SEG_SIZE" ]; then
                        crit "${LAST_ARCHIVED_WAL} is ${sz} bytes, expected ${SEG_SIZE}: truncated. It looks present and breaks recovery."
                    else
                        ok "${LAST_ARCHIVED_WAL} present in the archive, ${sz} bytes"
                    fi
                fi
            fi
        else
            crit "${ARCHIVE_DIR} contains no WAL segments at all: either the path is wrong or archiving has never succeeded into it"
        fi

        [ "$ZERO_FILES" -gt 0 ] && crit "${ZERO_FILES} zero-length file(s) in ${ARCHIVE_DIR}: an interrupted copy left a segment name with no content. Do not delete them until you know which segment is really missing."
        [ "$WRONG_SIZE" -gt 0 ] && crit "${WRONG_SIZE} segment file(s) in ${ARCHIVE_DIR} are not ${SEG_SIZE} bytes: truncated, or the cluster uses a different --wal-segsize (pass --segment-size)."
        [ "$TEMP_LEFTOVERS" -gt 0 ] && warn_ "${TEMP_LEFTOVERS} temporary *.pgdrill-tmp.* file(s) left in ${ARCHIVE_DIR}: harmless in themselves (they are never read as segments) but they mean an archive_command was killed mid-copy. Check the segment it was writing is present."
    fi
fi

# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------
STATUS="HEALTHY"
EXIT_CODE=0
if [ "${#PROBLEMS[@]}" -gt 0 ]; then
    STATUS="UNHEALTHY"; EXIT_CODE=1
elif [ "${#WARNINGS[@]}" -gt 0 ]; then
    STATUS="HEALTHY (with warnings)"; EXIT_CODE=0
fi

if [ "$JSON" = "1" ]; then
    printf '{\n'
    printf '  "produced_by": "check_archive.sh",\n'
    printf '  "status": "%s",\n' "$(json_escape "$STATUS")"
    printf '  "exit_code": %s,\n' "$EXIT_CODE"
    printf '  "checked_at": "%s",\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf '  "server": {"host": "%s", "port": "%s", "user": "%s"},\n' \
        "$(json_escape "$HOST")" "$PORT" "$(json_escape "$USER")"
    printf '  "in_recovery": %s,\n' "$([ "$IN_RECOVERY" = "t" ] && printf true || printf false)"
    printf '  "archive_mode": "%s",\n' "$(json_escape "$ARCHIVE_MODE")"
    printf '  "wal_level": "%s",\n' "$(json_escape "$WAL_LEVEL")"
    printf '  "archive_timeout_seconds": %s,\n' "${ARCHIVE_TIMEOUT:-0}"
    printf '  "archived_count": %s,\n' "$ARCHIVED_COUNT"
    printf '  "failed_count": %s,\n' "$FAILED_COUNT"
    printf '  "last_archived_wal": "%s",\n' "$(json_escape "$LAST_ARCHIVED_WAL")"
    printf '  "last_archived_time": "%s",\n' \
        "$([ -n "$LAST_ARCHIVED_EPOCH" ] && iso_from_epoch "$LAST_ARCHIVED_EPOCH")"
    printf '  "last_failed_wal": "%s",\n' "$(json_escape "$LAST_FAILED_WAL")"
    printf '  "last_failed_time": "%s",\n' \
        "$([ -n "$LAST_FAILED_EPOCH" ] && iso_from_epoch "$LAST_FAILED_EPOCH")"
    printf '  "archive_age_seconds": %s,\n' "${ARCHIVE_AGE:-null}"
    printf '  "current_wal_lsn": "%s",\n' "$(json_escape "$CUR_LSN")"
    printf '  "current_wal_segment": "%s",\n' "$(json_escape "$CUR_SEGMENT")"
    printf '  "lag_segments": %s,\n' "$LAG_SEGMENTS"
    printf '  "max_age_seconds": %s,\n' "$MAX_AGE"
    printf '  "max_lag_segments": %s,\n' "$MAX_LAG_SEGMENTS"
    printf '  "archive_dir": "%s",\n' "$(json_escape "$ARCHIVE_DIR")"
    printf '  "archive_file_count": %s,\n' "$ARCHIVE_FILE_COUNT"
    printf '  "oldest_archived": "%s",\n' "$(json_escape "$OLDEST_ARCHIVED")"
    printf '  "newest_archived": "%s",\n' "$(json_escape "$NEWEST_ON_DISK")"
    printf '  "first_gap_segment": "%s",\n' "$(json_escape "$GAP_SEGMENT")"
    printf '  "zero_length_files": %s,\n' "$ZERO_FILES"
    printf '  "wrong_size_files": %s,\n' "$WRONG_SIZE"
    printf '  "temp_leftovers": %s,\n' "$TEMP_LEFTOVERS"
    printf '  "problems": ['
    _first=1
    for it in "${PROBLEMS[@]:-}"; do [ -n "$it" ] || continue
        [ "$_first" = "1" ] || printf ', '; printf '"%s"' "$(json_escape "$it")"; _first=0; done
    printf '],\n'
    printf '  "warnings": ['
    _first=1
    for it in "${WARNINGS[@]:-}"; do [ -n "$it" ] || continue
        [ "$_first" = "1" ] || printf ', '; printf '"%s"' "$(json_escape "$it")"; _first=0; done
    printf '],\n'
    printf '  "checks_passed": ['
    _first=1
    for it in "${OKS[@]:-}"; do [ -n "$it" ] || continue
        [ "$_first" = "1" ] || printf ', '; printf '"%s"' "$(json_escape "$it")"; _first=0; done
    printf '],\n'
    printf '  "notes": ['
    _first=1
    for it in "${NOTES[@]:-}"; do [ -n "$it" ] || continue
        [ "$_first" = "1" ] || printf ', '; printf '"%s"' "$(json_escape "$it")"; _first=0; done
    printf ']\n}\n'
    exit "$EXIT_CODE"
fi

if [ "$NAGIOS" = "1" ]; then
    case "$STATUS" in
        HEALTHY) NAG="OK" ;;
        "HEALTHY (with warnings)") NAG="WARNING" ;;
        *) NAG="CRITICAL" ;;
    esac
    printf 'ARCHIVE %s - %s problem(s), %s warning(s) | archived=%s failed=%s lag_segments=%s archive_age=%ss\n' \
        "$NAG" "${#PROBLEMS[@]}" "${#WARNINGS[@]}" \
        "$ARCHIVED_COUNT" "$FAILED_COUNT" "$LAG_SEGMENTS" "${ARCHIVE_AGE:-0}"
    for it in "${PROBLEMS[@]:-}"; do [ -n "$it" ] && printf '%s\n' "$it"; done
    exit "$EXIT_CODE"
fi

if [ "$QUIET" = "1" ] && [ "$EXIT_CODE" = "0" ]; then
    exit 0
fi

printf 'WAL archive health: %s\n' "$STATUS"
printf 'host %s:%s  checked %s\n' "$HOST" "$PORT" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
printf -- '---------------------------------------------------------------------------\n'
for it in "${PROBLEMS[@]:-}"; do [ -n "$it" ] && printf 'CRIT  %s\n' "$it"; done
for it in "${WARNINGS[@]:-}"; do [ -n "$it" ] && printf 'WARN  %s\n' "$it"; done
for it in "${OKS[@]:-}";      do [ -n "$it" ] && printf 'OK    %s\n' "$it"; done
printf -- '---------------------------------------------------------------------------\n'
printf 'archived=%s failed=%s last_archived=%s lag=%s seg archive_age=%ss\n' \
    "$ARCHIVED_COUNT" "$FAILED_COUNT" "${LAST_ARCHIVED_WAL:-none}" \
    "$LAG_SEGMENTS" "${ARCHIVE_AGE:-?}"
for it in "${NOTES[@]:-}"; do [ -n "$it" ] && printf 'NOTE  %s\n' "$it"; done
printf 'RESULT %s (exit %s)\n' "$STATUS" "$EXIT_CODE"

# The machine-readable line. Deliberately spelled  HEALTH_STATUS=  and not
# STATUS=, so that a consumer merging every PGDR_EVENT line it sees cannot
# confuse "the archive is healthy" with "the drill passed".
event "EVENT=archive_health" "HEALTH_STATUS=$(printf '%s' "$STATUS" | tr ' ' '_')" \
      "HEALTH_EXIT=${EXIT_CODE}" \
      "PROBLEMS=${#PROBLEMS[@]}" "WARNINGS=${#WARNINGS[@]}" \
      "LAST_ARCHIVED_WAL=${LAST_ARCHIVED_WAL:-none}" "LAG_SEGMENTS=${LAG_SEGMENTS}" \
      "ARCHIVE_AGE_S=${ARCHIVE_AGE:-unknown}"

exit "$EXIT_CODE"
