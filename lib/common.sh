#!/usr/bin/env bash
# ===========================================================================
# pgdrill common library
# Part of the PostgreSQL Backup & PITR Restore-Drill Kit
#
# Sourced by the scripts in backup/, drill/ and monitoring/. It is deliberately
# small: logging, path/version resolution, port picking, permissions and a few
# time helpers. Nothing in here talks to the database for you.
#
# Environment variables understood here (all optional):
#   PGBIN         directory holding the PostgreSQL binaries
#                 (default: resolved from `pg_config --bindir`, then PATH,
#                  then /usr/lib/postgresql/<ver>/bin)
#   PG_OS_USER    OS user that owns the cluster and the backup files
#                 (default: postgres). Only used when running as root.
#   PGDR_QUIET    set to 1 to suppress informational logging
# ===========================================================================

# Guard against double-sourcing.
if [ -n "${PGDR_COMMON_SH_LOADED:-}" ]; then
    return 0 2>/dev/null || true
fi
PGDR_COMMON_SH_LOADED=1

set -o pipefail

# --- output -----------------------------------------------------------------
# Everything informational goes to stderr so that stdout stays parseable
# (report.py and monitoring consumers rely on this).

PGDR_QUIET="${PGDR_QUIET:-0}"

_pgdr_ts() { date -u '+%Y-%m-%dT%H:%M:%SZ'; }

log()  { [ "$PGDR_QUIET" = "1" ] && return 0; printf '%s [INFO ] %s\n' "$(_pgdr_ts)" "$*" >&2; }
warn() { printf '%s [WARN ] %s\n' "$(_pgdr_ts)" "$*" >&2; }
err()  { printf '%s [ERROR] %s\n' "$(_pgdr_ts)" "$*" >&2; }
die()  { err "$*"; exit 1; }

# Emit a machine-readable event line. The drill parses these.
# Format: PGDR_EVENT <key>=<value> [<key>=<value> ...]
event() { printf 'PGDR_EVENT %s\n' "$*" >&2; }

# --- time -------------------------------------------------------------------
# Seconds since the epoch with sub-second precision (float, e.g. 1717000000.123)
now_epoch() {
    local t
    t="$(date +%s.%N 2>/dev/null)" || t="$(date +%s)"
    case "$t" in
        *.*) printf '%s\n' "$t" ;;
        *)   printf '%s.000000000\n' "$t" ;;
    esac
}

# Wall clock in UTC, ISO-8601 with an explicit offset-less Z marker.
now_iso() { date -u '+%Y-%m-%dT%H:%M:%SZ'; }

# Timestamp suitable for PostgreSQL: always UTC with an explicit +00 offset.
# Never rely on the server's TimeZone setting for a recovery target.
now_pg_ts() { date -u '+%Y-%m-%d %H:%M:%S.%6N+00'; }

epoch_to_pg_ts() {
    # $1 = epoch seconds (float). Convert without depending on the local TZ.
    TZ=UTC date -d "@$(printf '%.6f' "$1")" '+%Y-%m-%d %H:%M:%S.%6N+00'
}

# Float arithmetic helpers (bash has no float maths).
fsub() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.3f", a-b}'; }
fadd() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.3f", a+b}'; }
fge()  { awk -v a="$1" -v b="$2" 'BEGIN{exit !(a>=b)}'; }
fle()  { awk -v a="$1" -v b="$2" 'BEGIN{exit !(a<=b)}'; }

# Human-readable duration from a float number of seconds.
fmt_dur() {
    awk -v s="$1" 'BEGIN{
        if (s < 0) s = 0;
        if (s < 60)      { printf "%.3fs", s }
        else if (s < 3600){ printf "%dm %.1fs", int(s/60), s-int(s/60)*60 }
        else             { printf "%dh %dm %.0fs", int(s/3600), int((s%3600)/60), s%60 }
    }'
}

# Bytes -> human readable.
fmt_bytes() {
    awk -v b="$1" 'BEGIN{
        split("B KiB MiB GiB TiB", u, " ");
        i = 1;
        while (b >= 1024 && i < 5) { b /= 1024; i++ }
        printf (i==1 ? "%d %s" : "%.1f %s"), b, u[i]
    }'
}

# Directory size in bytes.
dir_bytes() {
    # du -sb is GNU; fall back to du -sk.
    du -sb "$1" 2>/dev/null | awk '{print $1}' || \
        du -sk "$1" 2>/dev/null | awk '{print $1*1024}'
}

# --- prerequisites ----------------------------------------------------------

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

# Resolve the PostgreSQL binary directory. Prefers $PGBIN, then pg_config,
# then whatever is on PATH, then the Debian/Ubuntu layout.
pgbin() {
    if [ -n "${PGBIN:-}" ] && [ -x "${PGBIN}/pg_ctl" ]; then
        printf '%s\n' "$PGBIN"; return 0
    fi
    if command -v pg_config >/dev/null 2>&1; then
        local b; b="$(pg_config --bindir 2>/dev/null)"
        if [ -n "$b" ] && [ -x "$b/pg_ctl" ]; then printf '%s\n' "$b"; return 0; fi
    fi
    if command -v pg_ctl >/dev/null 2>&1; then
        dirname "$(command -v pg_ctl)"; return 0
    fi
    local d
    for d in /usr/lib/postgresql/*/bin /usr/local/pgsql/bin /opt/homebrew/opt/postgresql*/bin; do
        if [ -x "$d/pg_ctl" ]; then printf '%s\n' "$d"; return 0; fi
    done
    return 1
}

# Print the major version of a given binary set (e.g. 17).
pg_major_version() {
    local b; b="$(pgbin)" || return 1
    "$b/postgres" --version 2>/dev/null | sed -E 's/.* ([0-9]+)(\.[0-9]+)?.*/\1/'
}

# Ask the SERVER its version number (e.g. 170011). Requires a reachable server.
# Usage: server_version_num HOST PORT USER [DB]
server_version_num() {
    local b; b="$(pgbin)" || return 1
    "$b/psql" -X -q -A -t \
        -h "${1:-${PGHOST:-/var/run/postgresql}}" \
        -p "${2:-${PGPORT:-5432}}" \
        -U "${3:-${PGUSER:-postgres}}" \
        -d "${4:-postgres}" \
        -c 'SHOW server_version_num' 2>/dev/null | tr -d '[:space:]'
}

# True (exit 0) when the server version >= the given major version.
server_at_least() {
    local want="$1" got
    got="$(server_version_num "$2" "$3" "$4" "$5")"
    [ -n "$got" ] || return 1
    [ "$got" -ge "$(( want * 10000 ))" ]
}

# --- paths / permissions ----------------------------------------------------

# chown a path to $PG_OS_USER when we are root. No-op otherwise.
# Real clusters run as a non-root user; if you run these scripts as root and
# forget this, the restored cluster will refuse to start with a permission
# error (see docs/COMMON-FAILURES.md, failure mode 5).
pg_owner_give() {
    local path="$1" user="${PG_OS_USER:-postgres}"
    [ -e "$path" ] || return 0
    if [ "$(id -u)" = "0" ]; then
        if id -u "$user" >/dev/null 2>&1; then
            chown -R "$user":"$user" "$path" 2>/dev/null || \
                warn "could not chown $path to $user"
        else
            warn "PG_OS_USER=$user does not exist; leaving ownership of $path alone"
        fi
    fi
}

# Set file modes for the whole kit: directories 755, *.sh and *.py 755 (they
# are meant to be run), everything else 644. Run at install time.
#
# This walks the tree with python3 rather than `find`. Two reasons, both
# learned the hard way:
#   * `find` silently under-reports files on some overlay/FUSE filesystems and
#     in containers with unusual mount setups, so a `find -exec chmod` sweep
#     can leave a script non-executable while looking like it worked. A script
#     the buyer cannot run is a support ticket.
#   * the python3 walk chmods inside the same traversal that lists the files, so
#     what was changed is exactly what was seen.
# find(1) is still used as a fallback when python3 is unavailable.
install_perms() {
    local dir="$1"
    [ -d "$dir" ] || die "install_perms: $dir is not a directory"

    if command -v python3 >/dev/null 2>&1; then
        python3 - "$dir" <<'PYPERMS'
import os, sys
root = sys.argv[1]
n = 0
for dirpath, dirnames, filenames in os.walk(root):
    dirnames.sort()
    try:
        os.chmod(dirpath, 0o755)
    except OSError as e:
        print("chmod %s: %s" % (dirpath, e), file=sys.stderr)
    for name in sorted(filenames):
        full = os.path.join(dirpath, name)
        mode = 0o755 if name.endswith(('.sh', '.py')) else 0o644
        try:
            os.chmod(full, mode)
            n += 1
        except OSError as e:
            print("chmod %s: %s" % (full, e), file=sys.stderr)
print("install_perms: %d files under %s" % (n, root), file=sys.stderr)
PYPERMS
        return 0
    fi

    warn "python3 not found; falling back to find(1) for the permission sweep"
    find "$dir" -type d -print 2>/dev/null | while read -r d; do chmod 755 "$d"; done
    find "$dir" -type f -name '*.sh' -print 2>/dev/null | while read -r f; do chmod 755 "$f"; done
    find "$dir" -type f -name '*.py' -print 2>/dev/null | while read -r f; do chmod 755 "$f"; done
    find "$dir" -type f ! -name '*.sh' ! -name '*.py' -print 2>/dev/null | while read -r f; do chmod 644 "$f"; done
}

# A PGDATA must not be group- or world-accessible or the server refuses to start.
secure_pgdata() {
    local d="$1"
    [ -d "$d" ] || die "secure_pgdata: $d is not a directory"
    chmod 700 "$d"
    local f
    for f in postgresql.conf pg_hba.conf pg_ident.conf postgresql.auto.conf \
             backup_label recovery.signal standby.signal; do
        [ -e "$d/$f" ] && chmod 600 "$d/$f"
    done
    chmod 700 "$d/pg_wal" 2>/dev/null || true
    return 0
}

# --- ports ------------------------------------------------------------------

# Find a TCP port on 127.0.0.1 that nothing is listening on.
free_port() {
    local start="${1:-55000}" p
    for p in $(seq "$start" $((start + 200))); do
        if ! (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
            printf '%s\n' "$p"; return 0
        fi
        exec 3>&- 2>/dev/null || true
    done
    return 1
}

# Wait until a cluster on host:port answers, or timeout (seconds).
wait_for_server() {
    local host="$1" port="$2" user="$3" timeout="${4:-60}" t=0 b
    b="$(pgbin)" || return 1
    while [ "$t" -lt "$timeout" ]; do
        if "$b/pg_isready" -q -h "$host" -p "$port" -U "$user" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
        t=$(( t + 1 ))
    done
    return 1
}

# --- WAL helpers ------------------------------------------------------------
#
# PORTABILITY AND PRECISION - why there is a hand-rolled hex parser below.
#
# The obvious way to turn "00000001000000000000002A" into a number is
#     awk 'BEGIN{print strtonum("0x" $1)}'
# and it is wrong twice over.
#
#   1. strtonum() is a GAWK EXTENSION. Debian and Ubuntu install mawk as
#      /usr/bin/awk, where strtonum() does not exist and the expression dies
#      with "function strtonum never defined" - so every helper below would
#      return an empty string on the most common Linux distribution.
#
#   2. Even on gawk, a 24-hex-digit segment name is a 64-bit value being pushed
#      through an IEEE double, whose mantissa holds 53 bits. Near 2^64 the
#      spacing between representable doubles is 4096, so
#          000000010000000000000001  and  000000010000000000000002
#      both convert to 18446744073709551616 and compare EQUAL. A gap detector
#      built on that reports a healthy archive while a segment is missing.
#
# So: parse the hex by hand, and keep every intermediate field below 2^32,
# where doubles are exact.

# _hex2dec <hex-digits> -> decimal digits on stdout; non-zero exit on bad input.
# Pure POSIX awk: works with mawk, gawk and busybox awk.
_hex2dec() {
    [ -n "${1:-}" ] || return 1
    printf '%s\n' "$1" | awk '
        function hex2dec(s,   i, c, d, val, n) {
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
        { v = hex2dec($1); if (v < 0) exit 1; printf "%.0f\n", v }
    '
}

# _lsn_to_dec <X/Y LSN> -> decimal byte offset on stdout; non-zero on bad input.
# A real LSN is far below 2^53, so a double is exact for it.
_lsn_to_dec() {
    local hi lo
    case "${1:-}" in */*) ;; *) return 1 ;; esac
    hi="$(_hex2dec "${1%%/*}")" || return 1
    lo="$(_hex2dec "${1##*/}")" || return 1
    awk -v h="$hi" -v l="$lo" 'BEGIN{ printf "%.0f\n", h * 4294967296 + l }'
}

# Public wrappers around the LSN arithmetic, so scripts do not have to reach
# into a private helper. An LSN is a 64-bit byte offset, which is far below
# 2^53 in any real cluster, so a double represents it exactly.
lsn_to_dec() { _lsn_to_dec "${1:-}"; }

# True (exit 0) when LSN $1 is at or after LSN $2.
lsn_ge() {
    local a b
    a="$(lsn_to_dec "${1:-}")" || return 1
    b="$(lsn_to_dec "${2:-}")" || return 1
    awk -v a="$a" -v b="$b" 'BEGIN{ exit !(a >= b) }'
}

# Number of 16MB WAL segments between two LSNs (a LSN is 'X/Y' hex).
# Prints a float to stdout; negative means $1 is behind $2. "NaN" + exit 1 on
# malformed input, so a caller can tell "no distance" from "distance 0".
wal_segments_between() {
    local lsn_a="$1" lsn_b="$2" segsize="${3:-16777216}"
    local da db
    da="$(_lsn_to_dec "$lsn_a")" || { printf 'NaN\n'; return 1; }
    db="$(_lsn_to_dec "$lsn_b")" || { printf 'NaN\n'; return 1; }
    awk -v a="$da" -v b="$db" -v s="$segsize" 'BEGIN{ printf "%.2f", (b - a) / s }'
}

# WAL segment file name (24 hex chars) that contains the given LSN.
# Usage: lsn_to_segment 0/14EEA00 1  -> 000000010000000000000001
lsn_to_segment() {
    local lsn="$1" tli="${2:-1}" off
    off="$(_lsn_to_dec "$lsn")" || { printf 'UNKNOWN\n'; return 1; }
    awk -v off="$off" -v tli="$tli" 'BEGIN{
        seg = 16777216;
        logid = int(off / (seg * 256));
        segno = int(off / seg) % 256;
        printf "%08X%08X%08X\n", tli, logid, segno;
    }'
}

# A WAL segment name as a SORTABLE KEY: three zero-padded 10-digit decimal
# fields (timeline, log id, segment number). String comparison of two keys
# gives WAL order, and sort(1) sorts segment names correctly. Each field is
# below 2^32 so nothing is lost to floating point.
# NOTE: this is a string key, not a number. Do not do arithmetic on it; use
# segment_add() to step forward.
segment_to_num() {
    local s="${1:-}" tli log seg
    [ "${#s}" -eq 24 ] || { printf 'INVALID\n'; return 1; }
    tli="$(_hex2dec "${s:0:8}")"   || return 1
    log="$(_hex2dec "${s:8:8}")"   || return 1
    seg="$(_hex2dec "${s:16:8}")"  || return 1
    awk -v a="$tli" -v b="$log" -v c="$seg" \
        'BEGIN{ printf "%010.0f%010.0f%010.0f\n", a, b, c }'
}

# True (exit 0) when segment $1 is the same as or later than segment $2.
segment_ge() {
    local a b
    a="$(segment_to_num "$1")" || return 1
    b="$(segment_to_num "$2")" || return 1
    [ "$a" \> "$b" ] || [ "$a" = "$b" ]
}

# Step a segment name forward (or back) by $2 segments. Handles the carry from
# segment number into log id and from log id into timeline, and wraps the
# timeline the way PostgreSQL's segment arithmetic does for our purposes.
#   segment_add 0000000100000000000000FF 1 -> 000000010000000000000100
segment_add() {
    local s="${1:-}" n="${2:-1}" tli log seg tot newtli newtot
    [ "${#s}" -eq 24 ] || { printf 'INVALID\n'; return 1; }
    tli="$(_hex2dec "${s:0:8}")"  || return 1
    log="$(_hex2dec "${s:8:8}")"  || return 1
    seg="$(_hex2dec "${s:16:8}")" || return 1
    # NOTE: the awk variable is l_id, not log - `log` is a builtin function
    # name in awk and mawk refuses "can not command line assign to log".
    awk -v tli="$tli" -v l_id="$log" -v seg="$seg" -v n="$n" 'BEGIN{
        total  = (l_id + tli * 4294967296) * 256 + seg + n
        if (total < 0) total = 0
        segno  = total % 256
        logid  = int(total / 256) % 4294967296
        tl     = int(total / (256 * 4294967296))
        printf "%08X%08X%08X\n", tl, logid, segno
    }'
}

# Pretty-print a byte count as a WAL segment count, e.g. "3.42 segments".
segments_fmt() {
    awk -v s="$1" -v seg=16777216 'BEGIN{ printf "%.2f segments (%.1f MiB)", s/seg, s/1048576 }'
}
