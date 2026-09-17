#!/usr/bin/env python3
# ===========================================================================
# report.py - turn a restore drill run into a measured RPO/RTO report
# Part of the PostgreSQL Backup & PITR Restore-Drill Kit
#
# The drill (drill/restore_drill.sh) emits machine-readable lines as it runs:
#
#     PGDR_EVENT EVENT=<tag> KEY=VALUE KEY="value with spaces" ...
#
# This script reads those lines back - from a log file or from stdin - and
# produces the report you actually file: what recovery target was reached, how
# much data was in the last recovered commit (measured RPO), how long the
# outage lasted from simulated loss to a queryable cluster (measured RTO, split
# into copy and replay), which WAL segments were replayed, what each
# verification check compared, and a PASS/FAIL verdict.
#
# It is deliberately defensive about missing data. A run that died half way
# through produces a report that says which parts were NOT MEASURED and exits
# non-zero, rather than printing a confident number built from nothing.
#
# Usage:
#   ./report.py --log /var/log/pgdrill/drill.log
#   ./report.py --log drill.log --json
#   ./restore_drill.sh 2>&1 | ./report.py
#   ./report.py --log drill.log --expect-fail    # for --negative-control in CI
#
# Exit status:
#   0  verdict PASS (or FAIL when --expect-fail was given, i.e. the drill
#      failed exactly as a negative control should)
#   1  verdict FAIL
#   2  no usable drill output: nothing was measured
#   3  could not read the input
# ===========================================================================

import argparse
import json
import re
import shlex
import sys
from datetime import datetime, timezone

PROG = "report.py"
SEGMENT_RE = re.compile(r"^[0-9A-Fa-f]{24}$")


# ---------------------------------------------------------------------------
# parsing
# ---------------------------------------------------------------------------
def parse_events(stream):
    """Return (events, warnings).

    events is an ordered list of dicts. Every event carries a special
    '_tag' key (the value of EVENT=, or the first bare token if the producer
    used the older bare-tag form).
    """
    events = []
    warnings = []
    for lineno, raw in enumerate(stream, 1):
        line = raw.rstrip("\n").rstrip("\r")
        if line.startswith("\ufeff"):
            line = line.lstrip("\ufeff")
        if "PGDR_EVENT" not in line:
            continue
        # Tolerate the tag being preceded by a prefix (timestamp, [INFO ], etc).
        idx = line.find("PGDR_EVENT ")
        payload = line[idx + len("PGDR_EVENT "):].strip()
        if not payload:
            continue
        try:
            tokens = shlex.split(payload)
        except ValueError:
            # An unbalanced quote: fall back to whitespace splitting so a
            # single malformed line cannot lose the whole run.
            tokens = payload.split()
            warnings.append("line %d: unbalanced quoting, parsed loosely: %s"
                            % (lineno, payload[:120]))
        ev = {"_tag": None, "_line": lineno}
        bare = None
        for tok in tokens:
            if "=" in tok:
                key, _, value = tok.partition("=")
                if key and re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
                    ev[key] = value
                    continue
            if bare is None:
                bare = tok
        # The tag is the value of EVENT=... (the form restore_drill.sh and
        # check_archive.sh emit). Earlier producers used a bare leading tag, so
        # fall back to that, then to a known leading key. Getting this wrong
        # makes every event look untagged, and the report then says NOT
        # MEASURED for a run that measured everything - so it is spelled out
        # rather than done cleverly.
        ev["_tag"] = ev.get("EVENT") or bare
        if ev["_tag"] is None:
            for k in ("KIND", "STEP", "CHECK"):
                if k in ev:
                    ev["_tag"] = ev[k]
                    break
        events.append(ev)
    return events, warnings


def first(events, tag, key, default=None):
    for ev in events:
        if ev.get("_tag") == tag and key in ev:
            return ev[key]
    return default


def last(events, tag, key, default=None):
    out = default
    for ev in events:
        if ev.get("_tag") == tag and key in ev:
            out = ev[key]
    return out


def last_any(events, tag):
    out = None
    for ev in events:
        if ev.get("_tag") == tag:
            out = ev
    return out


def merge_tag(events, tag):
    """Merge every event carrying this tag. Later values win per key, but keys
    only present earlier are kept: a producer is allowed to emit a short
    summary line after a detailed one, and losing the detail would make the
    report claim a number was never measured when it was."""
    out = {}
    for ev in events:
        if ev.get("_tag") == tag:
            for k, v in ev.items():
                if k.startswith("_"):
                    continue
                out[k] = v
    return out


# ---------------------------------------------------------------------------
# formatting helpers
# ---------------------------------------------------------------------------
def fmt_seconds(value):
    """'2.412' -> '2.412 s (2s 412ms)'; None -> 'NOT MEASURED'."""
    if value is None or value == "":
        return "NOT MEASURED"
    try:
        s = float(value)
    except (TypeError, ValueError):
        return str(value)
    if s < 0:
        return "%.3f s (negative: the target predates the newest surviving commit)" % s
    if s < 60:
        return "%.3f s" % s
    if s < 3600:
        return "%d min %.1f s" % (int(s // 60), s - 60 * int(s // 60))
    return "%d h %d min" % (int(s // 3600), int((s % 3600) // 60))


def fmt_bytes(value):
    try:
        b = float(value)
    except (TypeError, ValueError):
        return str(value) if value not in (None, "") else "unknown"
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if b < 1024 or unit == "TiB":
            return ("%.0f %s" % (b, unit)) if unit == "B" else ("%.1f %s" % (b, unit))
        b /= 1024.0
    return "%.1f TiB" % b


def seg_num(name):
    """24-hex WAL segment name -> comparable integer."""
    if not name or not SEGMENT_RE.match(name):
        return None
    return int(name, 16)


def seg_range(start, end):
    """How many segments a start..end range spans, inclusive. None if unknown."""
    a, b = seg_num(start), seg_num(end)
    if a is None or b is None:
        return None
    return (b - a) + 1


# ---------------------------------------------------------------------------
# build the structured result
# ---------------------------------------------------------------------------
def build(events):
    drill = merge_tag(events, "drill")
    target = merge_tag(events, "target")
    pre = None
    post = None
    for ev in events:
        if ev.get("_tag") == "data" and ev.get("PHASE") == "pre":
            pre = ev
        elif ev.get("_tag") == "data" and ev.get("PHASE") == "post":
            post = ev
    backup = merge_tag(events, "backup")
    restore = merge_tag(events, "restore")
    replay = merge_tag(events, "replay")
    loss = merge_tag(events, "loss")
    rpo = merge_tag(events, "rpo")
    rto = merge_tag(events, "rto")
    health = merge_tag(events, "archive_health")
    result = merge_tag(events, "result")

    checks = []
    for ev in events:
        if ev.get("_tag") == "verify" and "CHECK" in ev:
            checks.append({
                "check": ev.get("CHECK"),
                "status": ev.get("STATUS"),
                "expected": ev.get("EXPECTED"),
                "actual": ev.get("ACTUAL"),
                "detail": ev.get("DETAIL"),
            })

    steps = []
    for ev in events:
        if ev.get("_tag") == "step":
            steps.append({
                "step": ev.get("STEP"),
                "status": ev.get("STATUS"),
                "duration_s": ev.get("DURATION_S"),
            })

    failed = [c for c in checks if c["status"] != "PASS"]
    measured_checks = [c for c in checks]
    skipped = [c for c in checks if c["status"] == "SKIPPED"]

    if result.get("STATUS") in ("PASS", "FAIL"):
        verdict = result["STATUS"]
    elif failed:
        verdict = "FAIL"
    elif not checks:
        verdict = "NOT MEASURED"
    else:
        verdict = "FAIL"       # checks ran but no RESULT line: the run was cut short

    if verdict != "PASS" and not failed and checks:
        failed = [{
            "check": "(no failing check recorded)",
            "status": "INCOMPLETE",
            "expected": "a RESULT line",
            "actual": "none - the drill stopped before it produced a verdict",
            "detail": "the log ends early; look at the last STEP line above",
        }]

    # replay range
    replayed_list = [s for s in (replay.get("REPLAYED_SEGMENTS") or "").split(",") if s]
    start_seg = replay.get("REPLAY_START_SEGMENT") or restore.get("REPLAY_START_SEGMENT")
    end_seg = replay.get("REPLAY_END_SEGMENT")
    span = seg_range(start_seg, end_seg)
    segments_served = replay.get("SEGMENTS_RESTORED")
    segments_missing = replay.get("SEGMENTS_MISSED")

    return {
        "produced_by": PROG,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "verdict": verdict,
        "mode": result.get("MODE") or drill.get("MODE") or "unknown",
        "drill": {
            "tool": drill.get("TOOL"),
            "started_at": drill.get("STARTED_AT"),
            "started_epoch": drill.get("STARTED_EPOCH"),
            "duration_s": result.get("DURATION_S"),
            "run_dir": result.get("RUN_DIR") or drill.get("RUN_DIR"),
            "pg_major": drill.get("PG_MAJOR"),
            "recovery_target_inclusive": drill.get("RECOVERY_TARGET_INCLUSIVE") or target.get("INCLUSIVE"),
            "expected_rows": {
                "before_target": pre.get("ORDER_ROWS") if pre else None,
                "after_target": post.get("ORDER_ROWS") if post else None,
                "static": pre.get("CUSTOMER_ROWS") if pre else None,
            },
        },
        "backup": {
            "dir": backup.get("BACKUP_DIR"),
            "size_bytes": backup.get("SIZE_BYTES"),
            "size_human": fmt_bytes(backup.get("SIZE_BYTES")),
            "duration_s": backup.get("DURATION_S"),
            "start_lsn": backup.get("START_LSN"),
            "end_lsn": backup.get("END_LSN"),
            "min_recovery_lsn": backup.get("MIN_RECOVERY_LSN"),
            "checkpoint_lsn": backup.get("CKPT_LSN"),
            "system_identifier": backup.get("SYSID"),
        },
        "rpo": {
            "target_ts": target.get("TARGET_TS") or rpo.get("TARGET_TS"),
            "target_lsn": target.get("TARGET_LSN"),
            "target_segment": target.get("TARGET_SEGMENT"),
            "target_origin": target.get("ORIGIN"),
            "newest_surviving_marker_ts": rpo.get("NEWEST_SURVIVING_MARKER_TS"),
            "newest_surviving_marker_id": rpo.get("NEWEST_SURVIVING_MARKER_ID"),
            "measured_rpo_s": rpo.get("MEASURED_RPO_S"),
            "measured_rpo_human": fmt_seconds(rpo.get("MEASURED_RPO_S")),
            "archive_age_at_target_s": rpo.get("ARCHIVE_AGE_AT_TARGET_S"),
            "last_recoverable_timestamp": target.get("TARGET_TS") or rpo.get("TARGET_TS"),
            "note": ("measured RPO = recovery target minus the commit time of the newest "
                     "surviving row; that is the amount of committed data the last "
                     "recoverable point sits behind the moment you asked for"),
        },
        "rto": {
            "loss_epoch": rto.get("LOSS_EPOCH") or loss.get("EPOCH"),
            "loss_at": loss.get("LOSS_AT"),
            "restore_start_epoch": rto.get("RESTORE_START_EPOCH"),
            "queryable_epoch": rto.get("QUERYABLE_EPOCH"),
            "promoted_epoch": rto.get("PROMOTED_EPOCH"),
            "measured_rto_s": rto.get("MEASURED_RTO_S"),
            "measured_rto_human": fmt_seconds(rto.get("MEASURED_RTO_S")),
            "measured_rto_promoted_s": rto.get("MEASURED_RTO_PROMOTED_S"),
            "measured_rto_promoted_human": fmt_seconds(rto.get("MEASURED_RTO_PROMOTED_S")),
            "restore_to_queryable_s": rto.get("RESTORE_TO_QUERYABLE_S"),
            "copy_s": rto.get("COPY_S"),
            "replay_s": rto.get("REPLAY_S"),
            "start_s": rto.get("START_S"),
            "excludes": rto.get("EXCLUDES"),
        },
        "wal_replay": {
            "restore_command": restore.get("RESTORE_COMMAND"),
            "replay_redo_lsn": replay.get("REPLAY_REDO_LSN") or restore.get("REPLAY_REDO_LSN"),
            "start_segment": start_seg,
            "end_segment": end_seg,
            "end_lsn": replay.get("REPLAYED_LSN"),
            "segments_span": span,
            "segments_served_by_restore_command": segments_served,
            "segments_requested_but_absent": segments_missing,
            "segments_replayed": replayed_list,
            "in_recovery_final": replay.get("IN_RECOVERY_FINAL"),
            "recorded_at": replay.get("RECORDED_AT"),
        },
        "archive_health": {
            "status": health.get("HEALTH_STATUS"),
            "exit_code": health.get("HEALTH_EXIT"),
            "problems": health.get("PROBLEMS"),
            "warnings": health.get("WARNINGS"),
            "last_archived_wal": health.get("LAST_ARCHIVED_WAL"),
            "lag_segments": health.get("LAG_SEGMENTS"),
            "archive_age_s": health.get("ARCHIVE_AGE_S"),
        },
        "checks": checks,
        "failed_checks": failed,
        "checks_summary": {
            "total": len(measured_checks),
            "passed": len([c for c in measured_checks if c["status"] == "PASS"]),
            "failed": len([c for c in checks if c["status"] == "FAIL"]),
            "skipped": len(skipped),
        },
        "steps": steps,
    }


# ---------------------------------------------------------------------------
# text report
# ---------------------------------------------------------------------------
def render_text(rep):
    out = []
    w = out.append
    line = "-" * 78
    w("RESTORE DRILL REPORT - RPO / RTO")
    w(line)
    w("verdict                  : %s" % rep["verdict"])
    w("mode                     : %s" % rep["mode"])
    w("drill started            : %s" % (rep["drill"]["started_at"] or "unknown"))
    w("drill duration           : %s" % fmt_seconds(rep["drill"]["duration_s"]))
    w("postgres major version   : %s" % (rep["drill"]["pg_major"] or "unknown"))
    w("run directory            : %s" % (rep["drill"]["run_dir"] or "unknown"))
    w("base backup              : %s (%s) taken in %s"
      % (rep["backup"]["dir"] or "unknown", rep["backup"]["size_human"],
         fmt_seconds(rep["backup"]["duration_s"])))
    w("")

    rpo = rep["rpo"]
    w("RPO - how far back the data goes")
    w(line)
    w("  recovery target requested    : %s" % (rpo["target_ts"] or "NOT MEASURED"))
    w("  target LSN / segment         : %s / %s"
      % (rpo["target_lsn"] or "?", rpo["target_segment"] or "?"))
    w("  last recoverable timestamp   : %s" % (rpo["last_recoverable_timestamp"] or "NOT MEASURED"))
    w("  newest surviving commit      : %s (marker id %s)"
      % (rpo["newest_surviving_marker_ts"] or "NOT MEASURED",
         rpo["newest_surviving_marker_id"] or "?"))
    w("  MEASURED RPO                 : %s" % rpo["measured_rpo_human"])
    w("  archive lag at the target    : %s"
      % (fmt_seconds(rpo["archive_age_at_target_s"]) if rpo["archive_age_at_target_s"] not in (None, "")
         else "NOT MEASURED"))
    w("  (%s)" % rpo["note"])
    w("")

    rto = rep["rto"]
    w("RTO - how long the outage lasted")
    w(line)
    w("  simulated loss at            : %s" % (rto["loss_at"] or rto["loss_epoch"] or "unknown"))
    w("  MEASURED RTO (loss -> first query served) : %s" % rto["measured_rto_human"])
    w("  MEASURED RTO (loss -> promoted, writable) : %s" % rto["measured_rto_promoted_human"])
    w("  breakdown: copy the backup %s | start+replay %s"
      % (fmt_seconds(rto["copy_s"]), fmt_seconds(rto["replay_s"])))
    w("  excludes: %s" % (rto["excludes"] or "unknown"))
    w("")

    wal = rep["wal_replay"]
    w("WAL replay - what actually came out of the archive")
    w(line)
    w("  restore_command             : %s" % (wal["restore_command"] or "unknown"))
    w("  replay started from LSN     : %s" % (wal["replay_redo_lsn"] or "unknown"))
    w("  segment range replayed      : %s .. %s%s"
      % (wal["start_segment"] or "?", wal["end_segment"] or "?",
         "  (%s segments)" % wal["segments_span"] if wal["segments_span"] else ""))
    w("  segments served by restore_command : %s" % (wal["segments_served_by_restore_command"] or "0"))
    w("  segments asked for but absent      : %s  (normal at the end of an archive)"
      % (wal["segments_requested_but_absent"] or "0"))
    if wal["segments_replayed"]:
        w("  segments replayed (unique, recorded by restore_command - not inferred):")
        segs = wal["segments_replayed"]
        for i in range(0, len(segs), 4):
            w("    " + "  ".join(segs[i:i + 4]))
    w("  last replayed LSN           : %s" % (wal["end_lsn"] or "unknown"))
    w("  still in recovery at the end: %s" % (wal["in_recovery_final"] or "unknown"))
    w("")

    ah = rep["archive_health"]
    w("Archive chain health, checked on the live cluster before the loss")
    w(line)
    w("  check_archive.sh            : %s (exit %s)"
      % (ah["status"] or "NOT RUN", ah["exit_code"] if ah["exit_code"] is not None else "?"))
    w("  last archived WAL           : %s (lag %s segment(s), age %s)"
      % (ah["last_archived_wal"] or "unknown", ah["lag_segments"] or "?",
         fmt_seconds(ah["archive_age_s"]) if ah["archive_age_s"] not in (None, "") else "?"))
    w("")

    cs = rep["checks_summary"]
    w("Verification - %d checks, %d passed, %d failed, %d skipped"
      % (cs["total"], cs["passed"], cs["failed"], cs["skipped"]))
    w(line)
    if rep["checks"]:
        w("  %-38s %-8s %-22s %s" % ("CHECK", "STATE", "EXPECTED", "ACTUAL"))
        for c in rep["checks"]:
            w("  %-38s %-8s %-22s %s"
              % (str(c["check"])[:38], str(c["status"]), str(c["expected"])[:22], str(c["actual"])))
    else:
        w("  NO CHECKS WERE RECORDED - the drill did not reach its verification phase.")
    w("")

    if rep["failed_checks"]:
        w("WHAT DIFFERS (this is the part to read)")
        w(line)
        for c in rep["failed_checks"]:
            w("  %s: %s" % (c["check"], c["status"]))
            w("    expected : %s" % c["expected"])
            w("    actual   : %s" % c["actual"])
            if c.get("detail"):
                w("    why it matters: %s" % c["detail"])
        w("")

    if rep["steps"]:
        w("Timeline")
        w(line)
        for s in rep["steps"]:
            w("  %-18s %-8s %s" % (s["step"], s["status"], fmt_seconds(s["duration_s"])))
        w("")

    w("SCOPE OF THESE NUMBERS - read before quoting them")
    w(line)
    w("  Measured: correctness of this restore on this machine, this data volume,")
    w("  this hardware, and the wall-clock time from simulated loss to a queryable")
    w("  cluster. NOT measured: offsite fetch time, production data volume, replay")
    w("  distance on your real storage, application cut-over, detection time and")
    w("  decision time. A local drill is a correctness test and a regression")
    w("  baseline; it is not a production RTO estimate. See docs/RPO-RTO.md.")
    w(line)
    w("RESULT: %s" % rep["verdict"])
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(
        prog=PROG,
        description="Turn a restore-drill log into an RPO/RTO report.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Reads PGDR_EVENT lines produced by drill/restore_drill.sh.")
    ap.add_argument("--log", "-l", metavar="FILE",
                    help="drill log to read (default: stdin)")
    ap.add_argument("--json", action="store_true",
                    help="print one JSON object instead of the text report")
    ap.add_argument("--out", metavar="FILE",
                    help="also write the report to this file")
    ap.add_argument("--expect-fail", action="store_true",
                    help="exit 0 when the verdict is FAIL. Use this for a "
                         "--negative-control run in CI: the drill is supposed to "
                         "fail, and a run that passes has broken the test.")
    ap.add_argument("--allow-incomplete", action="store_true",
                    help="exit 0 even when the verdict is NOT MEASURED")
    ap.add_argument("-q", "--quiet", action="store_true",
                    help="print nothing but the verdict line")
    args = ap.parse_args()

    if args.log:
        try:
            with open(args.log, "r", encoding="utf-8", errors="replace") as fh:
                events, warnings = parse_events(fh)
        except OSError as exc:
            sys.stderr.write("%s: cannot read %s: %s\n" % (PROG, args.log, exc))
            return 3
    else:
        events, warnings = parse_events(sys.stdin)

    rep = build(events)
    rep["source"] = args.log or "stdin"
    rep["parse_warnings"] = warnings

    if args.json:
        text = json.dumps(rep, indent=2, sort_keys=False)
    elif args.quiet:
        text = "RESULT: %s" % rep["verdict"]
    else:
        text = render_text(rep)
        if warnings:
            text += "\n\nPARSE WARNINGS\n" + "\n".join("  " + w for w in warnings)

    print(text)
    if args.out:
        try:
            with open(args.out, "w", encoding="utf-8") as fh:
                fh.write(text + "\n")
        except OSError as exc:
            sys.stderr.write("%s: cannot write %s: %s\n" % (PROG, args.out, exc))
            return 3

    verdict = rep["verdict"]
    if verdict == "PASS":
        return 0
    if verdict == "FAIL":
        if args.expect_fail:
            sys.stderr.write(
                "%s: verdict FAIL, which is the expected outcome for a negative "
                "control: the drill detected the mismatch and exited non-zero.\n" % PROG)
            return 0
        return 1
    # NOT MEASURED
    sys.stderr.write(
        "%s: no verification output found in the log - nothing was measured. "
        "This is not a pass.\n" % PROG)
    return 0 if args.allow_incomplete else 2


if __name__ == "__main__":
    sys.exit(main())
