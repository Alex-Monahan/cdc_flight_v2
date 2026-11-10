"""Command line of the oracle (`tools/oracle`). See docs/oracle.md.

Exit status: 0 equal, 1 different, 2 usage or error, 3 a side did not reach the position (refused).
Nothing secret is printed: the source password comes from the localpg home (or the conninfo from an environment
variable that is named, never echoed), and all output goes through the evidence redactor.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from cdc_v2.evidence import default_redactor

from cdc_v2 import lsn
from cdc_v2.deadline import Deadline, DeadlineExceeded

from . import mdexec
from .comparison import Destination, OracleConfig, OracleError, PositionNotReached, Source, compare
from .position import wait_for_applied

EXIT_EQUAL, EXIT_DIFFERENT, EXIT_ERROR, EXIT_TIMEOUT = 0, 1, 2, 3


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="oracle", description="Compare Postgres source tables with a MotherDuck "
                                "destination database (multisets, canonical forms per type).")
    s = p.add_argument_group("source")
    s.add_argument("--source-db", help="source database name (with --source-port)")
    s.add_argument("--source-port", type=int, help="port of a tools/localpg node, e.g. 55432 or 55433")
    s.add_argument("--source-role", default="admin", choices=["admin", "runtime", "bootstrap"],
                   help="localpg role (default admin)")
    s.add_argument("--pg-home", default=os.environ.get("CDC_V2_PG_HOME", "~/.cache/cdc_flight_v2/pg"),
                   help="tools/localpg home that holds the passwords (default $CDC_V2_PG_HOME)")
    s.add_argument("--source-conninfo-env", metavar="VAR",
                   help="read a libpq conninfo string from this environment variable instead (stage 2); it must "
                        "name the port")
    d = p.add_argument_group("destination")
    d.add_argument("--dest-db", required=True, help="MotherDuck database (cdc_v2_*)")
    d.add_argument("--position", help="the source position Q (X/Y) both sides must have reached; required unless "
                                      "--unpositioned")
    d.add_argument("--slot-name", help="slot whose applied position the destination must have reached")
    d.add_argument("--unpositioned", action="store_true",
                   help="compare without a position (never a gate result; the output says so)")
    d.add_argument("--wait", type=float, default=0.0, metavar="SECONDS",
                   help="first wait up to SECONDS until the destination has applied --position")
    o = p.add_argument_group("comparison")
    o.add_argument("--tables", help="comma-separated schema.table list (default: every user table)")
    o.add_argument("--exclude-schema", action="append", default=[], help="source schema pattern to leave out")
    o.add_argument("--chunk-rows", type=int, default=OracleConfig.chunk_rows)
    o.add_argument("--workers", type=int, default=OracleConfig.source_workers, help="source snapshot connections")
    o.add_argument("--sample", type=int, default=OracleConfig.sample_limit, help="sample rows per category")
    o.add_argument("--max-diff-buckets", type=int, default=OracleConfig.max_diff_buckets)
    p.add_argument("--json", metavar="FILE", help="write the full result as JSON ('-' for stdout)")
    p.add_argument("--quiet", action="store_true", help="no summary on stderr")
    return p


def source_from_args(args) -> Source:
    if args.source_conninfo_env:
        conninfo = os.environ.get(args.source_conninfo_env)
        if not conninfo:
            raise OracleError("environment variable %s is empty" % args.source_conninfo_env)
        if "port=" not in conninfo:
            raise OracleError("the conninfo must name the port")
        return Source(conninfo=conninfo)
    if not args.source_port or not args.source_db:
        raise OracleError("give --source-port and --source-db, or --source-conninfo-env")
    home = Path(args.pg_home).expanduser()
    with open(home / "secrets" / "passwords.json") as f:
        creds = json.load(f)[args.source_role]
    default_redactor().add(creds["password"])
    if args.source_role == "bootstrap":
        params = dict(host=str(home / "run"), port=args.source_port, dbname=args.source_db)
    else:
        params = dict(host="localhost", port=args.source_port, dbname=args.source_db, sslmode="verify-full",
                      sslrootcert=str(home / "tls" / "ca.crt"))
    params.update(user=creds["user"], password=creds["password"])
    return Source(**params)


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    red = default_redactor()
    try:
        source = source_from_args(args)
        dest = Destination(args.dest_db)
        position = lsn.parse(args.position) if args.position else None
        if args.wait and position is not None:
            d = Deadline.after(args.wait)
            con = mdexec.connect("md:%s?attach_mode=single" % args.dest_db, deadline=d)
            try:
                wait_for_applied(con, position, d, args.slot_name)
            finally:
                con.close()
        cfg = OracleConfig(chunk_rows=args.chunk_rows, source_workers=args.workers, sample_limit=args.sample,
                           max_diff_buckets=args.max_diff_buckets,
                           tables=[t.strip() for t in args.tables.split(",")] if args.tables else None,
                           exclude_source_schemas=tuple(args.exclude_schema))
        result = compare(source, dest, cfg, position=position, slot_name=args.slot_name,
                         unpositioned=args.unpositioned)
    except (PositionNotReached, DeadlineExceeded) as e:
        print(red.redact("oracle: %s" % e), file=sys.stderr)
        return EXIT_TIMEOUT
    except Exception as e:                            # noqa: BLE001 - reported, redacted
        print(red.redact("oracle: error: %s: %s" % (type(e).__name__, e)), file=sys.stderr)
        return EXIT_ERROR
    if args.json:
        body = json.dumps(red.redact_obj(result.to_dict()), indent=2, sort_keys=True, default=str)
        if args.json == "-":
            print(body)
        else:
            Path(args.json).write_text(body + "\n", encoding="utf-8")
    if not args.quiet:
        print(red.redact(result.summary()), file=sys.stderr)
    return EXIT_EQUAL if result.equal else EXIT_DIFFERENT


if __name__ == "__main__":
    sys.exit(main())
