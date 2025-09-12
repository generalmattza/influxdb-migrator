#!/usr/bin/env python3
"""
Copy time-series data from one InfluxDB v2 instance to another.
Reads credentials (url, org, token) from simple key-value .influx.toml files.

Expected .influx.toml format:
    url = "http://localhost:8086"
    token = "<auth token>"
    org = "my-org"
    timeout = 6000                # optional (ms)
    connection_pool_maxsize = 25  # optional
    auth_basic = false            # optional

Other keys are ignored.

Tips:
- Use --verify to count rows per window without writing.
- Use --dry-run to log detailed point information without writing.
- On PowerShell, quote relative starts like '--start "-4d"'.
- Use --tag-map to remap tag values: key=from->to, key=~/regex/->replacement, or key=prefix*->replacement (repeatable).
- Use --tag-inject to add new tags: new_tag=value or new_tag=old_tag:from->to, new_tag=old_tag:~/regex/->replacement, or new_tag=old_tag:prefix*->replacement (repeatable).
- Use --verbose to enable detailed logging of mappings and injections.
- Use --measurement multiple times to collect data from multiple measurements (e.g., --measurement heaters --measurement sensors).
- Use --measurement-regex to match measurements with a regex pattern (e.g., --measurement-regex "^(heaters|sensors)$"). Mutually exclusive with --measurement.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
import re
import toml
from typing import List, Tuple, Literal
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

from influxdb_client import InfluxDBClient, Point, WriteOptions

RFC3339_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")
DURATION_RE = re.compile(r"^-(\d+)([smhdw])$")
TAG_MAP_RE = re.compile(r"^(?P<key>[^=]+)=(?P<from>[^>]+)->(?P<to>.+)$")
NAME_MAP_RE = re.compile(r"^(?P<from>.+?)->(?P<to>.+)$")
TAG_INJECT_RE = re.compile(r"^(?P<new_key>[^=]+)=(?:(?P<source_key>[^:]+):)?(?P<from>[^>]+)->(?P<to>.+)$")


def parse_time(ts: str | None) -> str | None:
    if ts is None:
        return None
    ts = ts.strip()
    if ts == "now()":
        return ts
    if RFC3339_RE.match(ts):
        return ts
    if DURATION_RE.match(ts):
        return ts
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception as e:
        raise ValueError(f"Unrecognized time format: {ts}") from e


def parse_window(s: str) -> timedelta:
    m = re.match(r"^(\d+)([smhdw])$", s)
    if not m:
        raise ValueError("Window must be like '30m', '6h', '1d', '2w'")
    n = int(m.group(1))
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[m.group(2)]
    return timedelta(seconds=n * mult)


def load_influx_config(path: str) -> dict:
    cfg = toml.load(path)
    parsed = {
        "url": cfg.get("url"),
        "org": cfg.get("org"),
        "token": cfg.get("token"),
        "timeout": cfg.get("timeout"),
        "connection_pool_maxsize": cfg.get("connection_pool_maxsize"),
        "auth_basic": cfg.get("auth_basic"),
    }
    safe_log = {k: v for k, v in parsed.items() if k != "token"}
    logger.debug("Loaded Influx config from %s: %s", path, safe_log)
    return parsed


def build_flux(bucket: str, start: str, stop: str | None, measurements: list[str] | None, measurement_regex: str | None, tags: list[str] | None, fields: list[str] | None) -> str:
    lines = [
        f'from(bucket: "{bucket}")',
        f"  |> range(start: {start}" + (f", stop: {stop})" if stop else ")"),
    ]
    if measurement_regex:
        # Remove optional leading ~/ and trailing / if present
        regex = measurement_regex.strip()
        if regex.startswith("~/") and regex.endswith("/"):
            regex = regex[2:-1]
        lines.append(f"  |> filter(fn: (r) => r._measurement =~ /{regex}/)")
    elif measurements and len(measurements) > 0:
        ors = " or ".join([f'r._measurement == "{m}"' for m in measurements])
        lines.append(f"  |> filter(fn: (r) => {ors})")
    if tags:
        for f in tags:
            if "=~/" in f:
                k, regex = f.split("=~/", 1)
                regex = regex.rstrip("/")
                lines.append(f"  |> filter(fn: (r) => exists r.{k} and r.{k} =~ /{regex}/)")
            else:
                k, v = f.split("=", 1)
                lines.append(f"  |> filter(fn: (r) => exists r.{k} and r.{k} == \"{v}\")")
    if fields and len(fields) > 0:
        ors = " or ".join([f"r._field == \"{fld}\"" for fld in fields])
        lines.append(f"  |> filter(fn: (r) => {ors})")
    else:
        lines.append("  |> filter(fn: (r) => r._field =~ /.*/)")
    query = "\n".join(lines)
    logger.debug("Built flux query:\n%s", query)
    return query


MapRule = Tuple[Literal["exact", "regex", "wildcard"], str, object, str]
NameMapRule = Tuple[Literal["exact", "regex"], object, str]
InjectRule = Tuple[Literal["static", "exact", "regex", "wildcard"], str, str | None, object, str]


def parse_tag_maps(map_args: List[str] | None) -> List[MapRule]:
    """Parse --tag-map specs into rules.

    Supported forms (repeatable):
      --tag-map site=old->new                # exact match
      --tag-map host=~/^prod-(\d+)$/->prod-$1  # regex with backrefs
      --tag-map id=temperature*->control      # wildcard prefix match
    """
    rules: List[MapRule] = []
    if not map_args:
        return rules
    for s in map_args:
        m = TAG_MAP_RE.match(s)
        if not m:
            raise ValueError(f"Invalid --tag-map spec: {s}")
        tag = m.group("key").strip()
        frm = m.group("from").strip()
        to = m.group("to")
        if frm.startswith("~/") and frm.endswith("/"):
            pattern = re.compile(frm[2:-1])
            rules.append(("regex", tag, pattern, to))
        elif "*" in frm:
            pattern = re.compile("^" + re.escape(frm).replace("\\*", ".*") + "$")
            rules.append(("wildcard", tag, pattern, to))
        else:
            rules.append(("exact", tag, frm, to))
    return rules


def parse_name_maps(map_args: List[str] | None) -> List[NameMapRule]:
    """Parse name map specs into rules (for measurements or fields).

    Supported forms (repeatable):
      --measurement-map old->new                # exact match
      --measurement-map ~/^prod-(\d+)$/->prod-$1  # regex with backrefs
    """
    rules: List[NameMapRule] = []
    if not map_args:
        return rules
    for s in map_args:
        m = NAME_MAP_RE.match(s)
        if not m:
            raise ValueError(f"Invalid map spec: {s}")
        frm = m.group("from").strip()
        to = m.group("to")
        if frm.startswith("~/") and frm.endswith("/"):
            pattern = re.compile(frm[2:-1])
            rules.append(("regex", pattern, to))
        else:
            rules.append(("exact", frm, to))
    return rules


def parse_tag_injects(inject_args: List[str] | None) -> List[InjectRule]:
    """Parse --tag-inject specs into rules.

    Supported forms (repeatable):
      --tag-inject new_tag=value                # static tag
      --tag-inject new_tag=old_tag:old->new     # exact match from existing tag
      --tag-inject new_tag=old_tag:~/regex/->replacement  # regex from existing tag
      --tag-inject new_tag=old_tag:prefix*->replacement  # wildcard from existing tag
    """
    rules: List[InjectRule] = []
    if not inject_args:
        return rules
    for s in inject_args:
        m = TAG_INJECT_RE.match(s)
        if not m:
            if "=" in s and "->" not in s:
                new_key, value = s.split("=", 1)
                rules.append(("static", new_key.strip(), None, value.strip(), value.strip()))
                continue
            raise ValueError(f"Invalid --tag-inject spec: {s}")
        new_key = m.group("new_key").strip()
        source_key = m.group("source_key")
        frm = m.group("from").strip()
        to = m.group("to")
        if frm.startswith("~/") and frm.endswith("/"):
            pattern = re.compile(frm[2:-1])
            rules.append(("regex", new_key, source_key, pattern, to))
        elif "*" in frm:
            pattern = re.compile("^" + re.escape(frm).replace("\\*", ".*") + "$")
            rules.append(("wildcard", new_key, source_key, pattern, to))
        else:
            rules.append(("exact", new_key, source_key, frm, to))
    return rules


def apply_tag_maps(tag: str, value: str, rules: List[MapRule], verbose: bool = False) -> str:
    if not rules:
        return value
    for typ, t, from_obj, repl in rules:
        if t != tag:
            continue
        if typ == "exact":
            if value == from_obj:
                if verbose:
                    logger.debug("Tag remap: %s: '%s' -> '%s'", tag, value, repl)
                return repl
        else:  # regex or wildcard
            if from_obj.match(value):
                if typ == "wildcard":
                    prefix = from_obj.pattern.lstrip("^").rstrip(".*$").replace("\\", "")
                    if value.startswith(prefix):
                        new_value = repl + value[len(prefix):]
                        if verbose:
                            logger.debug("Tag remap: %s: '%s' -> '%s'", tag, value, new_value)
                        return new_value
                    return value
                else:  # regex
                    new_value = from_obj.sub(repl, value)
                    if new_value != value:
                        if verbose:
                            logger.debug("Tag remap: %s: '%s' -> '%s'", tag, value, new_value)
                        return new_value
    return value


def apply_tag_injects(point: Point, rec_values: dict, rules: List[InjectRule], verbose: bool) -> Point:
    if not rules:
        return point
    for typ, new_key, source_key, from_obj, to in rules:
        if typ == "static":
            point.tag(new_key, from_obj)
            if verbose:
                logger.debug("Injected static tag: %s=%s", new_key, from_obj)
        elif source_key in rec_values and rec_values[source_key] is not None:
            value = str(rec_values[source_key])
            if typ == "exact":
                if value == from_obj:
                    point.tag(new_key, to)
                    if verbose:
                        logger.debug("Injected tag: %s=%s (from %s=%s)", new_key, to, source_key, value)
            else:  # regex or wildcard
                if from_obj.match(value):
                    if typ == "wildcard":
                        prefix = from_obj.pattern.lstrip("^").rstrip(".*$").replace("\\", "")
                        if value.startswith(prefix):
                            new_value = to + value[len(prefix):]
                            point.tag(new_key, new_value)
                            if verbose:
                                logger.debug("Injected tag: %s=%s (from %s=%s)", new_key, new_value, source_key, value)
                    else:  # regex
                        new_value = from_obj.sub(to, value)
                        if new_value != value:
                            point.tag(new_key, new_value)
                            if verbose:
                                logger.debug("Injected tag: %s=%s (from %s=%s)", new_key, new_value, source_key, value)
    return point


def apply_name_maps(value: str, rules: List[NameMapRule], verbose: bool = False) -> str:
    if not rules:
        return value
    for typ, from_obj, repl in rules:
        if typ == "exact":
            if value == from_obj:
                if verbose:
                    logger.debug("Name remap: '%s' -> '%s'", value, repl)
                return repl
        else:  # regex
            new_value = from_obj.sub(repl, value)
            if new_value != value:
                if verbose:
                    logger.debug("Name remap: '%s' -> '%s'", value, new_value)
                return new_value
    return value


def record_to_point(rec, tag_rules: List[MapRule] | None = None, measurement_rules: List[NameMapRule] | None = None, field_rules: List[NameMapRule] | None = None, inject_rules: List[InjectRule] | None = None, verbose: bool = False) -> Point:
    skip = {"result", "table", "_start", "_stop", "_time", "_measurement", "_field", "_value"}
    measurement = rec.get_measurement()
    new_measurement = apply_name_maps(measurement, measurement_rules or [], verbose)
    p = Point(new_measurement)
    for k, v in rec.values.items():
        if k in skip or k.startswith("_") or v is None:
            continue
        after = apply_tag_maps(k, str(v), tag_rules or [], verbose)
        p.tag(k, after)
    p = apply_tag_injects(p, rec.values, inject_rules or [], verbose)
    field = rec.get_field()
    new_field = apply_name_maps(field, field_rules or [], verbose)
    p.field(new_field, rec.get_value())
    p.time(rec.get_time())
    return p


def point_to_string(p: Point) -> str:
    """Convert a Point to a human-readable string for logging."""
    tags = ", ".join(f"{k}={v}" for k, v in p._tags.items())
    return f"Point(measurement={p._name}, tags=[{tags}], field={p._fields}, time={p._time})"


def build_client(cfg: dict) -> InfluxDBClient:
    kwargs = {
        "url": cfg["url"],
        "org": cfg["org"],
        "token": cfg["token"],
        "timeout": cfg.get("timeout") or 60000,
    }
    if cfg.get("connection_pool_maxsize"):
        kwargs["connection_pool_maxsize"] = cfg.get("connection_pool_maxsize")
    if cfg.get("auth_basic") is not None:
        kwargs["auth_basic"] = bool(cfg.get("auth_basic"))
    return InfluxDBClient(**kwargs)


def copy_window(src_client: InfluxDBClient, dst_client: InfluxDBClient, flux: str, batch_size: int, org: str, dst_bucket: str, tag_rules: List[MapRule], measurement_rules: List[NameMapRule], field_rules: List[NameMapRule], inject_rules: List[InjectRule], verbose: bool, dry_run: bool = False) -> int:
    q = src_client.query_api()
    w = dst_client.write_api(write_options=WriteOptions(batch_size=batch_size)) if not dry_run else None
    count = 0
    batch: List[Point] = []
    for rec in q.query_stream(query=flux, org=org):
        try:
            if not rec or rec.get_measurement() is None or rec.get_field() is None or rec.get_time() is None:
                continue
        except AttributeError:
            continue
        point = record_to_point(rec, tag_rules, measurement_rules, field_rules, inject_rules, verbose)
        if dry_run:
            logger.info("Dry run: would write %s", point_to_string(point))
        else:
            batch.append(point)
        count += 1
        if not dry_run and len(batch) >= batch_size:
            w.write(record=batch, bucket=dst_bucket)
            batch.clear()
    if not dry_run and batch:
        w.write(record=batch, bucket=dst_bucket)
    if w:
        w.__del__()
    return count


def debug_argv() -> List[str]:
    """Return a sample argv list for debugging.
    Edit these values to suit your environment.

    Using a list bypasses shell parsing (so '-4d' won't be misinterpreted).
    """
    return [
        "--src-config", r".\.influx.plungecaster-old.toml",
        "--dst-config", r".\.influx.metsys-node1.toml",
        "--src-bucket", "plungecaster",
        "--dst-bucket", "plungecaster",
        "--start", "2025-07-22T15:00:00Z",
        "--stop", "2025-07-24T08:00:00Z",
        "--measurement-regex", "^(heaters|sensors)$",
        "--tag-map", "id=heaters*->control",
        "--tag-map", "device=PlungeCaster_Heater_ADSClient->CX-68ABF8",
        "--tag-inject", "env=production",
        "--tag-inject", "source=plunge_caster_heater_control_CX68ABF8",
        "--tag-inject", "panel_name=Plunge Caster Heater Control",
        "--measurement-map", "heaters->control",
        "--field-map", "temp->temperature",
        "--verbose",
        "--dry-run",
    ]


def run(argv: List[str] | None = None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--src-config", required=True, help="Path to source .influx.toml")
    ap.add_argument("--dst-config", required=True, help="Path to dest .influx.toml")
    ap.add_argument("--src-bucket", required=True)
    ap.add_argument("--dst-bucket", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--stop")
    ap.add_argument("--measurement", action="append", dest="measurements", help="Specific measurement to collect (repeatable)")
    ap.add_argument("--measurement-regex", help="Regex pattern to match measurements (mutually exclusive with --measurement)")
    ap.add_argument("--field", action="append", dest="fields")
    ap.add_argument("--tag", action="append", dest="tags")
    ap.add_argument("--window", default="6h")
    ap.add_argument("--batch-size", type=int, default=5000)
    ap.add_argument("--verify", action="store_true", help="Only query and print a count per window; do not write")
    ap.add_argument("--dry-run", action="store_true", help="Log detailed point information without writing")
    ap.add_argument("--tag-map", action="append", dest="tag_maps", help="Remap tag values: key=from->to, key=~/regex/->replacement, or key=prefix*->replacement (repeatable)")
    ap.add_argument("--measurement-map", action="append", dest="measurement_maps", help="Remap measurement names: from->to or ~/regex/->replacement (repeatable)")
    ap.add_argument("--field-map", action="append", dest="field_maps", help="Remap field names: from->to or ~/regex/->replacement (repeatable)")
    ap.add_argument("--tag-inject", action="append", dest="tag_injects", help="Inject new tags: new_tag=value or new_tag=old_tag:from->to, new_tag=old_tag:~/regex/->replacement, or new_tag=old_tag:prefix*->replacement (repeatable)")
    ap.add_argument("--verbose", action="store_true", help="Enable detailed logging of mappings and injections")
    args = ap.parse_args(argv)

    if args.verify and args.dry_run:
        print("Error: --verify and --dry-run cannot be used together.", file=sys.stderr)
        sys.exit(1)
    if args.measurements and args.measurement_regex:
        print("Error: --measurement and --measurement-regex cannot be used together.", file=sys.stderr)
        sys.exit(1)

    # Set logging level based on verbosity
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    src_cfg = load_influx_config(args.src_config)
    dst_cfg = load_influx_config(args.dst_config)

    start = parse_time(args.start)
    stop = parse_time(args.stop) if args.stop else None
    window_td = parse_window(args.window)
    logger.debug("Start=%s Stop=%s Window=%s", start, stop, window_td)

    src = build_client(src_cfg)
    dst = build_client(dst_cfg)

    tag_rules = parse_tag_maps(args.tag_maps)
    if tag_rules:
        logger.info("Loaded %d tag remap rule(s)", len(tag_rules))
    measurement_rules = parse_name_maps(args.measurement_maps)
    if measurement_rules:
        logger.info("Loaded %d measurement remap rule(s)", len(measurement_rules))
    field_rules = parse_name_maps(args.field_maps)
    if field_rules:
        logger.info("Loaded %d field remap rule(s)", len(field_rules))
    inject_rules = parse_tag_injects(args.tag_injects)
    if inject_rules:
        logger.info("Loaded %d tag inject rule(s)", len(inject_rules))

    def to_dt(s: str) -> datetime:
        if s.startswith("-"):
            m = DURATION_RE.match(s)
            n = int(m.group(1))
            mult = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[m.group(2)]
            return datetime.now(timezone.utc) - timedelta(seconds=n * mult)
        if s == "now()":
            return datetime.now(timezone.utc)
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

    abs_start = to_dt(start)
    abs_stop = to_dt(stop) if stop else datetime.now(timezone.utc)

    total = 0
    cur = abs_start
    i = 0
    try:
        while cur < abs_stop:
            i += 1
            win_stop = min(cur + window_td, abs_stop)
            s = cur.isoformat().replace("+00:00", "Z")
            e = win_stop.isoformat().replace("+00:00", "Z")
            flux = build_flux(args.src_bucket, s, e, args.measurements, args.measurement_regex, args.tags, args.fields)
            logger.info(f"[window {i}] {s} → {e}")
            if args.verify:
                c = sum(1 for _ in src.query_api().query_stream(query=flux, org=src_cfg["org"]))
                logger.info(f"found {c} records")
                moved = 0
            else:
                moved = copy_window(
                    src, dst, flux, args.batch_size,
                    org=src_cfg["org"], dst_bucket=args.dst_bucket,
                    tag_rules=tag_rules, measurement_rules=measurement_rules, field_rules=field_rules,
                    inject_rules=inject_rules, verbose=args.verbose, dry_run=args.dry_run
                )
            total += moved
            if args.verify:
                logger.info(f"found {moved} records (total {total})")
            elif args.dry_run:
                logger.info(f"would write {moved} records (total {total})")
            else:
                logger.info(f"wrote {moved} records (total {total})")
            cur = win_stop
        if args.verify:
            logger.info("Verify complete.")
        elif args.dry_run:
            logger.info(f"Dry run complete. Would write {total} records.")
        else:
            logger.info(f"Done. {total} records written.")
    finally:
        src.__del__()
        dst.__del__()


if __name__ == "__main__":
    run()