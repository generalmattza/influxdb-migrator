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
- On PowerShell, quote relative starts like '--start "-4d"'.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
import re
import toml
from typing import List
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

from influxdb_client import InfluxDBClient, Point, WriteOptions

RFC3339_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")
DURATION_RE = re.compile(r"^-(\d+)([smhdw])$")


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
        # optional client tuning
        "timeout": cfg.get("timeout"),
        "connection_pool_maxsize": cfg.get("connection_pool_maxsize"),
        "auth_basic": cfg.get("auth_basic"),
    }
    safe_log = {k: v for k, v in parsed.items() if k != "token"}
    logger.debug("Loaded Influx config from %s: %s", path, safe_log)
    return parsed


def build_flux(bucket: str, start: str, stop: str | None, measurement: str | None, tags: list[str] | None, fields: list[str] | None) -> str:
    lines = [
        f'from(bucket: "{bucket}")',
        f"  |> range(start: {start}" + (f", stop: {stop})" if stop else ")"),
    ]
    if measurement:
        lines.append(f"  |> filter(fn: (r) => r._measurement == \"{measurement}\")")
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
        # Wildcard all fields if none provided
        lines.append("  |> filter(fn: (r) => r._field =~ /.*/)")
    query = "\n".join(lines)
    logger.debug("Built flux query:\n%s", query)
    return query


def record_to_point(rec) -> Point:
    skip = {"result", "table", "_start", "_stop", "_time", "_measurement", "_field", "_value"}
    p = Point(rec.get_measurement())
    for k, v in rec.values.items():
        if k in skip or k.startswith("_") or v is None:
            continue
        p.tag(k, str(v))
    p.field(rec.get_field(), rec.get_value())
    p.time(rec.get_time())
    return p


def build_client(cfg: dict) -> InfluxDBClient:
    # InfluxDBClient expects timeout in ms; pass through if provided
    kwargs = {
        "url": cfg["url"],
        "org": cfg["org"],
        "token": cfg["token"],
        "timeout": cfg.get("timeout") or 60000,
    }
    if cfg.get("connection_pool_maxsize"):
        kwargs["connection_pool_maxsize"] = cfg["connection_pool_maxsize"]
    if cfg.get("auth_basic") is not None:
        kwargs["auth_basic"] = bool(cfg.get("auth_basic"))
    return InfluxDBClient(**kwargs)


def copy_window(src_client: InfluxDBClient, dst_client: InfluxDBClient, flux: str, batch_size: int, org: str) -> int:
    q = src_client.query_api()
    w = dst_client.write_api(write_options=WriteOptions(batch_size=batch_size))
    count = 0
    batch: List[Point] = []
    # Pass org explicitly to avoid silent empty results in some environments
    for rec in q.query_stream(query=flux, org=org):
        if getattr(rec, "_measurement", None) is None:
            continue
        batch.append(record_to_point(rec))
        count += 1
        if len(batch) >= batch_size:
            w.write(record=batch)
            batch.clear()
    if batch:
        w.write(record=batch)
    w.__del__()
    return count


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src-config", required=True, help="Path to source .influx.toml")
    ap.add_argument("--dst-config", required=True, help="Path to dest .influx.toml")
    ap.add_argument("--src-bucket", required=True)
    ap.add_argument("--dst-bucket", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--stop")
    ap.add_argument("--measurement")
    ap.add_argument("--field", action="append", dest="fields")
    ap.add_argument("--tag", action="append", dest="tags")
    ap.add_argument("--window", default="6h")
    ap.add_argument("--batch-size", type=int, default=5000)
    ap.add_argument("--verify", action="store_true", help="Only query and print a count per window; do not write")
    args = ap.parse_args()

    src_cfg = load_influx_config(args.src_config)
    dst_cfg = load_influx_config(args.dst_config)

    start = parse_time(args.start)
    stop = parse_time(args.stop) if args.stop else None
    window_td = parse_window(args.window)
    logger.debug("Start=%s Stop=%s Window=%s", start, stop, window_td)

    src = build_client(src_cfg)
    dst = build_client(dst_cfg)

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
            flux = build_flux(args.src_bucket, s, e, args.measurement, args.tags, args.fields)
            print(f"[window {i}] {s} → {e}", file=sys.stderr)
            if args.verify:
                c = sum(1 for _ in src.query_api().query_stream(query=flux, org=src_cfg["org"]))
                print(f"  found {c} records", file=sys.stderr)
                moved = 0
            else:
                moved = copy_window(src, dst, flux, args.batch_size, org=src_cfg["org"])
            total += moved
            if not args.verify:
                print(f"  wrote {moved} records (total {total})", file=sys.stderr)
            cur = win_stop
        if args.verify:
            print("Verify complete.")
        else:
            print(f"Done. {total} records written.")
    finally:
        src.__del__()
        dst.__del__()


if __name__ == "__main__":
    main()