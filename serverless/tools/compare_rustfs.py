#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

"""Compare fresh local shell and stock remote-backed clusters through separate RustFS wire meters."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
from pathlib import Path
import random
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener

HTTP = build_opener(ProxyHandler({}))


def loopback(value):
    url = urlsplit(value)
    if (url.scheme != "http" or url.hostname != "127.0.0.1" or not url.port
            or url.path or url.query or url.fragment or url.username is not None):
        raise argparse.ArgumentTypeError("use an explicit http://127.0.0.1:PORT endpoint")
    return value


def rest(base, path, method="GET", body=None):
    payload = None if body is None else json.dumps(body).encode("utf-8")
    request = Request(base + path, data=payload, method=method, headers={"Content-Type": "application/json"})
    with HTTP.open(request, timeout=10) as response:
        return json.load(response)


def counters(meter):
    with HTTP.open(meter + "/__serverless_wire_counts", timeout=5) as response:
        return {operation: int(count) for operation, count in (line.split() for line in response.read().decode().splitlines())}


def difference(before, after):
    result = {key: value - before.get(key, 0) for key, value in after.items()}
    if any(result.get(key, 0) < 0 for key in before) or not before.keys() <= after.keys():
        raise RuntimeError("the wire meter restarted during a measurement")
    result["HTTP_ATTEMPTS"] = sum(value for key, value in result.items() if key != "SDK_RETRIES")
    return result


def cpu_and_rss(pids):
    cpu = rss = 0
    for pid in pids:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        cpu += int(fields[11]) + int(fields[12])
        rss += int(fields[21])
    return cpu / os.sysconf("SC_CLK_TCK"), rss * os.sysconf("SC_PAGE_SIZE")


def p99(values):
    return sorted(values)[math.ceil(len(values) * .99) - 1] if values else None


def run(args, endpoint, meter, pids, workload, cache_path):
    def name(index):
        return f"{args.prefix}-{index:06d}"

    for index in range(args.population):
        rest(endpoint, "/" + name(index), "PUT", {
            "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": args.refresh},
            "mappings": {"properties": {"value": {"type": "keyword"}}},
        })

    def idle():
        before = counters(meter)
        time.sleep(args.idle_seconds)
        return difference(before, counters(meter))

    dormant = idle()
    cache_before = rest(endpoint, cache_path)
    before = counters(meter)
    cpu_before, rss_before = cpu_and_rss(pids)
    start = time.monotonic()

    def write_and_observe(sequence, index):
        planned = start + sequence / args.rate
        time.sleep(max(0, planned - time.monotonic()))
        target = "/" + name(index)
        doc = str(sequence)
        try:
            acknowledged = rest(endpoint, target + "/_doc/" + doc, "PUT", {"value": "benchmark"})
            if acknowledged.get("result") != "created":
                raise RuntimeError(f"unexpected write response: {acknowledged}")
        except (URLError, TimeoutError):
            return None, None
        ack = time.monotonic()
        while time.monotonic() < ack + args.freshness_ms / 1000:
            try:
                hits = rest(endpoint, target + "/_search", "POST", {"query": {"ids": {"values": [doc]}}})
                if any(hit["_id"] == doc for hit in hits["hits"]["hits"]):
                    return (ack - planned) * 1000, (time.monotonic() - ack) * 1000
            except HTTPError as error:
                if error.code not in (429, 502, 503, 504):
                    raise
            except (URLError, TimeoutError):
                pass
            time.sleep(.05)
        return (ack - planned) * 1000, None

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [pool.submit(write_and_observe, sequence, index) for sequence, index in enumerate(workload)]
        observations = [future.result() for future in futures]

    active = difference(before, counters(meter))
    cpu_after, rss_after = cpu_and_rss(pids)
    cache_after = rest(endpoint, cache_path)
    acks = [ack for ack, _ in observations if ack is not None]
    visible = [delay for _, delay in observations if delay is not None]
    return {
        "offered": len(workload),
        "acknowledged": len(acks),
        "visible_within_target": sum(delay <= args.freshness_ms for delay in visible),
        "ack_p99_ms": p99(acks),
        "freshness_p99_ms": p99(visible),
        "cpu_seconds_active": cpu_after - cpu_before,
        "rss_bytes_before_active": rss_before,
        "rss_bytes_after_active": rss_after,
        "cache_before_active": cache_before,
        "cache_after_active": cache_after,
        "dormant_idle_wire": dormant,
        "active_wire": active,
        "held_idle_wire": idle(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for target in ("shell", "stock"):
        parser.add_argument("--" + target, required=True, type=loopback)
        parser.add_argument("--" + target + "-meter", required=True, type=loopback)
        parser.add_argument("--" + target + "-pids", required=True, type=int, nargs="+")
    parser.add_argument("--prefix", default="rustfs-compare")
    parser.add_argument("--population", type=int, default=100)
    parser.add_argument("--active", type=int, default=10)
    parser.add_argument("--writes", type=int, default=100)
    parser.add_argument("--rate", type=float, default=10)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--hot-share", type=float, default=.8, help="fraction sent to one hot index")
    parser.add_argument("--idle-seconds", type=float, default=60)
    parser.add_argument("--refresh", default="1s")
    parser.add_argument("--ack-ms", type=float, required=True)
    parser.add_argument("--freshness-ms", type=float, required=True)
    parser.add_argument("--min-success", type=float, default=.99)
    parser.add_argument("--dormant-reference", type=Path, help="smaller-population result at the same idle window and active set")
    parser.add_argument("--dormant-request-slack", type=int, default=0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not (1 <= args.active <= args.population and min(args.writes, args.rate, args.concurrency, args.idle_seconds,
                                                        args.ack_ms, args.freshness_ms) > 0):
        parser.error("workload counts, rate, durations and latency targets must be positive")
    if not (0 <= args.hot_share <= 1 and 0 < args.min_success <= 1):
        parser.error("hot share and minimum success must be fractions")
    if args.dormant_request_slack < 0:
        parser.error("dormant request slack cannot be negative")
    if len({args.shell, args.stock, args.shell_meter, args.stock_meter}) != 4:
        parser.error("both deployments and both meters must have separate endpoints")
    random_source = random.Random(0)
    workload = [0 if random_source.random() < args.hot_share else random_source.randrange(args.active) for _ in range(args.writes)]
    result = {"store": "local RustFS", "population": args.population, "active": args.active, "hot_share": args.hot_share,
              "idle_seconds": args.idle_seconds,
              "rate": args.rate, "refresh": args.refresh, "targets": {"ack_ms": args.ack_ms, "freshness_ms": args.freshness_ms,
                                                                      "min_success": args.min_success}}
    cache_paths = {
        "shell": "/_serverless/stats",
        "stock": "/_nodes/stats/indices?filter_path=nodes.*.indices.query_cache,nodes.*.indices.request_cache,nodes.*.indices.segments",
    }
    for target, cache_path in cache_paths.items():
        measured = run(args, getattr(args, target), getattr(args, target + "_meter"), getattr(args, target + "_pids"), workload, cache_path)
        if target == "shell":
            for label in ("cache_before_active", "cache_after_active"):
                measured[label] = measured[label].get("block_cache", {})
        measured["met_live_targets"] = (measured["visible_within_target"] >= args.writes * args.min_success
                                        and measured["ack_p99_ms"] is not None and measured["ack_p99_ms"] <= args.ack_ms)
        result[target] = measured
    dormant_ok = True
    if args.dormant_reference:
        reference = json.loads(args.dormant_reference.read_text(encoding="utf-8"))
        if reference["population"] >= args.population or any(reference[key] != result[key] for key in ("idle_seconds", "active")):
            parser.error("the dormant reference must have a smaller population and the same idle window and active set")
        old_attempts = reference["shell"]["dormant_idle_wire"]["HTTP_ATTEMPTS"]
        new_attempts = result["shell"]["dormant_idle_wire"]["HTTP_ATTEMPTS"]
        dormant_ok = new_attempts <= old_attempts + args.dormant_request_slack
        result["dormant_check"] = {"reference": old_attempts, "allowed_growth": args.dormant_request_slack, "passed": dormant_ok}
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return 0 if dormant_ok and all(result[target]["met_live_targets"] for target in ("shell", "stock")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
