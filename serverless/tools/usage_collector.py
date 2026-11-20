#!/usr/bin/env python3
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
"""Sets each tenant's cost level from its total across every node.

A node sees only its own share of a tenant, so a tenant spread over many nodes can be over its budget in total while
under it on each. This polls every node's GET /_serverless/usage once a minute, adds up each tenant's cost since the
last poll, and gives it a level with the same rule a node uses: a minute over its budget raises it to 1, two more
minutes over to 2, then 3; five minutes under lowers it one. When the levels change it writes them to the cluster
settings, serverless.cost.tenant_levels, which every node reads; nothing is written while they stay the same.

It keeps nothing on disk: on start it takes the levels already in the cluster settings as its own. Levels hold
through incomplete observations. After a missed sample, restart, or membership change, the collector waits for
two comparable snapshots before resuming its timers.

    usage_collector.py --nodes 10.0.0.1:9200,10.0.0.2:9200 [--interval 60] [--once] [--dry-run]
    usage_collector.py --self-test
"""

import argparse
import json
import sys
import time
import urllib.request

LEVEL_KEY = "serverless.cost.tenant_levels"
LEVEL_UP_AFTER = 60.0
NEXT_LEVEL_AFTER = 120.0
LEVEL_DOWN_AFTER = 300.0
MAX_LEVEL = 3


class Tenant:
    def __init__(self, level=0, now=0.0):
        self.level = level
        self.over_since = None
        self.under_since = None
        self.changed_at = now if level > 0 else None

    def step(self, over, now):
        if over:
            self.under_since = None
            if self.over_since is None:
                self.over_since = now
            wait = LEVEL_UP_AFTER if self.level == 0 else NEXT_LEVEL_AFTER
            since = self.over_since if self.level == 0 else max(self.over_since, self.changed_at or self.over_since)
            if self.level < MAX_LEVEL and now - since >= wait:
                self.level += 1
                self.changed_at = now
        else:
            self.over_since = None
            if self.under_since is None:
                self.under_since = now
            since = max(self.under_since, self.changed_at or self.under_since)
            if self.level > 0 and now - since >= LEVEL_DOWN_AFTER:
                self.level -= 1
                self.changed_at = now


def parse_levels(value):
    levels = {}
    for pair in (value or "").split(","):
        tenant, eq, level = pair.rpartition("=")
        tenant = tenant.strip()
        if eq and tenant:
            try:
                n = int(level.strip())
            except ValueError:
                continue
            if 0 < n <= 4:
                levels[tenant] = n
    return levels


def format_levels(levels):
    return ",".join("%s=%d" % (tenant, level) for tenant, level in sorted(levels.items()) if level > 0)


class Collector:
    """The rollup and the levels; the HTTP calls are passed in, so the logic runs without a cluster."""

    def __init__(self, fetch, write, initial_levels, now):
        self.fetch = fetch
        self.write = write
        self.last = {}  # node -> (started_millis, {index: cost_units})
        self.last_poll = None
        self.tenants = {t: Tenant(level, now) for t, level in initial_levels.items()}
        self.written = format_levels(initial_levels)

    def poll(self, nodes, now):
        cost_by_tenant = {}
        budget_by_tenant = {}
        samples = {}
        expected = dict.fromkeys(nodes)
        comparable = self.last.keys() == expected.keys()
        for node in expected:
            try:
                answer = self.fetch(node)
                started = answer["started_millis"]
            except Exception as e:
                print("could not read %s: %s" % (node, e), file=sys.stderr)
                continue
            previous = self.last.get(node)
            same_process = previous is not None and previous[0] == started
            comparable = comparable and same_process
            before = previous[1] if same_process else {}
            totals = {}
            for index, line in (answer.get("indices") or {}).items():
                if index.startswith("_"):
                    continue
                cost = float(line.get("cost_units", 0.0))
                totals[index] = cost
                tenant = line.get("tenant", index)
                delta = cost - before.get(index, 0.0)
                if delta < 0:
                    delta = cost
                cost_by_tenant[tenant] = cost_by_tenant.get(tenant, 0.0) + delta
                budget = line.get("budget_per_minute")
                if budget is not None:
                    budget_by_tenant[tenant] = max(budget_by_tenant.get(tenant, 0), budget)
            samples[node] = (started, totals)
        elapsed = None if self.last_poll is None else now - self.last_poll
        self.last = samples
        self.last_poll = now
        if not comparable or not expected or samples.keys() != expected.keys() or elapsed is None or elapsed <= 0:
            for state in self.tenants.values():
                state.over_since = None
                state.under_since = None
            return None
        for tenant, cost in cost_by_tenant.items():
            budget = budget_by_tenant.get(tenant)
            if budget is None:
                continue
            per_minute = cost * 60.0 / elapsed
            self.tenants.setdefault(tenant, Tenant()).step(per_minute > budget, now)
        # A tenant that cost nothing anywhere is under its budget.
        for tenant, state in self.tenants.items():
            if tenant not in cost_by_tenant:
                state.step(False, now)
        levels = {t: s.level for t, s in self.tenants.items() if s.level > 0}
        value = format_levels(levels)
        if value != self.written:
            self.write(value)
            self.written = value
        self.tenants = {t: s for t, s in self.tenants.items() if s.level > 0 or s.over_since is not None}
        return levels


def http_json(method, url, body=None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8") or "{}")


def flat_setting(settings, key):
    persistent = settings.get("persistent", settings)
    if key in persistent:
        return persistent[key]
    node = persistent
    for part in key.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node if isinstance(node, str) else None


def self_test():
    answers = {}
    writes = []
    collector = Collector(lambda node: answers[node], writes.append, {}, 0.0)
    totals = {"a": 0.0, "b": 0.0}

    def answer(started, a, b):
        return {
            "started_millis": started,
            "indices": {
                "x-a": {"tenant": "a", "budget_per_minute": 20, "cost_units": a},
                "x-b": {"tenant": "b", "budget_per_minute": 20, "cost_units": b},
                "_node": {"cost_units": 999.0},
            },
        }

    now = 0.0
    nodes = ["n1", "n2"]
    levels = []
    for minute in range(12):
        # Tenant a: 15 a minute on each of two nodes, 30 in total, over its 20. Tenant b: 5 on each, under.
        totals["a"] += 15.0
        totals["b"] += 5.0
        answers["n1"] = answer(1, totals["a"], totals["b"])
        answers["n2"] = answer(1, totals["a"], totals["b"])
        levels.append(collector.poll(nodes, now))
        now += 60.0
    assert levels[0] is None, levels[0]
    assert levels[1] == {}, levels[1]
    assert levels[2].get("a") == 1, levels
    assert levels[4].get("a") == 2, levels
    assert levels[6].get("a") == 3, levels
    assert "b" not in levels[11], levels
    assert writes == ["a=1", "a=2", "a=3"], writes
    # Then a goes quiet: down one level every five minutes.
    for minute in range(16):
        answers["n1"] = answer(1, totals["a"], totals["b"])
        answers["n2"] = answer(1, totals["a"], totals["b"])
        collector.poll(nodes, now)
        now += 60.0
    assert writes == ["a=1", "a=2", "a=3", "a=2", "a=1", ""], writes
    # No node answering changes nothing.
    def down(node):
        raise OSError("refused")

    quiet = Collector(down, writes.append, {"a": 2}, now)
    for minute in range(10):
        assert quiet.poll(nodes, now + 60.0 * minute) is None
    assert quiet.tenants["a"].level == 2
    # One quiet peer cannot stand in for a missing spending node.
    def partial(node):
        if node == "n2":
            raise OSError("refused")
        return {"started_millis": 1}

    incomplete = Collector(partial, writes.append, {"a": 2}, now)
    for minute in range(7):
        incomplete.poll(nodes, now + 60.0 * minute)
    assert incomplete.tenants["a"].level == 2

    # A late counter covers three minutes. Its delta is not one minute of spending.
    samples = {"n1": answer(1, 0.0, 0.0), "n2": answer(1, 0.0, 0.0)}
    delayed = Collector(samples.__getitem__, writes.append, {}, 0.0)
    delayed.poll(nodes, 0.0)
    del samples["n2"]
    delayed.poll(nodes, 60.0)
    delayed.poll(nodes, 120.0)
    samples["n2"] = answer(1, 30.0, 0.0)
    delayed.poll(nodes, 180.0)
    assert "a" not in delayed.tenants
    samples["n2"] = answer(1, 40.0, 0.0)
    assert delayed.poll(nodes, 240.0) == {}
    samples["n2"] = answer(2, 10.0, 0.0)
    assert delayed.poll(nodes, 300.0) is None
    samples["n2"] = answer(2, 20.0, 0.0)
    assert delayed.poll(nodes, 360.0) == {}
    assert delayed.poll(["n2"], 420.0) is None

    # Started from what the cluster already has.
    again = Collector(lambda node: answers[node], writes.append, parse_levels("a=2, bad, c=x"), now)
    assert again.written == "a=2" and again.tenants["a"].level == 2
    assert parse_levels(" a=1,b b=3,=2,c=9") == {"a": 1, "b b": 3}
    print("self-test passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--nodes", help="host:port of every node, separated by commas")
    parser.add_argument("--interval", type=float, default=60.0, help="seconds between polls (60)")
    parser.add_argument("--once", action="store_true", help="poll twice, one interval apart, and exit")
    parser.add_argument("--dry-run", action="store_true", help="print the levels instead of writing them")
    parser.add_argument("--self-test", action="store_true", help="check the rollup and the levels without a cluster")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if not args.nodes:
        parser.error("--nodes is required")
    nodes = [n.strip() for n in args.nodes.split(",") if n.strip()]

    def fetch(node):
        return http_json("GET", "http://%s/_serverless/usage" % node)

    def write(value):
        if args.dry_run:
            print("%s=%s" % (LEVEL_KEY, value))
            return
        for node in nodes:
            try:
                http_json("PUT", "http://%s/_cluster/settings" % node, {"persistent": {LEVEL_KEY: value}})
                print("set %s=%s" % (LEVEL_KEY, value))
                return
            except Exception as e:
                print("could not write through %s: %s" % (node, e), file=sys.stderr)
        raise RuntimeError("no node took the levels")

    initial = {}
    for node in nodes:
        try:
            initial = parse_levels(flat_setting(http_json("GET", "http://%s/_cluster/settings?flat_settings=true" % node), LEVEL_KEY))
            break
        except Exception as e:
            print("could not read the levels through %s: %s" % (node, e), file=sys.stderr)
    collector = Collector(fetch, write, initial, time.monotonic())
    polls = 0
    while True:
        levels = collector.poll(nodes, time.monotonic())
        polls += 1
        if levels is not None:
            print("levels: %s" % (format_levels(levels) or "none"))
        if args.once and polls >= 2:
            return
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
