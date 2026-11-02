#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright OpenSearch Contributors.
"""Exploratory resource and fluid models. Every service rate and size is an assumption."""
import argparse
import csv
from fractions import Fraction
import json
import math
from pathlib import Path

MIB = 1024 ** 2
GIB = 1024 ** 3
ASSUMPTIONS = {
    "maximum_groups_per_host": 16,
    "maximum_payload_bytes": 4 * MIB,
    "journal_queue_bytes_per_group": 8 * MIB,
    "peer_rpc_wire_margin_bytes": MIB,
    "peer_rpc_minimum_calls_per_group": 4,
    "peer_rpc_maximum_calls_per_host": 128,
    "peer_rpc_frame_allowance_per_call": 4,
    "peer_rpc_decoder_workers": 4,
    "peer_rpc_thread_stack_bytes": MIB,
    "voters_per_group": 3,
    "virtual_buckets_per_cell": 65536,
    "bucket_record_bytes": 32,
    "journal_buffer_bytes_per_group": 32 * MIB,
    "retained_entries_per_group": 100000,
    "retained_metadata_bytes_per_entry": 128,
    "pending_operation_bytes_per_host": 128 * MIB,
    "preparation_request_bytes_per_host": 128 * MIB,
    "preparation_retained_request_multiplier": 6,
    "preparation_worker_stack_bytes": MIB,
    "fixed_runtime_bytes_per_host": 2 * GIB,
    "metadata_cache_bytes_per_host": 64 * MIB,
    "mapping_cache_bytes_per_host": 64 * MIB,
    "engine_resident_bytes": 32 * MIB,
    "engine_fds": 32,
    "engines_per_owned_active_shard": 2,
    "resident_engine_unit_cap": 256,
    "host_memory_bytes": 16 * GIB,
    "memory_admission_fraction": 0.75,
    "host_fd_limit": 65536,
    "fd_admission_fraction": 0.75,
    "operations_per_second_per_host": "2000000/12",
    "offered_operations_per_second": 1000000,
    "payload_bytes_per_operation": 1024,
    "search_copies": 2,
    "search_cross_az_fraction": "2/3",
    "effective_cross_az_usd_per_gb": 0.02,
    "put_usd_per_1000": 0.005,
    "legacy_mutations_per_batch": 64,
    "legacy_puts_per_batch": 2,
    "archive_puts_per_chunk": 2,
    "journal_host_usd_per_hour": 0.50,
    "search_host_usd_per_hour": 0.50,
    "caveat": "Assumed inputs, not measured JVM residency, OpenSearch throughput or provider prices.",
}


def capacity(descriptors, activity_bps, cells, groups_per_cell, hosts_per_cell, family):
    a = ASSUMPTIONS
    active = (descriptors * activity_bps + 9999) // 10000
    hosts = cells * hosts_per_cell
    groups = cells * groups_per_cell
    local_groups = math.ceil(3 * groups_per_cell / hosts_per_cell)
    # Uniform active ownership is optimistic; skew needs separate placement and admission.
    peak_led_groups = math.ceil(groups_per_cell / hosts_per_cell)
    owned = math.ceil(active * peak_led_groups / groups)
    repair_local_groups = math.ceil(groups_per_cell / (hosts_per_cell // 3 - 1))
    engines = owned * a["engines_per_owned_active_shard"]
    minimum_pending = local_groups * 6 * (a["maximum_payload_bytes"] + 512)
    pending_budget = max(a["pending_operation_bytes_per_host"], minimum_pending)
    preparation_request_charge = a["preparation_retained_request_multiplier"] * (a["maximum_payload_bytes"] + 512)
    preparation_pending = max(a["preparation_request_bytes_per_host"], local_groups * preparation_request_charge)
    preparation_stacks = local_groups * a["preparation_worker_stack_bytes"]
    peer_wire = a["journal_queue_bytes_per_group"] + a["peer_rpc_wire_margin_bytes"]
    peer_per_call = a["peer_rpc_frame_allowance_per_call"] * peer_wire
    peer_calls = max(a["peer_rpc_minimum_calls_per_group"] * local_groups,
                     min(a["peer_rpc_maximum_calls_per_host"], pending_budget // peer_per_call))
    peer_retained = peer_calls * peer_per_call
    peer_transient = a["peer_rpc_decoder_workers"] * peer_wire
    peer_stacks = (local_groups + 1) * a["peer_rpc_thread_stack_bytes"]
    peer_budget = peer_retained + peer_transient + peer_stacks
    fixed = (a["fixed_runtime_bytes_per_host"] + a["metadata_cache_bytes_per_host"]
             + a["mapping_cache_bytes_per_host"] + pending_budget + peer_budget + preparation_pending + preparation_stacks
             + a["virtual_buckets_per_cell"] * a["bucket_record_bytes"])
    journal = local_groups * (a["journal_buffer_bytes_per_group"]
                             + a["retained_entries_per_group"] * a["retained_metadata_bytes_per_entry"])
    memory = fixed + journal + engines * a["engine_resident_bytes"]
    available = max(0, int(a["host_memory_bytes"] * a["memory_admission_fraction"]) - fixed - journal)
    engine_budget = available // a["engine_resident_bytes"]
    group_rate = a["offered_operations_per_second"] / groups
    return {
        "family": family, "descriptors": descriptors, "activity_bps": activity_bps,
        "active_logical_shards": active, "cells": cells, "groups_per_cell": groups_per_cell,
        "hosts_per_cell": hosts_per_cell, "total_journal_hosts": hosts, "total_groups": groups,
        "local_group_assignments_ceiling": local_groups,
        "preparation_maximum_declared_request_bytes": preparation_request_charge,
        "preparation_pending_bytes_used_in_resource_estimate": preparation_pending,
        "preparation_worker_stack_bytes_used_in_resource_estimate": preparation_stacks,
        "peer_rpc_maximum_message_bytes": peer_wire,
        "peer_rpc_maximum_calls_per_host": peer_calls,
        "peer_rpc_retained_message_allowance_bytes": peer_retained,
        "peer_rpc_decoder_transient_allowance_bytes": peer_transient,
        "peer_rpc_group_and_timer_stack_bytes": peer_stacks,
        "peer_rpc_bytes_used_in_resource_estimate": peer_budget,
        "minimum_pending_bytes_for_group_reservations": minimum_pending,
        "pending_bytes_used_in_resource_estimate": pending_budget,
        "default_pending_budget_supports_group_count": a["pending_operation_bytes_per_host"] >= minimum_pending,
        "group_assignment_budget_ok": local_groups <= a["maximum_groups_per_host"],
        "same_az_repair_local_group_ceiling": repair_local_groups,
        "same_az_repair_with_existing_hosts_possible": repair_local_groups <= a["maximum_groups_per_host"],
        "three_az_repair_possible_while_one_az_is_absent": False,
        "peak_led_groups_per_host": peak_led_groups,
        "owned_active_shards_per_host_ceiling": owned, "preparation_and_serving_engines_per_host": engines,
        "assumed_resident_bytes_per_host": memory,
        "memory_admission_ok": memory <= a["host_memory_bytes"] * a["memory_admission_fraction"],
        "engine_count_budget_from_memory": engine_budget,
        "engine_count_admission_ok": engines <= a["resident_engine_unit_cap"],
        "assumed_engine_fds_per_host": engines * a["engine_fds"],
        "engine_fd_budget_ok": engines * a["engine_fds"] <= a["host_fd_limit"] * a["fd_admission_fraction"],
        "unpurged_log_seconds_if_one_position_per_operation": a["retained_entries_per_group"] / group_rate,
        "journal_host_usd_per_hour_assumed": hosts * a["journal_host_usd_per_hour"],
    }


def voter_hosts(group, hosts):
    per_az = hosts // 3
    home = group // 3 % per_az
    return tuple(((home + az * 17) % per_az) * 3 + az for az in range(3))


def fluid(groups, pause_ms, hot, failure, policy):
    hosts = 12
    rate = Fraction(ASSUMPTIONS["offered_operations_per_second"])
    capacity_per_host = Fraction(ASSUMPTIONS["operations_per_second_per_host"])
    arrival = ([rate / groups] * groups if not hot or groups == 1
               else [rate / 5] + [rate * Fraction(4, 5) / (groups - 1)] * (groups - 1))
    peers = [voter_hosts(g, hosts) for g in range(groups)]
    leaders = [peers[g][g % 3] for g in range(groups)]
    down = {0} if failure == "host" else {h for h in range(hosts) if h % 3 == 0}
    affected = [g for g in range(groups) if leaders[g] in down]
    relocated = leaders.copy()
    counts = [sum(h == host for h in leaders if h not in down) for host in range(hosts)]
    for g in sorted(affected, key=lambda x: -arrival[x]):
        eligible = [h for h in peers[g] if h not in down]
        assert len(eligible) >= 2  # The modeled one-host/AZ loss leaves a majority.
        chosen = min(eligible, key=lambda h: (counts[h], h))
        relocated[g] = chosen
        counts[chosen] += 1

    def service(placement, unavailable):
        if policy == "global_equal":
            # Reproduces the optimistic historical assumption, ignoring leader-host limits.
            return [capacity_per_host * hosts / groups if placement[g] not in unavailable else Fraction(0) for g in range(groups)]
        per_host = [sum(h == host for h in placement if h not in unavailable) for host in range(hosts)]
        return [capacity_per_host / per_host[h] if h not in unavailable else Fraction(0) for h in placement]

    before = service(leaders, set())
    during = service(leaders, down)
    after = service(relocated, down)
    queue = [Fraction(0)] * groups
    bounds = [mu / 2 for mu in before]  # Fixed half-second service budget; never expands on failure.
    rejected = completed = offered = Fraction(0)
    pre_failure_rejected = Fraction(0)
    peak_queue = Fraction(0)
    for segment, (duration, rates) in enumerate([
        (Fraction(1), before), (Fraction(pause_ms, 1000), during),
        (Fraction(4) - Fraction(pause_ms, 1000), after)
    ]):
        for g, mu in enumerate(rates):
            arrivals = arrival[g] * duration
            done = min(queue[g] + arrivals, mu * duration)
            end = queue[g] + arrivals - done
            lost = max(Fraction(0), end - bounds[g])
            queue[g] = end - lost
            offered += arrivals
            completed += done
            rejected += lost
            if segment == 0:
                pre_failure_rejected += lost
        peak_queue = max(peak_queue, sum(queue))
    outstanding = sum(queue)
    assert offered == completed + rejected + outstanding
    return {
        "groups": groups, "assumed_pause_ms": pause_ms, "hot_group_share": 0.2 if hot else 1 / groups,
        "failure": failure, "allocation_policy": policy, "leader_hosts": hosts,
        "affected_groups": len(affected), "offered": float(offered), "completed": float(completed),
        "rejected": float(rejected), "rejected_before_failure": float(pre_failure_rejected),
        "outstanding": float(outstanding), "peak_segment_boundary_queue": float(peak_queue),
        "healthy_groups_keep_service_during_pause": all(during[g] > 0 for g in range(groups) if g not in affected),
        "actual_election_or_latency_measurement": False,
    }


def costs(groups, flush_seconds, chunk_mib):
    a = ASSUMPTIONS
    rate = a["offered_operations_per_second"]
    bytes_per_second = rate * a["payload_bytes_per_operation"]
    # All modeled groups are dirty at the high default rate. No empty-chunk uploads are assumed.
    dirty_groups = groups * (-math.expm1(-rate * flush_seconds / groups))
    chunks_per_second = max(bytes_per_second / (chunk_mib * MIB), dirty_groups / flush_seconds)
    replication_gb_per_hour = bytes_per_second * 2 * 3600 / 1e9
    search_gb_per_hour = (bytes_per_second * a["search_copies"]
                         * float(Fraction(a["search_cross_az_fraction"])) * 3600 / 1e9)
    return {
        "total_groups": groups, "archive_flush_seconds": flush_seconds, "target_chunk_mib": chunk_mib,
        "archive_puts_per_second_assumed": chunks_per_second * a["archive_puts_per_chunk"],
        "archive_put_usd_per_hour_assumed": chunks_per_second * a["archive_puts_per_chunk"] * 3600 * a["put_usd_per_1000"] / 1000,
        "legacy_put_usd_per_hour_assumed": rate / a["legacy_mutations_per_batch"] * a["legacy_puts_per_batch"] * 3600 * a["put_usd_per_1000"] / 1000,
        "quorum_replication_gb_per_hour_payload_only": replication_gb_per_hour,
        "search_tail_cross_az_gb_per_hour_payload_only": search_gb_per_hour,
        "cross_az_usd_per_hour_assumed": (replication_gb_per_hour + search_gb_per_hour) * a["effective_cross_az_usd_per_gb"],
        "excludes": "compression, protocol framing, retries, GET/LIST/storage, segment copies, queries, SSD and compute",
    }


def write_csv(path, rows):
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    resource_rows = []
    for descriptors in (1000000, 10000000):
        for activity in (10, 100, 1000):
            for cells in (1, 4, 16):
                for groups in (16, 64, 256):
                    for hosts in (12, 24):
                        resource_rows.append(capacity(descriptors, activity, cells, groups, hosts, "per_cell_candidate"))
            # Same 384 journal hosts and 1024 groups, partitioned into different numbers of cells.
            for cells, groups, hosts in ((1, 1024, 384), (4, 256, 96), (16, 64, 24)):
                resource_rows.append(capacity(descriptors, activity, cells, groups, hosts, "fixed_fleet"))
    fluid_rows = [fluid(groups, pause, hot, failure, policy)
                  for groups in (1, 16, 64, 256) for pause in (50, 200, 1000)
                  for hot in (False, True) for failure in ("host", "az")
                  for policy in ("global_equal", "host_bounded")]
    cost_rows = [costs(groups, flush, chunk) for groups in (64, 256, 1024)
                 for flush in (5, 30, 120) for chunk in (4, 16, 64)]
    # Validate model properties, not a mirror of particular numeric results.
    assert all(row["healthy_groups_keep_service_during_pause"] for row in fluid_rows)
    assert all(not row["group_assignment_budget_ok"] for row in resource_rows
               if row["family"] == "per_cell_candidate" and row["groups_per_cell"] == 256)
    for name, rows in (("resources", resource_rows), ("failure_fluid", fluid_rows), ("costs", cost_rows)):
        write_csv(args.output / (name + ".csv"), rows)
    result = {"format_version": 1, "assumptions": ASSUMPTIONS, "resources": resource_rows,
              "failure_fluid": fluid_rows, "costs": cost_rows}
    (args.output / "model.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"resource_scenarios": len(resource_rows), "failure_scenarios": len(fluid_rows),
                      "cost_scenarios": len(cost_rows), "output": str(args.output)}))


if __name__ == "__main__":
    main()
