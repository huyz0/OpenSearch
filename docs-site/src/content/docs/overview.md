---
title: Overview
description: The problem serverless storage solves and why it's shaped the way it is.
---

## The problem

Classic OpenSearch couples compute and storage: every shard copy holds its full state on local disk, so scaling ingest, scaling search, and scaling idle capacity down to nothing are all the same operation — moving a shard, disk and all, between nodes. There's no way to size compute for a workload's query rate independently of how much data it holds, and no way to release compute for an index nobody is querying without also losing the shard's working state.

Serverless storage (`plugins/serverless-storage`) is an opt-in engine, enabled per index with `index.serverless_storage.enabled`, that removes that coupling. It moves the durable copy of a shard's data into an object store and treats a node's local disk purely as a cache. Compute becomes something you attach and detach from durable state instead of something durable state lives inside.

## The core idea: split the shard's role

A classic shard is one thing: it accepts writes and serves reads from the same local Lucene index. Serverless storage splits that into two roles that can run on different nodes, scale independently, and even not run at all:

- **Writer engine** — owns commits. Indexes into a local Lucene index, appends to a write-ahead log for durability between commits, and on each commit publishes a manifest describing the resulting segment files to the object store.
- **Reader engine** — read-only. Watches for new manifests and materializes them into a queryable Lucene reader, either by eagerly downloading segment bundles or lazily fetching only the files a query actually touches.

Because the reader engine's state is entirely rebuilt from object-store manifests, it has no local state that outlives it. That's what makes **scale-to-zero** possible: an idle shard's compute (writer and reader both) can be released entirely, with nothing lost, because the object store already holds everything needed to reconstruct it. Reactivation is a materialization from the latest manifest, not a recovery from a peer.

The same split is what makes **independent scaling** possible in the other direction too — a shard under heavy read load can run many reader replicas backed by the same durable state, without touching the writer at all.

## Why an object store, not local disk, is the source of truth

Bundle files (packed segment blobs) are the "storage layer" — uniformly distributed, effectively unlimited, and cheap. The **manifest** and **shard-head** layer on top of them is the "index layer" — the thing that says which bundle files currently constitute a shard's valid state, and who is allowed to change that. Almost all of this plugin's coordination logic (leases, CAS, fencing, GC, retention) exists to keep that index layer correct under concurrent writers, competing readers, and nodes that can disappear mid-operation — the object store itself only needs to be an append-mostly blob store.

## What this unlocks beyond scale-to-zero

- **Zero-copy resharding** — splitting or merging a shard clones manifest references to hash-range subsets of existing bundle files rather than rewriting segment bytes, so a split is cheap regardless of shard size.
- **No peer recovery** — a reader materializes from the object store, not from another shard copy, so allocation doesn't need classic peer-recovery machinery and isn't constrained by which nodes currently hold a copy.
- **Point-in-time recovery and snapshots** — because every commit is a durable, addressable manifest, pinning a past generation for PITR or snapshot purposes doesn't require keeping a live shard around.

## What this doesn't try to do

Resharding-by-copy auto-triggering, a general N-way merge (rather than a split's own two children merging back), and full snapshot-repository integration were each investigated and deliberately left out of scope — see [Core Changes](/changes/) for what's actually implemented versus what was considered and rejected.
