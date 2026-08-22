---
title: "Durability Posture"
description: What each durability tier actually protects against, which failures none of them cover, and why fleet-wide physical DR belongs to the object store rather than to this cluster.
---

## Why this page exists

Everything on this branch that is called a snapshot is a snapshot. None of it is, on its own, a backup in the sense most people mean when they say the word: an independent copy that survives the loss of the thing it was taken from.

That is not a defect. It is a deliberate consequence of a design where a shard's data is already in an object store, addressed by a manifest, and where the entire premise is that per-index work was removed so the fleet can be enormous. But it means the word "snapshot" in this system carries less insurance than it does in a classic OpenSearch cluster, and someone reading the [snapshot page](/design/snapshot-restore-proposal/) and stopping there will believe otherwise. This page is the correction, in one place.

## The three tiers, and what each one insures against

| tier | protects against | cost | where it belongs |
|---|---|---|---|
| **pin** (shallow) | our own garbage collection; a logical error, a bad write, a tenant rollback | one blob write per shard | per index, always available |
| **pointer snapshot** | the same, plus cataloguing and the standard tooling | a few hundred bytes | per index, on request |
| **independent copy** (deep) | loss of the source bucket, account, or region | O(bytes) | per index only on request; fleet-wide, at the store |

Conflating these is how a backup story becomes a false one. They are three different products that happen to share a verb.

### The pin protects against us, and nothing else

A pin stops `ManifestRetentionPolicy` from reclaiming a generation. That is the whole of it.

It does not stop a bucket lifecycle rule, an operator with console access, a compromised credential, or a region going away. Those are stopped by object versioning, a deny-delete policy, and replication — and they are deliberately out of band, because the two enforcements should not share a failure domain. A pin held by the same system that would do the deleting is not protection against that system being wrong.

### A pointer snapshot is a reference, not a copy

`_snapshot` against a serverless index writes a pointer to a manifest generation the shard already published. The repository stores a few hundred bytes. The data stays where it was.

So a snapshot taken into a GCS repository, from a cluster whose storage is in S3, does not put a single segment byte into GCS. If S3 becomes unreachable, that GCS snapshot restores nothing. The failure mode is easy to miss because the repository itself is perfectly healthy — the restore fails for a reason that is nowhere near the thing the operator is looking at. See the snapshot page for the full mechanism and for why this plugin, unlike remote-backed storage, has no classic path to fall back to.

### The deep snapshot is a real copy, and it has shipped

A snapshot that genuinely copies the bytes into the target repository was first proven end to end: a serverless index's data copied into an `fs` repository from a data node, finalized, and then restored through core's ordinary `_restore` under a new name with every document back. It produces an ordinary snapshot in the standard format, restorable by a cluster that has never heard of this plugin.

That proof is now a shipped API. `POST /_plugins/_serverless/storage/index/{index}/_snapshot_deep/...` and its per-shard sibling are backed by real transport actions (`IndexDeepSnapshotAction`, `ShardDeepSnapshotAction` and their transports), and the pin lifecycle around the copy exists — `PinLedger` records the pin and `PinLedgerSweepTask` reclaims ledgers whose pins have lapsed.

## Fleet-wide disaster recovery is the object store's job

The tempting design is a per-index deep snapshot applied across the fleet. It is the wrong layer, and at this branch's target population it is not a close call: it is O(N) cluster-manager work and an unbounded copy, for a fleet whose entire premise is that per-index work was removed.

Physical, fleet-wide DR belongs to the object store, where the provider does it asynchronously, at storage cost, with no per-index work in the cluster at all:

- **bucket versioning**, so an overwrite or delete is recoverable;
- **a deny-delete policy** on the credentials the cluster itself uses, so a bug here cannot destroy the record;
- **cross-region replication**, so the loss of a region is survivable.

None of that is code in this repository. What this repository owes it is this paragraph, saying that it is required and that nothing here substitutes for it.

Per-index deep copy earns its keep for the cases the store cannot express: tenant export, legal hold, and "protect these specific indices properly" as a policy on a named set.

## What is still open

Stated here rather than left to be discovered:

- **A pin taken through `_snapshot_pin` has no automatic release.** It is released by `_snapshot_release` or not at all. A pin ledger makes a failed release retryable and abandoned pins enumerable, and an unconfirmed pin expires after ten minutes — but a confirmed pin whose owner never comes back holds its generation indefinitely, which is what "pin" means and is why the operator-visible listing matters more than a timer.
- **Ledger sweeping exists (`PinLedgerSweepTask`), but nothing reconciles a ledger against the pins it names in either direction.** A ledger entry naming a pin the store no longer holds, or a held pin no ledger names, is still undetected.
- **A deep snapshot's pin lifecycle now exists**, and the constraint that shaped it is still worth knowing: a deep snapshot's repository entry carries no pin id, so unlike the pointer snapshot the repository cannot serve as the ledger — which is why the separate `PinLedger` exists and why a crashed copy is reclaimed by the sweep rather than by the repository.
- **Sub-commit precision is not available.** A point-in-time restore lands on the last commit at or before the instant, not on the instant. Going finer needs WAL records to carry a timestamp, which they do not.

## Related

- [Snapshot & Restore](/design/snapshot-restore-proposal/) — the mechanism, and why it is pointer-based
- [GC & Retention](/design/gc-retention/) — what reclaims a generation, and what holds one back
