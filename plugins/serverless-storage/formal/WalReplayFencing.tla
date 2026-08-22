----------------------------- MODULE WalReplayFencing -----------------------------
(***************************************************************************)
(* Models the specific race identified by hand-tracing in                 *)
(* rfc-serverless-opensearch.md &sect;6.4 (session note, not yet a fix):   *)
(* can a WAL-replay recovery ever incorporate an operation that was        *)
(* appended by a writer *after* that writer's term had already been        *)
(* superseded, even though the operation is (honestly) tagged with the     *)
(* term the writer itself still believed was current?                     *)
(*                                                                         *)
(* This is deliberately a small, abstracted model of just the race, not a *)
(* full WAL/engine model: it does not model shards, manifests, generations*)
(* or the ShardHead CAS at all (those are already covered by               *)
(* ShardHead.tla). What it models is the one thing that ShardHead.tla      *)
(* does not: `WalChunkService#append` is unconditional -- it never checks *)
(* a live lease/term before writing -- so a node can keep appending WAL    *)
(* records for a real window after its term has been superseded, simply   *)
(* because nothing has told it yet.                                       *)
(*                                                                         *)
(* Two replay strategies are modeled side by side, exactly like            *)
(* ShardHead.tla's Spec/SpecBuggy pattern:                                 *)
(*                                                                         *)
(*   NaiveReplay:  the design actually shipped in this session             *)
(*   (WalChunkReader#filterByShardAndMinimumTerm) -- includes any WAL       *)
(*   record whose own `tag` (the term its author believed was current) is  *)
(*   at least the floor being replayed from. This is what                  *)
(*   rfc-serverless-opensearch.md &sect;6.4 now documents as "necessary    *)
(*   but not sufficient."                                                  *)
(*                                                                         *)
(*   FixedReplay: the proposed fix -- a lease transfer additionally         *)
(*   snapshots the WAL's length at the moment the new term is granted      *)
(*   (`leaseTransferWalPos`), and replay only includes a record if it is   *)
(*   *both* tag-eligible *and* was appended no later than that snapshot.   *)
(*   Everything appended after the snapshot is excluded regardless of tag  *)
(*   -- including a stale writer's late, honestly-mistagged records.       *)
(*                                                                         *)
(* Both use the same sticky-ghost-variable pattern ShardHead.tla's         *)
(* AuthorshipHonest ended up needing (see that module's own STATUS note    *)
(* for why): a bare temporal property over actions is unsound here for     *)
(* the identical reason it was there, so `naiveViolation`/`fixedViolation` *)
(* are ordinary state variables, checked as plain invariants.              *)
(***************************************************************************)

(***************************************************************************)
(* STATUS: machine-checked with TLC (tla2tools.jar, TLC2 2.19).            *)
(*                                                                         *)
(* Run 1 (WalReplayFencing.cfg, combined): NaiveReplayNeverIncludesAPostFencingWrite  *)
(* is VIOLATED with a minimal 4-state counterexample -- AcquireLease bumps *)
(* term 1->2, n1 (still believing term 1) appends a record tagged tag=1    *)
(* while trueTerm=2, and NaiveReplay wrongly includes it (tag=1 >=          *)
(* ReplayFloor=1, and trueTerm=2 > tag=1). This is a machine-verified       *)
(* confirmation of the bug found by hand-tracing before any replay code    *)
(* was written (see rfc-serverless-opensearch.md &sect;6.4's session note).*)
(*                                                                         *)
(* Run 2 (WalReplayFencingFixedOnly.cfg, isolates the fix, run with        *)
(* `-deadlock` since the finite MaxTerm/MaxWalLen bounds produce legitimate*)
(* terminal states TLC would otherwise flag as deadlocks):                 *)
(* FixedReplayNeverIncludesAPostFencingWrite HOLDS across the complete     *)
(* reachable state space for the model's bound (2 nodes, term/WAL length   *)
(* up to 4/5): 603,722 distinct states, search depth 16, 0 states left on  *)
(* the queue -- exhaustive. The position-cutoff fix (bounding replay by    *)
(* the WAL length snapshotted at the moment the current term was granted,  *)
(* in addition to the term-tag filter) closes exactly the gap the naive    *)
(* filter leaves open.                                                     *)
(*                                                                         *)
(* Not yet done: the corresponding Java implementation. The fix as         *)
(* verified requires ShardHead to record `leaseTransferWalPos` (the WAL    *)
(* chunk sequence observed at the moment of lease grant) as part of the    *)
(* same CAS that grants a new term, and whatever replay/recovery mechanism *)
(* eventually gets built to filter by both term and this recorded position.*)
(*                                                                         *)
(* Reproduce:                                                              *)
(*   java -jar tla2tools.jar -config WalReplayFencing.cfg WalReplayFencing.tla *)
(*   java -jar tla2tools.jar -deadlock -config WalReplayFencingFixedOnly.cfg WalReplayFencing.tla *)
(*                                                                         *)
(* -------------------------------------------------------------------    *)
(* Follow-up verification (see write-routing-and-term-authority-progress.md, *)
(* Effort B, for full context): `AcquireLease` above models the           *)
(* (term bump, leaseTransferWalPos snapshot) pair as a SINGLE atomic step  *)
(* -- both change together in one action, with no other action able to    *)
(* interleave between them. `FixedReplayNeverIncludesAPostFencingWrite`'s  *)
(* HOLD result is only a faithful guarantee for the real system if core's  *)
(* real primary-activation path can actually provide that same atomicity.  *)
(* Traced directly (not just assumed) against                              *)
(* `IndexShard#bumpPrimaryTerm`/`#updateShardState`                        *)
(* (server/src/main/java/org/opensearch/index/shard/IndexShard.java):      *)
(*                                                                         *)
(*   - B7 (no new race at the CAS boundary): `bumpPrimaryTerm` asserts     *)
(*     `Thread.holdsLock(mutex)` on entry -- only one term bump can be in  *)
(*     flight per shard at a time, mutex-serialized. Two nodes/threads     *)
(*     cannot race the same shard's term grant concurrently by             *)
(*     construction; this rules out the analogue of two concurrent         *)
(*     `AcquireLease` steps for the same shard, which this model's own     *)
(*     single-`term`-variable design already assumes is impossible.        *)
(*                                                                         *)
(*   - B6 (does the real design close the gap FixedReplay proved): the     *)
(*     term bump and the `onBlocked` callback (the real candidate site for *)
(*     a serverless WAL-position-snapshot hook, see progress doc) both run *)
(*     inside the SAME `asyncBlockOperations`-guarded window -- operations *)
(*     are blocked from before the term increments until after the        *)
(*     callback returns, so nothing can append to the WAL between the two  *)
(*     halves of what this model treats as one atomic step. The real       *)
(*     design's atomicity is therefore not weaker than what `AcquireLease` *)
(*     assumes, and `FixedReplayNeverIncludesAPostFencingWrite`'s exhaustive*)
(*     HOLD result (603,722 states) applies to the real design as          *)
(*     specified, not just to this abstraction.                            *)
(*                                                                         *)
(* This closes B6/B7 by code-correspondence tracing rather than by adding  *)
(* new TLC-checked states: the existing model already assumes exactly the  *)
(* atomicity the real hook design needs to provide, and that assumption is *)
(* now confirmed achievable (mutex + full-duration operation block), not   *)
(* merely hoped for. No new violation was found. The Java implementation   *)
(* itself remains not yet done, per the note above.                        *)
(***************************************************************************)

EXTENDS Integers, Sequences

CONSTANTS
    Nodes,       \* the set of nodes that can hold the writer lease over time
    MaxTerm,     \* bound on how many times the lease can change hands, for finite-state checking
    MaxWalLen    \* bound on WAL length, for finite-state checking

VARIABLES
    term,               \* the actual, current authoritative term (ground truth)
    nodeBelievedTerm,   \* [Nodes -> Nat]: each node's own last-known term (can lag `term`)
    wal,                \* sequence of records: [author |-> Node, tag |-> Nat, trueTerm |-> Nat]
    leaseTransferWalPos,\* Len(wal) as of the most recent term bump -- FixedReplay's extra bound
    naiveViolation,     \* sticky: TRUE once NaiveReplay is shown to have included a post-fencing write
    fixedViolation      \* sticky: TRUE if FixedReplay ever did (expected: never)

Vars == <<term, nodeBelievedTerm, wal, leaseTransferWalPos, naiveViolation, fixedViolation>>

TypeOK ==
    /\ term \in 1..MaxTerm
    /\ nodeBelievedTerm \in [Nodes -> 1..MaxTerm]
    /\ Len(wal) <= MaxWalLen
    /\ leaseTransferWalPos \in 0..MaxWalLen
    /\ naiveViolation \in BOOLEAN
    /\ fixedViolation \in BOOLEAN

Init ==
    /\ term = 1
    /\ nodeBelievedTerm = [n \in Nodes |-> 1]
    /\ wal = <<>>
    /\ leaseTransferWalPos = 0
    /\ naiveViolation = FALSE
    /\ fixedViolation = FALSE

(* A lease change: term genuinely advances, ground truth. The new holder is not
   modeled explicitly (which node it is doesn't matter for this race) -- only that
   the authoritative term moves, and that this moment's WAL length is snapshotted
   for FixedReplay to use later. *)
AcquireLease ==
    /\ term < MaxTerm
    /\ term' = term + 1
    /\ leaseTransferWalPos' = Len(wal)
    /\ UNCHANGED <<nodeBelievedTerm, wal, naiveViolation, fixedViolation>>

(* A node discovers the term has moved on -- modeled as happening at some later,
   unbounded step (weak fairness only, no real-time bound), which is exactly the
   "unaware for a real window" gap this model exists to capture. *)
LearnTerm(n) ==
    /\ nodeBelievedTerm[n] < term
    /\ nodeBelievedTerm' = [nodeBelievedTerm EXCEPT ![n] = term]
    /\ UNCHANGED <<term, wal, leaseTransferWalPos, naiveViolation, fixedViolation>>

(* WalChunkService#append is unconditional: a node appends tagged with whatever
   term IT currently believes, with no check against the live/true term at all. *)
AppendToWal(n) ==
    /\ Len(wal) < MaxWalLen
    /\ wal' = Append(wal, [author |-> n, tag |-> nodeBelievedTerm[n], trueTerm |-> term])
    /\ UNCHANGED <<term, nodeBelievedTerm, leaseTransferWalPos, naiveViolation, fixedViolation>>

(* The floor a real recovery would replay from: one term back from current, i.e.
   "everything since the term immediately before this one, which is what the last
   durable manifest would have covered up to" (ShardHead.tla already establishes
   generation always resets cleanly on a term change, so this per-term
   abstraction, without modeling manifests/generations explicitly, is faithful). *)
ReplayFloor == term - 1

(* NaiveReplay: the design actually shipped -- WalChunkReader#filterByShardAndMinimumTerm.
   Checks the whole current WAL for any record that is (a) tag-eligible under the
   naive filter and (b) was, in ground truth, appended after the term had already
   moved past what its author believed (trueTerm > tag) -- i.e. a real post-fencing
   write that the naive filter would wrongly include. *)
NaiveReplay ==
    /\ \E i \in 1..Len(wal) :
        /\ wal[i].tag >= ReplayFloor
        /\ wal[i].trueTerm > wal[i].tag
    /\ naiveViolation' = TRUE
    /\ UNCHANGED <<term, nodeBelievedTerm, wal, leaseTransferWalPos, fixedViolation>>

(* FixedReplay: the proposed fix -- additionally bounded by leaseTransferWalPos, the
   WAL length snapshotted at the moment the *current* term was granted. Anything
   appended after that snapshot is excluded regardless of tag. *)
FixedReplay ==
    /\ \E i \in 1..Len(wal) :
        /\ wal[i].tag >= ReplayFloor
        /\ i <= leaseTransferWalPos
        /\ wal[i].trueTerm > wal[i].tag
    /\ fixedViolation' = TRUE
    /\ UNCHANGED <<term, nodeBelievedTerm, wal, leaseTransferWalPos, naiveViolation>>

Next ==
    \/ AcquireLease
    \/ \E n \in Nodes : LearnTerm(n)
    \/ \E n \in Nodes : AppendToWal(n)
    \/ NaiveReplay
    \/ FixedReplay

Spec == Init /\ [][Next]_Vars /\ WF_Vars(Next)

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)

(* Expected to FAIL: this is the bug. A stale writer, still believing an old
   term, can append a WAL record after the real term has moved on, and the
   naive tag-only filter has no way to exclude it. *)
NaiveReplayNeverIncludesAPostFencingWrite == naiveViolation = FALSE

(* Expected to HOLD: the position-cutoff fix closes the gap the naive filter
   leaves open. *)
FixedReplayNeverIncludesAPostFencingWrite == fixedViolation = FALSE

=============================================================================
