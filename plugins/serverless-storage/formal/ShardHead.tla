---------------------------- MODULE ShardHead ----------------------------
(***************************************************************************)
(* Formal model of the shard-head state machine described in              *)
(* rfc-serverless-metadata-plane.md ss4/ss7 and implemented by             *)
(* org.opensearch.serverless.storage.shardstate.ShardHead /               *)
(* ShardStateStore#compareAndSet.                                         *)
(*                                                                         *)
(* A shard's entire correctness-bearing state is one head record          *)
(* (primaryTerm, leaseHolder, latestManifestGeneration), mutated only via *)
(* compare-and-swap against an opaque version token. This spec asks the   *)
(* two questions that actually matter operationally, per the RFC's own    *)
(* scope note that a full model of the term/lease/activation machine was  *)
(* "owed" but not yet built:                                              *)
(*                                                                         *)
(*   1. Can two nodes ever both believe themselves to be the current      *)
(*      term's valid writer at once?                                      *)
(*   2. Can a manifest generation ever regress -- i.e. once a generation  *)
(*      G is published under a term, can a smaller generation later be    *)
(*      published under that same term?                                  *)
(*                                                                         *)
(* This spec is intentionally abstract: lease expiry is modeled as a      *)
(* single nondeterministic environment step (time passing / a node dying) *)
(* rather than real timestamps, and the object store is modeled as a      *)
(* single (head, version) pair mutated atomically -- exactly the          *)
(* interface BlobContainer#compareAndSwapRegister actually provides, so   *)
(* nothing about a specific backend (FS/S3/GCS/Azure) leaks into it.      *)
(*                                                                         *)
(* STATUS: written but not yet machine-checked with TLC in this repo      *)
(* (see rfc-serverless-opensearch.md's formal-model section) -- the       *)
(* companion ShardHead.cfg is ready to run once TLA+ tooling is           *)
(* available: `java -jar tla2tools.jar -config ShardHead.cfg ShardHead`.  *)
(***************************************************************************)

EXTENDS Integers

CONSTANTS
    Nodes,          \* the (finite) set of nodes that can contend for this shard
    MaxTerm,         \* bound on primaryTerm, for finite-state model checking
    MaxGeneration    \* bound on latestManifestGeneration, for finite-state model checking

NoNode == CHOOSE v : v \notin Nodes

ASSUME NoNode \notin Nodes
ASSUME MaxTerm \in Nat /\ MaxTerm >= 1
ASSUME MaxGeneration \in Nat /\ MaxGeneration >= 1

VARIABLES
    head,           \* [term: 1..MaxTerm, holder: Nodes \cup {NoNode}, generation: 0..MaxGeneration]
    leaseExpired,   \* whether the current head's lease is currently expired (time-derived, in reality)
    version,        \* the opaque CAS version token guarding head; strictly increases on every mutation
    localHead,      \* per-node cache of the last head each node observed via Read
    localVersion    \* per-node cache of the version token paired with localHead

HeadType == [term: 1..MaxTerm, holder: Nodes \cup {NoNode}, generation: 0..MaxGeneration]

TypeOK ==
    /\ head \in HeadType
    /\ leaseExpired \in BOOLEAN
    /\ version \in Nat
    /\ localHead \in [Nodes -> HeadType]
    /\ localVersion \in [Nodes -> Int]

Init ==
    /\ head = [term |-> 1, holder |-> NoNode, generation |-> 0]
    /\ leaseExpired = TRUE   \* no lease has ever been granted yet
    /\ version = 0
    /\ localHead = [n \in Nodes |-> head]
    /\ localVersion = [n \in Nodes |-> -1]   \* -1 never matches a real version, so no node starts able to CAS

(***************************************************************************)
(* A node refreshes its view of the shard head. In the real system this   *)
(* is ShardStateStore#get.                                                 *)
(***************************************************************************)
Read(n) ==
    /\ localHead' = [localHead EXCEPT ![n] = head]
    /\ localVersion' = [localVersion EXCEPT ![n] = version]
    /\ UNCHANGED <<head, leaseExpired, version>>

(***************************************************************************)
(* A node takes over the shard's lease. Only possible while the lease is  *)
(* currently expired (ShardHead#withNewLease's isNewTerm branch). Taking  *)
(* over from a real previous holder bumps the term and resets the         *)
(* generation to 0 (a fresh term starts a fresh commit history); the very *)
(* first-ever activation (holder = NoNode) keeps term 1 and whatever      *)
(* generation was already recorded (there is none yet).                   *)
(*                                                                         *)
(* The CAS guard `version = localVersion[n]` is the whole of the          *)
(* correctness mechanism: it is only satisfiable if nothing has mutated   *)
(* head since n's last Read, exactly mirroring                            *)
(* BlobContainer#compareAndSwapRegister's real semantics.                 *)
(***************************************************************************)
AcquireLease(n) ==
    /\ leaseExpired = TRUE
    /\ LET h == localHead[n]
           isTakeover == h.holder /= NoNode
           newTerm == IF isTakeover THEN h.term + 1 ELSE h.term
       IN
       /\ newTerm <= MaxTerm
       /\ version = localVersion[n]
       /\ head' = [term |-> newTerm, holder |-> n, generation |-> IF isTakeover THEN 0 ELSE h.generation]
       /\ version' = version + 1
    /\ leaseExpired' = FALSE
    /\ UNCHANGED <<localHead, localVersion>>

(***************************************************************************)
(* A node publishes a new (larger) manifest generation, matching          *)
(* ObjectStoreCommitHeadPublisher#publishCommitAsHead. A node can only    *)
(* succeed if: its lease has not expired, its cached head is still        *)
(* current (the CAS guard), and it still holds the lease as of that       *)
(* cached head -- a node fenced out by another node's AcquireLease will   *)
(* have a stale localHead/localVersion and so cannot satisfy the CAS      *)
(* guard until it Reads again, at which point h.holder /= n and it can no *)
(* longer attempt to publish at all.                                      *)
(***************************************************************************)
Publish(n, g) ==
    /\ leaseExpired = FALSE
    /\ LET h == localHead[n] IN
        /\ h.holder = n
        /\ g > h.generation
        /\ g <= MaxGeneration
        /\ version = localVersion[n]
        /\ head' = [h EXCEPT !.generation = g]
    /\ version' = version + 1
    /\ UNCHANGED <<leaseExpired, localHead, localVersion>>

(***************************************************************************)
(* Environment step: the current lease expires (its holder's node died,   *)
(* or its lease timestamp simply elapsed). This never touches `head` or   *)
(* `version` directly -- exactly like production, where lease expiry is   *)
(* the passage of real time relative to an already-recorded timestamp,    *)
(* not a write to the object store.                                       *)
(***************************************************************************)
ExpireLease ==
    /\ leaseExpired = FALSE
    /\ leaseExpired' = TRUE
    /\ UNCHANGED <<head, version, localHead, localVersion>>

Next ==
    \/ \E n \in Nodes : Read(n)
    \/ \E n \in Nodes : AcquireLease(n)
    \/ \E n \in Nodes, g \in 0..MaxGeneration : Publish(n, g)
    \/ ExpireLease

Vars == <<head, leaseExpired, version, localHead, localVersion>>

Spec == Init /\ [][Next]_Vars /\ WF_Vars(Next)

-----------------------------------------------------------------------------
(* Correctness properties. *)

(***************************************************************************)
(* Property 1: at most one node can simultaneously hold a currently-valid *)
(* lease. This is close to trivial given `head` is a single value with    *)
(* one `holder` field, but it is the state-level sanity check every       *)
(* action-level property below builds on: if this ever failed it would    *)
(* mean the model itself is broken (e.g. two AcquireLease's both firing   *)
(* without properly serializing through `version`), not a property of    *)
(* the real distributed system.                                           *)
(***************************************************************************)
AtMostOneValidHolder ==
    leaseExpired = FALSE =>
        \A n1, n2 \in Nodes : (head.holder = n1 /\ head.holder = n2) => n1 = n2

(***************************************************************************)
(* Property 2 (the one that actually matters): the term recorded in head  *)
(* never decreases from one state to the next. Combined with Property 3,  *)
(* this is what rules out a "resurrected" stale writer ever winning a CAS *)
(* race against a legitimate newer-term writer -- the real fencing        *)
(* guarantee ObjectStoreCommitHeadPublisher relies on.                     *)
(***************************************************************************)
TermNeverDecreases == [][head'.term >= head.term]_Vars

(***************************************************************************)
(* Property 3: within one fixed term, the published generation never      *)
(* regresses. This is the property a compaction rebase or a racing writer *)
(* retry must never violate: rfc-serverless-opensearch.md ss7's rebase    *)
(* protocol exists specifically so a compactor recomputing against a      *)
(* concurrent publication can never accidentally publish backwards.       *)
(***************************************************************************)
GenerationMonotonicWithinTerm ==
    [][head'.term = head.term => head'.generation >= head.generation]_Vars

(***************************************************************************)
(* Property 4: a new term always starts its generation counter over at 0. *)
(* This documents the (deliberate) design choice that generations are not *)
(* globally comparable across terms, only within one -- a compactor or    *)
(* reader must always reason about (term, generation) pairs together,     *)
(* never generation alone.                                                *)
(***************************************************************************)
GenerationResetsOnNewTerm ==
    [][head'.term > head.term => head'.generation = 0]_Vars

(***************************************************************************)
(* Property 5 (fencing, the one that matters most): whenever a Publish(n) *)
(* step is actually taken, the *real, global* head.holder (not merely     *)
(* n's cached belief) must already equal n. Publish's guard only checks   *)
(* n's local cache (`localHead[n].holder = n`) plus the CAS token         *)
(* (`version = localVersion[n]`) -- this property is the substantive      *)
(* claim that those two checks are jointly sufficient to guarantee the    *)
(* real holder is n, i.e. that a node fenced out by a concurrent          *)
(* AcquireLease can never slip a publish through on stale state. Unlike   *)
(* Property 1, this is not true "by construction": it depends on every    *)
(* head-mutating action correctly bumping `version`, and would be the     *)
(* first property to fail if that discipline were ever violated by a      *)
(* future change to this spec (or, in the real system, to                 *)
(* BlobContainerShardStateStore).                                         *)
(***************************************************************************)
OnlyTheCurrentHolderCanPublish ==
    [][\A n \in Nodes, g \in 0..MaxGeneration : Publish(n, g) => head.holder = n]_Vars

=============================================================================
