---------------------------- MODULE ShardHead ----------------------------
(***************************************************************************)
(* Formal model of the shard-head state machine described in              *)
(* rfc-serverless-metadata-plane.md ss4/ss7 and implemented by             *)
(* org.opensearch.serverless.storage.shardstate.ShardHead /               *)
(* ShardStateStore#compareAndSet, extended with the compaction service    *)
(* (LuceneMergeCompactionPublisher / CompactionRebaseExecutor) that       *)
(* independently advances the same generation counter.                    *)
(*                                                                         *)
(* A shard's entire correctness-bearing state is one head record          *)
(* (primaryTerm, leaseHolder, latestManifestGeneration, publishedBy),     *)
(* mutated only via compare-and-swap against an opaque version token.     *)
(* This spec asks the questions that actually matter operationally:       *)
(*                                                                         *)
(*   1. Can two nodes ever both believe themselves to be the current      *)
(*      term's valid writer at once?                                      *)
(*   2. Can a manifest generation ever regress -- i.e. once a generation  *)
(*      G is published under a term, can a smaller generation later be    *)
(*      published under that same term?                                  *)
(*   3. Can a writer's publish attempt ever be reported to its caller as  *)
(*      successful when the shard head does not actually reflect that     *)
(*      writer's own content? (AuthorshipHonest, below -- this is the     *)
(*      question that surfaces the real bug this session found and fixed *)
(*      in ObjectStoreCommitHeadPublisher.)                                *)
(*                                                                         *)
(* This spec is intentionally abstract: lease expiry is modeled as a      *)
(* single nondeterministic environment step (time passing / a node dying) *)
(* rather than real timestamps, and the object store is modeled as a      *)
(* single (head, version) pair mutated atomically -- exactly the          *)
(* interface BlobContainer#compareAndSwapRegister actually provides, so   *)
(* nothing about a specific backend (FS/S3/GCS/Azure) leaks into it.      *)
(*                                                                         *)
(* STATUS: machine-checked with TLC (tla2tools.jar, TLC2 2.19).           *)
(*                                                                         *)
(* Run 1 (ShardHead.cfg, Spec -- the real, fixed production code): all    *)
(* properties, including the AuthorshipHonest invariant, hold across the  *)
(* complete reachable state space for the model's bound (3 nodes,         *)
(* term/generation up to 3). See ShardHead.cfg for the exact numbers.     *)
(*                                                                         *)
(* Run 2 (ShardHeadBuggy.cfg, SpecBuggy -- the pre-fix production code,   *)
(* modeled via the extra PublishBuggy action): AuthorshipHonest is        *)
(* violated, and TLC reports a concrete counterexample trace -- a         *)
(* machine-verified demonstration that the bug this session found in      *)
(* ObjectStoreCommitHeadPublisher#publishCommitAsHead (the                *)
(* "manifest.generation() <= currentHead.latestManifestGeneration() =>    *)
(* return true" shortcut, which reported success without the head ever    *)
(* reflecting the writer's own content once a compactor had published     *)
(* ahead of it) was a genuine protocol-level violation, not merely an     *)
(* implementation detail. The counterexample TLC finds is exactly this    *)
(* scenario: a writer acquires the lease, a compactor independently       *)
(* advances the live generation while the writer's cached view is stale,  *)
(* and the writer's buggy publish attempt at its own next generation is   *)
(* accepted as "success" even though `head.publishedBy` is the compactor, *)
(* not the writer. Every other property still holds under SpecBuggy,      *)
(* consistent with the bug never actually mutating `head` incorrectly --  *)
(* it only lies to its caller about what happened, which is exactly why   *)
(* AuthorshipHonest, not the state-monotonicity properties, is the one    *)
(* that catches it.                                                       *)
(*                                                                         *)
(* Getting this far took three real mistakes, each only caught by running *)
(* TLC and noticing a result that didn't match hand-tracing a reachable   *)
(* counterexample -- not by inspection:                                   *)
(*   1. The original `NoNode == CHOOSE v : v \notin Nodes` is an          *)
(*      unbounded CHOOSE, which TLC cannot evaluate at all (only bounded  *)
(*      `CHOOSE x \in S : P(x)` forms are supported) -- fixed by          *)
(*      declaring NoNode (and Compactor) as their own CONSTANTs (model    *)
(*      values in the .cfg files) instead of trying to derive them.       *)
(*   2. AuthorshipHonest was first written as `[][EXPR]_Vars`. The        *)
(*      standard semantics of `[A]_e` is `A \/ UNCHANGED e` -- and        *)
(*      `PublishBuggy`'s entire postcondition IS `UNCHANGED Vars`, so     *)
(*      that bracket made the property vacuously true on exactly the      *)
(*      transitions it exists to catch. TLC reported "no error" under     *)
(*      SpecBuggy, which contradicted a hand-traced counterexample.       *)
(*   3. The fix attempted next -- referencing the bare action formulas    *)
(*      `Publish(n,g)`/`PublishBuggy(n,g)` directly inside a `[]`-wrapped  *)
(*      property -- is unsound for a different reason: TLA+ action        *)
(*      formulas are checked structurally against any `(state, state')`   *)
(*      pair, independent of which actual Next-disjunct produced it. An   *)
(*      idempotent `Read` that happened to leave every variable's VALUE   *)
(*      unchanged satisfied `PublishBuggy`'s shape by coincidence, making *)
(*      the property fail even under the correct `Spec`, which never      *)
(*      takes a `PublishBuggy` step at all. The robust fix, used below,   *)
(*      is a dedicated ghost variable (`authorshipViolated`) that only    *)
(*      `PublishBuggy` itself ever sets -- turning "did this specific      *)
(*      action fire" into an observable fact in the state, checked as an   *)
(*      ordinary INVARIANT, rather than trying to reconstruct it after    *)
(*      the fact from a value-comparison that other actions can           *)
(*      coincidentally also satisfy.                                      *)
(*                                                                         *)
(* Run 3 (ShardHeadDecoupled.cfg, SpecDecoupled -- a PROPOSED, not yet      *)
(* implemented-in-Java redesign, run to de-risk it before touching the     *)
(* hot-path writer-publish Java code): decouples a writer's generation      *)
(* numbering from local Lucene state entirely -- PublishDecoupled always    *)
(* computes its target live as head.generation + 1 and fences against the  *)
(* live head.holder (not a cached localHead[n] belief), mirroring how       *)
(* CompactorPublish already works. All properties hold across the complete *)
(* reachable state space for the model's bound (2,742,008 distinct states, *)
(* depth 22, 0 states left on queue -- exhaustive). One design flaw was     *)
(* already caught and fixed by hand, before ever running TLC on this       *)
(* variant: an earlier draft fenced on the writer's own stale cached       *)
(* belief (localHead[n].holder = n) instead of the live head.holder = n,   *)
(* which would have let a writer already fenced out by a newer term's      *)
(* AcquireLease still slip a generation bump through under the new         *)
(* holder's identity -- see PublishDecoupled's own comment.                 *)
(*                                                                         *)
(* Still not covered: clones/cross-index bundle references (ss14).        *)
(*                                                                         *)
(* Reproduce: `java -jar tla2tools.jar -config ShardHead.cfg ShardHead.tla`*)
(* and        `java -jar tla2tools.jar -config ShardHeadBuggy.cfg ShardHead.tla`*)
(* and        `java -jar tla2tools.jar -config ShardHeadDecoupled.cfg ShardHead.tla`*)
(***************************************************************************)

EXTENDS Integers

CONSTANTS
    Nodes,          \* the (finite) set of nodes that can contend for this shard
    MaxTerm,         \* bound on primaryTerm, for finite-state model checking
    MaxGeneration,   \* bound on latestManifestGeneration, for finite-state model checking
    NoNode,          \* sentinel "no holder/author yet" value, its own model value (see STATUS)
    Compactor        \* sentinel identity for the compaction service, distinct from every real Node

ASSUME NoNode \notin Nodes
ASSUME Compactor \notin Nodes
ASSUME Compactor /= NoNode
ASSUME MaxTerm \in Nat /\ MaxTerm >= 1
ASSUME MaxGeneration \in Nat /\ MaxGeneration >= 1

VARIABLES
    head,                \* [term, holder, generation, publishedBy] -- see HeadType
    leaseExpired,        \* whether the current head's lease is currently expired (time-derived, in reality)
    version,             \* the opaque CAS version token guarding head; strictly increases on every mutation
    localHead,           \* per-node cache of the last head each node observed via Read
    localVersion,        \* per-node cache of the version token paired with localHead
    authorshipViolated   \* sticky ghost flag: set TRUE only by PublishBuggy, only when it fires in a
                          \* state where the head is genuinely NOT already authored by the calling
                          \* writer -- see PublishBuggy's own comment and the STATUS note above for
                          \* why this exists instead of expressing the property directly over actions.

(***************************************************************************)
(* `publishedBy` is a ghost/history field: nothing in the real ShardHead  *)
(* class has a literal "who authored this generation" attribute (a real  *)
(* manifest's content implicitly carries that), but this spec doesn't     *)
(* model manifest content at all, only generation numbers -- without some *)
(* way to name "whose data is actually visible," AuthorshipHonest (the    *)
(* property that catches the real bug) would have nothing to compare      *)
(* against. Every head-mutating action sets it explicitly.                 *)
(***************************************************************************)
HeadType == [
    term: 1..MaxTerm,
    holder: Nodes \cup {NoNode},
    generation: 0..MaxGeneration,
    publishedBy: Nodes \cup {NoNode, Compactor}
]

TypeOK ==
    /\ head \in HeadType
    /\ leaseExpired \in BOOLEAN
    /\ version \in Nat
    /\ localHead \in [Nodes -> HeadType]
    /\ localVersion \in [Nodes -> Int]
    /\ authorshipViolated \in BOOLEAN

Init ==
    /\ head = [term |-> 1, holder |-> NoNode, generation |-> 0, publishedBy |-> NoNode]
    /\ leaseExpired = TRUE   \* no lease has ever been granted yet
    /\ version = 0
    /\ localHead = [n \in Nodes |-> head]
    /\ localVersion = [n \in Nodes |-> -1]   \* -1 never matches a real version, so no node starts able to CAS
    /\ authorshipViolated = FALSE

(***************************************************************************)
(* A node refreshes its view of the shard head. In the real system this   *)
(* is ShardStateStore#get.                                                 *)
(***************************************************************************)
Read(n) ==
    /\ localHead' = [localHead EXCEPT ![n] = head]
    /\ localVersion' = [localVersion EXCEPT ![n] = version]
    /\ UNCHANGED <<head, leaseExpired, version, authorshipViolated>>

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
       /\ head' = [
              term |-> newTerm,
              holder |-> n,
              generation |-> IF isTakeover THEN 0 ELSE h.generation,
              publishedBy |-> IF isTakeover THEN NoNode ELSE h.publishedBy
          ]
       /\ version' = version + 1
    /\ leaseExpired' = FALSE
    /\ UNCHANGED <<localHead, localVersion, authorshipViolated>>

(***************************************************************************)
(* A node publishes a new (larger) manifest generation, matching the      *)
(* FIXED ObjectStoreCommitHeadPublisher#publishCommitAsHead. A node can    *)
(* only succeed if: its lease has not expired, its cached head is still    *)
(* current (the CAS guard), and it still holds the lease as of that        *)
(* cached head -- a node fenced out by another node's AcquireLease will    *)
(* have a stale localHead/localVersion and so cannot satisfy the CAS       *)
(* guard until it Reads again, at which point h.holder /= n and it can no  *)
(* longer attempt to publish at all.                                       *)
(***************************************************************************)
Publish(n, g) ==
    /\ leaseExpired = FALSE
    /\ LET h == localHead[n] IN
        /\ h.holder = n
        /\ g > h.generation
        /\ g <= MaxGeneration
        /\ version = localVersion[n]
        /\ head' = [h EXCEPT !.generation = g, !.publishedBy = n]
    /\ version' = version + 1
    /\ UNCHANGED <<leaseExpired, localHead, localVersion, authorshipViolated>>

(***************************************************************************)
(* The compaction service (LuceneMergeCompactionPublisher +               *)
(* CompactionRebaseExecutor): always re-reads the live head immediately   *)
(* before its CAS attempt on every retry (the real rebase loop re-reads    *)
(* `current` at the top of every iteration), so unlike a writer it never   *)
(* needs a separate Read() step first -- collapsing "re-read, recompute    *)
(* newGeneration = current.latestManifestGeneration()+1, CAS" into one     *)
(* atomic step is a faithful abstraction of a retry-until-success loop     *)
(* whose only observable outcome is eventual success or giving up (giving *)
(* up is not modeled: it only ever costs wasted work in the real system,   *)
(* per rfc-serverless-opensearch.md ss7.4, so it has no correctness        *)
(* property to violate). Preserves whatever term/holder the live head      *)
(* already has -- a compaction never changes who the lease holder is.      *)
(***************************************************************************)
CompactorPublish ==
    /\ head.generation < MaxGeneration
    /\ head' = [head EXCEPT !.generation = @ + 1, !.publishedBy = Compactor]
    /\ version' = version + 1
    /\ UNCHANGED <<leaseExpired, localHead, localVersion, authorshipViolated>>

(***************************************************************************)
(* Models the PRE-FIX ObjectStoreCommitHeadPublisher: when a writer's      *)
(* target generation `g` is found, on a fresh read, to already be at or    *)
(* behind the live head's generation (`g <= head.generation` -- only ever  *)
(* reachable here via an intervening CompactorPublish, since a different   *)
(* writer under a different term is already excluded by the `h.holder = n` *)
(* guard below), the OLD code took a shortcut: `return true` without any   *)
(* CAS attempt and without the head ever being mutated to reflect this     *)
(* writer's content. That is exactly what this action models: its guard    *)
(* is satisfiable in precisely the bug's trigger condition, and its        *)
(* postcondition on `head` (and friends) is UNCHANGED -- the caller is     *)
(* told "this generation is now (or already was) reflected in the shard's  *)
(* published head" (the real method's actual doc comment) when it is not.  *)
(*                                                                         *)
(* Sets `authorshipViolated' := TRUE` precisely when this fires in a state *)
(* where the live head is NOT already authored by this same writer (i.e.  *)
(* excludes the harmless case of a genuinely idempotent retry of a         *)
(* writer's own prior success) -- see the module header's STATUS note for *)
(* why a ghost variable is used here instead of referencing this action    *)
(* directly inside a temporal property.                                    *)
(*                                                                         *)
(* Deliberately excluded from `Next` (the real, current spec) -- included  *)
(* only in `NextBuggy`, so `Spec` represents production code as it is      *)
(* today and `SpecBuggy` represents it as it was before this session's fix.*)
(***************************************************************************)
PublishBuggy(n, g) ==
    /\ leaseExpired = FALSE
    /\ LET h == localHead[n] IN
        /\ h.holder = n
        /\ g >= 1   \* real manifest generations are always >= 1 (see ShardHead#initial(),
                     \* generation 0 is the "nothing published yet" sentinel, never a real
                     \* commit's own target) -- without this, TLC's first (smallest, per
                     \* breadth-first search) counterexample was the degenerate g=0 case,
                     \* not the intended "a compactor genuinely overtook this writer" one.
        /\ g <= head.generation
        /\ g <= MaxGeneration
    /\ authorshipViolated' = (authorshipViolated \/ head.publishedBy /= n)
    /\ UNCHANGED <<head, leaseExpired, version, localHead, localVersion>>

(***************************************************************************)
(* Models the PROPOSED redesign (not yet implemented in Java -- this is    *)
(* the design verification the RFC's AuthorshipHonest section flags as    *)
(* the real remaining fix, done here BEFORE touching production code):     *)
(* a writer's generation target is no longer fixed ahead of time from      *)
(* local Lucene state and then checked against a possibly-stale cached     *)
(* head. Instead, exactly like CompactorPublish, it is always recomputed   *)
(* live as `head.generation + 1` at the moment of the CAS attempt, and     *)
(* the fencing check is against the LIVE `head.holder`, not a cached       *)
(* `localHead[n]` -- collapsing "re-read, recompute, CAS" into one atomic  *)
(* step, the same retry-loop abstraction CompactorPublish already uses.    *)
(* This structurally cannot exhibit the AuthorshipHonest bug at all: there *)
(* is no branch that reports success without actually mutating `head` to  *)
(* match. What this action exists to check instead is a DIFFERENT risk a  *)
(* naive version of this redesign can introduce: if the fencing check      *)
(* were against a STALE local belief instead of the live value (an        *)
(* earlier draft of this action did exactly that, checking                *)
(* `localHead[n].holder = n`), a writer already fenced out by a newer      *)
(* term's AcquireLease could still slip a generation bump through under    *)
(* the NEW term/holder's identity, corrupting the legitimate new holder's  *)
(* generation sequence and violating OnlyTheCurrentHolderCanPublishDecoupled*)
(* below -- caught by hand-tracing before ever running TLC on it, exactly  *)
(* the kind of mistake this whole exercise exists to catch before it       *)
(* becomes a Java change.                                                  *)
(***************************************************************************)
PublishDecoupled(n) ==
    /\ leaseExpired = FALSE
    /\ head.holder = n
    /\ head.generation < MaxGeneration
    /\ head' = [head EXCEPT !.generation = @ + 1, !.publishedBy = n]
    /\ version' = version + 1
    /\ UNCHANGED <<leaseExpired, localHead, localVersion, authorshipViolated>>

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
    /\ UNCHANGED <<head, version, localHead, localVersion, authorshipViolated>>

Next ==
    \/ \E n \in Nodes : Read(n)
    \/ \E n \in Nodes : AcquireLease(n)
    \/ \E n \in Nodes, g \in 0..MaxGeneration : Publish(n, g)
    \/ CompactorPublish
    \/ ExpireLease

(* The pre-fix production code, for comparison -- see PublishBuggy's comment above. *)
NextBuggy ==
    \/ Next
    \/ \E n \in Nodes, g \in 0..MaxGeneration : PublishBuggy(n, g)

(* The proposed redesign -- see PublishDecoupled's comment above. Replaces Publish(n,g)
   entirely (a real writer would use one generation-numbering scheme, not both at once). *)
NextDecoupled ==
    \/ \E n \in Nodes : Read(n)
    \/ \E n \in Nodes : AcquireLease(n)
    \/ \E n \in Nodes : PublishDecoupled(n)
    \/ CompactorPublish
    \/ ExpireLease

Vars == <<head, leaseExpired, version, localHead, localVersion, authorshipViolated>>

Spec == Init /\ [][Next]_Vars /\ WF_Vars(Next)
SpecBuggy == Init /\ [][NextBuggy]_Vars /\ WF_Vars(NextBuggy)
SpecDecoupled == Init /\ [][NextDecoupled]_Vars /\ WF_Vars(NextDecoupled)

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
(* Property 6 (authorship, the one that catches the real bug this session *)
(* found and fixed), restated as a plain INVARIANT rather than a temporal  *)
(* property over actions -- see the module header's STATUS note for why.  *)
(* Holds under `Spec` (PublishBuggy is never in `Next`, so                *)
(* `authorshipViolated` can never be set); TLC finds a counterexample      *)
(* under `SpecBuggy`.                                                      *)
(***************************************************************************)
AuthorshipHonest == authorshipViolated = FALSE

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
(* concurrent publication can never accidentally publish backwards. Holds *)
(* under SpecBuggy too -- PublishBuggy never mutates head at all, so it   *)
(* cannot itself cause a regression; the bug it models is a lie to its    *)
(* caller, not a state-level violation, which is exactly why this         *)
(* property alone would never have caught it (see AuthorshipHonest).      *)
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
(* Property 5 (fencing): whenever a Publish(n) step is actually taken,    *)
(* the *real, global* head.holder (not merely n's cached belief) must     *)
(* already equal n. Publish's guard only checks n's local cache           *)
(* (`localHead[n].holder = n`) plus the CAS token                         *)
(* (`version = localVersion[n]`) -- this property is the substantive      *)
(* claim that those two checks are jointly sufficient to guarantee the    *)
(* real holder is n, i.e. that a node fenced out by a concurrent          *)
(* AcquireLease can never slip a publish through on stale state.          *)
(***************************************************************************)
OnlyTheCurrentHolderCanPublish ==
    [][\A n \in Nodes, g \in 0..MaxGeneration : Publish(n, g) => head.holder = n]_Vars

(***************************************************************************)
(* The `PublishDecoupled`/`SpecDecoupled` analogue of Property 5, checked  *)
(* under the proposed redesign. Unlike `OnlyTheCurrentHolderCanPublish`,   *)
(* this one is expected to hold close to "by construction" -- the whole    *)
(* point of the redesign is checking `head.holder = n` live, so a violation*)
(* here would mean the guard itself is wrong, not that the interaction     *)
(* with other actions is subtle. Checked anyway as a basic sanity          *)
(* confirmation before trusting the more substantive properties below it   *)
(* (TermNeverDecreases, GenerationMonotonicWithinTerm,                     *)
(* GenerationResetsOnNewTerm, AtMostOneValidHolder -- all reused unchanged  *)
(* under `SpecDecoupled`, since none of them reference `Publish` by name). *)
(* Referencing `PublishDecoupled(n)` directly inside this property (rather *)
(* than needing a ghost variable the way `AuthorshipHonest` did) is sound  *)
(* here specifically because `PublishDecoupled`'s postcondition always      *)
(* substantively changes `head` (generation strictly increases, publishedBy*)
(* is set to `n` -- never `Compactor`, since `Compactor \notin Nodes`) in a *)
(* way no other action in `NextDecoupled` can coincidentally produce --     *)
(* checked by hand for each of Read/AcquireLease/CompactorPublish/         *)
(* ExpireLease, the same class of mistake `AuthorshipHonest` fell into     *)
(* with `PublishBuggy` (whose postcondition was `UNCHANGED` everything,    *)
(* trivially coincidable with any other no-op-shaped transition).          *)
(***************************************************************************)
OnlyTheCurrentHolderCanPublishDecoupled ==
    [][\A n \in Nodes : PublishDecoupled(n) => head.holder = n]_Vars

=============================================================================
