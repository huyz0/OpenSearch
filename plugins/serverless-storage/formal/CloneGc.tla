----------------------------- MODULE CloneGc -----------------------------
(***************************************************************************)
(* Models the interaction between zero-copy clone's source-side GC pin      *)
(* (rfc-serverless-opensearch.md &sect;14, &sect;18.5) and the source        *)
(* shard's own background GC sweep (`GcSchedulerTask`/                       *)
(* `BundleReferenceCounter`/`ManifestRetentionPolicy`) -- the "coordination- *)
(* free GC" correctness surface &sect;18.5 flags as the one place a design   *)
(* bug destroys data, and which the existing `ShardHead.tla` model            *)
(* explicitly does not cover ("Still does not cover clones/cross-index       *)
(* references").                                                             *)
(*                                                                           *)
(* `ShardCloner.clone` (Java) reads the source shard's current head          *)
(* generation, reads that generation's manifest (a real blob fetch that       *)
(* only succeeds if the generation is still live -- i.e. GC has not yet      *)
(* swept it), and only THEN adds the durable pin protecting it. Between       *)
(* the manifest read succeeding and the pin actually being durably           *)
(* recorded, nothing stops `GcSchedulerTask`'s own independent sweep from     *)
(* running: if the source shard has meanwhile committed a newer generation    *)
(* (making the just-read one no longer the head) and that sweep runs          *)
(* before the pin lands, the just-read generation gets deleted -- and the     *)
(* pin that arrives moments later ends up protecting bundles that are        *)
(* already gone, exactly the corruption &sect;18.5 warns about.               *)
(*                                                                           *)
(* This model checks both the shipped ("Buggy": pin-after-read) ordering     *)
(* and a fix ("Fixed": pin-before-read, using the generation number already  *)
(* known from the `ShardHead` read itself -- `readManifest`'s own return      *)
(* value adds nothing `ShardCloner` doesn't already have) side by side,       *)
(* against the identical shared timeline of commits/sweeps -- the same        *)
(* Spec/SpecBuggy-style comparison `ShardHead.tla`/`WalReplayFencing.tla`      *)
(* already use, via two independent ghost variables rather than two           *)
(* separate specs, so both results come from one single TLC run.              *)
(***************************************************************************)

(***************************************************************************)
(* STATUS: machine-checked with TLC (tla2tools.jar, TLC2 2.19), CloneGc.cfg,*)
(* MaxGeneration = 3 (generations 0..3, four total).                        *)
(*                                                                           *)
(* NoCloneEverReferencesADeletedGenerationBuggy is VIOLATED with a minimal   *)
(* counterexample: generation 0 exists, BuggyCloneReadManifest captures      *)
(* head=0, Commit advances to generation 1 (0 is no longer head), GCSweep    *)
(* deletes 0 (superseded, not yet pinned), then BuggyCloneAddPin finally     *)
(* fires and pins generation 0 -- which by then no longer exists. This is a  *)
(* machine-verified confirmation that `ShardCloner.clone`'s shipped          *)
(* pin-after-read ordering has a genuine TOCTOU race, not a hypothetical one.*)
(*                                                                           *)
(* NoCloneEverReferencesADeletedGenerationFixed HOLDS across the complete    *)
(* reachable state space for this bound: the pin-before-read fix closes the  *)
(* gap, since GCSweep's own deletable-generation formula already excludes    *)
(* anything currently in `pinnedGens`, and the fix ensures the generation    *)
(* enters `pinnedGens` before any sweep can observe it as unpinned.          *)
(*                                                                           *)
(* Fixed in Java in the same session this model was written --              *)
(* `ShardCloner.clone` now calls `sourcePinRegistry.addPin` using             *)
(* `head.primaryTerm()`/`head.latestManifestGeneration()` directly, before   *)
(* calling `sourceManifestStore.readManifest`, not after.                    *)
(*                                                                           *)
(* Reproduce:                                                                *)
(*   java -jar tla2tools.jar -config CloneGc.cfg CloneGc.tla                *)
(***************************************************************************)

EXTENDS Integers

CONSTANTS
    MaxGeneration    \* bound on how many generations Commit may create, for finite-state checking

VARIABLES
    liveGenerations, \* set of generation numbers still existing on the source shard (not yet GC'd)
    nextGen,         \* the next generation number Commit will create
    pinnedGens,       \* set of generations currently protected by an active durable clone pin
    buggyPending,     \* NoGen, or the generation number an in-flight *buggy* clone has read but not yet pinned
    buggyCorrupted,   \* ghost: TRUE once a buggy clone has pinned a generation that no longer exists
    fixedPending,     \* NoGen, or the generation number an in-flight *fixed* clone has pinned but not yet read
    fixedCorrupted    \* ghost: TRUE once a fixed clone's "read" would have found the generation already gone

Vars == <<liveGenerations, nextGen, pinnedGens, buggyPending, buggyCorrupted, fixedPending, fixedCorrupted>>

NoGen == -1

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

Init ==
    /\ liveGenerations = {0}
    /\ nextGen = 1
    /\ pinnedGens = {}
    /\ buggyPending = NoGen
    /\ buggyCorrupted = FALSE
    /\ fixedPending = NoGen
    /\ fixedCorrupted = FALSE

(* A new commit on the source shard -- the head moves forward, any prior
   generation immediately becomes eligible for GC (subject to pinning). *)
Commit ==
    /\ nextGen <= MaxGeneration
    /\ liveGenerations' = liveGenerations \cup {nextGen}
    /\ nextGen' = nextGen + 1
    /\ UNCHANGED <<pinnedGens, buggyPending, buggyCorrupted, fixedPending, fixedCorrupted>>

(* GcSchedulerTask's own sweep, abstracted: deletes every generation that is
   neither the current head nor currently pinned -- the retention-window time
   delay itself is abstracted away entirely, the same way ShardHead.tla
   abstracts lease TTLs into plain nondeterministic "eventually" transitions. *)
GCSweep ==
    LET head == Max(liveGenerations)
        deletable == {g \in liveGenerations : g # head /\ g \notin pinnedGens}
    IN
        /\ deletable # {}
        /\ liveGenerations' = liveGenerations \ deletable
        /\ UNCHANGED <<nextGen, pinnedGens, buggyPending, buggyCorrupted, fixedPending, fixedCorrupted>>

(* deleteClone releasing an *already-completed* clone's pin -- deliberately
   excludes fixedPending: `deleteClone` needs that clone's own `CloneLineage`
   record, which `ShardCloner.clone` (a single synchronous call) only writes
   *after* the pin-then-manifest-read sequence this model checks has already
   finished, so nothing could legitimately call `deleteClone` for a clone
   that is still mid-flight between its own pin and its own read. Any other,
   already-pinned generation (from an earlier, already-completed clone) is
   fair game, modeling genuine concurrent activity from unrelated callers. *)
UnpinClone ==
    \E g \in (pinnedGens \ {fixedPending}) :
        /\ pinnedGens' = pinnedGens \ {g}
        /\ UNCHANGED <<liveGenerations, nextGen, buggyPending, buggyCorrupted, fixedPending, fixedCorrupted>>

----------------------------------------------------------------------------
(* Buggy: ShardCloner.clone's shipped ordering -- read the manifest (only
   possible while the generation is still live) first, pin it second. *)

BuggyCloneReadManifest ==
    /\ buggyPending = NoGen
    /\ liveGenerations # {}
    /\ buggyPending' = Max(liveGenerations)  \* the read succeeds: this generation is live right now
    /\ UNCHANGED <<liveGenerations, nextGen, pinnedGens, buggyCorrupted, fixedPending, fixedCorrupted>>

BuggyCloneAddPin ==
    /\ buggyPending # NoGen
    /\ buggyCorrupted' = (buggyCorrupted \/ buggyPending \notin liveGenerations)
    /\ pinnedGens' = pinnedGens \cup {buggyPending}
    /\ buggyPending' = NoGen
    /\ UNCHANGED <<liveGenerations, nextGen, fixedPending, fixedCorrupted>>

----------------------------------------------------------------------------
(* Fixed: pin the current head generation first (its number is already known
   from the ShardHead read alone -- no manifest fetch needed to learn it),
   read the manifest second. *)

FixedCloneAddPin ==
    /\ fixedPending = NoGen
    /\ liveGenerations # {}
    /\ LET head == Max(liveGenerations)
       IN
         /\ pinnedGens' = pinnedGens \cup {head}
         /\ fixedPending' = head
    /\ UNCHANGED <<liveGenerations, nextGen, buggyPending, buggyCorrupted, fixedCorrupted>>

FixedCloneReadManifest ==
    /\ fixedPending # NoGen
    /\ fixedCorrupted' = (fixedCorrupted \/ fixedPending \notin liveGenerations)
    /\ fixedPending' = NoGen
    /\ UNCHANGED <<liveGenerations, nextGen, pinnedGens, buggyPending, buggyCorrupted>>

----------------------------------------------------------------------------

Next ==
    \/ Commit
    \/ GCSweep
    \/ UnpinClone
    \/ BuggyCloneReadManifest
    \/ BuggyCloneAddPin
    \/ FixedCloneAddPin
    \/ FixedCloneReadManifest

Spec == Init /\ [][Next]_Vars

TypeOK ==
    /\ liveGenerations \subseteq (0..MaxGeneration)
    /\ pinnedGens \subseteq (0..MaxGeneration)
    /\ nextGen \in 1..(MaxGeneration + 1)
    /\ buggyPending \in ({NoGen} \cup (0..MaxGeneration))
    /\ fixedPending \in ({NoGen} \cup (0..MaxGeneration))
    /\ buggyCorrupted \in BOOLEAN
    /\ fixedCorrupted \in BOOLEAN

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)

(* Expected to FAIL: this is the bug -- see STATUS above for the concrete
   counterexample TLC finds. *)
NoCloneEverReferencesADeletedGenerationBuggy == buggyCorrupted = FALSE

(* Expected to HOLD: the pin-before-read fix closes the race. *)
NoCloneEverReferencesADeletedGenerationFixed == fixedCorrupted = FALSE

=============================================================================
