# Round 005: The doors into the mapping store, and what they do when it is gone

Goal: every path that reaches the gated mapping store either runs off the cluster state thread or
fails where it can be attributed, and every path that finds the store missing behaves the same way
as every other, rather than reads refusing while writes quietly rebuild.

Exit: no path reaches the mapping write on a thread where blocking is unsafe; a lost mapping index
produces one behaviour rather than two; and the four claims round 004's own review found standing
after they had been falsified are corrected.

Why this and not eviction under load, which the RFC's order of work still puts first: round 004 left
two defects whose shapes are a failed bulk write and a silently rebuilt system index, and one of them
is reachable by indexing a document into a name that does not exist yet. Eviction is a measurement
task about a ceiling. These are live. Eviction is deferred a second time, deliberately, and that is
worth saying out loud rather than letting it slide.

## Standing notes

- T-numbers: the tree cites to T49. This round is T50 to T58. Check with
  `git grep -hoE '\bT[0-9]{1,3}\b' -- server/src plugins/serverless-storage/src | sort -u -V | tail`
  before allocating more, because round 004 got this wrong by trusting a sentence in STATE.md.
- T50 to T53 were filed during round 004 with criteria. They are restated here so this plan stands
  alone.
- Round 004's retro asks for a worktree build and four to ten runs to be budgeted for any measurement
  task. This round has none, which is itself a signal: it is a round of fixes.

## Tasks

### T50 — A mapping written after its index was deleted is stranded forever

- Depends on: none
- Goal: T47 prunes a gated index's mapping when the index is deleted. A write arriving after the
  prune re-creates the document and nothing will ever remove it again. The window is real: a
  tombstone invalidates only the writing node's descriptor cache, so another node keeps resolving the
  name until its freshness window expires, and an in-flight put-mapping or dynamic inference on that
  node writes generation 1 under the deleted UUID.
- Acceptance:
  - a test that writes a mapping for a UUID whose tombstone is already durable, and asserts either
    the write is refused or the document does not survive
  - whichever way it is closed is named in the code: refusing needs the tombstone consulted on the
    mapping path; letting it land needs something that prunes it later, and a second sweep is a cost
    to justify rather than assume
  - the same test covers the read-side symptom: a query against the still-resolving name must not
    silently return zero hits on a declared field
  - mutation: removing the guard leaves a document behind and fails the test
- Risk: medium
- Kind: fix
- **One approach attempted and refuted. Start from this rather than repeating it.** Moving the prune
  from the deletion to `TombstoneScrubber`, so a mapping lives exactly as long as its tombstone, is
  wrong for two independent reasons, both found by review after it was built and both verified in the
  code:
  - The scrubber is opt-in. `serverless_storage.descriptor.tombstone_scrub_interval` defaults to
    zero, and the plugin README recommends an object-store lifecycle rule instead, which expires the
    tombstone blob without running any of this. So in the default deployment and in the recommended
    one, nothing prunes at all, which is the leak T47 fixed, reintroduced.
  - Tombstones are keyed by index name, not by uuid: `tombstoneKeyFor(name)` is
    `TOMBSTONE_PREFIX + name`, and a second deletion of the same name overwrites the first. The
    scrubber's worklist therefore carries one uuid per name, while mappings are keyed per uuid, so a
    tenant recycling one name N times strands N-1 mappings permanently. "The worklist comes free"
    was the design's central claim and it is false.
  What survives from the attempt: the prune has to stay on a path that runs unconditionally, and any
  mechanism keyed off tombstones has to handle name reuse before it can be trusted with a uuid.

### T51 — Three doors into gated creation have no admission check

- Depends on: none
- Goal: `createIndex` is not the only way into the gated branch. Auto-creation, rollover and data
  stream creation call `applyCreateIndexRequest` from inside a cluster state update task, so no
  admission check runs and none can. With a gated template in place, indexing a document into a name
  that does not exist reaches the mapping write on the cluster manager's update thread. Before T49
  that blocked or deadlocked; since T49 the tripwire refuses it, which fails the bulk. Better, and
  still wrong.
- Acceptance:
  - a test that puts a gated template and then indexes a document into a matching name that does not
    exist, asserting the document is indexed rather than the request refused
  - the same for a rollover into a template-gated name
  - whatever routes those callers off-thread is one mechanism rather than three, named in the code
  - mutation: reverting it fails both tests with the offending thread named
- Risk: high
- Kind: fix

### T52 — Admission and creation resolve templates differently in three cases

- Depends on: none
- Goal: `settingsForAdmission` is close to what creation computes and not identical. Creation
  resolves a v2 template against the data stream name when there is one, and skips templates entirely
  for a resize target and for a system index. Admission does none of those, so a resize target or a
  system index whose name matches a gated template is over-admitted and pays the whole creation twice
  through the fallback.
- Acceptance:
  - one function computes what both readers use, or each difference is enumerated in code with its
    reason
  - a test for the resize-target case, which is the one with a cost today: it asserts the creation is
    not admitted as gated
  - mutation: reintroducing the divergence fails that test
- Risk: low
- Kind: fix

### T53 — The threading proof cannot attribute a store call to a phase

- Depends on: T56
- Goal: the recording store notes the calling thread and discards the index, so a phase's "it reached
  the store" assertion is satisfied by any call landing in its window, including one belonging to
  another index. The phases are only as strong as their attribution.
- Acceptance:
  - the recorder keeps the index alongside the thread, and each phase asserts its own index appears
  - the deletion prune added by T47 gets a phase, since it is the one caller whose thread nothing
    checks
  - mutation: making a phase's own call disappear fails that phase rather than passing on another's
- Risk: low
- Kind: proof

### T54 — Writes rebuild a lost mapping index quietly while reads refuse loudly

- Depends on: none
- Goal: T48 taught reads to refuse once `.opensearch-index-mappings` has been lost, and wired the
  signal into `read` only. `compareAndSwap` still short-circuits on a cached "the index exists" flag,
  so the next gated creation after a deletion lets auto-creation rebuild the index at cluster
  defaults. Three consequences, all silent: T45's shard setting is void; `fields` is dynamically
  mapped, so the index absorbs the union of every gated index's field names, which is the residency
  problem this area exists to remove one level down and will eventually hit the total-fields limit;
  and `fieldTypeCounts` is no longer nested, so the stats aggregator matches nothing and cluster
  stats reports zero fields for every gated index, successfully.
- Acceptance:
  - the cached flag is cleared by the same signal `read` consults, so the next write recreates the
    index with its intended settings and mapping rather than inheriting defaults
  - a test that deletes the mapping index, drives one gated creation, and asserts the recreated index
    has the configured shard count and a disabled `fields` object
  - a test that the same sequence leaves cluster stats reporting the gated indices rather than zero
  - mutation: leaving the flag set fails both
- Risk: medium
- Kind: fix

### T55 — A stale read failure is reported as contention

- Depends on: none
- Goal: `updateMapping` records the last read failure and never clears it, so one transient failure on
  attempt 0 followed by fifteen genuine lost swaps raises the transient failure as the cause. That is
  the misdiagnosis T43 set out to remove, reintroduced by T43's own code. The retry it added also has
  no backoff, so sixteen attempts against a relocating shard fire within microseconds and the
  resilience the comment claims is not there.
- Acceptance:
  - a test with a store double failing the first read and then succeeding while losing swaps, which
    asserts the raised error names contention rather than the read failure
  - a test that a store failing every read raises the read failure
  - whatever spacing the retries get is justified in the comment or the claim is removed
  - mutation: not clearing the recorded failure fails the first test
- Risk: low
- Kind: fix

### T56 — Seven implementations of a three-method interface

- Depends on: none
- Goal: `MappingGenerationStore.Store` has seven implementations across tests, four of them the same
  decorator differing only in which counters they carry. `Store` has no default methods deliberately,
  so each new interface method is added to all seven by hand: T47 already paid that, and two of the
  doubles gained a `delete` that throws an assertion nothing enforces, because the caller catches
  `Throwable`. T41's central property is asserted through two independently written counting doubles
  in two files.
- Acceptance:
  - one test-scoped decorator records reads, swaps and deletes with the index each was called for, and
    the four counting doubles are replaced by it
  - the two doubles asserting "nothing on this path deletes a mapping" either keep a claim something
    checks or drop it
  - every existing test that used the replaced doubles passes unchanged
- Risk: low
- Kind: fix

### T57 — `settingsForAdmission` runs twice per creation, against two snapshots

- Depends on: none
- Goal: T49's stated goal was one computation with two readers. What shipped computes it twice: once
  on a transport thread at routing, once on the cluster manager's update thread inside
  `whyATemporaryIndexServiceIsStillNeeded`, each reading `clusterService.state()` separately, while
  every other resolution in that method uses the state the task is folding. So every creation on a
  cluster with the plugin installed, gated or not, adds a cluster state read and a full template
  resolution to the thread this whole area exists to keep work off.
- Acceptance:
  - the second reader uses the settings the first computed, or the state the task is folding, and not
    a fresh `clusterService.state()`
  - a test asserts template resolution happens once per creation, using a counting seam or a
    cluster-state read counter
  - the comment claiming the readers share a computation is true afterwards or gone
- Risk: medium
- Kind: fix

### T58 — Four claims falsified during round 004 and left standing

- Depends on: none
- Goal: the round corrected several of these deliberately and missed four. Each is a sentence a reader
  goes to in order to learn a contract, and each now says something the code disproves.
- Acceptance:
  - `DescriptorGate`'s comment above `MappingGenerationStore.register` names all three callers and
    stops asserting the property T49 exists because it was false
  - `IndexBackedMappingStoreTests`'s class javadoc stops stating unconditionally that external
    versioning prevents a merge from empty, since two tests in the same file cover the case where the
    document it would refuse against is gone
  - `Store.read`'s contract says null means absent and only absent; the only implementation answers
    null for a case it cannot find out about after a cluster manager restart following a deletion.
    Either the contract is qualified or the implementation is
  - each correction cites what falsified it, so the next reader can tell a correction from an opinion
- Risk: low
- Kind: fix
