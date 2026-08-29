# Two processes contending for the same shard

Every test before this ran nodes as objects inside the test's own JVM. That is enough for logic and not
enough for the claim the design rests on: that a node which **dies** is recovered from. `close()` unwinds
cleanly, releases its lease and publishes on the way out — it is the opposite of a crash. Only a real
process can be killed, and only a real process can be frozen.

## Getting a forked node to run at all

Three obstacles, each worth recording because none is obvious:

1. **`posix_spawn` failed with EPERM.** Not a file mode: the OpenSearch test framework installs a seccomp
   system-call filter that blocks `execve`, deliberately, because a node has no business spawning
   children. These tests get their own gradle task (`processTest`) with `tests.system_call_filter=false`
   rather than the whole module losing the guard.
2. **The child died inside `NodeEnvironment`.** `Randomness` decides it is "running tests" purely by
   whether `randomizedtesting` is loadable, then demands a test context a real `main()` cannot provide.
   The child is given the **main source set's** runtime classpath — server + shell + netty4, no test
   framework — which is also simply the more faithful thing to fork.
3. **Readiness.** The HTTP port is 0, so the parent cannot know the address in advance, and polling a
   port that is not open yet is indistinguishable from polling one that never will be. The bootstrap
   writes a readiness file once it is actually serving. A file rather than a line on stdout because
   `System.out` is forbidden in production code here — and it is the better mechanism anyway: it does not
   race with log output and it survives whatever is or is not tailing the process.

## Two real bugs, both only reachable across processes

### A node forwarding a write to itself

`activateWriter` wins the compare-and-swap **before** it opens the shard. In that window the head names
the node and the node has no open shard, so a concurrent write read the head, saw itself as owner,
forwarded to itself over the transport, and was refused by its own receiving handler — surfacing as a
**500 for a normal, brief, self-resolving state**.

Now answered with `503 activation_in_progress`, which is a status a client retries. A failed forward is
also 503 rather than the exception's own status, with the cause in the message: a forward that fails means
routing was stale, and rendering that as a 500 tells a client the write is hopeless when the right answer
is "ask again in a moment".

Reproducing it is a race that fires on a cold JVM and not a warm one, so the regression test builds the
window deliberately in one JVM instead: take the shard, then close it locally without touching the head.

### A frozen owner wedging every writer that routes to it

`SIGSTOP` a node and its TCP socket stays open, so peers connect fine and hang. Two separate unbounded
waits, and fixing either alone leaves the other:

- **The transport handshake**, before a byte of the write is sent. `connectToNode` with no
  `ConnectionProfile` waits the default 30 seconds.
- **The response**, on a connection already established. `TransportRequestOptions.EMPTY` and a bare
  `actionGet()` wait forever.

Both now bounded by the **lease TTL**, which is the principled number rather than a round one: there is no
value in waiting longer than the lease, because a peer that has not answered within it is a peer whose
lease is lapsing, and the right move then is to re-read the head. Unbounded, a stop-the-world GC or a
suspended container takes the whole write thread pool of every healthy node routing to it.

Each has its own test — cold connection and warm connection — because a single test covers only one.

## What the canaries changed

The first version of this suite had four tests that all passed. Then the canaries ran, and **three of the
four defects were not caught**:

| Test as first written | Why it proved nothing |
|---|---|
| "two processes race for one shard" | The writes were sequential. Whichever node was asked first activated; the other saw a live owner and forwarded without ever attempting acquisition. Deleting "a live lease is not stolen" left it passing. Now both nodes are released off a barrier and write concurrently. |
| "a restarted owner cannot overwrite its successor" | A killed-and-restarted node is not a zombie: it comes back empty, wins a fresh CAS, and owns the shard legitimately at a higher term. It never publishes at a stale one. Now `SIGSTOP`/`SIGCONT`. |
| "a frozen owner does not wedge writers" | My own retry-on-timeout in the test helper papered over the hang I had just fixed. Now asserts on **elapsed time and a returned status**, because the failure mode is not a wrong answer, it is no answer. |

## The test that still proves nothing, and is labelled as such

`testAFrozenNodeResumingAfterTakeoverLeavesTheDataIntact` passes with **term fencing deleted**, with the
publish-path **ownership recheck deleted**, with **both** deleted, and with the **heartbeat release**
deleted. Four canaries, none caught. Whatever keeps the resumed node harmless, it is none of those, and I
did not isolate what it is.

It is kept because the scenario is real and exercised end to end across two processes, and a regression
that broke it would be worth knowing about. Its javadoc says plainly that it is an outcome check and not
evidence that zombie writes are fenced. The mechanism that **is** proven is
`ServerlessSchedulerTests.testAFencedPublishReleasesTheShardImmediately`, in one JVM, where the window can
be held open deliberately instead of hoped for.

Two things had to be corrected before it measured even that much, and both generalise:

- **The publish debounce must straddle the freeze.** Too short and the zombie publishes before it is
  frozen, wakes with nothing new, and the idle-shard guard alone keeps it harmless. Too long and it never
  publishes at all. Neither extreme tests anything.
- **`assertBusy` cannot express a monotonicity claim.** It retries until the condition holds, so a value
  that dipped and recovered satisfies it exactly as well as one that never dipped — and a clobber is
  transient by nature, because the successor republishes and repairs it. The invariant has to be sampled
  and fail on the first bad sample.

## A design observation the canaries surfaced

`BackgroundReconciler.publish` publishes at **the term it reads from the head**, not the term the node
acquired at. So a zombie that got past the ownership recheck would write stale bytes under its
successor's *current* term, and `SegmentPublisher`'s `existing.term() > term` could never fire. The
ownership recheck and term fencing are **complementary, not layered**: fencing covers the race between the
recheck and the write; the recheck covers everything before it. Neither is a backstop for the other.

## What this does NOT establish

- **Nothing about zombie publish fencing across processes**, per above.
- **Two processes, not many.** No test runs three or more, and nothing exercises a shard moving twice.
- **One observed flake.** The warm-connection frozen-owner test timed out once at ~30s in roughly five
  runs, then passed three consecutive full runs. Not reproduced, not explained, not to be treated as
  stable.
- **Still a local filesystem (D5).** These are real processes against a shared directory, not an object
  store.
- **`kill -9` and `SIGSTOP` only.** No partial network failure, no disk-full, no clock skew between the
  two processes — which remains untested precisely because both share one machine clock.
