# T18: making a gated creation mean something

Two spikes found two defects on the gated creation path. They look separate and they are one defect seen
twice, so they get one fix.

- **T17** ([GatedCreationDurabilityIT](../plugins/serverless-storage/src/internalClusterTest/java/org/opensearch/serverless/storage/descriptor/GatedCreationDurabilityIT.java)) — with the descriptor index closed, `create index` returns `acknowledged=true`. The index exists in no cluster state entry and no descriptor. It exists nowhere.
- **T23** ([GatedCreationUniquenessIT](../plugins/serverless-storage/src/internalClusterTest/java/org/opensearch/serverless/storage/descriptor/GatedCreationUniquenessIT.java)) — eight clients creating the same gated name concurrently were **all eight** acknowledged.

Both tests are committed under `AwaitsFix` so the defects stay reproducible.

## What the path does today

```
transport
  └─ MetadataCreateIndexService.onlyCreateIndex
       └─ clusterService.submitStateUpdateTasks(CreateIndexTask)     ── batched
            └─ createIndexExecutor: for each task
                 └─ applyCreateIndexRequest  → builds IndexMetadata
                      └─ clusterStateCreateIndex
                           ├─ gated?  IndexDescriptorPublisher.publish(md); return currentState
                           └─ else    Metadata.builder().put(md).build()
       └─ AckedClusterStateUpdateTask.onAllNodesAcked → listener.onResponse(newResponse(true))
```

`publish` routes through `DescriptorGate` to `DescriptorStore.putAsync`, which submits and returns, logging
failures at warn.

Three consequences follow, and each is one of the observed defects:

1. **`publish` returns whether a publisher was invoked, not whether the write landed.** So the gated branch
   cannot fail even when the write cannot possibly succeed. That is T17.
2. **`putAsync` is a plain put, not `op_type=create`.** `DescriptorStore.create` implements H3's uniqueness
   gate, is tested, and is not called from the creation path. That is T23.
3. **The acknowledgement is chained to a cluster state update that, for a gated index, does nothing.** It
   returns `currentState` unchanged, so it always succeeds, so the ack always fires.

T23 is worse than a race and worth being precise about. An ordinary index gets uniqueness from the executor
threading state through its loop: the second task in a batch sees the first task's index in the state it is
handed. A gated index is never added to that state, so the second task validates against a state the first
did not change. There is no window to lose, only a guarantee that was never there.

## The constraint that produced this

W4 is why `putAsync` exists. `IndexDescriptorPublisher.publish` is also called from
`Metadata.Builder.publishDescriptorIfIncremental`, which runs while a cluster state is being built. A
blocking index write there deadlocks: the write needs a cluster state to route, and the thread that would
supply it is the one blocked. It hung the node rather than failing, which is how it was found.

That constraint is real and any fix has to respect it. **The cluster manager thread must never block on a
descriptor write.**

## The insight the fix rests on

The cluster manager thread does not need to wait. *The client's acknowledgement* needs to wait. Those are
separable, because `CreateIndexTask` extends `AckedClusterStateUpdateTask`, whose contract is:

```java
public void onAllNodesAcked(@Nullable Exception e) {
    listener.onResponse(newResponse(e == null));
}
```

The listener is invoked from the acknowledgement path, not from `execute`. So the cluster manager thread can
issue the write and return immediately, and the response can be deferred until the write completes. Nothing
blocks the thread W4 protects.

## Design

**1. `DescriptorStore.createAsync(descriptor) -> CompletableFuture<Boolean>`**

Non-blocking `op_type=create`. Completes `true` when this call created the descriptor, `false` on
`VersionConflictEngineException` (someone else won, which H3 calls the mechanism working), exceptionally on
anything else. The existing blocking `create` stays for callers that want it.

**2. The gated branch issues the create and publishes the future**

`clusterStateCreateIndex` currently calls `publish` for its side effect. It instead obtains the future and
hands it to a sink supplied by the caller, threaded down from the executor, which is the only place holding
both the task and the request.

**3. `CreateIndexTask` chains its response on that future**

```java
@Override
public void onAllNodesAcked(Exception e) {
    if (descriptorWrite == null) {          // ordinary index: unchanged
        super.onAllNodesAcked(e);
        return;
    }
    descriptorWrite.whenComplete((created, failure) -> {
        if (failure != null)      listener.onFailure(failure);
        else if (created == false) listener.onFailure(new ResourceAlreadyExistsException(index));
        else                       listener.onResponse(newResponse(e == null));
    });
}
```

`onAckTimeout` needs the same treatment. `listener` is private in the parent, so it needs a protected
accessor or the chaining needs to live in an override that has it.

## What this gives

| | before | after |
|---|---|---|
| write fails | `acknowledged=true` | request fails |
| eight concurrent same-name creations | 8 acknowledged | 1 acknowledged, 7 `ResourceAlreadyExists` |
| cluster manager thread | never blocks | never blocks |
| batching | preserved | preserved |
| ordinary index path | — | untouched |

It also removes the descriptor write's request preparation from the cluster manager thread, which S36
measured at roughly 10 percent of it. That is a side effect rather than the reason.

## Cases worked through

**Two tasks in one batch, same name.** Both build metadata against the same state, both call `createAsync`.
One completes `true`, the other `false` and fails with `ResourceAlreadyExists`. Correct, and it is the case
that fails today.

**State update fails after the descriptor landed.** For a gated index the state update is a no-op returning
`currentState`, so it cannot fail on its own. If the batch fails for another task, the descriptor exists and
the index genuinely exists, so a retry gets `ResourceAlreadyExists`. Truthful, and the safe direction.

**Descriptor write outlives the ack timeout.** `onAckTimeout` fires with the write still in flight. Because
the write is the creation, the honest response is failure with the index possibly existing, which is the
same contract any timed-out create has.

**Non-gated indices.** `descriptorWrite` is null and `super.onAllNodesAcked` runs. The H2b dual-write from
`Metadata.Builder` is untouched and stays `putAsync`, because for an index that is in cluster state a lost
descriptor costs a comparison rather than the index. Failure semantics differ between the two call sites
because what is at stake differs, and that is now explicit rather than incidental.

## The alternative, and why not

Move the whole pipeline off the cluster manager thread and never submit a task for a gated index. That is
what H3 describes and it is architecturally cleaner.

Rejected for now on three counts. It needs the gated decision made *before* the pipeline runs, from request
settings plus resolved templates, and a second decision point that can disagree with
`DescriptorOnlyCreation.skipsClusterState` is exactly the divergence `DescriptorGate`'s own comment warns
about. It changes sequencing for every index rather than for the gated branch. And its main additional
benefit is throughput, which T14 and T16 established is not the constraint: creation saturates near 850 per
second against a cluster manager thread that is 75 percent busy, and a hundred million indices is 1.35 days
either way.

Worth revisiting if gated creation ever becomes throughput-bound. It is not today.

## Verification

The two `AwaitsFix` annotations come off, and both tests must pass:

- T17: creation fails when the descriptor index is closed, and succeeds when it is healthy.
- T23: exactly one of eight concurrent creations is acknowledged, with the ordinary-index control still at
  one of eight.

Plus a case neither spike covers: a descriptor write that fails *after* the state update, which should
surface as a failed request rather than a warn log.
