# M54 (finished) — the other half of changing an index after you make it

M52 made mappings mutable and left settings write-once. Both compared serverless products ship both, and the
shape of the work is the same: compare-and-swap the descriptor, then apply to the shards this node already
holds. What kept it out of M52 is a validity question mappings do not have.

## Which settings may change, and who decides

Core divides index settings into **dynamic** ones, changeable on a live index, and **static** ones, which
classic OpenSearch will only change on a **closed** index. This design has no closed state — an index is being
served or it does not exist — so a static setting has no moment at which it could be applied. It is refused
rather than stored and quietly ignored, which would leave a value in the descriptor that nothing acts on.

`IndexScopedSettings#isDynamicSetting` answers the question. Nothing here keeps a list of its own: that would
be a second account of a registry that grows with every release, and a second account is one that drifts.

Two settings are refused for reasons of their own:

- **`number_of_shards`** is structural. The shard list lives in the descriptor and routing is a function of it,
  so changing the count would silently re-route every document that already exists.
- **`number_of_replicas`** above zero, for the reason it is refused at creation: a shard here has one writer
  and its redundancy is the object store, so recording a replica count would report a redundancy this
  deployment does not provide that way. Zero is accepted — a client spelling out what it already gets is asking
  for nothing it cannot have.

**An unknown setting is a 400, not a 501**, and the distinction is deliberate: a typo is the caller's to fix, so
it gets core's own `illegal_argument_exception`; the refusals above are 501 because the fix is not in the
request.

## What had to move with it

A **settings version** on the descriptor, for exactly the reason mappings needed one.
`IndexService#updateMetadata` decides whether to push a change into an open shard by comparing
`IndexMetadata#getSettingsVersion`, and asserts the new one is strictly greater. Without it every node would
read the change and decline to apply it — the same silent failure M52 hit, and the reason canary 185 is worth
having.

`ShardReconciler#refreshSettings`, the counterpart of `refreshMapping`. The reconcile pass opens and closes
shards; it has never had a reason to notice that an index it already holds says something different than it
did.

**Settings merge rather than replace**, which is what `PUT /_settings` does in OpenSearch: a caller changing one
setting does not silently clear the others. Canary 187 exists because getting that backwards is easy and
invisible.

## The test that matters is the shard's own view

Storing a setting and reading it back through `GET /_settings` proves a string round-tripped through the
object store. What has to be true is that the shard already serving traffic picked it up, so
`testADynamicSettingReachesAShardAlreadyOpen` reads `IndexService#getIndexSettings` — the object the shard
actually consults — rather than the descriptor it came from.

## Canaries

- **184 — an open shard is not told the settings changed.** Caught: the shard keeps its old refresh interval.
- **185 — the settings version never moves.** Caught by the same test, which is its purpose: core declines to
  apply a change whose version has not moved, so this fails identically to never writing it.
- **186 — a static setting is stored rather than refused.** Caught.
- **187 — a settings change replaces rather than merges.** Caught: the earlier setting disappears.

## What this does NOT establish

- **Static settings cannot be changed at all**, by any route. Classic OpenSearch offers close-change-open;
  there is no close here, so the only way to change one is to create a new index. That is a real limit and not
  a rough edge.
- **Nothing validates a dynamic setting's *value*** beyond core's own parsing at apply time. A syntactically
  valid but nonsensical value is stored and reaches the shard, where core rejects it — later, and on a
  different request than the one that set it.
- **A settings change races with a shard opening elsewhere**, exactly as a mapping change does: a node opening
  a shard between the swap and its next descriptor read opens at the old settings and corrects on its next
  pass.
- **`PUT /_settings` on a pattern is not supported** — one index at a time. Classic accepts a pattern; expanding
  one here would mean a compare-and-swap per index with no atomicity across them, which is a different
  operation wearing the same name.
- **Nothing here is proven against a real object store** (D5/R11), unchanged.

M54 is done. Still open from the comparison: `_msearch`, `_analyze`, index and component templates, and ingest
pipelines.
