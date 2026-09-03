# M53 (finished) — discovery, and a scaling question answered by using the right endpoint

Two of the gaps the vendor comparison found, plus the third instance of a pattern that keeps turning up.

## `_field_caps` — the largest single thing missing

Both AWS OpenSearch Serverless and Elastic Cloud Serverless ship it, and clients reach for it first: it is how
a UI learns which fields exist, which can be searched, and which can be aggregated. Before M52 it fell through
to core's default `400 no handler found for uri`; M52 made that an honest 501, which is not the same as being
built. Now it is built.

**It needs no shard, and that is the point.** Field capabilities are a property of the mapping, and the mapping
is in the descriptor. `IndicesService#createIndexMapperService` builds a mapper service from index metadata
alone — the same trick M52 used to validate a mapping update — and `MapperService#fieldTypes` reports each
field's type, `isSearchable` and `isAggregatable`, computed by core from the field's own mapper rather than
from anything on disk. So it answers against an index whose shards are all dormant, which is the normal
resting state here and exactly when a client doing discovery needs an answer.

**One trap, worth recording because it fails silently.** `createIndexMapperService` builds the service; it does
not load the mapping into it. The first version read `fieldTypes()` straight after construction and answered
`{"fields":{}}` for every index — a confident wrong answer, not an error. The mapping has to be merged in
first, under `MAPPING_RECOVERY`, exactly as `MappingUpdateHandler` does.

**Conflicts are reported, not resolved.** Two indices can map the same field differently. OpenSearch lists each
type with the indices that use it and omits the `indices` key when everything agrees — which is how a client
detects a conflict at all. Collapsing it to one answer would be choosing for the caller. Where indices
disagree, `searchable` and `aggregatable` are the conjunction across them: a query that works against one index
and silently matches nothing in another is worse than being told it cannot be run.

**Metadata fields are flagged** the way OpenSearch flags them, because `fields=*` returns `_id`, `_seq_no` and
the rest and a caller needs to tell them from its own.

## `_cat/indices` and the scaling question

The question was whether to serve `_cat/indices` with a cursor, or to return the first hundred. Neither.

**`_cat/indices` has no cursor and cannot have one** — its contract is "return everything". OpenSearch reached
this conclusion about its own `_cat` APIs and, in 2.18, left them alone and added `_list/indices`, whose
contract *is* a page at a time. Serving that contract involves no lying.

**"First hundred" is the option to reject**, and this codebase already wrote the argument against it, in
`TooManyMatchesException`'s own javadoc: *"an answer cut off at a limit looks exactly like a complete one."* A
client asking for the indices of a ten-thousand-index deployment and getting a hundred rows with no marker is
the same failure class as the create-index envelope M52 fixed.

**So: `GET /_list/indices/{prefix}*`, bounded and refusing.** `namesWithPrefix` resolves through one
`listBlobsByPrefixInSortedOrder` call with a cap and refuses past it — the rule search wildcards already
follow, reused rather than re-decided. That refusal is the endpoint's whole argument, and
`testAPrefixMatchingMoreThanTheCapIsRefused` is the test that makes it, added only after canary 180 showed the
first version of this suite passed with truncation in place.

**Why the unscoped form stays refused, stated precisely.** `DescriptorStore#listPage` looks like a cursor and
is not: it calls the unbounded `listBlobs()`, sorts in memory, and slices, so walking N indices costs N full
listings. Its own javadoc already admitted an earlier comment had falsely claimed otherwise. A real cursor
needs `start-after`, which S3 supports natively and core's `BlobContainer` does not expose — `listBlobs`,
`listBlobsByPrefix` and `listBlobsByPrefixInSortedOrder` are all it has. So `next_token` is refused by name
rather than accepted and ignored, because a caller passing one is resuming a walk and answering from the
beginning would mean never finishing and never being told why.

`docs.count` and `store.size` are shard-level facts this reads no shards for, so they are omitted rather than
reported as zero.

## The wrong reason, in its third place

Fourteen endpoints shared one refusal: *"there is no cluster-wide state in a serverless cluster."* For most of
them that was never why.

- `_cat/indices`, `_cat/shards`, `_cat/aliases`, `_cat/count`, `_cat/segments` are refused because the listing
  is **unbounded** — the data exists and is readable one index at a time. A caller told there is no cluster
  state would reasonably conclude it does not exist.
- `_cluster/reroute`, `_cluster/allocation/explain`, `_cat/allocation`, `_cat/recovery` are refused because
  **there is no allocator**.
- `_cat/pending_tasks` is refused because there is **no task queue**.
- `_cat/thread_pool` is refused because it needs the **same fan-out `_nodes/stats` is waiting on**.

Each now says its own. Only `_cluster/state`, `_cluster/stats` and bare `_cat` keep the shared reason, which
is true of them.

This is the third instance: M50 found it in `_bulk`, M51 in the node endpoints, M53 here. The common shape is a
reason that was true when written, for one endpoint, and then copied to others it was never true of.

## Canaries

- **180 — a too-broad prefix is truncated rather than refused.** **Did not fire against the first version of
  this suite**, which is the finding: every other test exercised the *unscoped* refusal, which is a
  registration, while the bounded refusal is the actual behaviour. A test that creates more indices than the
  cap was added, and the canary then fired.
- **181 — field types are read from a mapper service the mapping was never loaded into.** Caught: every index
  reports no fields.
- **182 — a mapping conflict is reported without attributing it.** Caught by counting `indices` keys.
- **183 — a refusal goes back to blaming missing cluster state.** Caught.

An assertion in the conflict test had to be rewritten before it meant anything: it matched the response's
top-level `indices` array rather than the per-type one, so it would have passed whatever the handler did. It
counts occurrences now.

## What this does NOT establish

- **There is still no cursor.** `_list/indices` answers in one page or refuses; it does not paginate. Until
  `BlobContainer` grows `start-after`, a deployment whose indices do not fit under any usable prefix has no way
  to enumerate them through this API, and that is a real limit rather than a rough edge.
- **`_list/shards` is refused**, on the same grounds as `_cat/shards`. `/_cluster/health/{index}?level=shards`
  answers for one index.
- **`include_unmapped` is refused.** It reports fields an index does not map, which needs a notion of the union
  of fields across indices that this implementation does not compute.
- **`docs.count` and `store.size` are absent from `_list/indices`**, so a client using it to find large indices
  cannot. Adding them means reading shards, which changes the endpoint's cost class.
- **Nothing here is proven against a real object store** (D5/R11), unchanged.

M53 is done. Still open from the comparison: `PUT /{index}/_settings`, `_msearch`, `_analyze`, index and
component templates, and ingest pipelines.
