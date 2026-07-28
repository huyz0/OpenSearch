# How TiDB scales database metadata, and what applies here

Read against `pingcap/tidb` at `ab4e1dcf964` (28 July 2026). Line references are into that tree.

TiDB has the problem this area has: a catalog that every request consults, held in memory on every
node, that stops fitting once the object count gets large. They solved it in 2024 with what they call
infoschema v2. The shape of the answer is close enough to the descriptor design that the differences
are worth reading carefully.

## The ceiling they hit, and where they put the bound

Infoschema v1 loads every `TableInfo` into memory eagerly on every TiDB node. Same failure as
`Metadata`: memory grows with catalog size, and a full reload walks all of it.

V2 is selected by `tidb_schema_cache_size`, default 512 MB (`vardef/tidb_vars.go:1830`). Zero means
v1 and eager. Non-zero means v2, and the number is a **byte budget on the cache**, not an object
count. What stays resident is names and IDs; the `TableInfo` itself is fetched from TiKV on access
and cached under that budget.

Our caches are entry-bounded (`H11`, `H12`). A byte budget is the better bound for the same reason it
is here: entry count does not predict residency when the entries vary in size, and residency is the
ceiling we are actually defending.

## The resident structure is an MVCC B-tree, which is the interesting part

`Data` (`infoschema/infoschema_v2.go:101`) holds six B-trees. The two that matter:

- `byName`, sorted by `{dbName, tableName, schemaVersion}` to `tableID`
- `byID`, sorted by `{tableID, schemaVersion}` to `dbID`

Every item carries the schema version it was written at, plus a `tomb` flag. A lookup is a
`DescendLessOrEqual` from `{name, MaxInt64}` taking the first item whose version is at or below the
reader's version (`search`, line 806). A drop writes a tombstone at the new version rather than
deleting (`remove`, line 227).

The comment at line 104 states the invariant that makes this cheap:

> If the schema version +1 but a specific table does not change, the old record is kept and no new
> `{dbName, tableName, schemaVersion+1}` record is added.

So a version bump writes only the entries that changed. There is no per-version catalog object. That
is the direct answer to the throughput ceiling `H0a` identified, where `Metadata.build()` is O(N) per
change because each cluster state version owns its own immutable map.

Writes are lock-free CAS over a lazily cloned tree (`btreeSet`, line 87): load the pointer, `Clone()`,
insert, compare-and-swap, retry on conflict. `google/btree`'s `Clone` marks the shared structure
read-only and copies nodes only on write, so cloning a tree of a million items costs the path being
modified rather than the tree. Readers hold the old pointer and see a consistent snapshot with no
lock at all.

Old versions are collected in batches by `GCOldVersion` (line 446), 1024 items at a time, below a
version floor.

## Full load avoids deserialising

`GetAllNameToIDAndTheMustLoadedTableInfo` (`meta/meta.go:1351`) iterates the raw KV hash for a
database and extracts the id and lowercase name **out of the serialised JSON with a regex**, never
unmarshalling. It only unmarshals when `isTableInfoMustLoad` (line 1301) finds a marker by raw
`bytes.Index` substring search.

A cold start therefore costs a scan plus two regex matches per table, not N JSON unmarshals. Our
descriptor read deserialises the whole document to answer questions that only need name and uuid.

## What is forced resident is an explicit, short list

`checkAttributesInOrder` (`meta/meta.go:1286`) names every reason a table must stay in memory:
partitioning, table lock, TiFlash replica, temp table type, placement policy ref, TTL info,
affinity, plus any table with foreign keys. Everything else is lazy.

The reasoning is at `infoschema_v2.go:137`:

> We observe the pattern that list table API always come with filter.

So `tableInfoResident` is a B-tree of exactly the tables a bulk scan could ask about, and
`ListTablesWithSpecialAttribute` (line 1740) serves those scans from it instead of enumerating the
catalog. This is `H19` and `H20` with a different name. Their version is stronger in one respect: the
list is closed and stated in one place, so adding a feature that needs residency forces an edit to
that list. Ours is spread across the callers that happen to enumerate.

## The value cache is SIEVE, and its read does not reorder

`infoschema/sieve.go`. `Get` takes a mutex but only sets `visited = true` on the entry. No list
splice, no reordering. Eviction is a hand walking the list clearing visited bits, from the SIEVE
paper.

That is `P2` and `P3` with a citation. Two differences:

- They still take a global mutex on `Get`. We went further and made the read lock-free
  (`ConcurrentHashMap` plus a write-stamp), which `P1` measured at 114x over the monitor-guarded LRU
  at sixteen threads. Their `Get` is short but still serialises.
- Their capacity is in bytes and settable at runtime, with `SetCapacityAndWaitEvict` draining
  synchronously. Ours is entry-count and fixed at construction.

## Concurrent misses are collapsed, and ours are not

`loadTableInfo` (`infoschema_v2.go:1401`) wraps the TiKV read in a `singleflight.Group` keyed
`dbID-tblID-schemaVersion`. Concurrent misses for the same table at the same version issue one read.

`DescriptorStore.get` after `P8` has a 1 s TTL cache and no such collapse. Fan-out at 100M means many
shards resolving the same name at once, and today each one issues its own get. This is the one
finding from this study that is directly actionable here.

## Propagation carries a version number, not state

The DDL owner bumps a global schema version in TiKV and writes one `SchemaDiff` per DDL under
`Diff<version>`. Each node writes the version it has reached to etcd at
`/tidb/ddl/all_schema_versions/<id>`, and the owner waits for every node to report the new one
(`ddl/schemaver/syncer.go:351`). With metadata lock enabled it waits for the lock instead: nodes
report per-job versions and the owner proceeds only when no session still holds the old schema.

Nothing large is broadcast. The unit of publication is an `int64` and a small diff record; each node
pulls what it needs. Our cluster state publication ships the change to every node and every node
applies it, which is why `H3` had to remove creation from that path rather than make it cheaper.

A node behind by k versions applies k diffs (`issyncer/loader.go:325`). Past
`LoadSchemaDiffVersionGapThreshold = 10000`, or on any diff failure, it falls back to a full load.

## What not to copy

The diff-or-full-reload fallback. Their own comment at `loader.go:202` is unusually direct:

> tryLoadSchemaDiffs has potential risks of failure. And it becomes worse in history reading cases.
> It is only kept because there is no alternative diff/partial loading solution.

A fallback whose failure mode is "load the entire catalog" is a cliff, not a safety net. At 100M the
full load is the thing being avoided, so an incremental path that can silently drop into it converts
a slow moment into an outage. If we adopt versioned incremental application, the fallback has to be
something other than a full rebuild.

## Cost they accepted

A cache miss on `TableByName` is a remote point read at a snapshot timestamp, on the request path.
They mitigate with singleflight, a 3 s KV read timeout so a slow meta region leader does not burn the
DDL lease, and `keepAlive`/`recentMinTS` (line 1075) which reports the minimum timestamp any live
infoschema is reading at so GC cannot collect underneath it.

That last one has no analogue here and does not need one, because our descriptor index is not
MVCC-collected under the reader. It is worth noting as the class of hazard that appears once catalog
reads become storage reads: the read is no longer instantaneous, so anything that reclaims by time
can reclaim what an in-flight read still needs.

## Ranked takeaways

1. Collapse concurrent descriptor misses. Small, local to `DescriptorStore`, fixes a real fan-out
   gap.
2. Make cache bounds byte budgets rather than entry counts, and settable at runtime.
3. Close the residency list: one place naming every reason an index must stay in memory.
4. Avoid deserialising during bulk descriptor reads when only name and uuid are needed.
5. If `Metadata.build()` ever has to stay on the write path, the MVCC B-tree keyed by
   `(name, version)` with tombstones and lazy-clone CAS is the structure that removes the per-version
   O(N) rebuild.
