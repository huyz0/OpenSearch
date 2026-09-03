# M57 (finished) — scripts, and the endpoints that looked like typos

Three things, all of which came out of the four-way comparison against standard OpenSearch, AWS OpenSearch
Serverless and Elastic Cloud Serverless.

## Scripts: "no modules" was never "no module code"

The refusal said *"no scripting engine is registered."* True, and worth asking why. The shell has always
depended on `modules:transport-netty4` directly, with the comment: *"The shell picks its transport directly
instead of discovering it through the plugin system."*

**So the no-modules decision was never about the code in `modules/` — it was about discovery from disk.** A
shell that chooses its transport by name can choose its script engine by name. `modules:lang-painless` is now
a dependency and `PainlessModulePlugin#getScriptEngine` supplies the engine to the `ScriptService` that was
already being constructed with `Map.of()`.

That distinction is the whole milestone. Nothing about "modules are deliberately not loaded" changed; a second
thing was chosen explicitly, exactly as the first was.

What works now: **script queries, `script_score`, `script_fields`, scripted aggregations, and scripted
updates** — including `params`, and `ctx.op` set to `noop` or `delete`. A script that fails to compile is a
400 carrying core's own compile error and script stack, which is the caller's mistake reported as core words
it.

**Stored scripts stay refused, and the reason is now structural rather than stale.** `ScriptService` is a
`ClusterStateApplier` and reads stored scripts out of cluster metadata; there is no cluster state here, so
there is nowhere to look one up. AWS makes the same call — inline supported, `/_scripts` refused — which is
some evidence it is the right line rather than a local convenience. A script referenced by `id` in an update
is refused by name rather than compiled into nothing.

### Two bugs, both caught by asserting the wrong thing first

**`script_fields` computed and were then dropped.** Registering the engine made the scripts run; the search
renderer had never emitted a `fields` object, so a caller asking for a derived value got hits containing
nothing. That reads as "the script produced nothing" rather than "the renderer forgot it" — a wrong answer
wearing the shape of an empty one. `sort` values were missing for the same reason and are now emitted too,
which paginating clients had been re-deriving by hand.

**A scripted update emptied the document.** `ctx._source` *is* the merged map, so the sequence
`merged.clear(); merged.putAll(edited)` clears the source it is about to copy from. The response said
`"result":"updated"` with a bumped version, and the document was empty. The probe did not catch it because the
probe read the response; the test caught it because it read the **document**. Canary 199's first version also
missed it, targeting the identity check rather than the defensive copy that actually fixes it.

## The endpoints that looked like typos

The comparison found 27 endpoints that did not answer the way D2 promises: 22 falling through to core's
`400 no handler found for uri`, and five worse than that.

**The five were answering as a missing index.** `GET /{index}` matches any single-segment path, so
`GET /_stats` came back as `404 no such index: _stats` — a confident wrong answer about something that was
never an index, which sends a caller looking for a missing index instead of a missing endpoint.

M50 fixed exactly this for `/_search`, by routing `/_search`. That fixes one path and leaves the next one. The
fix here is a **backstop**: an index name beginning with an underscore is an API this shell does not
implement, answered 501 with that said plainly. It catches the whole class, including endpoints nobody has
thought of yet.

The same shape existed twice more, and neither would have been found by reading the route list:

- `GET /_nodes/_local` answered *"no live node matches [_local]"*. `_local`, `_master` and
  `_cluster_manager` are selectors, not names.
- `GET /_snapshot/_status` answered *"no such repository: _status"*.
- `GET /_nodes/hot_threads` answered *"no live node matches [hot_threads]"* — and this one has no underscore,
  so the backstop does not catch it. Node sub-APIs are named explicitly. A node could in principle be called
  `hot_threads`; the trade is between refusing that node and telling every caller of a real API their node is
  missing, and the second is the far likelier mistake.

The remaining 22 now each carry a reason under one of the causes the refusal list already gives — no
allocator, no cluster-wide state, no modules, no closed index — plus two new ones for reshaping
(`_shrink`/`_split`/`_clone` need a `_reindex` this design does not have) and template simulation.

**A real index that is not there is still a 404**, which is the distinction the whole change exists to
preserve, and is asserted.

## Canaries

- **197 — no script engine is registered.** Caught: every script fails to compile.
- **198 — computed fields are dropped by the renderer.** Caught.
- **199 — the `ctx._source` aliasing bug, restored.** Caught. **Its first version did not fire**, because it
  removed the identity check and left the defensive copy that is what actually fixes the bug. Recorded because
  a canary aimed at the wrong half of a fix is worth knowing about — it is the second time in three milestones.
- **200 — an unimplemented API is read as an index name.** Caught.

## What this does NOT establish

- **Stored scripts are unavailable by any route**, and so is `/_scripts/painless/_execute`.
- **Only Painless is registered.** Expression and Mustache are separate modules; a search template
  (`_search/template`) therefore has no engine and is not routed.
- **Nothing bounds what a script may cost.** Core's compilation-rate limit applies, but there is no per-script
  time or memory ceiling here beyond what the engine itself enforces, and a deliberately expensive script on a
  shared node is not something this milestone addresses.
- **The node sub-API list is closed and hand-written.** A node genuinely named `hot_threads`, `usage`,
  `info`, `stats` or `reload_secure_settings` cannot be looked up by name.
- **Nothing here is proven against a real object store** (D5/R11), unchanged.

M57 is done. Against the four-way comparison, what remains is ingest pipelines, the classic alias shapes, and
a handful of AWS-only endpoints (`_explain`, `_validate/query`, `_resolve/index`, `_rank_eval`).
