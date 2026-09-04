# M62 (finished) — the items M61 left, and why each turned out to be buildable

M61 ended with a short list of things "deliberately still not done": creation metadata on an index, core's
alias shapes and the reverse lookup, stored scripts, search templates, ranking evaluation, search
pipelines, and data streams (with rollover under them). Each had a reason, and each reason was of the kind
this surface keeps producing: true of the thing it was first written about, copied to something it was
never true of. This milestone takes the list.

## Creation metadata: a descriptor field, not an invention

`GET /{index}` lacked `creation_date` and `settings.index.version.created` because the descriptor did not
record them, and inventing a moment would have been the confident wrong answer this surface refuses. The
descriptor now records both at creation — from the plane's clock and `Version.CURRENT`, on every path that
creates an index (the REST handler, the plugin client, a restore, a rollover, a data stream). A descriptor
from before this has neither, and the response omits them rather than guessing: the fields are written only
when known, so an old descriptor is byte-identical to what the class wrote before, and a new one parses on
an older node.

## The reverse alias lookup: a verified hint

"Which aliases name this index" was refused because aliases are found by name and never enumerated, and
answering it would mean listing every alias. It is answered now without a listing: each descriptor carries
`aliased_by`, the names of the aliases believed to name it, written **before** an alias is created and
**after** one is removed. That ordering is what makes the hint safe: it may over-approximate — an alias
whose creation then lost its race, a removal whose second write failed — and never under-approximates. A
reader verifies each name against its alias record, one register read per alias that names the index, and
drops the ones that do not. `GET /{index}/_alias`, the `aliases` block on `GET /{index}`, and the `aliases`
array on `_resolve/index` all answer from it. `GET /_alias/{name}` answers core's shape, keyed by index,
and the shell's own `{alias, indices[]}` shape is retired.

## Stored scripts: one overridden method

The refusal said stored scripts resolve through cluster state, and they did — through one `protected`
method of `ScriptService`, `getScriptFromClusterState`. `ServerlessScriptService` overrides it to read this
deployment's own script register. Everything else is core's: the engines (painless, and now mustache), the
contexts, the caches, the compile-rate limits.

**How a script put on one node reaches another.** Core does it through cluster state. Here the store keeps
a marker that moves on every change; a node reads it once per reconcile pass — one small register read —
and re-reads the scripts only when the number moved. Three moments apply the store to a node: serving a
put or delete (so the caller's next request sees it), the pass after the marker moved, and a lookup that
misses (so a script put elsewhere a moment ago is found on first use rather than after the next pass).
That is the same shape as membership: a cheap read per pass, a listing only when something changed.

`PUT /_scripts/{id}` compiles the script when a context is named, as core does, so a script that cannot
compile is refused where the operator is standing. `_update` by `script.id`, a script query by id, a
search template by id, and a rank-eval template all resolve through the store.

## Search templates, ranking evaluation, search pipelines: three modules, chosen by name

The same decision as painless (M57) and ingest-common (M58), applied three more times. `lang-mustache`
supplies the template engine; the module's own `SearchTemplateRequest` parses the body and its own
`parseRequest` splits an `_msearch/template` batch. What the module's transport action does with a
rendered template — parse it into a source and search — is done by handing the source to the same
`SearchHandler.plan` a plain body reaches, so a templated search *is* the search its rendering would have
been. `rank-eval` supplies the metrics and the response; the loop that turns hits and ratings into a score
is the module's transport action's, mirrored line for line, with each rated request's search run through
the ordinary fan-out. `search-pipeline-common` supplies the processors; core's own `PipelineWithMetrics`
builds the pipeline and core's own `PipelinedRequest` carries a request through it. The adapter lives in
core's package because those pieces are package-private — the one place this shell shares a package with
core, and it says so.

Request processors run over the request core parsed, before anything is resolved or fanned out, so a
`filter_query` narrows what every shard is asked. Response processors run over the `SearchResponse` the
fan-out builds, before it is rendered — which is what building a real response in M61 made possible.
Every processor in the common module answers synchronously; the adapter refuses one that does not rather
than blocking the transport thread, which is how the first version of it reset the caller's connection.
Named pipelines live in a register, compiled at `PUT` with a refusal that names the processors this node
can build; an inline `search_pipeline` block in the body compiles per request. `phase_results_processors`
are refused: they run between a coordinator's query and fetch phases, and this fan-out has no seam there.

## Rollover: the refusal named the reason it works

"Rollover moves an alias atomically, which each alias being its own compare-and-swap cannot do." Moving one
alias from one index to another is exactly one compare-and-swap, and M59 already served `POST /_aliases`
for that case. What is not atomic is the pair — create the index, then move the name — and that is
reported rather than hidden: a swap lost to a concurrent rollover answers 409 and names the index that was
created and not adopted. Conditions are evaluated against what this deployment records: `max_age` against
the creation time the descriptor now carries, `max_docs` against a count fanned out over the index's
shards. Sizes are not reported here, so a size condition is refused rather than answered as never met.
`dry_run` names the index it would make. The new index inherits templates the way any creation does.

## Data streams: an alias with a generation

A data stream is a name written through to its newest backing index, searched across all of them, and
rolled over to a new one. Every part of that already existed. An alias record now carries a `data_stream`
flag, a generation, and a timestamp field; backing indices are named `.ds-<name>-<generation>` as core
names them; a write addressed to the name resolves to the newest backing index; a search through it is a
search through an alias. `PUT /_data_stream/{name}` requires a matching index template that declares
`data_stream`, as core requires, and maps the timestamp field as a date on every backing index. A write
through the name must be a create, as core requires — an index, update or delete through it is refused
naming the backing index to address instead. `_rollover` on a data stream appends a backing index and
moves the generation in the same compare-and-swap. `GET /_data_stream/{name}` answers core's shape and
takes a prefix pattern; the bare form stays refused as the enumeration it is; `_stats` stays refused
because sizes are not reported.

## What this deliberately still does not do

- **`GET /_data_stream`, `GET /_alias`, `GET /_cat/aliases`** without a name stay refused: each is an
  enumeration of every alias in the deployment. The named and prefixed forms answer.
- **A rollover's size conditions** are refused: store size is not reported anywhere on this surface.
- **Phase-results search processors** are refused, for the reason above.
- **Data-stream lifecycle** (`_lifecycle`, retention) is not part of this surface.

M62 is done: 486 tests, 7 of them new, and the list at the end of M61 is empty.
