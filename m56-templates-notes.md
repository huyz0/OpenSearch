# M56 (finished) — templates, and why ingest pipelines are still refused

The last two items from the vendor comparison. One is built; the other is refused for a reason that is finally
the real one.

## Index and component templates

**The refusal these replace was wrong about itself.** It said *"there is no cluster state for a template to
live in."* A template needs somewhere to live, not specifically cluster state, and a register on an object
store is somewhere — it is where every index descriptor already lives. That is the **fifth** instance of this
shape: M50 in `_bulk`, M51 in the node endpoints, M53 across fourteen refusals, M55 in `_analyze`, and now
here. Every one is a reason that was true of one thing and got copied to another it was never true of.

**Why enumerating templates is allowed where enumerating indices is not**, since it looks like an exception to
a rule this design applies everywhere. Index creation reads *every* template to find the matching ones — an
enumeration on a hot path. The difference is where the bound is: indices are unbounded by design, so any
listing must be truncated or ruinous, whereas the number of templates is bounded **on the way in**.
`TemplateStore` refuses the thousand-and-first. Bounding an input is not truncating an answer, and it keeps
the cost on the operation that caused it rather than on index creation.

**Resolution follows OpenSearch's composable rule rather than inventing one.** Among matching index
templates, the highest priority wins **outright** — not merged with the others, because two templates
disagreeing about a field's type has no sensible resolution and choosing one is what priority is *for*. Ties
break by name so the answer does not depend on listing order. The winner's `composed_of` components merge in
order, its own `template` block goes on top, and then **the request goes on top of everything**: a caller who
spells out a mapping is not overruled by configuration they may not know exists, while the fields they did not
mention still arrive.

**The merge is recursive**, so two components each contributing different fields under `properties` both
survive. A shallow merge would silently drop the earlier one's fields — which is precisely what composing is
meant to prevent, and is canary 195.

**A missing component fails the create.** Skipping it and creating the index anyway is the tempting behaviour
and the wrong one: an index quietly created without the analysis settings a component was meant to contribute
surfaces much later as a query matching nothing, with no trace of why. It is a 400 rather than a 500, because
it is configuration the caller can fix.

## The bug the test framework caught, and why it mattered

The first version of `TemplateStore` read **every** blob in its container and parsed each as a template.
Lucene's `ExtrasFS` — which the test framework runs precisely to find code that assumes it owns a directory —
drops a file named `extra0` into directories. Index creation reads every template, so the moment that file
appeared, **creating any index failed across the whole deployment** with *"stored template [extra0] could not
be read"*, in a suite that had nothing to do with templates.

An operator or another tool leaving a file in that container would do the same in production, and the failure
would look nothing like its cause.

The fix is the rule `WalStore` and the point-in-time store already follow: **absent, foreign and corrupt are
three different answers.** Templates are stored under a `template-` prefix, so a name this system did not mint
is somebody else's file — left alone, not read and not deleted — while a name it did mint that will not parse
is still corruption and still reported.

Canary 196 is the whole scheme removed, which reproduces the original defect exactly. A first attempt at that
canary did *not* fire, because it targeted the redundant half of the guard: the filter in `all()` is belt to
`get()`'s braces, and removing only the filter changes nothing.

**Two more bugs the probe caught before the tests existed.** The create response reported the *requested* shard
count while the index was made with the template's — telling a caller they got one shard when they got three.
And a missing component surfaced as a 500. Both were found by reading bodies from a running node.

**One subtlety worth recording**, because getting it wrong is invisible: "one shard" is both a legitimate
request and what an absent request looks like. A create path that cannot tell them apart silently ignores
every template's `number_of_shards`. `CreateRequest` now tracks whether the caller actually said, which is
canary 192.

## Ingest pipelines: refused, and now for the real reason

The old reason was *"there is no cluster state for a pipeline to live in"* — the same wrong reason templates
just disproved. A pipeline stores perfectly well in a register.

**What is actually missing is what would run it.** Every processor a real pipeline uses — `set`, `rename`,
`gsub`, `grok`, `date`, `convert` — lives in the `ingest-common` **module**, and this shell deliberately loads
no modules (`ServerlessNode#buildPluginsService` says why: most of `modules/` is machinery this shell
replaced). `server` itself ships only `DropProcessor` and `PipelineProcessor`, so a stored pipeline would have
almost nothing it could construct.

**Storing pipelines without running them would be worse than refusing them.** A client sets a pipeline, writes
with `?pipeline=`, gets a 200, and nothing happened — the confident wrong answer this surface exists to
refuse, in one of its quietest forms.

So the refusal now says exactly that, and names what enabling it would take: **a decision about loading
modules, not about adding an endpoint.** That is a different kind of change from anything in M50–M56, and it
is not one to make in passing.

## Canaries

- **192 — a template's shard count is silently ignored.** Caught.
- **193 — a missing component template is skipped.** Caught: the index is created without it.
- **194 — priority is ignored and the last template listed wins.** Caught.
- **195 — templates merge shallowly, dropping a component's fields.** Caught.
- **196 — the store owns its whole directory again.** Caught. The first version of this canary did not fire
  and is recorded above, because a canary that targets the wrong half of a guard is worth knowing about.

## What this does NOT establish

- **A template applies once, at creation, and is not a live link.** Editing a template does not change indices
  it already created, and deleting one does not either. That is OpenSearch's behaviour too, but it is worth
  stating because "template" suggests otherwise.
- **No `_cat/templates`** — `GET /_index_template` returns them all and `GET /_index_template/{prefix}*`
  narrows by name; a table renderer for them is not routed.
- **No `data_stream` block, no `_simulate_index_template`, no `version` or `_meta` semantics.** A template
  carrying those stores and returns them verbatim; nothing reads them.
- **Templates are not validated beyond their shape.** A template whose mapping is nonsense stores fine and
  fails at index creation, on a different request than the one that set it — the same limitation M54 records
  for settings values.
- **The thousand-template cap is not configurable**, and is enforced per store, so a deployment may hold a
  thousand index templates and a thousand component templates.
- **Nothing here is proven against a real object store** (D5/R11), unchanged.

M56 is done, and with it every item from the vendor comparison except ingest pipelines, which is now a
scoping decision rather than an open question.
