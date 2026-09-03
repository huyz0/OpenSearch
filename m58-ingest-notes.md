# M58 (finished) — ingest pipelines, and the dynamic mapping underneath them

The last item from the vendor comparison, and a pre-existing bug it uncovered that matters more.

## Ingest pipelines

M56 refused them and said why: storing one was never the problem — templates store the same way — but every
processor a real pipeline uses lives in the `ingest-common` **module**, and this shell loads none. It recorded
that enabling them was *"a decision about loading modules, not about adding an endpoint."*

M57 made that decision, by choosing `lang-painless` the way the transport had always been chosen and
establishing that "no modules are loaded" was about **discovery from disk**, not about the code in `modules/`.
So this is that decision applied a third time rather than a new one.

**What replaces `IngestService`.** Core's ingest service holds pipelines in cluster state and hands them out
by id. Pipelines here live in a register — the same `TemplateStore` templates use, which means M56's ExtrasFS
lesson applies without being re-learned — and are compiled from their stored JSON by `Pipeline#create`, core's
own compiler, over core's own processor factories. Nothing here interprets a processor.

**A pipeline is compiled when it is stored.** That is the point of doing the work at `PUT`: a pipeline naming
a processor this deployment does not have, or configuring one wrongly, is refused where the operator is
standing. Accepting it and failing on the first write that referenced it would surface the mistake days later,
in somebody else's request, with nothing connecting it to the change that caused it. The refusal **lists the
available processors**, because "unknown processor [foo]" without a list sends an operator to another
product's documentation.

**The pipeline runs before anything is routed or written**, so a caller is never told a write succeeded
against a document the pipeline was supposed to shape and did not. A dropped document is reported as dropped
rather than as a write — a caller who cannot tell those apart cannot tell whether their pipeline works, which
is the whole reason for having one.

`Processor.Parameters` is given what this shell has and null for what it does not: no `IngestService`, because
pipelines come from a register, and no `Client`, because nothing here re-enters the API. A processor needing
either fails at compile time, naming itself.

## The bug underneath: dynamic mapping did not exist

Probing a pipeline that adds a field turned up something much larger. **An index created without an explicit
mapping could not accept a single document.**

When a document carries a field the mapping does not describe, core's engine does not index it and does not
fail either — it returns `MAPPING_UPDATE_REQUIRED` along with the mapping addition that would let the write
succeed. A classic node sends that to the cluster manager, waits, and retries. Nothing here was doing the
equivalent, so the write answered `500 indexing 1 returned MAPPING_UPDATE_REQUIRED`.

**It survived fifty-seven milestones because every test in this repository declared its mappings.** The
probe that found it created an index with settings only, which is the most ordinary thing a client does.
Verified against the committed baseline before writing a line of the fix, because "did I just break this"
and "was this always broken" are different findings.

The fix is the machinery M52 already built: merge core's addition into the descriptor under a
compare-and-swap, apply it to the open shard, retry the write **once**. Once, because a second
`MAPPING_UPDATE_REQUIRED` after the update has been applied means something other than a missing field, and
retrying forever would hide it. Two writers introducing different fields at the same time both keep theirs,
because that is what the compare-and-swap is for.

Deliberately the same path `PUT /{index}/_mapping` takes: two ways of changing a mapping would be two ways
for it to drift.

## Canaries

- **201 — the mapping never grows to fit a document.** Caught: the ordinary write returns 500 again.
- **202 — growing the mapping forgets what it already had.** Caught: the first field disappears when the
  second arrives.
- **203 — the pipeline runs and its result is discarded.** Caught. This is the canary that matters for
  pipelines, because a discarded result still answers 201 — which is why the test asserts the stored document
  rather than the response.
- **204 — a pipeline is stored without being compiled.** Caught.

## What this does NOT establish

- **`?pipeline=` works on single-document writes only.** `_bulk` does not honour it, and neither does
  `index.default_pipeline`. A client that sets a default pipeline on an index gets no pipeline and no warning,
  which is the same silent shape this milestone exists to avoid — it is the first thing to fix here.
- **No `_ingest/pipeline/_simulate`.** An operator cannot test a pipeline without writing a document.
- **No processor bound.** A pipeline may do arbitrary work per document, and nothing here caps how long or how
  much memory that takes.
- **Dynamic mapping infers types the way core does**, which means a field's type is decided by the first
  document that carries it. That is classic behaviour and is worth knowing rather than discovering.
- **The mapping grows on the write path**, so a burst of documents each introducing a field is a burst of
  descriptor compare-and-swaps. Bounded by the number of distinct new fields rather than by the number of
  documents, but not free.
- **Nothing here is proven against a real object store** (D5/R11), unchanged.

M58 is done, and with it every capability the four-way comparison found in both commercial products.
