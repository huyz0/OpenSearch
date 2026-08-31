# M40 — a filter that can change the answer

`ActionGate` (M27) gave plugins the seam authorization is made of: a filter sees an action name and a
request, and may refuse. It could not do the other half. Every operation completed its filter chain with
`Admitted` — a token saying the work had happened — because the shell's operations return their own types
and manufacturing an `ActionResponse` to carry them looked like lossy invention.

The consequence was stated plainly in the class's own documentation and in the status document: *"a filter
that inspects or rewrites responses will not work here"*. That rules out document-level security, field
redaction, and every other control that depends on seeing what came back — which is most of what a security
plugin does after it has decided you are allowed in.

## A response view, written where the operation is

The gate now takes an optional `ResponseView<T>`: how to render an operation's result as the response a
filter expects, and how to read back whatever the chain produced. A view is the translation for *one*
operation, written beside that operation, rather than a general conversion layer that would have to be
right for everything and would rot the moment it was not.

Two exist. A search shows a real `SearchResponse` and takes back its hits, total and aggregations. A get
shows a real `GetResponse` and takes back its source. `_mget` gets it for free, because it routes through
the same operation — asserted rather than assumed, since "it goes through the same code" is exactly the
belief worth one line to check.

Everything else keeps `ActionGate.opaque()`, which is the old behaviour and is still the honest answer for
an operation whose result no `ActionResponse` can carry without invention. It is deliberately not a
placeholder to be filled in later with something approximate.

## What the view will not let a filter change

**Coverage.** How many shards answered is a fact about this node's fan-out. A filter that dropped hits has
not made an index unreachable, and letting a rewritten response carry its own shard counts would let a
redaction masquerade as a partial answer — turning a security control into a false report about
availability. `_shards` and `complete` come from the outcome either way.

**Who served a read**, and whether it was realtime. Facts about routing, not about the document.

**The type.** A filter that answers a search with something that is not a search response is refused rather
than quietly ignored. Taking the original back would undo whatever the filter meant to do, silently, which
is the worst of the three options.

## A redaction that covered only search would not be one

The first version of the test asserted that a get still returned the redacted field — recording the
behaviour, as the reader-staleness test once did. That is a hole with a URL: a field hidden from search and
readable at `/alpha/_doc/1` is one request away from not being hidden, and it *looks* like protection.
The get view exists because of that assertion, and the test now demands the opposite of what it first
recorded.

## Canaries

- **73 — a search response is not shown to the filters.** Caught: the field comes back.
- **74 — the gate discards what the chain answered.** Caught: the filter runs, rewrites, and is ignored.
- **75 — a get response is not shown to the filters.** Caught.

## What is still missing for a real security plugin

- Filters run on the coordinating node only. A forwarded write carries no identity, so a filter cannot
  re-decide at the shard.
- No action registry, so a plugin's own transport actions still cannot run.
- The document-level case is *possible* now and not *demonstrated*: dropping hits works by the same
  mechanism as redacting them, and no test yet drops one on a per-document rule.
