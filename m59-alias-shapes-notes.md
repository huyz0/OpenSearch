# M59 (finished) — the classic alias spellings, and two questions that never needed a cluster

The last items from the four-way comparison.

## The alias API was the last invented shape

Standard OpenSearch and AWS both use `PUT /{index}/_alias/{name}` and `POST /_aliases`. This shell served a
name-scoped API of its own — `PUT /_alias/{name}` with the indices in a body — and refused theirs. That is
exactly the category M47 cleared out everywhere else: a shell-specific invention with no architectural reason,
surviving only because M47's audit predated noticing it.

Both spellings work now. The name-scoped one is kept rather than replaced, because this shell's own callers use
it and breaking them to fix a compatibility gap is a strange trade — the same call M52 made for the
point-in-time spellings.

**An alias is a set, and the index-scoped verbs act on that set.** `PUT /{index}/_alias/{name}` adds one index
rather than replacing what is there, and `DELETE` removes one. **Removing the last index removes the alias**,
because an alias standing for nothing resolves to nothing and reads exactly like an empty index — the failure
this whole surface is arranged to avoid, and one that would sit there until somebody noticed.

## `POST /_aliases`: served for the case it exists to serve

The four-way comparison recorded this one as refusable, on the grounds that its point is an *atomic* swap and
each alias here is its own compare-and-swap with no transaction across them. That was half right, and the
half that was wrong is the useful half.

**The atomic move touches one alias.** Remove `current` from yesterday's index, add it to today's: two
actions, one alias, **one register**. That is a single compare-and-swap and is genuinely atomic — a caller
searching the alias throughout sees yesterday's index or today's and never neither. Which is the whole reason
the API exists.

So actions on a single alias are served, and actions spanning several are refused with what would go wrong
said plainly: several registers, no transaction, so the request could apply partly. That is the same objection
that keeps `PUT /_settings` off patterns, and it now sits in exactly the place it is true of rather than over
the whole endpoint.

## `_validate/query` and `_resolve/index`

Both were refused with reasons that were true of neither.

**Validation is parsing.** A caller sends a query to find out whether it is well formed before committing to a
search that might be expensive; the answer is whatever the parser says, through the node's own registry — the
same parse every search already does. The response says `parsing only; field existence is not checked`,
because "valid" here is narrower than a caller may assume and the gap between those two readings is where a
confident wrong answer would live.

**Resolution over a named set is not an enumeration.** The old reason called it one. Naming indices, or a
prefix, is a descriptor read or the same bounded listing search wildcards use — the distinction
`_list/indices/{prefix}*` already rests on. `data_streams` is present and empty rather than absent, so a
client walking all three arrays does not have to branch on which this deployment has.

**`_rank_eval` stays refused**, and its reason is now the real one: it issues a set of searches and does
arithmetic over how well each ranked a known-good answer. Every part of that is a search this surface already
serves, so it belongs in the harness doing the evaluating rather than in the node being evaluated.

## Canaries

- **205 — attaching an index replaces the alias instead of adding to it.** Caught.
- **206 — actions spanning aliases are applied one at a time.** Caught: both aliases end up created, which is
  the partial application the refusal exists to prevent.
- **207 — an alias over nothing is left standing.** Caught.
- **208 — validation never parses the query.** Caught: an unparseable query reports valid.

## What this does NOT establish

- **`GET /{index}/_alias` — every alias an index is under — is not served.** That is a reverse lookup this
  design does not offer: aliases are found by name, and finding which cover a given index means reading them
  all. `GET /{index}/_alias/{name}` answers the bounded form of the question.
- **`GET /_alias` unscoped stays refused**, as enumeration.
- **Alias filters and routing are not supported.** An alias here is a set of index names and nothing else; a
  `filter` or `index_routing` in an action body is ignored rather than refused, which is the one place in this
  milestone where something is accepted and not honoured. It should be a refusal and is not, and that is the
  first thing to fix here.
- **`_explain` is still refused.** Explaining a score needs the scorer for one document on one shard, which is
  a per-shard internal this surface does not expose — the one endpoint from the comparison whose original
  reason survived contact.
- **Nothing here is proven against a real object store** (D5/R11), unchanged.

M59 is done. Every capability the four-way comparison found in AWS or Elastic is now served or refused for a
reason that is true of it.
