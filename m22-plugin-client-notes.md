# M22: the `Client` a plugin gets

The first piece of a plugin host, and the one everything else needs.

## Why this was the keystone

Plugins that keep state — Security's config, an ISM policy store — do it by calling `Client` against an
index. The shell constructs a `NodeClient` and **never gives it an action registry**, so every such call
failed. That single gap was the difference between "plugin support is incomplete" and "plugin support is
impossible".

It looked like it needed the `action/` package §6.3 decided not to build. It does not:
**`AbstractClient` funnels its entire surface — dozens of methods, every builder — through one abstract
`doExecute`.** So a working client for the shell is a dispatch table, not a transport-action layer. The
decision that keeps this project small survives intact.

## Zero core coupling

Everything used here is already public: `Client`, `AbstractClient`, `Plugin`, `SystemIndexPlugin`,
`IdentityPlugin`, and `RestController`'s handler-wrapper slot. **No `server/` edits**, and D4's direction
rule still holds — `server/` references nothing in `serverless/`, checked rather than assumed.

## An allowlist that refuses out loud

Supported: index, get, delete, create-index — what a plugin needs to keep state in an index. Everything
else throws, naming the action.

That refusal is load-bearing and its test is the sharpest one here. An unimplemented action is one line
away from returning a default-constructed response, and the failure mode is not a crash: a search would
answer **"no results"** and a delete **"done"**. A plugin would be told, plausibly, that its config index
is empty. This is D2's rule one layer down — 501 with a reason, never an empty success.

Sequence numbers and versions are reported unassigned rather than invented. `WalRecord` records document
state, not history; fabricating numbers would let a plugin build optimistic concurrency on a guarantee
this system does not make.

## The routing was about to be forked a third time

Single-document routing — describe the index, hash to a shard, serve here or forward to the owner, say
something useful when nobody can — lived in `DocumentHandler` and again in `GetHandler`. The client would
have been the third copy, and three copies of a routing rule is three chances to disagree about who owns
a shard.

`ShardOperations` is that logic, once, with **typed failures rather than rendered ones**: REST turns "not
here" into a 421 naming the owner or a 503 to retry; a plugin's client turns the same thing into an
exception it can catch. `GetHandler` now sits on it, which is how the extraction is verified — its six
tests, canary-verified last milestone, all still pass. `DocumentHandler`, `SearchHandler` and
`BulkHandler` still carry their own; that duplication is real, bounded, and the next thing to remove.

## Two bugs the extraction surfaced

- **A forwarded get named the wrong node.** The first version had the handler re-derive who answered from
  the placement, which always said "me". `ShardOperations.get` now returns who actually read it.
- **A connect failure escaped as a 500.** Resolving a peer connects to it, so `peer()` throws as readily
  as the forward does — and the original handler had both inside one `try`. Extracting them split the
  two, and a transport failure surfaced as a server error for what is a routing problem and a retry.

Both were caught by existing tests, which is the argument for refactoring under a suite rather than
before one.

## Canaries

| Planted defect | Caught by |
|---|---|
| Answer unimplemented actions with a default instead of refusing | the refusal test |
| Serve only shards this node owns; drop forwarding | the routing test |

A third canary — returning a default `SearchResponse` — **did not compile**, and a compile failure does
not count as caught. Replaced with one that does.

## What this does NOT establish

- **No plugin has been loaded.** `PluginsService` is still constructed with an empty list. This is the
  interface a plugin would use, tested directly; the lifecycle (`createComponents` without Guice, REST
  handler registration, the handler-wrapper slot) is the next piece.
- **No bulk, no update, no admin beyond create-index.** Refused by name.
- **No system-index semantics.** `.plugin_config` is an ordinary index here — not hidden from listings,
  not protected from deletion, not auto-created from a `SystemIndexPlugin` descriptor.
- **`ActionFilter`s remain impossible**, so a plugin's authorization cannot work. Unchanged by this and
  unchanged by anything short of rebuilding `action/`.
- **Nothing measured.** What a client call costs against an object store is unknown.

# Addendum: search, and the third copy of the fan-out

Search is now served too, which completes the minimum a plugin needs: it is how a plugin like Security
loads its whole config at startup.

Doing it removed the routing duplicate this milestone was about. The fan-out — ask every shard, merge by
score, cut to the window — lived in `SearchHandler`; a client search would have been a second copy of it.
`SearchFanout` is that logic once, and `SearchHandler` is now formatting over it. **A plugin's search and
a user's search are the same code**, which matters more than it sounds: a second implementation would be
the untested one, and it would be the one a plugin depends on.

That also means the refusals come free. A plugin asking for an aggregation gets the same 501-shaped answer
a user does, because there is one place that decides.

| Planted defect | Caught by |
|---|---|
| Fan out to only the first shard | the client's search test **and** two REST search tests |

One shared path, one canary, three tests notice — which is the argument for the extraction stated as a
number.

The client test uses a **two-shard** index deliberately. A client search that only worked where a single
shard answered alone would pass against a one-shard fixture and fail on the first real deployment.
