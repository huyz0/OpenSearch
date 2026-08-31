# M41 — a plugin's own actions, built without Guice

An action is how a plugin does anything that is not a document operation. The OpenSearch security plugin's
configuration-update API is one; so is every "do this thing" endpoint a plugin ships. On this shell a
plugin could not run one: the client implemented an allowlist of core's actions and refused the rest by
name, so `client.execute(itsOwnAction, request)` was told the shell does not implement it.

## This is not an action layer

§6.3 declined to build one, and this does not build one. There is no registry of core's actions, no
dispatch table for the shell's own work, and nothing about index or search goes through it. `PluginActions`
holds only what plugins brought, and the client asks it *after* its own allowlist has not matched.

## Constructor resolution, not injection

Core binds these with Guice. R4 says plugins run here without it. So each `TransportAction` is built by
picking the constructor whose parameters can all be supplied from what this node has — its services, and
whatever the plugin returned from `createComponents`, most specific first so a plugin's own component wins
a parameter over the node's.

Two properties of that rule are load-bearing, and both have tests:

- **It refuses rather than guesses.** A parameter nothing can supply fails the node's start with the class
  named and this node's inventory listed. Skipping the action instead would leave a plugin apparently
  installed and quietly broken, found much later by a caller getting an unexplained error.
- **Ambiguity is refused too.** Two constructors of equal arity both satisfiable would mean the node's
  behaviour depended on the order `getConstructors` returns — unspecified, and not a decision anybody made.

## What a plugin action gets for free

`TransportAction#execute` runs the node's `ActionFilters` itself, so an action built here goes through the
same filters as everything else: a plugin cannot reach past authorization by shipping its own action. And
`HandledTransportAction` registers its own transport handler as it is constructed, so the action is
reachable node to node exactly as on a classic node.

The client deliberately does *not* also wrap the call in `ActionGate` — that would run every filter twice,
and a filter that counts or rate-limits would be wrong rather than merely slow.

## A test that could not fail

The second test says a plugin cannot take over `indices:data/write/index` by declaring it. It passed
immediately, and the canary for the ordering — moving the plugin lookup ahead of the allowlist — **did not
break it**.

The test was writing over HTTP, and the REST write path does not go through the client's dispatch at all.
So it asserted the ordering by exercising a code path the ordering does not govern. It writes through the
client now, and the canary is caught.

That ordering is the only thing between a plugin and every write on the node, which is exactly the kind of
claim a vacuous test is worst for.

## Canaries

- **76 — a plugin's action is never reached.** Caught.
- **77 — plugin actions consulted before the shell's allowlist.** Passed at first, which found the vacuous
  test above. Caught now.
- **78 — an unbuildable action is skipped instead of refused.** Caught.
- **79 — an ambiguous constructor is chosen anyway.** Caught.

## What is still missing

- A plugin's action runs on the node the request reached. Nothing forwards it, and nothing carries the
  caller's identity across a transport hop, so an action that must run on a particular node has no way to
  ask for that.
- `getActions()` support classes (`ActionHandler`'s `supportTransportActions`) are ignored. Nothing needs
  them yet; when something does, they are more constructor resolution and not a new idea.
