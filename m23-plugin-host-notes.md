# M23: a plugin, actually running

R4 says the shell "implements the plugin contract without an `Injector`". Nothing implemented it:
`PluginsService` was constructed with an empty list because `IndicesService` demands one, and no plugin
had ever been started. This is the part that makes a plugin run.

## Guice was never the hard part

`createComponents` is a plain method returning a collection of objects. Classic OpenSearch binds those
into an injector so other components can ask for them by type; the shell keeps them in a list. What a
plugin actually needs from the container — a `Client`, a thread pool, an environment — arrives as
arguments, and passing arguments is not a framework.

## Three hooks, and a plugin that uses all of them

- **`createComponents`** — build state, reach the `Client` from M22.
- **`getRestHandlers`** — serve its own endpoints.
- **`getRestHandlerWrapper`** — see every request before the handler does. This is where authentication
  belongs, and it is the same hook the OpenSearch security plugin uses.

The test plugin does all three, because a host that supports one hook and not the others is not a host.
It is a plugin we wrote rather than a real one on purpose: prove the seams with something whose behaviour
we control before anything real is aimed at them.

Plugin routes register **last**, so a plugin cannot take an endpoint the shell has already claimed —
`RestController` refuses a duplicate rather than replacing it, and refusing loudly at boot beats a system
whose write path depends on registration order.

## Three decisions worth stating

**A failing plugin stops the node.** The tempting behaviour is to log and carry on, because one bad plugin
taking down a node feels harsh. Security is why it is wrong: a plugin installed to enforce something,
which failed to load its configuration and was quietly skipped, leaves a node serving traffic with the
enforcement absent and nothing in the response to say so. Refusing to start is loud, and loud is
recoverable.

**Two request wrappers are refused, not ordered.** Two plugins both claiming to authenticate is a
configuration mistake whose consequences depend on which ran first. Answering it with a silent choice is
how a system ends up enforcing the weaker of two policies.

**The default identity is named rather than absent.** With no plugin, `IdentityService` returns core's
`NoopSubject`, whose principal is `Unauthenticated` and which fails no checks. That is why plain OpenSearch
is open without a security plugin, and why the shell is too — asserted in a test so it is a visible default
rather than an omission.

## A file-handle leak this found

The constructor's failure path terminated the thread pool and **did not close the `NodeEnvironment`**, which
holds `node.lock`. Any construction failure after that point leaked the handle for the life of the process
— on a real node, the data directory stays locked and a restart cannot use it.

Pre-existing and never reached: nothing had made construction fail on purpose until the two-wrappers test
did. The test framework's leak detector caught it immediately.

## Canaries

| Planted defect | Caught by |
|---|---|
| Ignore the plugin's request wrapper | three of the host tests |
| Skip a plugin whose `createComponents` throws | the failing-plugin test — **which had to be written first** |

The second is the instructive one. The javadoc claimed a failing plugin stops the node, and the canary
passed: no test made a plugin fail, so the claim was prose. Writing the test both closed that and found
the leak above.

## What this does NOT establish

- **No plugin is loaded from disk.** The node takes plugin instances; `ServerlessBootstrap` does not yet
  scan a plugins directory, and nothing has been tested against an installed plugin's classloader,
  `plugin-descriptor.properties`, or its dependency jars.
- **`createComponents` is passed four nulls** — `ResourceWatcherService`, `ScriptService`,
  `NamedWriteableRegistry`, `IndexNameExpressionResolver` — and a `RepositoriesService` supplier that
  returns null. A plugin that dereferences any of them fails, and the real security plugin uses several.
- **`ActionFilter`s remain impossible**, so plugin *authorization* still cannot work. Unchanged, and
  unchanged by anything short of rebuilding `action/`.
- **No system-index semantics**, no `SystemIndexPlugin` descriptors honoured, no transport interceptors.
- **The security plugin has not been tried.** Everything here makes the attempt possible; none of it makes
  it likely. That remains a spike that reports what breaks, not a milestone.
