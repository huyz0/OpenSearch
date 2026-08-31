# M29 — Letting a plugin see the wire

M28's audit ended with a specific claim: the shell can host the parts of OpenSearch Security that decide,
and not the parts that protect the wire. It named two reasons — the plugin cannot substitute its TLS
transport, and it cannot install a transport interceptor, which is how it propagates an authenticated user
between nodes.

Both had the same cause, and it was smaller than the sentence made it sound. The shell already uses core's
`NetworkModule`, which resolves `transport.type` and `http.type` by name across every `NetworkPlugin` it is
given and composes their interceptors. It was being given netty4 and nothing else.

## Two changes

**`NetworkPlugin`s are passed to the network module.** Netty first, then whatever is installed, so a node
with no such plugin is unchanged. That alone makes transport interceptors work.

**A plugin's `additionalSettings()` reaches the node.** This is what selects a substitute transport —
Security's contribution is literally two keys naming its own transport and HTTP classes — and the ordering
is the whole of it:

1. plugins' contributions, layered underneath
2. the operator's supplied settings, on top of those
3. the shell's own defaults, underneath both

The shell's transport choice used to sit **on top of everything**, which would have made `additionalSettings`
decorative for the one thing plugins most use it for. Now it applies only where nobody else had an opinion,
and a plugin can never override a decision an operator wrote down. Two plugins contributing the same key is
an error rather than a silent winner, because which one won would otherwise depend on load order.

## The asymmetry this exposed, and fixed

The first version of both changes did nothing in the test, because the test's plugin arrived through the
node's constructor and every extension point was being filtered through `PluginsService#filterPlugins` —
which knows only the plugins **it** loaded from disk.

So an installed `AnalysisPlugin` contributed its analyzers and the same class handed to a test did not. The
same was true for mappers, search plugins, network plugins and now settings. M25 recorded a narrow version
of this (`onIndexModule` fires only for installed plugins) as a curiosity; it was in fact a general split
running through the whole host, and every test that passed a plugin instance was testing a different code
path from the one an operator uses.

There is now one list and one set of answers, however a plugin arrived. `onIndexModule` remains the single
exception, because the `PluginsService` core builds is the thing `IndicesService` calls and it cannot be
told about a plugin it did not load.

## What is proved, and what is not

**Interceptors, end to end.** A plugin's interceptor sees the shell's own node-to-node forwarding by action
name — `internal:serverless/document/write` and `internal:serverless/document/get` — on the node that sent
it. The write is deliberately sent to the node that does *not* own the shard, so the shell has to forward.
That is the mechanism an authenticated user would travel on.

**Transport substitution is not proved here.** Implementing a `Transport` is a project, not a test fixture.
What is proved instead is the part that made substitution impossible: the setting that selects one now
reaches the node with the right precedence. Whether Security's transport then constructs successfully is
the next thing that could be wrong, and this milestone does not claim otherwise.

**The shell still does not propagate identity between nodes, and that is a decision.** Now that
interceptors work, a plugin could. The shell will not, because a `ThreadContext` header travels to any
process that can reach the transport port, and without node authentication that means anyone able to reach
that port could claim to be any user. Making identity travel would turn "the transport port is trusted
infrastructure" from a stated assumption into a load-bearing one. A plugin that also supplies a TLS
transport with client certificates has earned the right to do it; the shell has not.

## Canaries

| Defect | Caught by |
| --- | --- |
| Network plugins are not given to the network module | `testAPluginsInterceptorSeesForwardedWork` |
| The shell's defaults sit on top of a plugin's settings again | `testAPluginsAdditionalSettingsReachTheNodeButLoseToAnOperator` |
| A plugin's settings outrank an operator's | the same test |

## What is still missing

- **No `Transport` has been substituted**, so TLS through a plugin is enabled rather than demonstrated.
- **`onIndexModule` still fires only for installed plugins**, the one remaining place where how a plugin
  arrived changes what it gets.
- **No action registry**, so Security's config-update API would still not work.
- **Filters see requests, not responses**, so document-level security and field redaction cannot work.
- **No Guice**, by design (R4).

249 tests green across `test` (214), `pluginTest` (2), `processTest` (13) and `s3Test` (20), none skipped,
MinIO live. `server/` untouched; §4 rule 1 verified.
