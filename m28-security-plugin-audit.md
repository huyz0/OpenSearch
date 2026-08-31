# M28 — The OpenSearch Security plugin, audited against this host

Four milestones have been justified by "so that a plugin like OpenSearch Security could run here". That
claim has never been checked against the plugin itself. This checks it.

**It is an audit, not a run.** Building the security plugin against this fork means publishing
`3.8.0-SNAPSHOT` to a local Maven repository and building a large external project against it, which
answers "does that build succeed" rather than "does this host carry it". Reading which extension points the
plugin actually declares, and checking each against the shell, answers the question directly and
deterministically. The source was cloned read-only into a scratch directory and nothing was sent anywhere.

`OpenSearchSecurityPlugin extends OpenSearchSecuritySSLPlugin implements ClusterPlugin, MapperPlugin,
IdentityPlugin, ExtensionAwarePlugin, ExtensiblePlugin`, and its parent adds `SystemIndexPlugin,
NetworkPlugin`. Between them they override seventeen hooks.

## What the shell already carries

| Hook | Since | What it means for Security |
| --- | --- | --- |
| `createComponents` | M23 | Its whole object graph is built here |
| `getRestHandlers` | M23 | `_plugins/_security/...` endpoints are served |
| `getRestHandlerWrapper` | M23 | Its authentication runs on every request |
| `close` | M24 | Its resources are released with the node |
| `getCurrentSubject`, `getTokenManager`, `getPluginSubject` | M24 | The identity API a plugin queries |
| `getSystemIndexDescriptors` | M24 | `.opendistro_security` is unreachable over REST |
| `onIndexModule` | M25 | Its per-index hooks fire |
| `getFieldFilter` | M26 | Field-level filtering of *mappings* works, through `IndicesModule` |
| `getActionFilters` | M27 | **Privilege evaluation runs** |
| `getSettings` | — | Declared settings are readable; nothing validates them, and nothing needs to |
| `loadExtensions` | M25 | Free, because loading goes through core's own `PluginsService` |
| `onNodeStarted` | **this milestone** | See below |

## `onNodeStarted`, which was missing and is now not

Security's `onNodeStarted` calls `cr.initOnNodeStart()` — it reads its own configuration index — and
reloads its API tokens. It is a different event from `createComponents` and the plugin depends on the
difference: a plugin builds its state when components are created and *cannot use the node yet*.

The shell never called it. A plugin needing it would have been loaded, constructed, and never actually
started, with nothing in any log to say so. It is called now, after the node reports itself started, with
the local node's identity. A failure there is logged rather than fatal — unlike `createComponents`, by this
point the node is serving, and taking it down would turn a plugin's late-initialisation problem into an
outage.

## What the shell does not carry, and why

**`additionalSettings()` — not merged.** Security uses it to substitute its own transport and HTTP
implementations:

```java
builder.put(NetworkModule.TRANSPORT_TYPE_KEY, "...SecuritySSLNettyTransport");
builder.put(NetworkModule.HTTP_TYPE_KEY, "...SecurityHttpServerTransport");
```

Merging plugin settings would be a few lines (`PluginsService#updatedSettings`). It is deliberately not
done, because merging them and then ignoring them would be worse than not merging: the shell picks netty4
directly rather than resolving a transport by name, so those two keys would be accepted and have no effect,
and the node would look configured for TLS while serving in the clear. **This is the single most
consequential gap: without it Security cannot install its TLS transport, and Security without TLS is not
Security.**

**`NetworkPlugin` — not consulted at all.** `getSecureTransports`, `getSecureHttpTransports` and
`getSecureSettingFactory` are how the plugin supplies those implementations. The shell's transport choice is
a deliberate simplification, and this is its price.

**`getTransportInterceptors` — not consulted.** This is how Security propagates the authenticated user
across node-to-node calls. Its absence is the same fact already recorded in M24 and M27 from the other
direction: forwarding carries no identity, so the transport port is trusted infrastructure. It is one gap,
seen twice.

**`getActions` — no action registry.** Security registers `ConfigUpdateAction` and `ApiTokenUpdateAction` as
transport actions, and uses them to push configuration changes between nodes. The shell has no registry, and
its `NodeClient` delegates to `ServerlessClient`'s allowlist, so such a call fails by name rather than
silently. Its config-update API would not work; its `ActionFilter`-based evaluation, which is the part that
enforces anything, would.

**`getGuiceServiceClasses` — no injector, by design (R4).** Security registers one class, `GuiceHolder`,
whose purpose is to reach services it could not otherwise get. This is exactly the boundary §6.3 drew.

**`ExtensionAwarePlugin`** is for other plugins to extend Security, and is inert with none installed.

## The honest summary

Roughly two thirds of what the plugin declares is carried, including everything that decides whether a
request is allowed: authentication at the request wrapper, identity through core's `Subject` API, its config
index protected as a system index, and privilege evaluation through `ActionFilter`s.

What is missing is not authorization but **transport**: TLS, and identity propagation between nodes. A
deployment could run this plugin's policy engine and would be relying on the network being trusted, which is
precisely the assumption the plugin exists to remove. So the accurate statement is:

> The shell can host the parts of OpenSearch Security that decide, and not the parts that protect the wire.

Closing that means letting a `NetworkPlugin` supply the transport, which is a real design change to a
deliberate simplification, not an oversight to be patched. It is a milestone, and it now has a specific
shape rather than being "try Security and see".

## Cost of the audit

One shallow clone, no build, no network traffic out. The scratch clone is deleted; nothing in the repository
depends on it, and re-running the audit means re-cloning.

246 tests green across `test` (211), `pluginTest` (2), `processTest` (13) and `s3Test` (20), none skipped.
`server/` untouched.
