# M25 — A plugin installed from disk

Every milestone up to here handed the node plugin *instances*. That proved the seams and left the question
that actually decides whether the shell can host the OpenSearch ecosystem: can it take a directory under
`plugins/` holding a `plugin-descriptor.properties` and a jar, and run what is in it? A plugin nobody can
install is a plugin nobody has.

It can now, and the whole of the change is that the shell stopped lying to a class it was already
constructing.

## Core's loader, not one of ours

`ServerlessNode` has always built a `PluginsService` — with `(settings, null, null, emptyList())`, purely
because `IndicesService` demands one. Nothing was ever loaded through it. Giving it the real arguments is
the entirety of disk loading:

```java
new PluginsService(settings, environment.configDir(), null, pluginsDir, classpathPlugins(settings))
```

Reading a descriptor, checking that a plugin's declared OpenSearch version matches this one, building a
classloader over its jars, refusing a jar that collides with the server's, ordering `extended.plugins`
before their dependents — that is all behaviour plugin authors have already been tested against. A second
implementation of it would differ in small ways that only surface on somebody's cluster. Writing a loader
would have been more code and less compatibility.

**Modules are deliberately still not loaded.** A classic node loads `modules/` the same way it loads
`plugins/`, and most of what is in there is machinery this shell replaced — transport is chosen directly,
and the rest assumes a cluster with a manager. An operator installs a plugin; nobody installs a module.

**`IndicesService` now gets the real one too**, so an installed plugin receives `onIndexModule` exactly as
it would on a classic node. That was free, and it was the point of the stub being a stub.

## Two mechanisms, and why both stay

- **`plugins/`** — an installed plugin: descriptor, own classloader, own jars. What an operator does.
- **`serverless.plugins`** — a class already on the node's classpath, for a plugin a distribution ships
  rather than one an operator installs.

The second used to have its own reflection in `ServerlessBootstrap`, with its own idea of which constructor
to call. That is gone: it now builds a `PluginInfo` per named class and hands it to core as a classpath
plugin, so there is one instantiation convention rather than two that agree until they don't.

One deliberate divergence survives. Core's classpath path **logs** a `ClassNotFoundException` and carries
on; the shell resolves the class first and throws. The thing most likely to be listed in that setting is
the thing enforcing authentication, and a node that started without it and said so only in a log line is a
node serving unguarded.

## The test compiles its own plugin

`ServerlessInstalledPluginTests` writes Java source, invokes `javax.tools.JavaCompiler` against the test
classpath, packs the class files into a jar, writes a descriptor beside it, and starts a node.

That is not theatre; it is the only construction under which the assertion means anything. **A plugin class
already on the test classpath would be found by the parent classloader whether or not the shell built a
classloader at all** — the test would pass with the loading deleted. The test asserts the negative
directly: `Class.forName("org.opensearch.serverless.installed.GreetingPlugin")` from the test itself throws
`ClassNotFoundException`, and the loaded plugin's classloader is not the test's.

What the installed plugin then demonstrates, over HTTP:

- its **route is served** — `GET /_installed/hello`;
- **`createComponents` handed it a working `Client` and `ThreadPool`**, built by classes its own classloader
  cannot see, which is the interesting half of a classloader boundary;
- the **system index it declared is guarded** — `GET /.greeting_state/_search` is refused with 403, so
  M24's rule follows a plugin across the loader rather than only applying to plugins the shell was handed.

Three more tests cover what an operator gets wrong: a plugin built for another OpenSearch version stops the
node and the message names the version; a directory under `plugins/` with no descriptor stops the node
rather than being skipped; and a node with no `plugins/` directory at all — nearly every node, and every
other test in the suite — is unchanged.

## Two things it found

**A dependency the shell had never needed.** `PluginsService.loadBundle` constructs an
`ExtendedPluginsClassLoader`, which lives in `libs/opensearch-plugin-classloader` and is a `compileOnly`
dependency of `server`. A real distribution puts it on the runtime classpath; the shell had no reason to
until now, and the first installed plugin died with `NoClassDefFoundError`. Added as `runtimeOnly`.

**The authentication plugin could not have been installed.** Core's `loadPlugin` refuses a plugin class
with more than one public constructor — "no unique public constructor". `ServerlessAuthPlugin` had two: the
real one, and a second taking a clock so a test could age the credential cache without sleeping. It worked
in every test, because every test constructed it directly, and it would have failed the moment anybody put
it in `plugins/`. It is now one public constructor plus a `withClock` factory, which `getConstructors()`
does not see.

That is the second time in two milestones that writing something real against this host found a defect
that only shows up outside a test. It is the argument for doing it at all.

## Canaries

| Defect | Caught by |
| --- | --- |
| The plugins directory is never scanned | 3 tests |
| A plugin's declared system index is ignored | `testAPluginInstalledOnDiskIsLoadedAndRun`, `testTheAccountIndexIsNotReachableThroughRest` |

## What is still missing

- **`createComponents` is still passed four nulls** — `ResourceWatcherService`, `ScriptService`,
  `NamedWriteableRegistry`, `IndexNameExpressionResolver`. This is now the largest gap, and the real
  security plugin uses several of them. A plugin that touches one gets an NPE rather than a refusal, which
  is the wrong shape: D2 says refuse and say why.
- **`ActionFilter`s remain impossible**, so plugin *authorization* still cannot work for any plugin.
- **`onIndexModule` fires only for installed plugins**, not for instances passed to the constructor: the
  `PluginsService` core builds knows about the former and cannot be told about the latter. Nothing in the
  shell depends on it yet.
- **No plugin has been tried that has dependency jars, an `extended.plugins` chain, or a native
  controller.** The loader handles all three because it is core's; none of them is exercised here.
- **The real OpenSearch Security plugin has still not been pointed at this.** That is now a spike worth
  running, and it should report what breaks rather than be scheduled as a milestone.

231 tests green across `test` (198), `processTest` (13) and `s3Test` (20), none skipped, MinIO live.
`server/` untouched; §4 rule 1 verified.
