# M26 — Honouring what a plugin declares

M25 loaded a plugin from disk. This is the discovery that loading one and then ignoring what it declares is
not hosting it.

The shell built its analysis registry, its mapper registry and its search module from empty lists. An
installed `AnalysisPlugin` would load, log a cheerful line, and contribute nothing — no tokenizers, no
filters, no analyzers. A `MapperPlugin`'s field types and a `SearchPlugin`'s queries and aggregations were
in the same position. The plugin host could take a plugin and could not take what the plugin was for.

Four one-line changes, all of the form "pass the plugins where core expects them":

| Was | Is |
| --- | --- |
| `new AnalysisRegistry(env, emptyMap() × 9)` | `new AnalysisModule(env, filterPlugins(AnalysisPlugin)).getAnalysisRegistry()` |
| `new IndicesModule(emptyList())` | `new IndicesModule(filterPlugins(MapperPlugin))` |
| `new SearchModule(settings, List.of())` | `new SearchModule(settings, filterPlugins(SearchPlugin))` |
| `new ScriptService(settings, emptyMap(), emptyMap())` | the node's own, shared with plugins |

The analysis one had a second cost nobody had noticed. A hand-built `AnalysisRegistry` with nine empty maps
does not have **core's own built-in analyzers** either — `AnalysisModule` is what registers those. So the
shell was not merely ignoring plugins; it was running with less analysis than a stock node.

## The collaborators a plugin is handed

`createComponents` takes eleven arguments and the shell passed four of them as `null`. A plugin that
touched one got a `NullPointerException` raised inside its own code, with nothing in it to say that the
host had declined to provide something: neither a working collaborator nor a refusal that explains itself.

Three are now real:

- **`ResourceWatcherService`** — running. A plugin that keeps configuration on disk watches it through
  this. The shell watches nothing itself, which was the excuse; a collaborator a plugin needs is not made
  unnecessary by the host not needing it.
- **`ScriptService`** — real, with no engines registered. A plugin that compiles a script now gets core's
  own *"cannot compile: no lang registered"*, in the same words a classic node gives when the language's
  module is not installed.
- **`NamedWriteableRegistry`** — the node's own, the one its transport uses, not an empty one. A test
  asserts it can actually read a `match_all`, because an empty registry is non-null and useless.

Both new services are closed with the node, after the plugins that were given them.

### The fourth cannot be real, so it refuses

`IndexNameExpressionResolver` resolves names against a `ClusterState`'s `Metadata`. On a serverless node
that `ClusterState` is a node-local materialised view holding **the indices this node happens to have
open** — never the deployment's index set, which lives in the metadata plane and is deliberately not
enumerable (§6.3 refuses `/_serverless/indices` for exactly this reason).

So a real resolver here answers a wildcard with a subset, and a concrete name this node has not opened with
`IndexNotFoundException`. Both are wrong answers that look like right ones — and for a plugin evaluating
privileges over an index pattern, a subset **fails open**. `RefusingIndexNameExpressionResolver` throws
instead, naming the method and saying what to do. Three methods still work, because refusing them would be
theatre: `resolveDateMathExpression` is string arithmetic, `isSystemIndexAccessAllowed` reads a header, and
`getExpressionResolvers` returns a list.

**A test keeps that honest over time.** It reflects over core's class and fails if any public instance
method is not declared in the subclass. A method added upstream would otherwise be inherited, and the
inherited one computes an answer over one node's open indices and presents it as the deployment's — the
precise failure the class exists to prevent. An upstream addition now breaks the build.

## A real plugin, and what it broke

The proof is `analysis-icu` exactly as the build assembles it: a fourteen-megabyte `icu4j` and a Lucene
analysis jar, installed into `plugins/`, so the loader is exercised on a plugin that brings dependencies
rather than on a single class a test compiled.

The assertion is behavioural, because "it loaded" is not the claim. Two indices, the same document
(`Résumé`) in both, the same query (`resume`) against both. The one whose analyzer uses the plugin's
`icu_folding` filter matches; the one without it does not. The plugin either ran or it did not.

**Thai was the first attempt and is worth recording as a wrong turn.** Segmenting `ประเทศไทย` into
`ประเทศ` + `ไทย` would have been the more dramatic demonstration, and `icu_tokenizer` did not split it in
this build — word segmentation depends on ICU's dictionary break rules. An assertion that depends on
optional data fails for reasons that have nothing to do with the shell. Folding is table-driven and
unconditional.

### It found a bug that ruled out analysis plugins entirely

Getting that test to pass required fixing `IndexDescriptor`, which **could not carry a list-valued
setting**. Settings were serialized as flat key-to-string pairs, and `Settings.get` on a list returns its
bracketed `toString`, so a filter chain of `[icu_folding]` came back as one filter literally named
`"[icu_folding]"` and the analyzer failed to build.

Every custom analyzer has a `filter` list. So the shell could install an analysis plugin and could not
configure one — which is the entire reason to install one. Fixed by round-tripping through core's own
`Settings` XContent, which handles lists and, as it happens, still reads the flat form the old code wrote,
so descriptors already sitting in a register parse unchanged. Both halves are tested.

That is the third consecutive milestone where using the host for something real found a defect that no
amount of testing the host against a purpose-built plugin would have.

## A flake, and the right fix for it

The installation tests were flaky at about one run in three: *"Could not load plugin descriptor for plugin
directory [extra0]"*. `ExtrasFS`, the test framework's mock filesystem, plants a stray entry in every
directory it creates, including `plugins/` — and core then finds a directory with no descriptor and refuses
to start.

Core is right to refuse; `testAnIncompleteInstallationIsRefused` asserts that behaviour deliberately. The
fix is therefore to stop injecting it randomly, with `@SuppressFileSystems("ExtrasFS")` — which is what
core's own `PluginsServiceTests` does, for the same reason. Four consecutive clean runs after.

## Canaries

| Defect | Caught by |
| --- | --- |
| One resolver method left inherited from core | `testEveryResolverMethodThatTouchesClusterStateIsOverridden` |
| The watcher and script services are null again | 2 tests |
| Analysis plugins are not given to the analysis module | `testAnInstalledAnalysisPluginActuallyAnalyses` |
| Descriptor settings go back to the flat string form | `testADescriptorCarriesListValuedSettings`, the real-plugin test |

## What is still missing

- **`ActionFilter`s remain impossible**, so plugin *authorization* cannot work — for the shell's own
  authentication plugin or for anyone else's. This is now the largest single gap in the host.
- **`MapperPlugin` and `SearchPlugin` are wired but untested.** The analysis path has an end-to-end proof;
  the other two are one-line changes of the same shape with no test behind them, which is exactly the kind
  of thing that is quietly wrong. `mapper-murmur3` and a `SearchPlugin` would close that.
- **`onIndexModule` fires only for installed plugins**, not for instances passed to the constructor: the
  `PluginsService` core builds knows about the former and cannot be told about the latter.
- **`RepositoriesService` is still a supplier returning null**, because snapshots are out of scope (R7).
- **The real OpenSearch Security plugin has still not been pointed at this.** With `ActionFilter`s absent
  it cannot enforce anything, so the useful form of that exercise is a spike reporting what breaks, not a
  milestone claiming support.

238 tests green across `test` (204), `pluginTest` (1), `processTest` (13) and `s3Test` (20), none skipped,
MinIO live. `server/` untouched; §4 rule 1 verified.
