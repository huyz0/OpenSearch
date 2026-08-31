# M24 — Authentication, written as a plugin against our own host

M23 built a plugin host and proved it with a test plugin. A test plugin is a plugin whose behaviour was
chosen to make the host look good. This milestone puts something the deployment genuinely needs on the same
seams: authentication, on the critical path of every request, keeping its accounts in an index, reached
through the client.

The module is `serverless/auth`, and it depends on `:server` and **not** on `:serverless:shell`. That is the
argument, not a detail. Everything it uses — `Plugin`, `ActionPlugin`, `IdentityPlugin`, `Client`,
`RestHandler` — is API any third-party plugin has. If it had needed one shell class, the host would not be
a host, it would be a place where privileged code is allowed to live. A Gradle task fails the build if a
file under `serverless/auth` names `org.opensearch.serverless.shell`, `.metadata` or `.shard`, because a
later `implementation project(':serverless:shell')` added for convenience would dissolve the boundary
silently.

## What it does

- **HTTP Basic at the request wrapper.** Every request passes the wrapper before its handler; no credential
  is a 401 with a `WWW-Authenticate` challenge.
- **Accounts in an index** (`.serverless_auth` by default), one document per account, holding a salted
  PBKDF2-HMAC-SHA512 record and never a password.
- **One configured account**, whose password lives in the keystore, checked *before* the index.
- **`PUT|GET|DELETE /_serverless/security/users/{user}`**, served by the plugin, writing through the client.
- **`IdentityPlugin`**, so a plugin asking core's `IdentityService` who is calling gets the real answer
  rather than `Unauthenticated`.
- **`SystemIndexPlugin`**, declaring the account index as its own — which the shell now honours by refusing
  to route any REST request to it at all.

## What it does not do, said plainly

It authenticates and it does **not** authorize. Every authenticated caller can do everything. Privilege
evaluation in OpenSearch is keyed on action names at the `ActionFilter` layer, and §6.3 leaves that layer
unbuilt; there is no honest way to fake it. The one exception is a single hard-coded rule — only the
configured account may manage accounts — which exists because an ordinary account that could mint accounts
would make authentication decorative. It is one `if`, and it is named as one `if`. Calling it "roles" would
invite people to rely on something enforced in exactly one place.

Two more holes, neither of which is a bug to be fixed later without saying so now:

- **Node-to-node forwarding carries no identity.** A request authenticated on the node that received it is
  forwarded to the shard's owner as an internal transport call. The transport port must be treated as
  trusted infrastructure and kept off any network a client can reach.
- **A path with no registered handler answers 404 unauthenticated**, because the wrapper wraps handlers.
  That leaks which endpoints exist, and nothing else.

## Six decisions, each of which could have gone the other way

**The configured account is checked before the index, not after.** The store is an index, an index is
shards, and shards can be unowned, mid-activation, or on an unreachable node. If the only credentials lived
there, the failure that takes out the store also takes away the ability to log in and fix it. Checking it
first is what keeps a bad day from being a locked door — and it is why
`testTheConfiguredAccountSurvivesAnUnreadableStore` cuts the metadata plane out from under a running node
and still expects the operator to get in.

**An unreachable store is a 503, not a 401.** These are genuinely different and collapsing them is a
usability failure with a security shape: a user told "rejected" goes and changes a password that was never
the problem, while the real fault goes unreported. Every failure is still a *denial* — an unreadable store,
an unparseable record, a missing index, none of them authenticate anybody — but the caller is told which
kind.

**The password is a keystore setting, not a plain one.** The daemon already folds a keystore into its
settings for the object store's credentials, so there was a right place for this and no reason to invent a
weaker one. Core also refuses a secure setting found in `opensearch.yml` rather than reading it, so
configuring it in the clear is an error at startup instead of a quiet downgrade — `testThePasswordCannotBeConfiguredInTheClear`.

**A node with the plugin installed and no configured account refuses to start.** The alternative is a node
with authentication installed and no credential that could ever create the first account: not a guarded
door, a door locked with the key thrown away.

**Verified credentials are cached, and the window is a setting rather than a comment.** A derivation is a
few hundred milliseconds *by design* and a lookup may cross the network; paying both per request would put
authentication an order of magnitude above the request it protects. The cache holds a fingerprint of the
credential — never the password, never the stored record. The cost is that a password change or a removal
takes effect on *other* nodes only when their entries expire. The node that made the change drops its own
immediately, because the operator watching the node they just typed into should not see the removed account
still working. Both halves are tested: `testARemovedAccountStopsWorkingAtOnce` moves no clock at all, and
`testACredentialRemovedElsewhereStopsWorkingWhenTheCacheExpires` deletes the document behind the node's back
and asserts the credential survives inside the TTL and dies outside it. A cache that never re-checked would
pass every other test in the file.

**PBKDF2 rather than bcrypt or Argon2.** Both of those would be at least as good and both would mean a new
third-party dependency in a project whose whole shape is about not adding any. PBKDF2 is in the JDK, is the
one FIPS-approved option of the three, and is not the weak link here. The iteration count is stored *in the
record*, so raising the default applies to new and changed passwords while old ones keep verifying — which
is the only way a work factor ever actually gets raised in production.

## The escalation this milestone would otherwise have shipped

An account index is only safe while nobody can reach it through the ordinary API. Every authenticated
caller in this shell can do everything, so before the guard existed:

- any account could **read the password records** with `GET /.serverless_auth/_search`; and, far worse,
- any account could **write one** — a record whose hash derives from a password of the attacker's choosing,
  stored under somebody else's account name, *is* that account.

The configured account is not exempt from that second one. It is checked before the index, but it falls
through to the index when the password does not match — which it must, so that the configured name can be
overridden. So a forged `admin` record authenticates as `admin`.

That is privilege escalation, not a limitation, and writing it down would not have made it acceptable. It is
closed by honouring `SystemIndexPlugin#getSystemIndexDescriptors`: **no REST request reaches a declared
index, whoever sent it.** Refused entirely rather than refused to some callers, because "only
administrators may read this" is not a sentence a shell with no authorization layer can enforce, whereas
"nothing routes here" is. The owning plugin still reaches it through the `Client`, which is the path it was
always using, so the rule costs the plugin nothing and closes the hole completely rather than partially.

The guard is applied when handlers are **registered**, not inside each handler, because a guard a handler
has to remember to call is a guard that one handler will not call. `_bulk` is the single case registration
cannot cover — it names its indices in the body, so the path it arrived on says nothing about what it
touches — and `BulkHandler` checks each item itself.

`testTheAccountIndexIsNotReachableThroughRest` walks search, get, index-get, index-delete and the shard
catalogue, then attempts the forgery through `_bulk`. It asserts the *consequence* first — that the forged
credential does not authenticate — before asserting the refusal's wording, so that a canary removing the
check fails on the escalation rather than on a string. It does: with the bulk guard removed, an ordinary
account becomes `admin`.

A leading dot is decoration, not a rule. `testAnOrdinaryDottedIndexIsNotASystemIndex` writes to and reads
from `.ordinary` normally; what makes an index a system index is a plugin declaring it.

## Three things dogfooding found

Writing a real plugin against our own host broke three things that a test plugin could not have.

**1. The `NodeClient` handed to every REST handler was not wired to anything.** `getRestHandlers` gives a
plugin that client and nothing else, and every REST handler in the OpenSearch ecosystem is written against
it — but the shell constructed a bare `NodeClient` with no action registry, which the shell's own handlers
could ignore because they reach the node directly. The first plugin handler that needed to write to an index
would have thrown. Fixed by overriding `doExecute` to delegate to `ServerlessClient`, rather than calling
`initialize(...)`, which wants the map of `ActionType` to `TransportAction` that §6.3 declined to build. One
allowlist, one set of answers, whichever door a plugin came in by.

**2. Nothing ever closed a plugin.** `ServerlessPlugins#closeAll` was written in M23 and never called. That
stayed invisible for exactly as long as every plugin was a test plugin holding nothing; the first one that
owned a thread pool leaked it out of every node that ran it, and the leak detector said so.
`testAPluginIsClosedWithTheNode` now asserts it directly rather than leaving it to a detector in another
suite.

**3. The client's failures were untyped across the plugin boundary.** `ShardOperations` throws its own
exception types, which is right for the shell — they carry the owner and whether a retry is worth it. A
plugin cannot catch them, because the shell is deliberately not on its classpath, so they arrived as an
anonymous `IOException` whose meaning was only in its message. The store had to decide "index missing" from
"index unreadable" by matching prose. Fixed by translating at the client: `IndexNotFoundException` and
`NoShardAvailableActionException` are core's vocabulary and both sides have it.

## Authenticating on the HTTP event loop, and why there are two paths

The first version authenticated inline in the wrapper. Every node test failed with
`Expected current thread [...transport_worker...] to not be a transport thread. Reason: [Blocking
operation]` — the account lookup blocks on a future, and OpenSearch asserts against that on a transport
thread. It was not a test artefact: it was authentication blocking the event loop.

The fix is two paths, because checking a credential is two very different amounts of work:

- **A credential checked moments ago** costs a SHA-256 and a map lookup. It runs on the thread that read the
  request. This is the overwhelming majority of requests.
- **One this node has not seen** costs the derivation and possibly a read from the object store, and is
  handed to a pool.

Refusals that need no work at all — no header, the wrong scheme, an unreadable one — are answered inline,
because dispatching in order to say "no credentials were offered" would let an unauthenticated caller queue
work on the node.

**The pool is the plugin's own, not the node's `GENERIC`, for two reasons.** The first is a deadlock:
checking an account blocks on a client call, and the shell's client runs that call on `GENERIC`; a checker
holding a `GENERIC` thread while waiting for a `GENERIC` task is a hazard that only appears under load. The
second is that a small pool is a budget — every unrecognised credential costs a deliberately slow
derivation, so an attacker sending many wrong passwords is asking this node to spend CPU, and bounding the
threads that can be doing that at once bounds what they can take. The price is queued logins during such a
flood, which is the right way round.

## The daemon can turn it on

`serverless.plugins` names plugin classes for `ServerlessBootstrap` to construct. **This is not loading a
plugin from disk and calling it that would be a lie**: no separate classloader, no
`plugin-descriptor.properties`, no dependency resolution, and the class must already be on the node's
classpath. What it buys is that a plugin can be enabled by configuration rather than by editing the shell,
which is the difference between authentication that exists in a test and authentication an operator can
switch on. A name that does not resolve stops the node — the thing most likely to be listed here is the
thing enforcing authentication, and skipping it silently is how a node ends up serving unguarded.

## Canaries

Ten defects planted, each watched fail, each reverted.

| Defect | Caught by |
| --- | --- |
| A wrong password is accepted for a stored account | `testTheFirstAccountCreatesTheIndexAndWaitsForItsShard` |
| The credential cache never expires | 2 tests |
| Removing an account does not drop its cached credential | `testARemovedAccountStopsWorkingAtOnce` |
| An unreachable store is reported as a rejected credential | `testTheConfiguredAccountSurvivesAnUnreadableStore` |
| Any authenticated caller may manage accounts | `testOnlyTheConfiguredAccountMayManageAccounts` |
| Plugins are not closed with the node | `testAPluginIsClosedWithTheNode` |
| The caller's identity never reaches the thread context | 8 tests |
| The configured account is checked only after the store | 5 tests |
| The system-index guard passes everything through | `testTheAccountIndexIsNotReachableThroughRest` |
| `_bulk` does not check the index it was given | `testTheAccountIndexIsNotReachableThroughRest` |

The first one is worth noting: it was caught by exactly one test. Only one test exercises a *stored*
account offering the wrong password, because the other wrong-password cases go down the configured-account
path or return no record at all. It is caught by the test whose job it is, and the thinness is recorded
rather than papered over.

## What is still missing

*Written at the time. For the current position, which later milestones have moved, see
[`serverless-status.md`](serverless-status.md).*

- **No disk loading.** Named classes off the classpath is not a classloader, a descriptor, or dependency
  jars. This is still the largest gap between the host and a real plugin installation.
- **`createComponents` is still passed four nulls** — `ResourceWatcherService`, `ScriptService`,
  `NamedWriteableRegistry`, `IndexNameExpressionResolver` — and the real security plugin uses several.
- **System-index support covers the request path and nothing else.** A declared index is unreachable over
  REST, which is what closes the escalation below. It is not isolated in any other sense: it has no special
  mapping handling, no reserved settings, no protection from a future code path that reaches indices some
  other way, and the guard is a name match rather than anything the metadata plane knows about.
- **`ActionFilter`s remain impossible**, so plugin authorization cannot work — for this plugin or the real
  one.
- **No password rotation for the configured account** without a restart, and no account listing (deliberate:
  §6.3's enumeration rule applies to accounts as much as to indices).
- **Cost unmeasured.** The extra hop on a cache miss and the per-request cost of the cache-hit path have not
  been measured the way search and get were.

227 tests green across `test` (194), `processTest` (13) and `s3Test` (20), none skipped, MinIO live.
`server/` untouched; §4 rule 1 verified.
