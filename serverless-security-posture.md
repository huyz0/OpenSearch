# Security posture of the serverless shell

What the shell and its authentication plugin actually protect today, written for a reader of
[`rfc-serverless-opensearch.md` §12](rfc-serverless-opensearch.md) or of the shell RFC's R4/R7 rows, both
of which read as though more were done than is. The storage plugin's `EncryptingBlobContainer`,
`RestrictingBlobContainer` and per-record WAL envelope are real, and none of them is wired into the shell.
Where this document and a status line elsewhere disagree, this document is the one that was checked
against the code (branch `feature/serverlessplusplus`, review D, security fixes W6).

## 1. At rest

| What | Status |
|---|---|
| Segment bundles, manifests, WAL, registers | **Plaintext** in the object store. `SegmentPublisher`, `WalStore` and `MetadataPlane` write bytes as given. If the bucket applies server-side encryption, that is the bucket's; the shell neither asks for it nor checks it. |
| Per-index keys, per-record WAL envelopes | Absent from the shell. §12's "implemented and tested" describes the storage plugin. |
| Account records (`.serverless_auth`) | Salted PBKDF2-HMAC-SHA512 (210,000 iterations by default; the count is stored per record and capped on verify at four times the configured factor, never above 10⁷). The `hash` field is `index: false`. The document is otherwise plaintext, so the bucket holds crackable-with-effort hashes, not passwords. |
| Transport secret (`cluster-config/transport-secret`) | A plaintext register blob. **Anyone who can read the bucket holds it** (§3). |
| Node keystore | Must be password-less (`ServerlessBootstrap` refuses a protected one). The bootstrap password and object-store credentials are protected by file permissions only. |
| In-memory credential cache | Holds `HmacSHA256(process-key, user ‖ password)` for the cache TTL; the key is generated per process and never persisted, so a heap dump yields nothing crackable offline. |

## 2. In transit

| Port | Default | With a plugin |
|---|---|---|
| HTTP | Plaintext `netty4`. **HTTP Basic credentials cross the wire base64-encoded, i.e. in the clear.** | A `NetworkPlugin` may register an `HttpServerTransport` that terminates TLS; the operator selects it with `http.type`. This has not been demonstrated in this tree (`ServerlessTlsTests.TlsPlugin#getSecureHttpTransportSettingsProvider` returns empty). |
| Transport | Plaintext `netty4`. | A `SecureSettingsFactory` plugin turns on core's `SecureNetty4Transport`; mutual TLS with a trusted peer set is proven (`ServerlessTlsTests`). |

Until a TLS plugin is installed on both ports, the deployment's network must be one nobody untrusted can
reach. That is the rule the transport port has always lived under; it applies to the HTTP port as well,
and the auth plugin's javadoc says so.

## 3. Node-to-node trust: per-request MAC, with the static token still beside it

Every forwarded request (`internal:serverless/document/write`, `bulk`, `get`, `explain`, `search`,
`frozen-search`) now carries `x-serverless-transport-mac` = `generation:sender:timestamp:nonce:mac`,
where `mac` is HMAC-SHA256 under the deployment secret over the action, the sender id, the timestamp,
the nonce and a SHA-256 of the serialised request (`transport/TransportAuthenticator`). The receiver
accepts the current secret generation, or the previous one for one lease after a rotation, refuses a
timestamp outside one lease, and refuses a `(sender, nonce)` it has already seen. The secret lives in the
`cluster-config/transport-secret` register as `{generation, current, previous, rotated_at}`;
`MetadataPlane#rotateTransportSecret()` rotates it by compare-and-swap, every node re-reads it once a
lease or on meeting a newer generation, and a node two generations behind is refused until it re-reads.
`ServerlessTransportAuthTests` proves: wrong MAC refused, no credential refused, wrong token with no MAC
refused, replayed frame refused, and all four rotation paths (sender ahead → receiver re-reads; sender
one behind → accepted under previous; two behind → refused; re-read → served).

**During the move, the old static token is still presented beside the MAC** (`x-serverless-transport-token`)
and a receiver falls back to it only when no MAC header is present. That fallback is the remaining
exposure: a sender that signs nothing but holds the token is still a member. Remove the fallback once
every node in a deployment signs (one release), and the token header with it.

What still holds regardless of the MAC:

- **Confidentiality is TLS's job.** The MAC stops forgery and replay; a bystander on a plaintext transport
  still reads every payload. Install a `SecureSettingsFactory` plugin (§2).
- **Bucket read access is still cluster membership.** The secret is a plaintext register in the bucket;
  any principal with GET on `cluster-config/` can sign. Keep §12's tier scoping out of reach until the
  secret is held outside the bucket or encrypted under a node-side key.
- **The owner still runs no `ActionFilter` on a forwarded request.** It does now refuse a forwarded
  write to a system index unless the request carries the plugin-origin header the coordinator adds when
  the write started inside a plugin (`serverless_plugin_subject` on the thread), which closes the
  "forge an `admin` record through the transport" path for a member that is not a plugin.
- Refuse to start, or warn loudly, when forwarding would run over a non-secure `transport.type`.

## 4. Tier scoping

Absent. §12 asks for GET-only search compute, GET+PUT compaction and a DELETE-capable reconciler; the
shell has no `RestrictingBlobContainer` and every node is read-write on the whole bucket. Even if scoping
were added, §3's placement of the transport secret makes a GET-only tier a write-capable one.

## 5. Authentication (the `serverless/auth` plugin)

What it does: HTTP Basic at the REST wrapper, so it is outermost for every handler including plugins'
and `NotImplementedHandler`s; accounts in a system index reached through the plugin `Client`; one
configured account in the keystore, checked before the index, so an unreadable store is a 503 for
everyone else and still a 200 for the operator; `IdentityPlugin` so a plugin sees the real caller;
unknown names cost the same derivation a wrong password does.

What it deliberately does not do: authorize (every authenticated caller can do everything except manage
accounts), issue tokens, enforce a password policy, or supply TLS.

**Throttle semantics** (W6; `LoginThrottle`). Three keys, and what each may do:

- **`(address, account)` pair** — the only key that refuses. Consecutive failures from one address against
  one name earn an exponential wait (1 s doubling to `serverless.auth.throttle.max_delay`, default 60 s)
  during which that pair is answered 429 at the door with no work. A good account behind the same address
  has its own counter; the same account from another address has its own counter.
- **account** — across all addresses, earns a *delay*, never a refusal: the uncached check is scheduled
  after the wait (on the node's scheduler, never sleeping a checker thread) and then run. Nobody can lock
  an account by knowing its name.
- **address budget** — `serverless.auth.throttle.address_failures` (default 50) failures per 60 s window
  across all names; when reached, uncached attempts from the address are refused until the window turns.
  One bad account cannot reach it (its refused attempts are not failures); only a cross-name flood can.
  An uncached ordinary account arriving from a flooding address waits out the window — stated, not hidden.
- **The configured account is never refused, only delayed**, whichever key earned the wait, and the delay
  is capped at `max_delay`. Under sustained attack the operator logs in a minute late; they log in.
- **The cache is consulted before the throttle.** A caller this node verified within the TTL is served
  whatever the throttle thinks of its address. Cost: a guesser under a pair wait learns at once, rather
  than after the wait, if a guess is the password some other caller verified here in the last minute.
- **Pending delays are bounded** by `serverless.auth.check.queue_size`; past it the answer is the same 503
  a full checker queue gets. Counters live in an LRU of 10,000; spreading out evicts others' counters.
- **`X-Forwarded-For` is believed only from `serverless.auth.throttle.trusted_proxies`** (addresses or
  CIDR blocks). From a listed proxy the caller is the rightmost forwarded address that is not itself a
  listed proxy; an unreadable header falls back to the proxy's own address. Unconfigured, the header is
  ignored.
- **`UserSubject#authenticate`** (the plugin API) consults the same throttle, counts failures, and refuses
  a waiting account with 429 rather than delaying, because it is synchronous. It derives on the calling
  thread.

**Per node.** The throttle and the credential cache are per process. N nodes give a guesser N× the
budget; a wait earned on one node says nothing about another. Sharing them through the account index
would cost a read per failure on an unauthenticated path and is not done.

**Bounds an unauthenticated caller meets:** 4 checker threads, a queue of 64 (503 + `Retry-After` past
it), a 10,000-entry cache and throttle table, the marker read at most once a second, a stored record's
iteration count capped on verify, and a `_serverless/security/users` body that is not parsed for callers
other than the configured account. An unknown name costs one `get` of the account index — possibly
forwarded to the shard's owner — plus one derivation.

## 6. Authorization and the system-index guard

- `ActionFilter`s run once, on the node the request reached (§3). Coverage is pinned by
  `testEveryDataEndpointReachesAFilter`.
- **Plugin-subject convention** (W6). A plugin running under
  `IdentityService#getPluginSubject(plugin).runAs(...)` has the transient
  `serverless_plugin_subject = plugin:<class>` on the thread and no user principal;
  `getCurrentSubject().getPrincipal()` reports that name. The auth plugin's own reads and writes of
  `.serverless_auth` carry the same transient **beside** the request's principal, not instead of it: a
  filter sees `serverless_plugin_subject = plugin:org.opensearch.serverless.auth.ServerlessAuthPlugin`
  with no user during a login lookup (there is no caller yet), and sees the administrator as
  `serverless_auth_principal` plus the plugin marker during account management. An authorizer can
  therefore both allow a plugin its own system index and decide account management by caller; nothing in
  the shell needs to change for this, because the client preserves the thread context onto `GENERIC`.
- **System indices are refused by route** (`SystemIndices.guard` on the path's `index` parameter) plus
  per-name checks on resolved search targets, alias targets, bulk items and the written target. At the
  time of writing, two body-addressed routes are **not** covered: `_mget` with `_index` in the body, and
  `_snapshot` capture/restore-with-rename of a system index (review D findings 1 and 2). Until those land,
  any authenticated account can read the password records through either. The plugin `Client` has no
  system-index check at all, by design: an installed plugin can read or write any index.
- A pattern search that matches a system index is refused wholesale and names it; listing, field-caps and
  resolve silently drop the name instead. Names matching a system-index pattern are not reserved on
  creation paths (rollover, data streams, restore targets).

## 7. The plugin boundary

- No `SecurityManager` or policy; parity with core 3.x.
- Components are a list, not an injector. `ServerlessNode#buildPluginActions` resolves a plugin action's
  constructor against **every** plugin's components, so a component one plugin exports is handed to any
  other plugin that declares its type. The auth plugin now exports nothing from `createComponents` (its
  handlers hold the store directly). The shell-side fix — resolve against the declaring plugin's own
  components plus node services — is written up in `fix-W6.md`.
- `serverless.plugins` classes load on the node's classpath with no isolation; disk plugins go through
  core's `PluginsService`.

## 8. What an operator must do today

1. Put both ports on a network no untrusted party can reach, or install a TLS plugin for the transport
   (proven) and the HTTP port (not yet demonstrated), with mutual TLS on the transport.
2. Treat every principal with read access to the bucket as a cluster member with write access to every
   shard. Do not grant a "read-only" tier bucket access expecting it to be read-only.
3. Turn on bucket-side encryption if data at rest matters; the shell does not encrypt.
4. Set `serverless.auth.bootstrap.username` to something other than the default `admin`, keep the keystore
   file's permissions tight, and set `serverless.auth.throttle.trusted_proxies` if clients arrive through
   a load balancer.
5. Do not install plugins you would not hand the bucket credentials to: a plugin's `Client` can read and
   write the account index.

## 9. Where the tests are

`ServerlessAuthTests` (challenge, bootstrap, first account, revocation, marker, throttle doubling and
recovery, queue overflow, identity API, storage shape, system-index guard by route and `_bulk`),
`ServerlessThrottleTests` (shared address, recovery account never refused, planted work factor, address
budget, trusted proxies), `ServerlessTransportAuthTests` (wrong MAC, no credential, wrong token without
a MAC, replay, rotation), `ServerlessTlsTests`
(TLS and mutual TLS on the transport), `ServerlessAuthorizationTests` (filter coverage and response
rewriting). Not covered: `_mget` and `_snapshot` against a system index, HTTP-layer TLS, a MAC signed under the previous secret after the window has closed (needs a hand-computed MAC; the two-behind refusal covers the stale-generation property).
