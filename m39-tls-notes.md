# M39 — TLS through a plugin, which was impossible rather than undemonstrated

M29 wired `NetworkPlugin`s into the network module and the status document has said ever since that TLS
through a plugin was *enabled* and merely not demonstrated. That was wrong, and one argument was the whole
of it.

Core's netty4 module already ships `SecureNetty4Transport`. What it needs is somewhere to get an
`SSLEngine`, and the only way a plugin can supply one is a `SecureSettingsFactory` — passed to
`NetworkModule`, which calls `getSecureTransports` only when it has one. The shell passed
`Collections.emptyList()`. So a plugin doing everything right — implementing the factory, naming
`netty4-secure` in `additionalSettings` — was told the transport type did not exist.

It collects them from the plugins now. Core refuses more than one provider, which is the right rule and
not the shell's to soften: two plugins each believing they configure the node's TLS is a deployment nobody
can reason about.

## What the test proves, and what it borrows

The plugin supplies a provider over a self-signed certificate, both nodes trust it, and a write is
forwarded from a node that does not own the shard to the node that does. A transport that was selected and
then carried nothing would be a worse outcome than one that was never selected.

The encryption is core's. What is being proved is that the shell lets a plugin reach it — which is the part
that was broken, and the part that hosting OpenSearch Security depends on.

## Two things this cost, both worth having

**The keystore is committed.** Netty's self-signed certificate generator needs BouncyCastle, which this
module has only in FIPS builds, and generating one with `keytool` at test time is not available to a test
whose system-call filter blocks `execve`. So a PKCS12 keystore and truststore sit beside the test, exactly
as core's own secure-transport test does, with the password in the source file. They are fixtures for
`CN=localhost` and are as secret as core's `netty4-server-keystore`.

**It needs its own Gradle task.** Netty's SSL handler opens the connection from netty-common's protection
domain, which the test framework's security manager grants nothing — so the handshake fails with "Denied
access to: localhost:46061" in a build where a plain netty4 connection is fine. That is the framework's
policy and not the shell's behaviour, so one test class loses the manager rather than the module. The same
trade `pluginTest` already makes.

## A swallowed cause, found by needing it

The failure above first arrived as `connect_exception` and nothing else. `NotHereException` was built from
`e.getMessage()` and dropped the cause, so every forwarding failure in the system reported the *fact* of a
failed connection and none of the reason — a TLS handshake rejection and a dead node were the same
sentence. It carries the cause now, which is how the security manager denial was identified in one run
rather than by bisection.

## What this does not do

Node identity still does not travel with a forwarded request. TLS protects the wire; it does not yet
authenticate the peer, and the transport port remains trusted infrastructure. Mutual TLS is now reachable —
the provider interface has a client-auth setting and the shell no longer stands in the way — but nothing
here does it.

## Canaries

- **72 — the shell ignores a plugin's secure settings factory.** Restoring the empty list. Caught: the
  node comes up with no secure transport and the forwarded write cannot connect.
