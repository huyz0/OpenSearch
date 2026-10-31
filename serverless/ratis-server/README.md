# Curated Apache Ratis server artifact

Pinned upstream: Apache Ratis 3.3.1. The merged artifact replaces only
`org.apache.ratis.server.impl.RaftServerProxy` and its nested classes.
Other upstream classes and resources are checked byte-for-byte against the
original JAR by `verifyUpstreamContents`.

## Resource cleanup fix

When group initialization throws, upstream `groupAddAsync` removes the failed
division from its map without closing it. A later server close cannot find it,
leaving its storage lock and other resources held. The replacement closes the
failed division before propagating the original failure. Cleanup failures are
attached as suppressed exceptions. Election, quorum, log, and membership
algorithms are unchanged.

The source retains the upstream Apache license. Wildcard imports are expanded,
the repository formatter is applied, and legacy file/socket API usage is narrowly
exempted on the upstream class. No new public SDK API is introduced.

Binary SHA-256: `a635a8c69b5c59089b026992178ecea95ead7d709c5ff4f94834e06eaa0c911d`.
Original source entry SHA-256: `b3a61cc9a1053d1319ec88d939c7360e2e779bf88b5fa44d62c6fcb319597e5e`.
Sources: https://repo.maven.apache.org/maven2/org/apache/ratis/ratis-server/3.3.1/ratis-server-3.3.1-sources.jar

The merged JAR is used instead of the original server JAR, never alongside it.
All other Ratis components remain pinned to 3.3.1 and use the curated common
artifact's metadata directory synchronization fix.

The normal third-party dependency audit skips project artifacts and compile-only
inputs. `patchedArtifactAudit` separately audits the complete merged artifact;
it uses the same forbidden-API and JDK-jar-hell checks without blanket exclusions.
The runtime regression checks the actual artifact origin and refuses duplicate
copies of the replacement class.
