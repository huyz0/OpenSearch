# Ratis metadata directory synchronization

This module repackages Apache `ratis-common` 3.3.1 with one functional change:
`AtomicFileOutputStream.close()` synchronizes the parent directory after publishing
the renamed file. It retains the upstream class name and public API. The consensus
protocol, helpers, and remaining upstream classes are unchanged.

Ratis uses this stream to persist voting terms and vote identities. Synchronizing
the temporary file before renaming does not establish durability of the directory
entry produced by the rename. The replacement waits for both steps before returning
success. Directory synchronization failure propagates as `IOException`; there is
no fallback that silently treats the metadata update as durable.

The initial low-latency deployment target is Linux with local filesystems that
support directory synchronization. Unsupported directory operations fail closed.
This patch alone does not prove all journal, snapshot, membership, bootstrap, or
power-loss behavior; those remain part of the runtime storage audit and fault tests.

## Provenance

- Binary: `org.apache.ratis:ratis-common:3.3.1` from Maven Central.
- Upstream binary SHA-256:
  `8485d2051b173d80ccfd39924a597c6b7e256c34008eb2a979aa6641a15e8027`.
- Source: `ratis-common-3.3.1-sources.jar`, entry
  `org/apache/ratis/util/AtomicFileOutputStream.java`.
- Original source-entry SHA-256:
  `cf06dfea845ae55729d791ece557a0e002c09fecd64f5c9a2a6f4546ebc9f0c0`.
- The source retains its Apache license and adds the OpenSearch modification notice.
  Other source changes are formatting, API documentation, and a narrowly scoped
  forbidden-API annotation for the upstream `java.io.File` interface.

The jar task excludes the original replacement-class bytecode before adding the
compiled replacement. `verifyUpstreamContents` checks every other upstream entry
except the manifest byte-for-byte. Class-directory variants are disabled for this
module so consumers receive the complete merged jar. The artifact manifest records
the upstream version and `directory-fsync-v1` patch identity.

`dependencyLicenses` and `updateShas` include the original upstream jar as well as
its explicitly enumerated dependency closure. Original license and notice entries
are retained in the merged jar and copied into `licenses/`, including the bundled
third-party notices. No unpatched duplicate may appear on the consumer classpath.

The dependency audit retains exact allowlists for optional shaded Netty integrations
and known upstream intrinsic implementations. It fails on new missing references
and stale exclusions. The repository missing-doclet hardcodes `org.opensearch`, so
this upstream-namespace module uses the standard Javadoc task instead. Duplicate
class auditing targets the merged runtime jar rather than the intentional compiler
overlay containing both the original API and its replacement.

## Verification

```sh
./gradlew :serverless:ratis-common:check :serverless:log:test \
  :serverless:testkit:consensusTest
```

The log-module tests check merged-artifact origin, uniqueness on the classpath,
complete publication, and an actual POSIX permission fault after rename that makes
directory synchronization fail. The fault is skipped on privileged or non-POSIX
filesystems where that injection is ineffective. The local three-replica Ratis
test then exercises persisted replay and leader replacement with the patched jar.

`patchedArtifactAudit` additionally scans the complete merged artifact, including
unchanged upstream common classes. It complements the existing dependency audit,
which skips project artifacts and compile-only inputs.
