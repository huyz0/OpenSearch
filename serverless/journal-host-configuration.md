# Journal host configuration

The normal `ServerlessBootstrap` launcher can open an explicitly assigned journal
host. This currently supplies replicated journal activation and membership control.
Index writes still use the existing object-store WAL; quorum-mode indexing and
committed search replicas remain under development.

## Provision storage identity

Mount and persist a dedicated SSD directory before launching a voter. Using the
same runtime classpath and JVM options as the daemon, invoke its main class with:

```text
org.opensearch.serverless.shell.ServerlessBootstrap --journal-identity /mnt/journal /tmp/voter-id
```

This command starts no node and makes no object-store request. It durably creates
or reads the disk lineage UUID under an exclusive lock, then writes that UUID to
a new output file. It refuses a missing directory, an unrelated nonempty directory,
a corrupt identity, a running owner, or an existing output file. Read the output
into the controller's placement configuration. Keep this output file outside the
journal directory so initial bootstrap finds only its identity and lock files.
Repeating discovery for the same
stopped disk returns the same UUID. A replacement SSD gets a new UUID and must
join through a durable replacement intent.

## Assignment file

Set `serverless.journal.assignment_file` to a JSON file. Paths in the assignment
and TLS settings resolve against `path.conf`, which defaults to `path.home/config`.
The journal directory must already exist. No startup setting authorizes creating
a missing mount or substituting a new disk identity.

Example structure, with UUIDs and placement replaced by the controller's actual
reserved deployment, group and provisioned disk identities:

```json
{
  "format_version": 1,
  "deployment_id": "00000000-0000-4000-8000-000000000001",
  "local_voter_id": "11111111-1111-4111-8111-111111111111",
  "directory": "/mnt/journal",
  "bootstrap": true,
  "groups": [
    {
      "id": "00000000-0000-4000-8000-000000000002",
      "peers": [
        {
          "id": "11111111-1111-4111-8111-111111111111",
          "host": "voter-a.journal.example",
          "port": 9876,
          "availability_zone": "az-a"
        },
        {
          "id": "22222222-2222-4222-8222-222222222222",
          "host": "voter-b.journal.example",
          "port": 9876,
          "availability_zone": "az-b"
        },
        {
          "id": "33333333-3333-4333-8333-333333333333",
          "host": "voter-c.journal.example",
          "port": 9876,
          "availability_zone": "az-c"
        }
      ]
    }
  ],
  "limits": {
    "maximum_groups": 4,
    "maximum_payload_bytes": 4194304,
    "maximum_pending_requests": 256,
    "maximum_pending_bytes": 134217728,
    "queue_bytes": 8388608,
    "segment_bytes": 16777216,
    "cached_segment_bytes": 16777216,
    "maximum_retained_entries": 100000,
    "minimum_free_bytes": 1073741824
  }
}
```

`bootstrap: true` is one-time authorization for a blank provisioned volume. For
subsequent process starts set it to `false`; recovery validates the existing
binding, voting state, durable boundary and log. An activated original voter
cannot bootstrap again. Restoring a live voter's files under another process or
recreating an established group after quorum loss is unsupported.

Keep each group's original three-peer genesis in every subsequent assignment.
Native replacement changes the catalog's membership head, not that genesis.
A replacement learner additionally supplies `joining_peer` using the same fields
as a peer above and its own provisioned UUID. Startup verifies its permanent
original placement and winning pending intent against the catalog before joining.
Availability-zone labels are placement inputs; an external controller must verify
the actual hosts occupy those failure domains.

The optional `limits` object accepts the fields shown, retaining defaults for
omitted values. Counts, byte limits and their relationships are validated by
`JournalConfiguration.Limits`. Four physical groups are the default limit, and
sixteen is the hard maximum. These groups multiplex logical shards; they are not
one physical group per index shard. Buffer and process sizing still require
workload qualification. Startup also reserves one maximum-size pending proposal
per active physical group: request capacity must be at least that group count,
and pending bytes must be at least `groups * 6 * (maximum_payload_bytes + 512)`.
The defaults support the default four-group bound. More active groups can require
a larger byte budget or smaller payload limit; changing `maximum_groups` alone
is insufficient. This check runs before binding/opening native journal storage.

The assignment is limited to 64 KiB. Unknown or duplicate fields, trailing content,
unsupported versions, noncanonical UUIDs, coercions from strings/floating point,
invalid three-AZ placements and oversized assignments are refused before node
construction. File configuration and a programmatic `JournalAssignment` cannot
both be supplied.

## Dedicated TLS material

Configure both paths:

```text
-Dserverless.journal.assignment_file=journal-assignment.json
-Dserverless.journal.keystore.path=journal-member.p12
-Dserverless.journal.truststore.path=journal-trust.p12
```

Use PKCS12 stores of at most 1 MiB each. The member store must contain exactly
one private key with an X.509 certificate chain. Its certificate must support
the client and server roles required by peer mutual TLS and match its advertised
hostname or IP. The truststore must contain explicit deployment-scoped X.509
trust anchors and no private keys. There is no fallback to JVM default trust or
the HTTP/transport credentials of the search node.

Store the passwords in the OpenSearch keystore under `path.conf`:

```text
serverless.journal.keystore.secure_password
serverless.journal.truststore.secure_password
```

Use the existing `opensearch-keystore add` command for each setting. Do not pass
passwords as `-D` properties. This launcher currently loads an unpassworded
OpenSearch keystore; its existing bootstrap refuses a password-protected keystore
because it has no interactive password prompt. The PKCS12 stores can have their
own passwords, and each private key uses its store password.

Native opening completes before the node advertises its metadata-plane lease.
All-voter initial activation and pending replacement progress asynchronously;
node startup alone does not prove a quorum is available. The bounded controller
uses cached catalog reads rather than an object-store request per journal write.
Closing the node closes the journal, but its disk remains quarantined until JVM
exit to prevent old native I/O from overlapping another voter process.
