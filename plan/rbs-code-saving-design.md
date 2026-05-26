# RBS Cost & Code-Saving Strategy Design

This document details the complete design, data structures, cost models, and recovery protocols for the Pluggable Tar and Node-Bundle strategies.

---

## 1. Architectural Goal
To reduce S3 API call volumes and costs in OpenSearch Remote-Backed Storage (RBS) by:
1. **Segment Tar Strategy**: Packaging segment files of a refresh batch into a single tar and reading via S3 Range GETs.
2. **Translog Node-Bundle Strategy**: Bundling translog files at node-level using a time-partitioned layout, master-coordinated `.idx` registry, and Range GETs for shard recovery.

---

## 2. Ingest Data Flow & Streaming Tar/Untar

* **Zero Memory Buffering**: Manual `TarOutputStream` and `TarInputStream` perform all read/write operations in a streaming fashion using a fixed-size buffer ($4\text{ KB} - 8\text{ KB}$). They never read whole files or entry payloads into JVM Heap.
* **Direct Streaming & IOPS Optimization**: Standard cloud storage upload APIs (like S3's `writeBlob`) require knowing the content length of the stream upfront. Because we mathematically pre-calculate all file payload offsets, padding, and USTAR headers, the exact total size of the streamed tar archive is deterministic and matches `currentOffset + 1024` (where 1024 represents the two 512-byte tar EOF blocks). Writing translog bundles to local temporary files first would consume valuable disk write IOPS, which is a bottleneck on high-throughput database nodes. By streaming the tar archive on-the-fly directly to S3 via a chained `SequenceInputStream`, we avoid local disk writes entirely. In the event of network hiccups or upload failure, we can simply reconstruct the stream and retry the upload from scratch since tar construction is computationally cheap.
* **Segment Referencing**: Segment files within a tar archive are registered in the remote metadata catalog with the naming convention `bundle_uuid.tar#offset` and their original length.
* **Segment Reads**: `RemoteSegmentStoreDirectory` checks if the remote filename contains `#`. If found, it parses the offset and delegates to `openBlockInput` in the remote data directory, executing an S3 Range GET.

---

## 3. Tar Archive Structure & Inline Index (`index.bin`)

To enable metadata retrieval without downloading the entire tar file, we enforce that **the very first entry of every tar archive must be the binary index file (`index.bin`)**.
* **Index File Location**: The tar archive starts with the USTAR header for `index.bin` (at byte 0). The `index.bin` payload begins at byte 512, padded to the next 512-byte boundary.
* **Range GET Read**: To parse the index, readers issue a Range GET for the first block (e.g., bytes 0–2047). If the index size exceeds the block size, a second GET is issued. In practice, the binary index is extremely small (<1 KB) and fits in the first block.

### A. Segment Tar Index Schema (`index.bin`):
Direct binary serialization:
1. **Magic Bytes**: 4 bytes (`STRI` - Segment Tar Registry Index)
2. **Version**: 1 byte (`1`)
3. **Number of files**: 2 bytes (short)
4. **Files array**: For each file:
   - **Filename length**: 2 bytes (short)
   - **Filename**: UTF-8 string bytes
   - **Offset in Tar**: 8 bytes (long)
   - **Length**: 8 bytes (long)
   - **Checksum**: 8 bytes (long)

### B. Translog Tar Index Schema (`index.bin`):
Direct binary serialization:
1. **Magic Bytes**: 4 bytes (`TTRI` - Translog Tar Registry Index)
2. **Version**: 1 byte (`1`)
3. **Node ID Length**: 2 bytes (short)
4. **Node ID**: UTF-8 string bytes
5. **Number of shards**: 2 bytes (short)
6. **Shards array**: For each shard:
   - **Index UUID Length**: 2 bytes (short)
   - **Index UUID**: UTF-8 string bytes
   - **Shard ID**: 4 bytes (int)
   - **Primary Term**: 8 bytes (long)
   - **Number of files**: 2 bytes (short)
   - **Files array**: For each file:
     - **Filename length**: 2 bytes (short)
     - **Filename**: UTF-8 string bytes
     - **Generation**: 8 bytes (long)
     - **Offset in Tar**: 8 bytes (long)
     - **Length**: 8 bytes (long)

---

## 4. S3 Path Prefixing & Partitioning

* **Default Strategy Hashing**: The default path strategy in OpenSearch hashes `shardId + indexUUID` using FNV-1a to prepend a hashed prefix to S3 paths. This spreads the concurrent writes of different shards across different S3 partitions, avoiding hot partitioning.
* **Why Tar Strategies Bypass Per-Shard Hashing**:
  * **Translog Node-Bundle Strategy**: Consolidates translog writes from multiple shards (often from different indexes) on the same node into a single archive. Since the archive contains data for multiple shards, using a single shard's hash prefix is impossible.
  * **Segment Tar Strategy**: Although segment tars are uploaded per shard, the bundling process consolidates what would have been 5–10 individual file writes into a single tar upload per refresh. This reduces S3 PUT request volume by 80–90%, making per-shard S3 partition hot-spotting a non-issue.
* **S3 Paths for Bundled Strategies**:
  * **Segment Tars**:
    `s3://bucket/<basePath>/<indexUUID>/<shardId>/segments/data/<bundle_uuid>.tar`
  * **Translog Tars (Node-Bundles)**:
    `s3://bucket/node_bundles/<nodeId>/year_<YYYY>/month_<MM>/day_<DD>/hour_<HH>/minute_<MM>/bundle_<uuid>.tar`
  * **Cluster-Level Index Files**:
    `s3://bucket/cluster_index/index_term_<term>_<timestamp>.idx`

---

## 5. Translog Bundling & Transport Actions

* **Bundling Queue (`NodeTranslogUploadQueue`)**: Shards queue translog updates on the data node.
* **Flushing Triggers**: The background aggregator thread drains the queue and initiates bundling when *any* of the following conditions are met:
  1. **Count**: $\ge 5$ shards have pending writes.
  2. **Size**: Combined translog bytes $\ge 10\text{ MB}$.
  3. **Duration (SLA)**: The oldest queued item has waited $\ge 500\text{ ms}$.
* **Idle Prevention**: If the queue is empty, the node remains idle and **does not write or upload any empty tar files to S3**.
* **ACK Gate**: Local translogs are only pruned once the active master returns an ACK to the data node's `NodeBundleReportRequest` transport action.

---

## 6. Master Node In-Memory Parallel-Array Registry

To support 50,000 active bundles and 500,000 shard mappings without garbage collection (GC) overhead, the master node stores the registry using **flat primitive parallel arrays** and dictionaries:

```
[Dictionaries]
Node ID Dictionary  : Map<String, Short>  (e.g., 50 entries)
Shard ID Dictionary : Map<String, Short>  (e.g., 5,000 entries)

[Bundle Arrays] (Length: 50,000)
bundleUuidsMostSig  : long[]   [ UUID MSB ]
bundleUuidsLeastSig : long[]   [ UUID LSB ]
bundleTimestamps    : long[]   [ Epoch Millis ]
bundleNodeIds       : short[]  [ Node Dict Index ]
bundleShardOffsets  : int[]    [ Pointer to Shard Entry Array ]
bundleShardCounts   : int[]    [ Count of Shards in this Bundle ]

[Shard Entry Arrays] (Length: 500,000)
shardIds            : short[]  [ Shard Dict Index ]
minGenerations      : long[]   [ Min Generation ]
maxGenerations      : long[]   [ Max Generation ]
primaryTerms        : long[]   [ Primary Term ]
```

### Memory Footprint Estimation:
$$\text{UUID Arrays} = 2 \times 50,000 \times 8 \text{ bytes} = 800\text{ KB}$$
$$\text{Timestamps} = 50,000 \times 8 \text{ bytes} = 400\text{ KB}$$
$$\text{Node Index} = 50,000 \times 2 \text{ bytes} = 100\text{ KB}$$
$$\text{Offsets \& Counts} = 2 \times 50,000 \times 4 \text{ bytes} = 400\text{ KB}$$
$$\text{Shard IDs} = 500,000 \times 2 \text{ bytes} = 1.0\text{ MB}$$
$$\text{Generations \& Terms} = 3 \times 500,000 \times 8 \text{ bytes} = 12.0\text{ MB}$$
$$\text{Total Registry Heap} \approx 14.7\text{ MB}$$

---

## 7. S3 Periodical Commits & N-Version Retention

* **Commit Task**: The master node flushes its registry to S3 every 5 minutes as `index_term_<term>_<timestamp>.idx` in a compact **binary format** to avoid Jackson/Gson heap allocation overhead.
* **Raft Term Fencing**: The Raft term in the filename allows recovering nodes to fence split-brain master nodes. Nodes/new masters accept the latest index file on S3 with a term $T_{index} \le T_{current}$ (retaining historical state on master election/failover) and only reject index files with a term $T_{index} > T_{current}$ (which indicates writes from an alternate/future term).
* **Idle PUT Skip**: If no new bundles have been reported or deleted, the master skips the periodic write, reducing idle PUT costs to **zero**.
* **N-Version Buffer**: The master retains the last 2 index versions to recover in case of write failures, deleting older versions asynchronously via free S3 `DELETE` calls.

---

## 8. Master Failover Delta Scan

When a new master is elected, it reconstructs the live registry in **$<300\text{ ms}$**:
1. It downloads the latest `index_term_<term>_<timestamp>.idx` file (takes $\approx 50\text{ ms}$).
2. It calculates the delta window: $[T_{idx} - 2\text{ minutes}, T_{now}]$.
3. It lists the time-partitioned folders in parallel for the delta window (5–7 LIST calls $\approx 80\text{ ms}$).
4. For any new bundles found, it executes Range GETs on the first block to read the inline `index.bin` ($\approx 100\text{ ms}$).
5. It merges the delta and immediately writes a new index.

---

## 9. Shard Recovery Flow

* **Coordinated Recovery**:
  1. The recovering node requests metadata location from the master via `GetTranslogLocationRequest`.
  2. The master resolves the entries in its parallel-array index and returns S3 paths, offsets, and lengths.
  3. The recovering node calls S3 Range GETs directly to download the translogs.
* **Index Caching Optimizations**:
  * *Node-Level `.idx` Cache*: To prevent concurrent recovering shards on the same node from downloading duplicate copies of the `.idx` file, the downloaded index is cached at the node level using a thread-safe cache with a short Time-To-Live (TTL) of 10 seconds.
  * *Shard-Level Inline Header Cache*: In the self-healing fallback path, the parsed inline `index.bin` of any scanned `bundle.tar` is cached in-memory for the duration of the recovery, ensuring the bundle's 512-byte header block is read at most once per shard recovery.

---

## 10. S3 Cost & Math Model (1 TB/day Ingest, 500 Shards)

### Baseline Shard Sync Cost:
* Shards sync translogs every 2 seconds:
  $$\text{PUT Requests/day} = 500 \text{ shards} \times \left( \frac{86400}{2} \right) \text{ uploads} \times 3 \text{ files} \approx 64.8 \text{ Million PUTs}$$
  $$\text{Monthly PUT API Cost} = 64.8\text{M} \times \frac{\$0.005}{1000} \times 30 \text{ days} \approx \mathbf{\$9,720.00}$$

### Node-Bundle Strategy Cost (500ms Queue Wait Time):
* With 10 data nodes, bundles are flushed every 500ms (or when min-shards/max-bytes are met).
* Average uploads per node = 2 bundles/second:
  $$\text{PUT Requests/day} = 10 \text{ nodes} \times 2 \times 86400 \text{ bundles} + 288 \text{ index updates} \approx 1,728,288 \text{ PUTs}$$
  $$\text{Monthly PUT API Cost} = 1.73\text{M} \times \frac{\$0.005}{1000} \times 30 \text{ days} \approx \mathbf{\$259.50}$$
  *(Under high load, bundles will hit the Shard Count / Size limit faster, achieving even greater compaction and lower API call volumes).*
* **API Cost Savings: >97.3% Reduction**

---

## 11. Pluggable Extension Points & Strategy Contracts

To support pluggable segment and translog storage layouts, OpenSearch Core (`server`) exposes new extension interfaces and registry hooks.

### A. The Plugin Interface (`RemoteStorePlugin`)
Plugins register their custom strategies by implementing the `RemoteStorePlugin` interface:
```java
@ExperimentalApi
public interface RemoteStorePlugin {
    default Map<String, RemoteStoreSegmentStrategy> getRemoteStoreSegmentStrategies() {
        return Collections.emptyMap();
    }

    default Map<String, RemoteStoreTranslogStrategy> getRemoteStoreTranslogStrategies() {
        return Collections.emptyMap();
    }
}
```
During node initialization, `IndicesService` collects these strategy maps from all active plugins, registers standard `"default"` fallback implementations, and exposes them. Shards lookup the strategy name defined in `IndexSettings` (`index.remote_store.segment.strategy` and `index.remote_store.translog.strategy`) and load the matching implementation from the registry maps.

### B. Segment Strategy Contract (`RemoteStoreSegmentStrategy`)
This interface encapsulates all read, write, metadata management, seeking, lock tracking, and garbage collection behaviors for segment files:
* **Upload Path**:
  * `upload(...)`: Executes the physical copy/upload of segment files to the remote repository.
  * `uploadMetadata(...)`: Uploads metadata files. Custom strategies like `rbs-tar` can make this a NO-OP since segment metadata is written inline as `index.bin` inside the archive.
* **Read Path**:
  * `openInput(...)` & `openBlockInput(...)`: Retrieve standard or block-based range GET slices from the remote store.
  * `fileLength(...)`: Queries the size of a segment file (e.g. from local memory mappings or parsed inline headers).
* **Metadata & Lock Tracking**:
  * `init(...)`, `readMetadata(...)`, `initializeToSpecificTimestamp(...)`, `initializeToSpecificCommit(...)`, `readLatestNMetadataFiles(...)`, and `getMetadataFileForCommit(...)` delegate all metadata discovery, parsing, and timestamp/commit lock management to the strategy.
* **Deletion & GC**:
  * `deleteFile(...)`: Removes a single file.
  * `deleteStaleSegments(...)`: Custom garbage collection routing. In `rbs-tar`, it group stale segments by physical tar file and deletes the tar only when all constituent segments are inactive.

### C. Translog Strategy Contract (`RemoteStoreTranslogStrategy`)
This interface manages the lifetime, transfer, and restoration of translog snapshots:
* `transferSnapshot(...)`: Uploads translog snapshots. Under the default strategy, it transfers files one-by-one; under the `rbs-tar` strategy, it delegates uploads to the node-level aggregation queue (`NodeTranslogUploadQueue`).
* `downloadTranslog(...)`: Downloads individual translog generations during recovery.
* `download(...)`: Custom download handler that allows a strategy to bypass the default recovery routine entirely. `TarTranslogUploadStrategy` uses this to coordinate recovery coordinates with the Master node, falling back to direct range-based S3 scans under master unavailability.

---

## 12. Complete Replacement of Default Behavior

Pluggable strategies can completely replace default remote store behaviors because the OpenSearch core has been decoupled from any specific storage format, file extension, metadata schema, or garbage collection model:

1. **Complete File Layout Autonomy**:
   * *Default Strategy*: Directly writes individual segment/translog files to S3 paths.
   * *Tar Strategy*: Bundles multiple files into a single `.tar` archive and uses custom naming conventions (e.g., `<bundle_uuid>.tar#offset`).
2. **Metadata-Free S3 Layouts**:
   * *Default Strategy*: Requires uploading separate `.metadata` (segments) or `txlog_*` (translog) files to S3, leading to additional PUT costs.
   * *Tar Strategy*: Integrates metadata inline inside the archives (as `index.bin`). It eliminates metadata files entirely, discovering state by listing archives and performing Range GETs on the inline headers.
3. **Optimized Garbage Collection (GC)**:
   * *Default Strategy*: GC runs per shard, invoking S3 lists and batch deletes for metadata/data files.
   * *Tar Strategy*: Segment GC tracks active dependencies across shared tar files; translog GC is offloaded to a central master scheduler checking index state, reducing S3 LIST calls to zero.
4. **Custom Recovery Routing**:
   * *Default Strategy*: One-by-one sequential file downloads from S3.
   * *Tar Strategy*: Queries master coordinates via TCP to fetch exact byte slices, avoiding redundant file-level downloads and extra S3 latency.

By delegating the entire lifecycle (upload, read, metadata, lock, and deletion) to the strategy interfaces, core classes like `RemoteSegmentStoreDirectory` and `RemoteFsTranslog` act as pure orchestrators, making the underlying S3 layout 100% pluggable.

---

## 13. Testing Strategy (TDD Approach)

1. **Red Test Creation**: Write unit/integration tests that assert on the expected outcomes (e.g. tar streaming output, block offsets, parallel registry lookups, zero-empty-upload assertions) before writing code.
2. **Minimal implementation**: Write the minimum strategy code required to pass the test cases.
3. **Verify via spotless**: Run `./gradlew spotlessApply` to verify and enforce imports-based code styles.
4. **Mock Repository**: Use the `FsBlobStoreRepository` test fixtures to simulate S3 remote stores locally and assert on exact API call counts.

