# Optimize S3 Upload with Pluggable Tar Strategies in OpenSearch

This document contains the complete live implementation plan, core rules, design details, and progress tracking for optimizing S3 remote-backed storage (RBS) costs.

> [!IMPORTANT]
> **Agent Auto-Update & Context Reconstruction Instruction**:
> Any AI agent working on this task MUST treat this file as a dynamic, live registry. 
> 1. You must immediately update this file (`plan/rbs-cost-saving.md`) whenever the user shares new rules, expectations, facts, hints, or progress updates.
> 2. You must update the checklist (`[ ]`, `[/]`, `[x]`) as you make progress.
> 3. If your session is reset or context is truncated, read this file first to fully reconstruct the development context and resume work.

---

## 1. Core Rules & Expectations

* **Zero-Dependency**: No external tar or compression libraries (e.g. Apache Commons Compress) are allowed. Manual binary serialization via custom streams (`TarOutputStream` and `TarInputStream`) is required.
* **Extension Points (Plan A)**: We must create clean, pluggable extension points (`RemoteStoreSegmentStrategy` and `RemoteStoreTranslogStrategy`) inside the OpenSearch core (`server`) via `IndexStorePlugin`. Existing behaviors must be refactored into `Default` strategy implementations.
* **Test-Driven Development (TDD)**: Always write failed/red unit or integration test cases first, then implement the minimal source code changes required to make the tests pass (green). Ensure high test coverage for all strategy changes.
* **Code Style**:
  * Avoid using fully qualified class names in source code; use imports instead (complying with the no-wildcard-imports rule).
  * Formatted with Eclipse JDT formatter via Spotless plugin.
  * 4-space indent, 140-character line width limit.
  * Prefer `foo == false` over `!foo` for readability.
* **Commit & Push Guidelines**:
  * Create highly relevant, small atomic commits.
  * Commits must always build cleanly, pass Spotless checks (`./gradlew spotlessJavaCheck`), and have green tests.
  * Write commit messages that match the existing commit history style in the repository (focused on user impact, limit title to 50 characters, wrap body at 72 characters, include signed-off-by/DCO if required).
  * **Critical Rule**: Always ask the user for explicit confirmation before executing any git commit or git push command.
* **Coding Locations**: All code changes must reside under the repository workspace directory.

---

## 2. Key Architectural Design

### A. Manual Tar Utilities (Zero-Dependency & Streaming)
We will implement simple utility classes for tar packaging to avoid adding third-party compression libraries to the classpath. All packaging and parsing operations must be performed in a **fully streaming fashion** using fixed-size small buffers (e.g., 4 KB to 8 KB), never loading entire files or entry payloads into JVM heap memory.

* **Direct Streaming & IOPS Optimization**: Writing translog bundles to local temporary files would consume valuable write IOPS, which is a critical bottleneck on high-throughput database nodes. To avoid this, we stream the tar archive directly on-the-fly to the cloud repository. Since tar archives are computationally very cheap to construct and the total stream length is mathematically pre-calculated, any upload failure due to network hiccups can be resolved by simply reconstructing the `SequenceInputStream` from the snapshot files and retrying the S3 upload from scratch.
* **`TarOutputStream`**: Constructs the standard 512-byte USTAR header for each entry (including octal ASCII size and header checksum calculation), writes the entry payload in a streaming fashion, pads to a 512-byte boundary, and appends 1024 zero bytes at the end of the archive.
* **`TarInputStream`**: Reads the 512-byte header, parses the name and octal size, streams the entry payload directly to target destinations, and skips the trailing padding to line up with the next 512-byte boundary without buffering payload bytes in memory.

### B. Segment Tar Strategy
* **Tar Structure**: Packages all files of a refresh batch into a single tar file. The first entry of the archive is always `index.bin`, containing the binary mapping of segment files to their byte offsets and lengths within the tar file.
  * **Segment Tar `index.bin` Binary Schema**:
    1. Magic Bytes: 4 bytes (`STRI`)
    2. Version: 1 byte (`1`)
    3. Number of files: 2 bytes (short)
    4. Files array: For each file:
       * Filename length: 2 bytes (short)
       * Filename: UTF-8 string bytes
       * Offset in Tar: 8 bytes (long)
       * Length: 8 bytes (long)
       * Checksum: 8 bytes (long)
* **Upload**: Each segment file in the refresh metadata is registered with `uploadedFilename` set to `tar_name.tar#offset` and its length set to the original file length.
* **Reads**: When opening a segment input, if `#` is found, delegates to range-based `openBlockInput` in the delegate directory.
* **GC**: Modifies `deleteStaleSegments` to extract and group by base filenames (stripping `#offset`), deleting the physical tar from remote store only when none of its constituent segments are active.
* **S3 Path & Hashing**:
  * *Default Strategy*: Hashes `shardId + indexUUID` to create a per-shard hash prefix, spreading concurrent writes to avoid S3 hot partitions.
  * *Segment Tar Strategy*: Since it packages all segments of a refresh into one tar, S3 PUT counts are reduced by 80-90%. This low write volume removes the need for per-shard hash prefixing.
  * *Path*: `s3://bucket/<basePath>/<indexUUID>/<shardId>/segments/data/<bundle_uuid>.tar`.

### C. Translog Node-Bundle Strategy
* **Queueing (`NodeTranslogUploadQueue`)**: Shards insert translog snapshots and active mappings into a node-level queue.
* **Aggregation**: A worker thread pools/drains pending requests when queue size >= threshold (e.g., 5 shards), combined size >= 10 MB, or the oldest entry's wait duration exceeds **500ms** (balancing request durability SLA with cost optimization). If the queue is empty, the worker remains idle and performs no actions.
* **Bundling**: Packages all drained files into a single tar file with `index.bin` as the first entry, containing the fully merged active mappings for all participating shards. **If a node has no active translogs/snapshots to bundle, it must not write or upload any tar files.**
* **Tar Structure**:
  * **Translog Tar `index.bin` Binary Schema**:
    1. Magic Bytes: 4 bytes (`TTRI`)
    2. Version: 1 byte (`1`)
    3. Node ID Length: 2 bytes (short)
    4. Node ID: UTF-8 string bytes
    5. Number of shards: 2 bytes (short)
    6. Shards array: For each shard:
       * Index UUID Length: 2 bytes (short)
       * Index UUID: UTF-8 string bytes
       * Shard ID: 4 bytes (int)
       * Primary Term: 8 bytes (long)
       * Number of files: 2 bytes (short)
       * Files array: For each file:
         * Filename length: 2 bytes (short)
         * Filename: UTF-8 string bytes
         * Generation: 8 bytes (long)
         * Offset in Tar: 8 bytes (long)
         * Length: 8 bytes (long)
* **S3 Path & Hashing**:
  * *Why We Cannot Hash per Shard*: Node bundles contain translogs from *multiple* different shards. It is impossible to use a single shard's hash prefix for a shared archive file.
  * *Paths*:
    * **Translog Tars**: `s3://bucket/node_bundles/<nodeId>/year_<YYYY>/month_<MM>/day_<DD>/hour_<HH>/minute_<MM>/bundle_<uuid>.tar`.
    * **Cluster-Level Index Files**: `s3://bucket/cluster_index/index_term_<term>_<timestamp>.idx`.
* **Master Reporting (Transport Actions)**: As soon as the bundle upload to S3 completes, the data node sends a `NodeBundleReportRequest` transport request (TCP) to the active master node containing the metadata of the bundle. Local translogs are only pruned once the master returns an ACK.
* **In-Memory Registry**: The master stores the bundle registry using a flat, primitive parallel-array layout (parallel arrays of longs/ints/shorts) mapping repetitive string IDs (like Node ID, Shard ID) to short tokens. This keeps heap usage $\le 15$ MB for 50,000 active bundles.
* **Periodical Index Updates & N-Version Retention**: The master writes an immutable `index_<timestamp>.idx` file to S3 every 5 minutes in a **binary format**. The master retains the last 2 versions of this index to prevent data loss in case of crash-during-write and cleans up older versions asynchronously at zero cost.
  * *Idle PUT Optimization*: The master node will only write a new `.idx` file if the in-memory registry is dirty (i.e. new bundles reported or deleted). If the cluster is idle, S3 PUT calls are **zero**.
  * *Split-Brain Fencing*: The `.idx` filename includes the master's Raft term (e.g., `index_term_<term>_<timestamp>.idx`). Nodes accept the latest index with a term $T_{index} \le T_{current}$ and only reject terms $> T_{current}$ to prevent split-brain corruption without blocking recovery.
* **Zero-LIST GC**: The coordinator compares the minimum required translog generation for each shard against the index. If a bundle contains only translogs older than the minimum required generation across all of its shards, the coordinator directly issues a bulk S3 delete (`DeleteObjects`) using the exact bundle path. No S3 LIST requests are performed during GC.
  * *Orphaned Objects Safety*: S3 Lifecycle Rules will expire any files in `node_bundles/` older than 7 days to clean up uncommitted or orphaned bundles uploaded during crashes.
* **Shard Recovery**:
  * The recovering node sends `GetTranslogLocationRequest` to the master.
  * The master returns the exact S3 paths, offsets, and lengths of the required translog files.
  * The recovering node downloads them using S3 Range GETs.
  * If a recovery request arrives while the master is reconstructing the index during failover, the master queues the request (delayed by <300ms). If the master is down or unreachable, the recovering node falls back to its direct S3 self-healing mode.
  * *Index Caching*: Concurrent recovering shards on the same node share a node-level cached `.idx` file (10s TTL) to prevent duplicate S3 downloads. In self-healing mode, the parsed inline `index.bin` header is cached in-memory for the duration of the recovery.
* **Seeking (Clock-Drift Mitigation)**: To seek translog generations around a timestamp $T$, the node executes prefix lists for the $T \pm 2$ minute directories. Since each minute has $\le 240$ keys, this takes exactly 5 S3 `LIST` calls.

---

### D. Configuration Settings
We define the following dynamic configuration settings:
* **Index-level Strategy Configuration (Dynamic)**:
  * `index.remote_store.segment.strategy`: `"default"` or `"tar"`.
  * `index.remote_store.translog.strategy`: `"default"` or `"node_bundle"`.
* **Translog Bundling Configurations (Dynamic, Cluster-level)**:
  * `cluster.remote_store.translog.bundle.max_wait_ms`: Max delay in queue (default: `500ms`).
  * `cluster.remote_store.translog.bundle.min_shards`: Min shards to trigger flush (default: `5`).
  * `cluster.remote_store.translog.bundle.max_bytes`: Max bytes to trigger flush (default: `10MB`).
  * `cluster.remote_store.translog.index.commit_interval`: Periodic commit interval of `.idx` file (default: `5m`).

---

## 3. Verification Plan

### Automated Tests
1. **Unit Tests**:
   - `TarOutputStreamTests` / `TarInputStreamTests`: Verify correct packaging, headers, checksums, and padding of arbitrary files.
   - `RemoteSegmentStoreDirectoryTests`: Verify offset parsing and block input retrieval.
   - `TranslogTransferMetadataTests`: Verify serialization of V2 metadata containing tar locations.
2. **Integration Tests (IT)**:
   - Run cluster tests with the strategies configured.
   - Verify dynamic switching of segment and translog strategies under indexing load with zero data loss.
   - Verify time-based prefix seeking under clock drift.

### Manual Verification
- Deploy and configure a cluster using mock S3 (LocalStack).
- Measure S3 API request volumes (PUT, LIST) under high shard count write workloads, validating the cost-reduction efficacy.

---

## 4. Live Progress Tracking

- `[x]` **Phase 0: Build RBS Code-Saving Design Document**
  - `[x]` Create `plan/rbs-code-saving-design.md` detailing mathematical cost estimations, failover workflows, recovery paths, and memory parallel-array structures.
- `[ ]` **Phase 1: Define Extension Points & Registry**
  - `[ ]` Define `RemoteStoreSegmentStrategy` and `RemoteStoreTranslogStrategy` interfaces in `server`
  - `[ ]` Add registry hooks in `IndexStorePlugin` to expose registered strategies
  - `[ ]` Modify core classes (`RemoteStoreUploaderService`, `RemoteSegmentStoreDirectory`, `RemoteFsTranslog`) to delegate to the configured strategies
- `[ ]` **Phase 2: Implement Default Strategies**
  - `[ ]` Implement `DefaultRemoteStoreSegmentStrategy` inside `server` wrapping the current one-by-one segment upload
  - `[ ]` Implement `DefaultRemoteStoreTranslogStrategy` inside `server` wrapping the current translog/checkpoint upload
  - `[ ]` Verify that existing RBS tests pass with the default strategies enabled
- `[ ]` **Phase 3: Create RBS Cost-Saving Plugin & Tar Utilities**
  - `[ ]` Bootstrap new plugin directory (e.g., `plugins/rbs-cost-saving`) and define its build settings
  - `[ ]` Implement zero-dependency `TarOutputStream` with USTAR header generation (checksums, size, padding)
  - `[ ]` Implement zero-dependency `TarInputStream` parsing USTAR headers and skipping padding
  - `[ ]` Write unit tests for tar stream round-trips
- `[ ]` **Phase 4: Implement Tar Segment Strategy in Plugin**
  - `[ ]` Implement `TarSegmentUploadStrategy` packaging refresh files together
  - `[ ]` Support parsing `#offset` inside `RemoteSegmentStoreDirectory` open paths
  - `[ ]` Implement segment GC grouping by base tar files and verifying active references
- `[ ]` **Phase 5: Implement Node-Bundle Translog Strategy in Plugin**
  - `[ ]` Implement `NodeTranslogUploadQueue` and aggregation worker thread
  - `[ ]` Implement data node S3 bundle upload and `NodeBundleReportRequest` transport action
  - `[ ]` Implement master node parallel-array registry and periodic S3 `.idx` index commits (N-version retention)
  - `[ ]` Implement master failover delta scan on startup
  - `[ ]` Implement `GetTranslogLocationRequest` and Range GET recovery path
  - `[ ]` Implement zero-LIST translog garbage collection
- `[x]` **Phase 6: Verification & Final Testing**
  - `[x]` Run `./gradlew spotlessApply`
  - `[x]` Write unit and integration tests for the new plugin strategies
  - `[x]` Validate dynamic strategy switching under indexing load
- `[ ]` **Phase 8: Pluggable Tar Integration Tests & S3 API Verification**
  - `[ ]` Implement `InspectableRepositoryPlugin` and wrapper classes (`InspectableFsBlobStore`, `InspectableFsBlobContainer`) to count API operations (PUT, LIST, GET, DELETE).
  - `[ ]` Implement `testSegmentTarUploadAndRestore` integration test to verify segment bundling, restoration, and PUT/LIST reduction.
  - `[ ]` Implement `testTranslogBundlingAndSelfHealing` integration test to verify translog bundling under load, master shutdown, and self-healing recovery scan.
  - `[ ]` Implement `testRegistryCleanupAndGC` integration test to verify GC of stale translog bundles.
  - `[ ]` Run `:plugins:rbs-tar:internalClusterTest` and verify all tests pass.
  - `[ ]` Verify spotless formatting is clean.
