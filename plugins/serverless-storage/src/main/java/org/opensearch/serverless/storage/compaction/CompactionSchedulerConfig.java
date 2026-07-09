/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;

/**
 * Everything a reader- or writer-shard engine needs to schedule its own {@link
 * CompactionSchedulerTask}, bundled into one value so background compaction can be threaded
 * through as a single optional (nullable) constructor parameter instead of six -- same shape as
 * {@code PitrRetentionConfig}. {@code null} where this type is accepted means "background
 * compaction is not configured for this shard," matching how {@code encryptionKeyProvider}/{@code
 * PitrRetentionConfig} being {@code null} means "this feature is off" elsewhere in this plugin.
 */
public record CompactionSchedulerConfig(TimeValue interval, BlobContainerManifestStore manifestStore,
    ObjectStoreCommitMaterializer materializer, ObjectStoreCommitPublisher commitPublisher, CompactionPolicy policy,
    CompactionRebaseExecutor rebaseExecutor) {
}
