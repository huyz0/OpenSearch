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
import org.opensearch.serverless.storage.scheduling.RewriteAdmissionController;
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
    CompactionRebaseExecutor rebaseExecutor, RewriteAdmissionController admissionController) {

    /**
     * Creates a config bundling everything needed to schedule background compaction for one shard.
     *
     * @param interval        delay between successive compaction ticks
     * @param manifestStore   store used to read the manifest at the shard's currently published generation
     * @param materializer    materializes a commit manifest's segments into a real Lucene directory
     * @param commitPublisher publishes a merged commit as a new manifest
     * @param policy          decides whether the shard is a compaction candidate, and how many segments to merge to
     * @param rebaseExecutor  runs the rebase-on-conflict publish attempt once the policy says the shard is a candidate
     * @param admissionController {@code null} to disable the node-wide concurrency cap on compaction/rewrite
     *                             ticks entirely; see {@link CompactionSchedulerTask}'s own javadoc.
     */
    public CompactionSchedulerConfig {
    }

    /** Delay between successive compaction ticks. */
    @Override
    public TimeValue interval() {
        return interval;
    }

    /** Store used to read the manifest at the shard's currently published generation. */
    @Override
    public BlobContainerManifestStore manifestStore() {
        return manifestStore;
    }

    /** Materializes a commit manifest's segments into a real Lucene directory. */
    @Override
    public ObjectStoreCommitMaterializer materializer() {
        return materializer;
    }

    /** Publishes a merged commit as a new manifest. */
    @Override
    public ObjectStoreCommitPublisher commitPublisher() {
        return commitPublisher;
    }

    /** Decides whether the shard is a compaction candidate, and how many segments to merge to. */
    @Override
    public CompactionPolicy policy() {
        return policy;
    }

    /** Runs the rebase-on-conflict publish attempt once the policy says the shard is a candidate. */
    @Override
    public CompactionRebaseExecutor rebaseExecutor() {
        return rebaseExecutor;
    }

    /** {@code null} to disable the node-wide concurrency cap on compaction/rewrite ticks entirely. */
    @Override
    public RewriteAdmissionController admissionController() {
        return admissionController;
    }
}
