/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobPath;

/**
 * Where each register lives.
 *
 * <p>Section 9.3's map, and the trap it avoids. Making cluster state <em>one</em> register would
 * serialize every mutation in the system through a single optimistic-concurrency point — retry storms
 * under load, and a quiet reinstatement of the O(all indices) cost section 5 exists to remove. So the
 * layout is partitioned by natural CAS granularity, and each register has a bounded, disjoint writer
 * population:
 *
 * <table>
 *   <caption>The register map</caption>
 *   <tr><th>Register</th><th>Writers</th><th>Write rate</th></tr>
 *   <tr><td>{@code cluster/config}</td><td>operators</td><td>human-scale</td></tr>
 *   <tr><td>{@code cluster/members/lease-&lt;nodeId&gt;}</td><td>that node only</td><td>one per TTL</td></tr>
 *   <tr><td>{@code indices/&lt;name&gt;}</td><td>index lifecycle</td><td>rare, per index</td></tr>
 *   <tr><td>{@code shards/&lt;index&gt;#&lt;id&gt;}</td><td>activation and failover</td><td>rare, per shard</td></tr>
 * </table>
 *
 * <p>Note what has <b>no register at all</b>: the routing table and the membership list. Both are
 * derived — membership by listing live leases, routing by reading shard-heads. Nothing agrees on them,
 * and nothing needs to, because safety comes from shard-head CAS rather than from a shared view.
 *
 * <p>Prefixes are {@link BlobPath} segments rather than slashes inside a blob name, because a container
 * is the unit a backend can list: {@code FsBlobContainer} resolves names flatly and globs within one
 * directory, so a name containing a slash would neither write nor list. The logical layout above is
 * unchanged; only who holds the separator moves.
 */
public final class RegisterMap {

    /** Separator between index name and shard number in a head's blob name. Invalid in index names. */
    public static final String SHARD_SEPARATOR = "#";

    private RegisterMap() {}

    /**
     * Returns the container path holding index descriptors.
     *
     * @param base the deployment's base path
     * @return the descriptors container path
     */
    public static BlobPath indices(BlobPath base) {
        return base.add("indices");
    }

    /**
     * Returns the container path holding shard-heads.
     *
     * @param base the deployment's base path
     * @return the shard-heads container path
     */
    public static BlobPath shards(BlobPath base) {
        return base.add("shards");
    }

    /**
     * Returns the container path holding node leases.
     *
     * @param base the deployment's base path
     * @return the members container path
     */
    public static BlobPath members(BlobPath base) {
        return base.add("cluster").add("members");
    }

    /**
     * Returns the container path holding one shard's published segments.
     *
     * <p>A container per shard rather than a shared one with prefixed names: segment files are listed
     * and deleted per shard, and giving each its own container keeps both operations a directory scan
     * rather than a filter over everything.
     *
     * @param base the deployment's base path
     * @param indexName the index
     * @param shardId the shard number
     * @return the shard data container path
     */
    public static BlobPath shardData(BlobPath base, String indexName, int shardId) {
        return base.add("segments").add(indexName + SHARD_SEPARATOR + shardId);
    }

    /**
     * Returns the blob name of an index descriptor within the descriptors container.
     *
     * @param indexName the index
     * @return the blob name
     */
    public static String descriptorBlob(String indexName) {
        return indexName;
    }

    /**
     * Returns the blob name of a shard-head within the shard-heads container.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @return the blob name
     */
    public static String shardHeadBlob(String indexName, int shardId) {
        return indexName + SHARD_SEPARATOR + shardId;
    }
}
