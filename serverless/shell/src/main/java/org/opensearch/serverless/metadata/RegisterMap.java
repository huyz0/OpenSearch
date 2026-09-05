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
     * Returns the container path holding frozen views of an index.
     *
     * <p>Its own container because the sweep has to list them all, and a listing that also had to step over
     * every index descriptor in the deployment would make the collector's cost depend on the population it
     * is collecting for.
     *
     * @param base the deployment's base path
     * @return the points-in-time container path
     */
    public static BlobPath pointsInTime(BlobPath base) {
        return base.add("pits");
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
     * Returns the container path holding the cluster config register.
     *
     * <p>§9.3's {@code /cluster/config}: one register, operator write rate, CAS on conflict. Siblings
     * {@link #members} under {@code cluster/} rather than sharing its container, because a container is
     * the unit a backend lists and nothing should ever need to list this one alongside node leases.
     *
     * @param base the deployment's base path
     * @return the cluster config container path
     */
    public static BlobPath clusterConfig(BlobPath base) {
        return base.add("cluster").add("config");
    }

    /** The single blob name the cluster config register is stored under. */
    public static final String CLUSTER_CONFIG_BLOB = "settings";

    /**
     * Returns the container path holding repository descriptors.
     *
     * <p>A repository here is a namespace within this deployment's own object store, not a distinct
     * storage backend or bucket — the same object store already backing every register and every shard's
     * segments. Registering one records a name and nothing else; there is nothing to configure that this
     * deployment's own {@code serverless.store.*} settings do not already fix.
     *
     * @param base the deployment's base path
     * @return the repositories container path
     */
    /**
     * Where index templates live: one register per template name.
     *
     * <p>A template is operator-authored configuration, not data, and there are tens of them rather than
     * millions — which is why enumerating them at index creation is affordable where enumerating indices
     * never is. The count is bounded on the way in rather than on the way out, so creating an index can
     * never fail for having too many templates to read.
     *
     * @param base the deployment root
     * @return the index-template container
     */
    public static BlobPath indexTemplates(BlobPath base) {
        return base.add("templates").add("index");
    }

    /**
     * Where component templates live: the reusable fragments an index template composes.
     *
     * @param base the deployment root
     * @return the component-template container
     */
    public static BlobPath componentTemplates(BlobPath base) {
        return base.add("templates").add("component");
    }

    /**
     * Where ingest pipelines live: one register per pipeline id.
     *
     * <p>The same shape as templates, and for the same reason — operator-authored configuration, few in
     * number, read by name. Core keeps pipelines in cluster state; there is none here, so they keep company
     * with every other piece of this deployment's configuration.
     *
     * @param base the deployment root
     * @return the pipeline container
     */
    public static BlobPath pipelines(BlobPath base) {
        return base.add("pipelines");
    }

    /**
     * Where stored scripts live: one register per script id, beside a version marker.
     *
     * <p>Core keeps stored scripts in cluster state and every node's {@code ScriptService} reads them from
     * the state it last applied. There is no cluster state here, so scripts keep company with templates and
     * pipelines, and each node applies the store to its own {@code ScriptService} when the marker moves.
     *
     * @param base the deployment root
     * @return the scripts container
     */
    public static BlobPath scripts(BlobPath base) {
        return base.add("scripts");
    }

    /**
     * Where search pipelines live: one register per pipeline id, the same shape as ingest pipelines.
     *
     * @param base the deployment root
     * @return the search-pipeline container
     */
    public static BlobPath searchPipelines(BlobPath base) {
        return base.add("search_pipelines");
    }

    public static BlobPath repositories(BlobPath base) {
        return base.add("repositories");
    }

    /**
     * Returns the container path holding snapshot records.
     *
     * <p>Its own container, the same reasoning as {@link #pointsInTime}: the collector and the delete
     * path both have to list every live one, and a listing that also stepped over every repository or
     * index descriptor would make their cost depend on a population they have nothing to do with.
     *
     * @param base the deployment's base path
     * @return the snapshots container path
     */
    public static BlobPath snapshots(BlobPath base) {
        return base.add("snapshots");
    }

    /**
     * Returns the blob name one snapshot is stored under: a repository and a name, joined the same way a
     * shard-head joins an index name and a shard number.
     *
     * @param repo the repository
     * @param name the snapshot name
     * @return the blob name
     */
    public static String snapshotKey(String repo, String name) {
        return repo + SHARD_SEPARATOR + name;
    }

    /**
     * Returns the container path a standard (non-shallow) snapshot copies one captured shard's segment
     * files into.
     *
     * <p>A shallow snapshot references a live shard's own blobs directly ({@link #shardData}), which is
     * what makes it free to take and what makes it depend on that shard's own storage continuing to exist.
     * A standard snapshot exists precisely to not depend on that: its bytes are copied here, under the
     * repository and snapshot's own names rather than any live index's, so the snapshot's lifecycle is
     * entirely its own — deleting the index it was taken from, or the garbage collector sweeping that
     * index's shard, never touches this path at all, because neither one lists it.
     *
     * @param base the deployment's base path
     * @param repo the repository
     * @param snapshot the snapshot name
     * @param indexName the captured index's name, at capture time
     * @param shardId the shard number
     * @return the container path
     */
    public static BlobPath snapshotShardData(BlobPath base, String repo, String snapshot, String indexName, int shardId) {
        return base.add("snapshot-data").add(repo + SHARD_SEPARATOR + snapshot).add(indexName + SHARD_SEPARATOR + shardId);
    }

    /**
     * Returns the container path holding one shard's published segments.
     *
     * <p>A container per shard rather than a shared one with prefixed names: segment files are listed
     * and deleted per shard, and giving each its own container keeps both operations a directory scan
     * rather than a filter over everything.
     *
     * <p><b>The index's uuid is in the path, and it is what makes an index's storage its own.</b> Keyed by
     * name alone, an index created with a name that had been used before opened the previous index's commit
     * and served its documents — a caller creating an empty index and being shown data. A uuid is minted
     * per create, so two indices of the same name cannot share a path however the earlier one ended, and
     * whatever the earlier one left behind is unreferenced rather than inherited.
     *
     * <p>The name stays in the path too, in front of the uuid, because an operator looking at a bucket
     * should be able to tell what they are looking at.
     *
     * @param base the deployment's base path
     * @param indexName the index
     * @param uuid the index's uuid, which distinguishes it from any earlier index of the same name
     * @param shardId the shard number
     * @return the shard data container path
     */
    public static BlobPath shardData(BlobPath base, String indexName, String uuid, int shardId) {
        return base.add("segments").add(indexName + SHARD_SEPARATOR + uuid + SHARD_SEPARATOR + shardId);
    }

    /**
     * Returns the container path listing the shards one node owns.
     *
     * <p>Added in phase 9, because measurement showed {@code truthFor} was reading every descriptor in
     * the deployment to find the handful a node hosts: 401 object-store operations at 200 indices and
     * 2,001 at 1,000, on the steady-state path, on every node, forever. That is the O(population) sweep
     * this whole architecture exists to remove, reintroduced in its own code.
     *
     * <p>The listing is a <b>hint, not truth</b>. A shard-head remains the only thing that says who owns
     * a shard; this only narrows the search from "every index" to "the ones this node last claimed", and
     * every entry is verified against its head before being believed.
     *
     * @param base the deployment's base path
     * @param nodeId the node
     * @return the assignments container path
     */
    public static BlobPath assignments(BlobPath base, String nodeId) {
        return base.add("assignments").add(nodeId);
    }

    /**
     * Returns the blob name of an assignment within a node's assignments container.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @return the blob name
     */
    public static String assignmentBlob(String indexName, int shardId) {
        return indexName + SHARD_SEPARATOR + shardId;
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
