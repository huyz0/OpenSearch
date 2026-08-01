/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The irreducible per-index facts, small enough to store outside cluster state.
 *
 * <p>Area H's premise is that cluster state should hold no entry per index. The obstacle is that a
 * coordinator receiving a request still has to answer two questions before it can do anything: does this
 * name exist, and what are its bones. This type is that answer, and it is deliberately the smallest thing
 * that suffices rather than a trimmed {@link IndexMetadata}.
 *
 * <p><b>Why these fields and no others.</b> The set is derived from what placement actually reads, which
 * was audited rather than guessed: {@code ComputedRoutingTable} uses the index (name and uuid), the shard
 * count, and the search-only replica count, and {@code ComputedPlacementGate} reads whether the index is
 * serverless. State and aliases are here because resolution needs them to answer without materializing
 * anything, and a resolver that had to load full metadata to decide whether an index is closed would
 * defeat the point.
 *
 * <p>Mappings and settings are deliberately absent. They live in the object store under a convention and
 * are fetched on demand, because they are the large part and the part a coordinator usually does not
 * need. A descriptor is around 200 bytes, so a hundred million of them is a twenty gigabyte index rather
 * than seventy gigabytes of heap on every cluster-manager-eligible node.
 *
 * <p>Immutable, and comparable by value, so two nodes resolving the same name agree by construction.
 */
public final class IndexDescriptor implements Writeable, ToXContentObject {

    /** What a descriptor says about an index that is no longer there. */
    public enum State {
        OPEN,
        CLOSE,
        /**
         * Deleted, and remembered rather than forgotten.
         *
         * <p>This is what replaces {@link IndexGraveyard}. A node adopting local shard data must consult
         * the descriptor and delete that data when it finds this, which is what stops a node partitioned
         * during a delete from resurrecting the index. The graveyard keeps a bounded list and forgets
         * older deletions; a tombstoned descriptor does not.
         */
        DELETED
    }

    private final String name;
    private final String uuid;
    private final int shardCount;
    private final int searchOnlyReplicaCount;
    private final boolean serverless;
    private final State state;
    private final List<String> aliases;
    private final long createdVersion;
    private final boolean system;
    private final boolean hidden;
    private final boolean remoteSnapshot;
    private final boolean warm;

    /**
     * The generation of this index's mapping object, which is what makes a cached mapping checkable.
     *
     * <p>H.7's read half in one field. A coordinator already fetches the descriptor to route, so carrying
     * the generation here means that same fetch says whether its cached mapping is stale. Without it every
     * request would need a second lookup, or an invalidation protocol, to answer a question the routing
     * fetch could have answered for free.
     *
     * <p>Advances only when the mapping actually changes. A generation that moved on every write would
     * invalidate every cache continuously, and one that never moved would serve stale field types forever.
     */
    private final long mappingGeneration;

    /**
     * When the index was created, which pagination orders by.
     *
     * <p>Carried because {@code IndexPaginationStrategy} orders by creation date then name, and a gated
     * index that cannot express that ordering cannot appear in a page at all (H16). Unlike the
     * suspended-shard field this replaced, it is written at creation from {@link IndexMetadata}, so it is
     * not another wire field with no producer.
     */
    private final long creationDate;

    public IndexDescriptor(
        String name,
        String uuid,
        int shardCount,
        int searchOnlyReplicaCount,
        boolean serverless,
        State state,
        List<String> aliases,
        long createdVersion,
        boolean system,
        boolean hidden,
        boolean remoteSnapshot,
        boolean warm,
        long mappingGeneration,
        long creationDate
    ) {
        this.name = Objects.requireNonNull(name, "descriptor needs a name");
        this.uuid = Objects.requireNonNull(uuid, "descriptor needs a uuid, since placement hashes it");
        this.shardCount = shardCount;
        this.searchOnlyReplicaCount = searchOnlyReplicaCount;
        this.serverless = serverless;
        this.state = Objects.requireNonNull(state, "descriptor needs a state");
        this.aliases = List.copyOf(aliases);
        this.createdVersion = createdVersion;
        this.system = system;
        this.hidden = hidden;
        this.remoteSnapshot = remoteSnapshot;
        this.warm = warm;
        this.mappingGeneration = mappingGeneration;
        this.creationDate = creationDate;
    }

    /**
     * Derives a descriptor from full metadata, which is how the dual-write phase keeps the two in step.
     *
     * <p>Deriving rather than constructing separately is what makes H2c's comparison meaningful: if the
     * descriptor were built from different inputs, agreement between the two resolution paths would prove
     * only that both were built from the same mistake.
     */
    public static IndexDescriptor from(IndexMetadata indexMetadata) {
        return new IndexDescriptor(
            indexMetadata.getIndex().getName(),
            indexMetadata.getIndexUUID(),
            indexMetadata.getNumberOfShards(),
            indexMetadata.getNumberOfSearchOnlyReplicas(),
            indexMetadata.getSettings().getAsBoolean("index.serverless_storage.enabled", false),
            indexMetadata.getState() == IndexMetadata.State.CLOSE ? State.CLOSE : State.OPEN,
            List.copyOf(indexMetadata.getAliases().keySet()),
            indexMetadata.getCreationVersion().id,
            indexMetadata.isSystem(),
            indexMetadata.isHidden(),
            indexMetadata.isRemoteSnapshot(),
            indexMetadata.isWarmIndex(),
            0L,
            indexMetadata.getCreationDate()
        );
    }

    public IndexDescriptor(StreamInput in) throws IOException {
        this.name = in.readString();
        this.uuid = in.readString();
        this.shardCount = in.readVInt();
        this.searchOnlyReplicaCount = in.readVInt();
        this.serverless = in.readBoolean();
        this.state = State.values()[in.readVInt()];
        this.aliases = List.copyOf(in.readStringList());
        this.createdVersion = in.readVLong();
        this.system = in.readBoolean();
        this.hidden = in.readBoolean();
        this.remoteSnapshot = in.readBoolean();
        this.warm = in.readBoolean();
        this.mappingGeneration = in.readVLong();
        this.creationDate = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(uuid);
        out.writeVInt(shardCount);
        out.writeVInt(searchOnlyReplicaCount);
        out.writeBoolean(serverless);
        out.writeVInt(state.ordinal());
        out.writeStringCollection(aliases);
        out.writeVLong(createdVersion);
        out.writeBoolean(system);
        out.writeBoolean(hidden);
        out.writeBoolean(remoteSnapshot);
        out.writeBoolean(warm);
        out.writeVLong(mappingGeneration);
        out.writeLong(creationDate);
    }

    /** The index, which is what placement hashes and what every shard id is built from. */
    public Index index() {
        return new Index(name, uuid);
    }

    public String name() {
        return name;
    }

    public String uuid() {
        return uuid;
    }

    public int shardCount() {
        return shardCount;
    }

    public int searchOnlyReplicaCount() {
        return searchOnlyReplicaCount;
    }

    public boolean serverless() {
        return serverless;
    }

    public State state() {
        return state;
    }

    /** Whether this index still exists, as opposed to being remembered so its data can be reclaimed. */
    public boolean exists() {
        return state != State.DELETED;
    }

    public List<String> aliases() {
        return Collections.unmodifiableList(aliases);
    }

    public long createdVersion() {
        return createdVersion;
    }

    /**
     * The four flags resolution needs and placement does not, taken from {@link LazyIndexMetadata}'s
     * field set rather than chosen independently.
     *
     * <p>This is a correction. H2a audited what <em>placement</em> reads and concluded the descriptor was
     * sufficient, which was true of routing and false of resolution. Wildcard expansion consults hidden
     * and system to decide what an expression may match, so a descriptor without them would silently
     * include indices that should be excluded, and that is a security-relevant difference for system
     * indices rather than only a correctness one.
     *
     * <p>Area A had already worked this out: {@code LazyIndexMetadata}, the holder that exists so an
     * index can be described without materializing it, carries exactly these four. Matching its field set
     * is deliberate.
     */
    public boolean system() {
        return system;
    }

    public boolean hidden() {
        return hidden;
    }

    public boolean remoteSnapshot() {
        return remoteSnapshot;
    }

    public boolean warm() {
        return warm;
    }

    /** The generation of this index's mapping object. */
    public long mappingGeneration() {
        return mappingGeneration;
    }

    /** When the index was created, which is what pagination orders by. */
    public long creationDate() {
        return creationDate;
    }

    private IndexDescriptor copyWith(long generation) {
        return new IndexDescriptor(
            name,
            uuid,
            shardCount,
            searchOnlyReplicaCount,
            serverless,
            state,
            aliases,
            createdVersion,
            system,
            hidden,
            remoteSnapshot,
            warm,
            generation,
            creationDate
        );
    }

    /**
     * This descriptor as {@link IndexMetadata}, for the paths that still speak that language.
     *
     * <p>Placement is one of them. {@code ComputedPlacementGate.ownsIndex} decides from settings on an
     * {@code IndexMetadata}, and a gated index has none, so P6 measured it owning nothing and being placed
     * nowhere. Synthesising the metadata rather than widening the routing seam keeps the change off a
     * signature that C3 and every routing caller depend on.
     *
     * <p>Deliberately minimal: the shard counts, the uuid and the serverless flag, which is what placement
     * reads. It is not a general-purpose reconstruction and must not become one, because the whole reason
     * the descriptor exists is that full {@code IndexMetadata} is what could not be afforded per index.
     */
    public IndexMetadata toIndexMetadata() {
        int shards = Math.max(1, shardCount);
        IndexMetadata.Builder builder = IndexMetadata.builder(name)
            .settings(
                org.opensearch.common.settings.Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.fromId((int) createdVersion))
                    .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                    .put("index.serverless_storage.enabled", serverless)
                    .build()
            )
            .numberOfShards(shards)
            .numberOfReplicas(0);
        for (int shard = 0; shard < shards; shard++) {
            builder.primaryTerm(shard, FIRST_PRIMARY_TERM);
        }
        return builder.build();
    }

    /**
     * The primary term every shard of a gated index is opened at.
     *
     * <p>Stated rather than left at the default, and the default is the reason. {@code IndexMetadata}
     * initialises absent terms to {@link org.opensearch.index.seqno.SequenceNumbers#UNASSIGNED_PRIMARY_TERM},
     * which is zero and means "no primary has been assigned"; a shard opened at that term is a shard that
     * announces it has no primary, which is not what a computed primary is.
     *
     * <p>Constant because nothing advances it. A published index's term is bumped by the cluster manager
     * each time a new primary is promoted, and a gated index has no cluster manager step to bump it in. That
     * is a real limitation rather than a solved problem: it means two nodes cannot be told apart by term if
     * placement ever moves a primary between them, and it is recorded in the spike results rather than
     * hidden here. What it does buy is agreement, since every node derives the same term from the same
     * descriptor, which is what the coordinator's term check against the data node's shard requires.
     */
    public static final long FIRST_PRIMARY_TERM = 1L;

    /** The same descriptor at a new mapping generation, which is what a mapping update records. */
    public IndexDescriptor withMappingGeneration(long generation) {
        return copyWith(generation);
    }

    /** The same descriptor, tombstoned. Deletion records rather than removes, so absence stays meaningful. */
    public IndexDescriptor tombstoned() {
        return new IndexDescriptor(
            name,
            uuid,
            shardCount,
            searchOnlyReplicaCount,
            serverless,
            State.DELETED,
            aliases,
            createdVersion,
            system,
            hidden,
            remoteSnapshot,
            warm,
            mappingGeneration,
            creationDate
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("uuid", uuid);
        builder.field("shard_count", shardCount);
        builder.field("search_only_replicas", searchOnlyReplicaCount);
        builder.field("serverless", serverless);
        builder.field("state", state.name());
        builder.field("aliases", aliases);
        builder.field("created_version", createdVersion);
        builder.field("system", system);
        builder.field("hidden", hidden);
        builder.field("remote_snapshot", remoteSnapshot);
        builder.field("warm", warm);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof IndexDescriptor == false) {
            return false;
        }
        IndexDescriptor other = (IndexDescriptor) o;
        return shardCount == other.shardCount
            && searchOnlyReplicaCount == other.searchOnlyReplicaCount
            && serverless == other.serverless
            && createdVersion == other.createdVersion
            && name.equals(other.name)
            && uuid.equals(other.uuid)
            && state == other.state
            && aliases.equals(other.aliases)
            && system == other.system
            && hidden == other.hidden
            && remoteSnapshot == other.remoteSnapshot
            && warm == other.warm;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            name,
            uuid,
            shardCount,
            searchOnlyReplicaCount,
            serverless,
            state,
            aliases,
            createdVersion,
            system,
            hidden,
            remoteSnapshot,
            warm
        );
    }

    @Override
    public String toString() {
        return "IndexDescriptor{" + name + "/" + uuid + ", shards=" + shardCount + ", state=" + state + "}";
    }
}
