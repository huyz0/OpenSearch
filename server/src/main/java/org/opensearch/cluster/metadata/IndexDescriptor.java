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
import java.util.Map;
import java.util.Objects;

/**
 * The irreducible per-index facts, small enough to store outside cluster state.
 *
 * <p>The gated-index premise is that cluster state should hold no entry per index. The obstacle is that a
 * coordinator receiving a request still has to answer two questions before it can do anything: does this
 * name exist, and what are its bones. This type is that answer, and it is deliberately the smallest thing
 * that suffices rather than a trimmed {@link IndexMetadata}.
 *
 * <p><b>Why these fields and no others.</b> The set is derived from what placement actually reads, which
 * was audited rather than guessed: {@code ComputedRoutingTable} uses the index (name and uuid), the shard
 * count, and the search-only replica count, and {@code ComputedPlacementGate} reads whether the index is
 * claimed. State and aliases are here because resolution needs them to answer without materializing
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

    /**
     * The divisor a document's routing hash is taken against, or 0 meaning "the same as the shard count".
     *
     * <p>Separate from {@link #shardCount} because after a split or shrink they diverge, and routing must
     * keep using the original. {@code IndexMetadata} has carried this since forever as
     * {@code index.number_of_routing_shards}; a descriptor that omits it makes every resharded index route
     * against the wrong divisor, which does not fail, it just puts documents on the wrong shard and returns
     * partial results.
     *
     * <p>Zero rather than a copy of the shard count, so that "never resharded" stays distinguishable from
     * "resharded and happens to match", and so a descriptor written before this field existed reads as the
     * former rather than as a coincidence.
     */
    private final int routingNumShards;

    /**
     * How many shards one routing value may spread across, or 0 meaning the default of 1.
     *
     * <p>{@code index.routing_partition_size} is an ordinary index setting a user can set at creation. Left
     * off the descriptor it defaults to 1, {@code isRoutingPartitionedIndex()} answers false, the partition
     * offset is always zero, and every document for a routing value lands on one shard instead of the
     * partition set. The setting is accepted and silently ignored, which is the worst of the three outcomes.
     */
    private final int routingPartitionSize;
    private final boolean claimed;
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
     * <p>The mapping seam's read half in one field. A coordinator already fetches the descriptor to route, so carrying
     * the generation here means that same fetch says whether its cached mapping is stale. Without it every
     * request would need a second lookup, or an invalidation protocol, to answer a question the routing
     * fetch could have answered for free.
     *
     * <p>Advances only when the mapping actually changes. A generation that moved on every write would
     * invalidate every cache continuously, and one that never moved would serve stale field types forever.
     */
    /**
     * When this descriptor was tombstoned, epoch millis, or {@code 0} if it is not a tombstone.
     *
     * <p>A tombstone is the one descriptor that is written once and then has to survive on its own for a
     * retention window, so it is the one that needs to be able to say how old it is. Live descriptors are
     * rewritten whenever anything about the index changes and can always be re-derived; a tombstone has
     * nothing behind it, which is the same property that makes losing one a resurrection.
     *
     * <p>Zero means "not a tombstone, or written before this field existed", and those two are deliberately
     * not distinguished. Both must be read as an unknown age, and an unknown age must never be treated as
     * old enough to reclaim.
     */
    private final long deletedAtMillis;

    private final long mappingGeneration;

    /**
     * When the index was created, which pagination orders by.
     *
     * <p>Carried because {@code IndexPaginationStrategy} orders by creation date then name, and a gated
     * index that cannot express that ordering cannot appear in a page at all. Unlike the
     * suspended-shard field this replaced, it is written at creation from {@link IndexMetadata}, so it is
     * not another wire field with no producer.
     */
    private final long creationDate;

    /**
     * Top-level field definitions embedded directly in the descriptor.
     */
    private final Map<String, Object> initialMapping;

    /**
     * A descriptor with default routing geometry, which is what an index that has never been resharded and
     * sets no partition size has. Kept so the many callers that mean exactly that do not have to say so.
     */
    public IndexDescriptor(
        String name,
        String uuid,
        int shardCount,
        int searchOnlyReplicaCount,
        boolean claimed,
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
        this(
            name,
            uuid,
            shardCount,
            searchOnlyReplicaCount,
            claimed,
            state,
            aliases,
            createdVersion,
            system,
            hidden,
            remoteSnapshot,
            warm,
            mappingGeneration,
            creationDate,
            0,
            0
        );
    }

    public IndexDescriptor(
        String name,
        String uuid,
        int shardCount,
        int searchOnlyReplicaCount,
        boolean claimed,
        State state,
        List<String> aliases,
        long createdVersion,
        boolean system,
        boolean hidden,
        boolean remoteSnapshot,
        boolean warm,
        long mappingGeneration,
        long creationDate,
        int routingNumShards,
        int routingPartitionSize
    ) {
        this(
            name,
            uuid,
            shardCount,
            searchOnlyReplicaCount,
            claimed,
            state,
            aliases,
            createdVersion,
            system,
            hidden,
            remoteSnapshot,
            warm,
            mappingGeneration,
            creationDate,
            routingNumShards,
            routingPartitionSize,
            0L
        );
    }

    /** The form that also carries a tombstone's age. See {@link #deletedAtMillis}. */
    public IndexDescriptor(
        String name,
        String uuid,
        int shardCount,
        int searchOnlyReplicaCount,
        boolean claimed,
        State state,
        List<String> aliases,
        long createdVersion,
        boolean system,
        boolean hidden,
        boolean remoteSnapshot,
        boolean warm,
        long mappingGeneration,
        long creationDate,
        int routingNumShards,
        int routingPartitionSize,
        long deletedAtMillis
    ) {
        this(
            name,
            uuid,
            shardCount,
            searchOnlyReplicaCount,
            claimed,
            state,
            aliases,
            createdVersion,
            system,
            hidden,
            remoteSnapshot,
            warm,
            mappingGeneration,
            creationDate,
            routingNumShards,
            routingPartitionSize,
            deletedAtMillis,
            null
        );
    }

    public IndexDescriptor(
        String name,
        String uuid,
        int shardCount,
        int searchOnlyReplicaCount,
        boolean claimed,
        State state,
        List<String> aliases,
        long createdVersion,
        boolean system,
        boolean hidden,
        boolean remoteSnapshot,
        boolean warm,
        long mappingGeneration,
        long creationDate,
        int routingNumShards,
        int routingPartitionSize,
        long deletedAtMillis,
        Map<String, Object> initialMapping
    ) {
        this.deletedAtMillis = deletedAtMillis;
        this.routingNumShards = routingNumShards;
        this.routingPartitionSize = routingPartitionSize;
        this.name = Objects.requireNonNull(name, "descriptor needs a name");
        this.uuid = Objects.requireNonNull(uuid, "descriptor needs a uuid, since placement hashes it");
        this.shardCount = shardCount;
        this.searchOnlyReplicaCount = searchOnlyReplicaCount;
        this.claimed = claimed;
        this.state = Objects.requireNonNull(state, "descriptor needs a state");
        this.aliases = List.copyOf(aliases);
        this.createdVersion = createdVersion;
        this.system = system;
        this.hidden = hidden;
        this.remoteSnapshot = remoteSnapshot;
        this.warm = warm;
        this.mappingGeneration = mappingGeneration;
        this.creationDate = creationDate;
        this.initialMapping = initialMapping != null ? Map.copyOf(initialMapping) : Map.of();
    }

    /**
     * Derives a descriptor from full metadata, including initial mapping fields.
     */
    public static IndexDescriptor from(IndexMetadata indexMetadata) {
        Map<String, Object> declaredFields = DescriptorRepresentable.fieldDefinitionsOrNull(indexMetadata);
        long gen = (declaredFields != null && declaredFields.isEmpty() == false) ? 1L : 0L;
        String claimedKey = IndexCreationStrategyRegistry.claimedIndexSettingKey();
        return new IndexDescriptor(
            indexMetadata.getIndex().getName(),
            indexMetadata.getIndexUUID(),
            indexMetadata.getNumberOfShards(),
            indexMetadata.getNumberOfSearchOnlyReplicas(),
            claimedKey != null && indexMetadata.getSettings().getAsBoolean(claimedKey, false),
            indexMetadata.getState() == IndexMetadata.State.CLOSE ? State.CLOSE : State.OPEN,
            List.copyOf(indexMetadata.getAliases().keySet()),
            indexMetadata.getCreationVersion().id,
            indexMetadata.isSystem(),
            indexMetadata.isHidden(),
            indexMetadata.isRemoteSnapshot(),
            indexMetadata.isWarmIndex(),
            gen,
            indexMetadata.getCreationDate(),
            indexMetadata.getRoutingNumShards(),
            indexMetadata.getRoutingPartitionSize(),
            0L,
            declaredFields
        );
    }

    @SuppressWarnings("unchecked")
    public IndexDescriptor(StreamInput in) throws IOException {
        this.name = in.readString();
        this.uuid = in.readString();
        this.shardCount = in.readVInt();
        this.searchOnlyReplicaCount = in.readVInt();
        this.claimed = in.readBoolean();
        this.state = State.values()[in.readVInt()];
        this.aliases = List.copyOf(in.readStringList());
        this.createdVersion = in.readVLong();
        this.system = in.readBoolean();
        this.hidden = in.readBoolean();
        this.remoteSnapshot = in.readBoolean();
        this.warm = in.readBoolean();
        this.mappingGeneration = in.readVLong();
        this.creationDate = in.readLong();
        this.routingNumShards = in.readVInt();
        this.routingPartitionSize = in.readVInt();
        this.deletedAtMillis = in.readLong();
        if (in.readBoolean()) {
            Map<String, Object> map = (Map<String, Object>) in.readGenericValue();
            this.initialMapping = map != null ? Map.copyOf(map) : Map.of();
        } else {
            this.initialMapping = Map.of();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(uuid);
        out.writeVInt(shardCount);
        out.writeVInt(searchOnlyReplicaCount);
        out.writeBoolean(claimed);
        out.writeVInt(state.ordinal());
        out.writeStringCollection(aliases);
        out.writeVLong(createdVersion);
        out.writeBoolean(system);
        out.writeBoolean(hidden);
        out.writeBoolean(remoteSnapshot);
        out.writeBoolean(warm);
        out.writeVLong(mappingGeneration);
        out.writeLong(creationDate);
        out.writeVInt(routingNumShards);
        out.writeVInt(routingPartitionSize);
        out.writeLong(deletedAtMillis);
        if (initialMapping == null || initialMapping.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeGenericValue(initialMapping);
        }
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

    /** The stored value, 0 when unset. Most callers want {@link #effectiveRoutingNumShards}. */
    public int routingNumShards() {
        return routingNumShards;
    }

    /** The stored value, 0 when unset. Most callers want {@link #effectiveRoutingPartitionSize}. */
    public int routingPartitionSize() {
        return routingPartitionSize;
    }

    /**
     * The divisor routing must actually use, resolving the unset sentinel the way IndexMetadata does.
     *
     * <p>Note the direction, which is easy to get backwards and was: this is the <em>pre-split</em> shard
     * space, so it is greater than or equal to the shard count and a whole multiple of it. Splitting grows
     * the shard count toward it and leaves this alone, which is what keeps documents routing to the same
     * place across a split. {@code IndexMetadata} asserts the multiple, so a stored value that violates it
     * fails loudly at construction rather than misrouting quietly.
     */
    public int effectiveRoutingNumShards() {
        int shards = Math.max(1, shardCount);
        return routingNumShards >= shards ? routingNumShards : shards;
    }

    /** The partition size routing must actually use, where 1 means not partitioned. */
    public int effectiveRoutingPartitionSize() {
        return routingPartitionSize > 0 ? routingPartitionSize : 1;
    }

    public int searchOnlyReplicaCount() {
        return searchOnlyReplicaCount;
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
     * <p>This is a correction. The original audit covered what <em>placement</em> reads and concluded the
     * descriptor was
     * sufficient, which was true of routing and false of resolution. Wildcard expansion consults hidden
     * and system to decide what an expression may match, so a descriptor without them would silently
     * include indices that should be excluded, and that is a security-relevant difference for system
     * indices rather than only a correctness one.
     *
     * <p>{@code LazyIndexMetadata} had already worked this out: the holder that exists so an
     * index can be described without materializing it carries exactly these four. Matching its field set
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

    /** Top-level field definitions embedded directly in the descriptor. */
    public Map<String, Object> initialMapping() {
        return initialMapping;
    }

    /** See the field's own javadoc: {@code 0} means unknown age and must never be read as old. */
    public long deletedAtMillis() {
        return deletedAtMillis;
    }

    private IndexDescriptor copyWith(long generation) {
        return new IndexDescriptor(
            name,
            uuid,
            shardCount,
            searchOnlyReplicaCount,
            claimed,
            state,
            aliases,
            createdVersion,
            system,
            hidden,
            remoteSnapshot,
            warm,
            generation,
            creationDate,
            routingNumShards,
            routingPartitionSize,
            deletedAtMillis,
            initialMapping
        );
    }

    public IndexDescriptor withState(State newState) {
        return new IndexDescriptor(
            name,
            uuid,
            shardCount,
            searchOnlyReplicaCount,
            claimed,
            newState,
            aliases,
            createdVersion,
            system,
            hidden,
            remoteSnapshot,
            warm,
            mappingGeneration,
            creationDate,
            routingNumShards,
            routingPartitionSize,
            deletedAtMillis,
            initialMapping
        );
    }

    public IndexDescriptor withAliases(List<String> newAliases) {
        return new IndexDescriptor(
            name,
            uuid,
            shardCount,
            searchOnlyReplicaCount,
            claimed,
            state,
            newAliases != null ? List.copyOf(newAliases) : List.of(),
            createdVersion,
            system,
            hidden,
            remoteSnapshot,
            warm,
            mappingGeneration,
            creationDate,
            routingNumShards,
            routingPartitionSize,
            deletedAtMillis,
            initialMapping
        );
    }

    public IndexDescriptor withMapping(long generation, Map<String, Object> newMapping) {
        return new IndexDescriptor(
            name,
            uuid,
            shardCount,
            searchOnlyReplicaCount,
            claimed,
            state,
            aliases,
            createdVersion,
            system,
            hidden,
            remoteSnapshot,
            warm,
            generation,
            creationDate,
            routingNumShards,
            routingPartitionSize,
            deletedAtMillis,
            newMapping
        );
    }

    /**
     * This descriptor as {@link IndexMetadata}, for the paths that still speak that language.
     *
     * <p>Placement is one of them. {@code ComputedPlacementGate.ownsIndex} decides from settings on an
     * {@code IndexMetadata}, and a gated index has none, so it was measured owning nothing and being placed
     * nowhere. Synthesising the metadata rather than widening the routing seam keeps the change off a
     * signature that every routing caller depends on.
     *
     * <p>Deliberately minimal: the shard counts, the uuid and the claimed flag, which is what placement
     * reads. It is not a general-purpose reconstruction and must not become one, because the whole reason
     * the descriptor exists is that full {@code IndexMetadata} is what could not be afforded per index.
     */
    public IndexMetadata toIndexMetadata() {
        int shards = Math.max(1, shardCount);
        org.opensearch.common.settings.Settings.Builder settingsBuilder = org.opensearch.common.settings.Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.fromId((int) createdVersion))
            .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
            // routing_partition_size is a plain setting and is read back from one. The routing
            // shard count is not: IndexMetadata holds it as a field and derives routingFactor from
            // it, so it has to go through the builder below or it is silently ignored.
            .put(IndexMetadata.SETTING_ROUTING_PARTITION_SIZE, effectiveRoutingPartitionSize());
        String claimedKey = IndexCreationStrategyRegistry.claimedIndexSettingKey();
        if (claimedKey != null) {
            settingsBuilder.put(claimedKey, claimed);
        }
        IndexMetadata.Builder builder = IndexMetadata.builder(name)
            .settings(settingsBuilder.build())
            .numberOfShards(shards)
            .numberOfReplicas(0)
            // The state, which this dropped until 2026-08-15 -- and dropping it made close a label rather
            // than an operation. MetadataIndexStateService's gated path writes descriptor.withState(CLOSE)
            // and answers acknowledged; every reader then synthesised metadata that said OPEN, because a
            // builder left alone defaults to it. A closed gated index went on accepting writes and searches
            // while an ordinary one refused both with IndexClosedException, and the test that covered close
            // asserted the descriptor's own state rather than what the index did afterwards, so nothing saw
            // it. An index that reports closed and keeps taking writes is worse than one that cannot close.
            .state(state == State.CLOSE ? IndexMetadata.State.CLOSE : IndexMetadata.State.OPEN);
        // The divisor a document's routing hash is taken against. Left unset, IndexMetadata uses the shard
        // count, which is right for an index that has never been resharded and wrong, silently, for one
        // that has: routingFactor comes out as 1 and every document routes to the wrong shard.
        builder.setRoutingNumShards(effectiveRoutingNumShards());
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

    /** Compact stream serialization for Object Storage blobs. */
    public void writeCompact(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(uuid);
        out.writeVInt(shardCount);
        out.writeVInt(searchOnlyReplicaCount);
        out.writeBoolean(claimed);
        out.writeEnum(state);
        out.writeVLong(createdVersion);
        out.writeBoolean(system);
        out.writeBoolean(hidden);
        out.writeBoolean(remoteSnapshot);
        out.writeBoolean(warm);
        out.writeVLong(mappingGeneration);
        out.writeVLong(creationDate);
        out.writeVInt(routingNumShards);
        out.writeVInt(routingPartitionSize);
        out.writeVLong(deletedAtMillis);
    }

    public static IndexDescriptor readCompact(StreamInput in) throws IOException {
        String name = in.readString();
        String uuid = in.readString();
        int shardCount = in.readVInt();
        int searchOnlyReplicaCount = in.readVInt();
        boolean claimed = in.readBoolean();
        State state = in.readEnum(State.class);
        long createdVersion = in.readVLong();
        boolean system = in.readBoolean();
        boolean hidden = in.readBoolean();
        boolean remoteSnapshot = in.readBoolean();
        boolean warm = in.readBoolean();
        long mappingGeneration = in.readVLong();
        long creationDate = in.readVLong();
        int routingNumShards = in.readVInt();
        int routingPartitionSize = in.readVInt();
        long deletedAtMillis = in.readVLong();

        return new IndexDescriptor(
            name,
            uuid,
            shardCount,
            searchOnlyReplicaCount,
            claimed,
            state,
            List.of(),
            createdVersion,
            system,
            hidden,
            remoteSnapshot,
            warm,
            mappingGeneration,
            creationDate,
            routingNumShards,
            routingPartitionSize,
            deletedAtMillis,
            null
        );
    }

    /** The same descriptor at a new mapping generation, which is what a mapping update records. */
    public IndexDescriptor withMappingGeneration(long generation) {
        return copyWith(generation);
    }

    /** The same descriptor, tombstoned. Deletion records rather than removes, so absence stays meaningful. */
    public IndexDescriptor tombstoned() {
        return tombstoned(0L);
    }

    /**
     * The same descriptor, tombstoned, and recording when.
     *
     * <p>The timestamp is what lets a tombstone be reclaimed later without a separate index saying it is
     * safe. Reclamation reads the tombstone's own age rather than trusting whatever worklist pointed at it,
     * so a corrupt or replayed worklist still cannot delete a young tombstone.
     *
     * <p>{@code 0} keeps the meaning it has on the field: unknown age, never old enough to reclaim.
     */
    public IndexDescriptor tombstoned(long deletedAtMillis) {
        return new IndexDescriptor(
            name,
            uuid,
            shardCount,
            searchOnlyReplicaCount,
            claimed,
            State.DELETED,
            aliases,
            createdVersion,
            system,
            hidden,
            remoteSnapshot,
            warm,
            mappingGeneration,
            creationDate,
            routingNumShards,
            routingPartitionSize,
            deletedAtMillis,
            initialMapping
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("uuid", uuid);
        builder.field("shard_count", shardCount);
        builder.field("search_only_replicas", searchOnlyReplicaCount);
        builder.field("claimed", claimed);
        builder.field("state", state.name());
        builder.field("aliases", aliases);
        builder.field("created_version", createdVersion);
        builder.field("system", system);
        builder.field("hidden", hidden);
        builder.field("remote_snapshot", remoteSnapshot);
        builder.field("warm", warm);
        if (initialMapping != null && initialMapping.isEmpty() == false) {
            builder.field("initial_mapping", initialMapping);
        }
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
            && claimed == other.claimed
            && createdVersion == other.createdVersion
            && name.equals(other.name)
            && uuid.equals(other.uuid)
            && state == other.state
            && aliases.equals(other.aliases)
            && system == other.system
            && hidden == other.hidden
            && remoteSnapshot == other.remoteSnapshot
            && warm == other.warm
            && Objects.equals(initialMapping, other.initialMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            name,
            uuid,
            shardCount,
            searchOnlyReplicaCount,
            claimed,
            state,
            aliases,
            createdVersion,
            system,
            hidden,
            remoteSnapshot,
            warm,
            initialMapping
        );
    }

    @Override
    public String toString() {
        return "IndexDescriptor{" + name + "/" + uuid + ", shards=" + shardCount + ", state=" + state + "}";
    }
}
