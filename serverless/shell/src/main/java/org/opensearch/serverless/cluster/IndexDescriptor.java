/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.cluster;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.indices.replication.common.ReplicationType;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The truth record for one index.
 *
 * <p>Stored as a blob in the object store, addressed by name and created with a compare-and-swap on a
 * register. It is also usable as a plain in-memory value, so the projection and reconciliation paths
 * can be tested without a store: the shape is the contract, not where it lives.
 *
 * <p>Note what is <em>not</em> here: no routing table, no allocation, no in-sync set. Those are not
 * properties of an index, they are properties of who happens to be serving it, and in this design they
 * come from shard-heads rather than from a descriptor.
 */
public final class IndexDescriptor {

    /**
     * The most shards one index may have.
     *
     * <p>This is a design constraint, not a tuning knob. The shard list is <b>self-contained here</b>:
     * every caller derives a shard's identity from {@link #numberOfShards()} rather than by enumerating
     * anything, so opening, collecting or describing an index's shards costs one descriptor read. That
     * only stays true while the list is small enough to sit inside a single object that a
     * compare-and-swap can replace atomically.
     *
     * <p>A few thousand is the right order: it is more shards than any single index needs, and it keeps
     * the descriptor to a size where lifecycle changes are one CAS rather than a distributed
     * transaction. An index that genuinely needs more shards wants more indices.
     */
    public static final int MAX_SHARDS = 4096;

    private final String name;
    private final String uuid;
    private final int numberOfShards;
    private final String mapping;
    private final long mappingVersion;
    private final long settingsVersion;
    private final Settings extraSettings;
    private final long createdAtMillis;
    private final int createdVersionId;
    private final List<String> aliasedBy;
    private final org.opensearch.serverless.routing.UnitLayout layout;
    /** The bucket this index's data lives in, or null for an index on units of its own. */
    private final String bucket;
    /** The live unit ids, sorted; null for an index without a layout, whose shards are all live. */
    private final int[] unitIds;

    /**
     * This descriptor rendered for routing, built once on demand.
     *
     * <p>Routing asks per document -- a bulk of ten thousand asks ten thousand times -- and rendering
     * parses the mapping, so the answer has to be remembered somewhere. Here, on the descriptor, rather
     * than in a map keyed by uuid: a descriptor is immutable and is replaced whenever the index changes,
     * so the memo cannot go stale and dies when the descriptor does. A static cache was tried first and
     * is why this comment exists -- it pinned an {@code IndexMetadata} per index for the life of the JVM,
     * which in a test run holding many nodes at once was enough to turn two suites into GC thrash and an
     * eight-hundred-second timeout.
     *
     * <p>Not volatile, and it does not need to be: the value is derived purely from final fields, so two
     * threads racing produce equal objects and either is correct. A torn read is impossible for a
     * reference.
     */
    private IndexMetadata routingMetadata;

    /**
     * Creates a descriptor.
     *
     * @param name the index name
     * @param uuid a stable uuid; the data plane rejects {@code _na_}
     * @param numberOfShards shard count
     * @param mapping the mapping source, or null for none
     * @param extraSettings settings layered over the defaults, or null
     */
    public IndexDescriptor(String name, String uuid, int numberOfShards, String mapping, Settings extraSettings) {
        this(name, uuid, numberOfShards, mapping, extraSettings, 1L);
    }

    /**
     * Creates a descriptor at a known mapping version.
     *
     * <p><b>Why a mapping needs a version here.</b> Core decides whether to re-apply a mapping to an open
     * shard by comparing {@code IndexMetadata#getMappingVersion} — an unchanged version means "nothing to do"
     * and the new source is ignored. A shell that edits the mapping and leaves the version alone would write
     * the change to the object store, hand it to every node, and have every node decline to apply it. The
     * version is the descriptor's, incremented on each accepted update, because the descriptor is the only
     * thing here that knows an update happened.
     *
     * @param name the index name
     * @param uuid a stable uuid; the data plane rejects {@code _na_}
     * @param numberOfShards shard count
     * @param mapping the mapping source, or null for none
     * @param extraSettings settings layered over the defaults, or null
     * @param mappingVersion increments on every accepted mapping change
     */
    public IndexDescriptor(String name, String uuid, int numberOfShards, String mapping, Settings extraSettings, long mappingVersion) {
        this(name, uuid, numberOfShards, mapping, extraSettings, mappingVersion, 1L);
    }

    /**
     * Creates a descriptor at known mapping and settings versions.
     *
     * <p>Settings need a version for the same reason mappings do, and it is a different reason from
     * bookkeeping: {@code IndexService#updateMetadata} decides whether to push a settings change into an
     * open shard by comparing {@code IndexMetadata#getSettingsVersion}, and asserts the new one is strictly
     * greater. A shell that changed settings and left the version alone would write the change to the object
     * store and have every node decline to apply it -- the same failure mapping updates would have had.
     *
     * @param name the index name
     * @param uuid a stable uuid; the data plane rejects {@code _na_}
     * @param numberOfShards shard count
     * @param mapping the mapping source, or null for none
     * @param extraSettings settings layered over the defaults, or null
     * @param mappingVersion increments on every accepted mapping change
     * @param settingsVersion increments on every accepted settings change
     */
    public IndexDescriptor(
        String name,
        String uuid,
        int numberOfShards,
        String mapping,
        Settings extraSettings,
        long mappingVersion,
        long settingsVersion
    ) {
        this(name, uuid, numberOfShards, mapping, extraSettings, mappingVersion, settingsVersion, 0L, 0, List.of());
    }

    /**
     * Creates a descriptor carrying when and by what it was created, and which aliases name it.
     *
     * @param name the index name
     * @param uuid the index uuid
     * @param numberOfShards the shard count
     * @param mapping the mapping, or null
     * @param extraSettings the settings beyond the shard count
     * @param mappingVersion how many times the mapping has changed
     * @param settingsVersion how many times the settings have changed
     * @param createdAtMillis when the index was created, or 0 when not recorded
     * @param createdVersionId the id of the OpenSearch version that created it, or 0 when not recorded
     * @param aliasedBy the aliases believed to name this index; verified against each alias record on read
     */
    public IndexDescriptor(
        String name,
        String uuid,
        int numberOfShards,
        String mapping,
        Settings extraSettings,
        long mappingVersion,
        long settingsVersion,
        long createdAtMillis,
        int createdVersionId,
        List<String> aliasedBy
    ) {
        this(
            name,
            uuid,
            numberOfShards,
            mapping,
            extraSettings,
            mappingVersion,
            settingsVersion,
            createdAtMillis,
            createdVersionId,
            aliasedBy,
            null
        );
    }

    /**
     * Creates a descriptor with a unit layout: an index created with a shard mode.
     *
     * <p>{@code numberOfShards} is then the number of physical shard <em>slots</em>: the layout's next
     * unit id, so every unit the index has ever had, live or retired by a split, is a shard below it.
     * Housekeeping that must reach every slot (purging, reclaiming, releasing) keeps enumerating
     * {@code 0..numberOfShards-1}; serving (search fan-out, health, catalog, points in time) enumerates
     * {@link #unitIds()}, the live units only. The layout's declared count is what the user asked for. In
     * fixed mode all three are equal; an adaptive index starts as one unit whatever it declared.
     *
     * @param name the index name
     * @param uuid the index uuid
     * @param numberOfShards the physical shard slots; with a layout, its next unit id
     * @param mapping the mapping, or null
     * @param extraSettings the settings beyond the shard count
     * @param mappingVersion how many times the mapping has changed
     * @param settingsVersion how many times the settings have changed
     * @param createdAtMillis when the index was created, or 0 when not recorded
     * @param createdVersionId the id of the OpenSearch version that created it, or 0 when not recorded
     * @param aliasedBy the aliases believed to name this index
     * @param layout the range map from routing hashes to units, or null for an index without a shard mode
     */
    public IndexDescriptor(
        String name,
        String uuid,
        int numberOfShards,
        String mapping,
        Settings extraSettings,
        long mappingVersion,
        long settingsVersion,
        long createdAtMillis,
        int createdVersionId,
        List<String> aliasedBy,
        org.opensearch.serverless.routing.UnitLayout layout
    ) {
        this(
            name,
            uuid,
            numberOfShards,
            mapping,
            extraSettings,
            mappingVersion,
            settingsVersion,
            createdAtMillis,
            createdVersionId,
            aliasedBy,
            layout,
            null
        );
    }

    /**
     * Creates a descriptor, optionally of an index whose data lives in a bucket.
     *
     * <p>A bucketed index has one shard and no shard mode: a bucket holds small indices only, and one that grows is
     * promoted to units of its own before it could need more.
     *
     * @param name the index name
     * @param uuid the index uuid
     * @param numberOfShards the physical shard slots; with a layout, its next unit id
     * @param mapping the mapping, or null
     * @param extraSettings the settings beyond the shard count
     * @param mappingVersion how many times the mapping has changed
     * @param settingsVersion how many times the settings have changed
     * @param createdAtMillis when the index was created, or 0 when not recorded
     * @param createdVersionId the id of the OpenSearch version that created it, or 0 when not recorded
     * @param aliasedBy the aliases believed to name this index
     * @param layout the range map from routing hashes to units, or null for an index without a shard mode
     * @param bucket the bucket holding this index's data, or null for an index on units of its own
     */
    public IndexDescriptor(
        String name,
        String uuid,
        int numberOfShards,
        String mapping,
        Settings extraSettings,
        long mappingVersion,
        long settingsVersion,
        long createdAtMillis,
        int createdVersionId,
        List<String> aliasedBy,
        org.opensearch.serverless.routing.UnitLayout layout,
        String bucket
    ) {
        if (bucket != null) {
            org.opensearch.serverless.bucket.BucketIds.validate(bucket);
            if (numberOfShards != 1 || layout != null) {
                throw new IllegalArgumentException(
                    "["
                        + name
                        + "] is in bucket ["
                        + bucket
                        + "] but has "
                        + numberOfShards
                        + " shards or a shard mode; a bucket holds one-shard indices"
                );
            }
        }
        this.bucket = bucket;
        this.name = Objects.requireNonNull(name);
        this.uuid = Objects.requireNonNull(uuid);
        if (numberOfShards < 1) {
            throw new IllegalArgumentException("numberOfShards must be positive, got " + numberOfShards);
        }
        if (numberOfShards > MAX_SHARDS) {
            throw new IllegalArgumentException(
                "numberOfShards must be at most "
                    + MAX_SHARDS
                    + ", got "
                    + numberOfShards
                    + ": an index's shard list lives in this descriptor so that nothing ever has to"
                    + " enumerate shards, and that only holds while the list stays small"
            );
        }
        this.numberOfShards = numberOfShards;
        this.mapping = mapping;
        this.mappingVersion = mappingVersion;
        this.settingsVersion = settingsVersion;
        this.extraSettings = extraSettings == null ? Settings.EMPTY : extraSettings;
        this.createdAtMillis = createdAtMillis;
        this.createdVersionId = createdVersionId;
        this.aliasedBy = List.copyOf(aliasedBy);
        if (layout != null && layout.nextUnitId() != numberOfShards) {
            // Every unit id is below the next one, so this also puts every unit inside the slots.
            throw new IllegalArgumentException(
                "[" + name + "] has " + numberOfShards + " shard slots but its layout's next unit id is " + layout.nextUnitId()
            );
        }
        this.layout = layout;
        if (layout == null) {
            this.unitIds = null;
        } else {
            final int[] ids = new int[layout.unitCount()];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = layout.units().get(i).id();
            }
            java.util.Arrays.sort(ids);
            this.unitIds = ids;
        }
    }

    /**
     * Returns the shards that hold documents now, in ascending order: {@code 0..numberOfShards-1} for an
     * index without a shard mode, the live unit ids otherwise. What a search fans out over, and what health,
     * the catalog and a point in time cover. A unit retired by a split keeps its slot until its data is
     * collected, but is not here.
     *
     * @return the live shard ids; a fresh array the caller may keep
     */
    public int[] unitIds() {
        if (unitIds == null) {
            final int[] all = new int[numberOfShards];
            for (int i = 0; i < all.length; i++) {
                all[i] = i;
            }
            return all;
        }
        return unitIds.clone();
    }

    /**
     * Returns whether a node may take a shard slot as its writer: any slot of an index without a shard mode; a live
     * unit, or a half of the split in progress, of one with a mode. A unit retired by a split is refused, so a write
     * routed with a stale layout cannot bring it back to life beside the halves that replaced it.
     *
     * @param slot the shard slot
     * @return true if the slot may be written
     */
    public boolean writableSlot(int slot) {
        if (slot < 0 || slot >= numberOfShards) {
            return false;
        }
        if (layout == null || java.util.Arrays.binarySearch(unitIds, slot) >= 0) {
            return true;
        }
        final org.opensearch.serverless.routing.UnitLayout.Split pending = layout.pending();
        return pending != null && (pending.left().id() == slot || pending.right().id() == slot);
    }

    /**
     * Returns the range map from routing hashes to units, or null for an index created without a shard mode.
     *
     * @return the layout, or null
     */
    public org.opensearch.serverless.routing.UnitLayout layout() {
        return layout;
    }

    /**
     * Returns the bucket this index's data lives in. A hint, checked against the bucket's manifest, which alone decides
     * membership; every request already reads the descriptor, so routing a bucketed index costs no extra read.
     *
     * @return the bucket id, or null for an index on units of its own
     */
    public String bucket() {
        return bucket;
    }

    /**
     * Returns this descriptor in a bucket, or out of one.
     *
     * @param newBucket the bucket id, or null for an index promoted to units of its own
     * @return the new descriptor
     */
    public IndexDescriptor withBucket(String newBucket) {
        return new IndexDescriptor(
            name,
            uuid,
            numberOfShards,
            mapping,
            extraSettings,
            mappingVersion,
            settingsVersion,
            createdAtMillis,
            createdVersionId,
            aliasedBy,
            layout,
            newBucket
        );
    }

    /**
     * Returns the shard count the user declared, which every user-facing shard count reports.
     *
     * <p>The same as {@link #numberOfShards()} for an index without a shard mode, and for a fixed one.
     *
     * @return the declared shard count
     */
    public int declaredShards() {
        return layout == null ? numberOfShards : layout.declaredShards();
    }

    /**
     * Returns this descriptor with a unit layout, its shard slots set to the layout's next unit id.
     *
     * @param newLayout the layout, or null to route as an index without a shard mode
     * @return the new descriptor
     */
    public IndexDescriptor withLayout(org.opensearch.serverless.routing.UnitLayout newLayout) {
        final int slots = newLayout == null ? numberOfShards : newLayout.nextUnitId();
        return new IndexDescriptor(
            name,
            uuid,
            slots,
            mapping,
            extraSettings,
            mappingVersion,
            // The slot count is the projected index's number_of_shards, so a node with the index open must take it as a
            // settings change, not keep the settings it already has at an unchanged version.
            slots == numberOfShards ? settingsVersion : settingsVersion + 1,
            createdAtMillis,
            createdVersionId,
            aliasedBy,
            newLayout,
            bucket
        );
    }

    /**
     * Returns when the index was created.
     *
     * @return epoch millis, or 0 when the descriptor predates this being recorded
     */
    public long createdAtMillis() {
        return createdAtMillis;
    }

    /**
     * Returns the id of the OpenSearch version that created the index.
     *
     * @return the version id, or 0 when not recorded
     */
    public int createdVersionId() {
        return createdVersionId;
    }

    /**
     * Returns the aliases believed to name this index.
     *
     * <p><b>A hint, not the truth.</b> The alias record is the truth about what an alias names; this list
     * is written beside it so the reverse question -- which aliases name this index -- can be answered
     * without enumerating every alias. It is written before an alias is created and after one is removed,
     * so it may over-approximate (a name whose creation then lost its race) and never under-approximates;
     * a reader checks each name against its record and drops the ones that do not name this index.
     *
     * @return the alias names, possibly including stale ones
     */
    public List<String> aliasedBy() {
        return aliasedBy;
    }

    /**
     * Returns a copy stamped with its creation moment and version.
     *
     * @param createdAtMillis when
     * @param createdVersionId by what
     * @return the copy
     */
    public IndexDescriptor createdAt(long createdAtMillis, int createdVersionId) {
        return new IndexDescriptor(
            name,
            uuid,
            numberOfShards,
            mapping,
            extraSettings,
            mappingVersion,
            settingsVersion,
            createdAtMillis,
            createdVersionId,
            aliasedBy,
            layout,
            bucket
        );
    }

    /**
     * Returns a copy with a different set of alias names.
     *
     * @param aliases the names
     * @return the copy
     */
    public IndexDescriptor withAliasedBy(List<String> aliases) {
        return new IndexDescriptor(
            name,
            uuid,
            numberOfShards,
            mapping,
            extraSettings,
            mappingVersion,
            settingsVersion,
            createdAtMillis,
            createdVersionId,
            aliases,
            layout,
            bucket
        );
    }

    /**
     * Returns the index name.
     *
     * @return the name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the index uuid.
     *
     * @return the uuid
     */
    public String uuid() {
        return uuid;
    }

    /**
     * Returns the shard count.
     *
     * @return number of shards
     */
    public int numberOfShards() {
        return numberOfShards;
    }

    /**
     * Renders this descriptor as the {@link IndexMetadata} the reused data plane expects.
     *
     * @param primaryTerms term per shard id, from the shard-heads; a shard with no entry gets term 1
     * @return the metadata
     * @throws IOException if the mapping cannot be parsed
     */
    /**
     * This descriptor as {@link IndexMetadata}, for routing a document to a shard.
     *
     * <p>Separate from {@link #toIndexMetadata} because it takes no primary terms -- routing never reads
     * them -- which is what makes one remembered rendering correct for every caller.
     *
     * @return the metadata core's {@code OperationRouting} needs
     * @throws IOException if the mapping cannot be parsed
     */
    public IndexMetadata routingMetadata() throws IOException {
        IndexMetadata rendered = routingMetadata;
        if (rendered == null) {
            rendered = toIndexMetadata(Map.of());
            routingMetadata = rendered;
        }
        return rendered;
    }

    /**
     * Renders this descriptor as the {@code IndexMetadata} the data plane expects.
     *
     * @param primaryTerms the term to stamp on each shard, by shard number; absent shards get term zero
     * @return the metadata
     * @throws IOException if the mapping cannot be parsed
     */
    public IndexMetadata toIndexMetadata(Map<Integer, Long> primaryTerms) throws IOException {
        final IndexMetadata.Builder builder = IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(extraSettings)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, createdVersionId == 0 ? Version.CURRENT : Version.fromId(createdVersionId))
                    .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    // No replicas: durability comes from the object store, not from copies on peers.
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
                    .build()
            );
        if (mapping != null) {
            builder.putMapping(mapping);
        }
        // Without this every node would read the new mapping and decline to apply it, because core gates
        // re-applying on the version rather than on the source.
        builder.mappingVersion(mappingVersion);
        builder.settingsVersion(settingsVersion);
        for (int shard = 0; shard < numberOfShards; shard++) {
            // The data plane refuses to activate a primary at term 0, so a term must always be
            // supplied. In this design its only legitimate source is the shard-head's CAS generation.
            builder.primaryTerm(shard, primaryTerms.getOrDefault(shard, 1L));
        }
        return builder.build();
    }

    /**
     * Returns the extra settings layered over the defaults.
     *
     * @return the settings
     */
    public Settings extraSettings() {
        return extraSettings;
    }

    /**
     * Returns the mapping source, or null.
     *
     * @return the mapping
     */
    public String mapping() {
        return mapping;
    }

    /**
     * Returns the mapping's version, which increments on every accepted change.
     *
     * @return the mapping version
     */
    public long mappingVersion() {
        return mappingVersion;
    }

    /**
     * Returns a copy carrying a new mapping, one version on.
     *
     * @param merged the merged mapping source
     * @return the updated descriptor
     */
    public IndexDescriptor withMapping(String merged) {
        return new IndexDescriptor(
            name,
            uuid,
            numberOfShards,
            merged,
            extraSettings,
            mappingVersion + 1,
            settingsVersion,
            createdAtMillis,
            createdVersionId,
            aliasedBy,
            layout,
            bucket
        );
    }

    /**
     * Returns the settings' version, which increments on every accepted change.
     *
     * @return the settings version
     */
    public long settingsVersion() {
        return settingsVersion;
    }

    /**
     * Returns a copy carrying new settings, one version on.
     *
     * @param merged the settings to layer over the defaults
     * @return the updated descriptor
     */
    public IndexDescriptor withSettings(Settings merged) {
        return new IndexDescriptor(
            name,
            uuid,
            numberOfShards,
            mapping,
            merged,
            mappingVersion,
            settingsVersion + 1,
            createdAtMillis,
            createdVersionId,
            aliasedBy,
            layout,
            bucket
        );
    }

    /**
     * Serializes this descriptor as the bytes of its register.
     *
     * <p>JSON rather than a binary form on purpose: a descriptor is the record an operator reaches for
     * when something has gone wrong, and being able to read it with the object store's own console is
     * worth more than the bytes it costs.
     *
     * @return the serialized descriptor
     * @throws IOException if serialization fails
     */
    public BytesReference toBytes() throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("name", name);
            builder.field("uuid", uuid);
            builder.field("number_of_shards", numberOfShards);
            if (mapping != null) {
                builder.field("mapping", mapping);
            }
            // Written only when it has moved, so a descriptor that has never had a mapping update is
            // byte-identical to what this class wrote before mappings could be updated at all.
            if (mappingVersion != 1L) {
                builder.field("mapping_version", mappingVersion);
            }
            if (settingsVersion != 1L) {
                builder.field("settings_version", settingsVersion);
            }
            // Each written only when known, so a descriptor that predates them is byte-identical to what
            // this class wrote before, and one written by this version still parses on an older node.
            if (createdAtMillis != 0L) {
                builder.field("created_at_millis", createdAtMillis);
            }
            if (createdVersionId != 0) {
                builder.field("created_version_id", createdVersionId);
            }
            if (aliasedBy.isEmpty() == false) {
                builder.field("aliased_by", aliasedBy);
            }
            // Only for an index created with a shard mode, so every other descriptor is byte-identical to
            // what this class wrote before layouts existed.
            if (layout != null) {
                builder.startObject("unit_layout");
                org.opensearch.serverless.routing.UnitLayoutFormat.toXContent(layout, builder);
                builder.endObject();
            }
            // Only for a bucketed index, so every other descriptor is byte-identical to what this class wrote before.
            if (bucket != null) {
                builder.field("bucket", bucket);
            }
            // Core's own settings serialization, rather than a flat key-to-string loop.
            //
            // The loop could not carry a list. Settings.get(key) on a list-valued setting returns its
            // bracketed toString, so `filter: [icu_folding]` came back as one filter literally named
            // "[icu_folding]" and the analyzer failed to build. Analysis configuration is mostly lists --
            // filter, char_filter, stopwords -- so that ruled out configuring an analysis plugin at all,
            // which is exactly what an installed analysis plugin is for. Found by installing one.
            //
            // Reading is symmetric and also accepts what the old loop wrote: Settings.fromXContent takes
            // flat dotted keys as happily as nested objects, so descriptors already in a register parse
            // unchanged.
            builder.startObject("settings");
            extraSettings.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            builder.endObject();
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    /**
     * Parses a descriptor from its register bytes.
     *
     * @param input the serialized descriptor
     * @return the parsed descriptor
     * @throws IOException if the bytes are not a well-formed descriptor
     */
    public static IndexDescriptor fromStream(InputStream input) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, input)
        ) {
            String name = null;
            String uuid = null;
            String mapping = null;
            long mappingVersion = 1L;
            long settingsVersion = 1L;
            int shards = -1;
            long createdAtMillis = 0L;
            int createdVersionId = 0;
            final List<String> aliasedBy = new java.util.ArrayList<>();
            Settings parsedSettings = Settings.EMPTY;
            org.opensearch.serverless.routing.UnitLayout layout = null;
            String bucket = null;
            String field = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != null && token != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    field = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT && "settings".equals(field)) {
                    parsedSettings = Settings.fromXContent(parser);
                } else if (token == XContentParser.Token.START_OBJECT && "unit_layout".equals(field)) {
                    layout = org.opensearch.serverless.routing.UnitLayoutFormat.fromXContent(parser);
                } else if (token == XContentParser.Token.START_ARRAY && "aliased_by".equals(field)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        aliasedBy.add(parser.text());
                    }
                } else if (field != null && (token == XContentParser.Token.START_ARRAY || token == XContentParser.Token.START_OBJECT)) {
                    // forward compatibility: a newer node may write a structure we do not know. Only under
                    // a field name: the first token is the record's own opening brace.
                    parser.skipChildren();
                } else if (token.isValue()) {
                    switch (field == null ? "" : field) {
                        case "name" -> name = parser.text();
                        case "uuid" -> uuid = parser.text();
                        case "mapping" -> mapping = parser.text();
                        case "mapping_version" -> mappingVersion = parser.longValue();
                        case "settings_version" -> settingsVersion = parser.longValue();
                        case "number_of_shards" -> shards = parser.intValue();
                        case "created_at_millis" -> createdAtMillis = parser.longValue();
                        case "created_version_id" -> createdVersionId = parser.intValue();
                        case "bucket" -> bucket = parser.text();
                        default -> {
                            // forward compatibility: a newer node may write fields we do not know
                        }
                    }
                }
            }
            if (name == null || uuid == null || shards < 1) {
                throw new IOException("malformed index descriptor: missing a required field");
            }
            return new IndexDescriptor(
                name,
                uuid,
                shards,
                mapping,
                parsedSettings,
                mappingVersion,
                settingsVersion,
                createdAtMillis,
                createdVersionId,
                aliasedBy,
                layout,
                bucket
            );
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof IndexDescriptor other) {
            return numberOfShards == other.numberOfShards
                && name.equals(other.name)
                && uuid.equals(other.uuid)
                && Objects.equals(mapping, other.mapping)
                && mappingVersion == other.mappingVersion
                && settingsVersion == other.settingsVersion
                && extraSettings.equals(other.extraSettings)
                && Objects.equals(layout, other.layout)
                && Objects.equals(bucket, other.bucket);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, uuid, numberOfShards, mapping, extraSettings, mappingVersion, settingsVersion, layout, bucket);
    }

    @Override
    public String toString() {
        return "IndexDescriptor[" + name + "/" + uuid + " shards=" + numberOfShards + "]";
    }
}
