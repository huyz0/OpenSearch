/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexMetadataHolder;
import org.opensearch.cluster.metadata.LazyIndexMetadata;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The part of an index's metadata every node needs resident, carried in the cluster metadata manifest
 * alongside the pointer to the index's blob.
 *
 * <p>Without this a node reading full cluster state has to fetch every index's blob before it can
 * build {@code Metadata}, because the derived name arrays and {@code indicesLookup} need a state, a
 * hidden and a system flag, the aliases and a shard count from each one. With it, those are already in
 * the manifest, and the blob is only fetched when something actually asks for the index. That is the
 * fetch {@link IndexMetadataHolder} exists to defer -- see
 * {@link org.opensearch.cluster.metadata.Metadata.Builder#putStub}.
 *
 * <p>Present only in manifests written at {@link ClusterMetadataManifest#CODEC_V5} or later, and even
 * there only when the node was configured to write it, so every reader has to cope with its absence.
 *
 * @opensearch.internal
 */
public class ManifestIndexDescriptor implements Writeable, ToXContentObject {

    static final String STATE_FIELD = "state";
    static final String ALIASES_FIELD = "aliases";
    static final String SYSTEM_FIELD = "system";
    static final String HIDDEN_FIELD = "hidden";
    static final String REMOTE_SNAPSHOT_FIELD = "remote_snapshot";
    static final String WARM_FIELD = "warm";
    static final String TOTAL_SHARDS_FIELD = "total_shards";

    private final IndexMetadata.State state;
    private final Map<String, AliasMetadata> aliases;
    private final boolean system;
    private final boolean hidden;
    private final boolean remoteSnapshot;
    private final boolean warmIndex;
    private final int totalNumberOfShards;

    public ManifestIndexDescriptor(
        IndexMetadata.State state,
        Map<String, AliasMetadata> aliases,
        boolean system,
        boolean hidden,
        boolean remoteSnapshot,
        boolean warmIndex,
        int totalNumberOfShards
    ) {
        this.state = Objects.requireNonNull(state, "state");
        this.aliases = aliases == null ? Collections.emptyMap() : Map.copyOf(aliases);
        this.system = system;
        this.hidden = hidden;
        this.remoteSnapshot = remoteSnapshot;
        this.warmIndex = warmIndex;
        this.totalNumberOfShards = totalNumberOfShards;
    }

    /** Taken from the index being written out, so the manifest and the blob cannot disagree. */
    public static ManifestIndexDescriptor of(IndexMetadata indexMetadata) {
        return of((IndexMetadataHolder) indexMetadata);
    }

    /**
     * Same, from a holder. Everything here is descriptor-level, so this does not materialize an index
     * that is currently deferred -- which matters when filling the descriptor in for indices that did
     * not change and so are not being written.
     */
    public static ManifestIndexDescriptor of(IndexMetadataHolder indexMetadata) {
        return new ManifestIndexDescriptor(
            indexMetadata.getState(),
            indexMetadata.getAliases(),
            indexMetadata.isSystem(),
            indexMetadata.isHidden(),
            indexMetadata.isRemoteSnapshot(),
            indexMetadata.isWarmIndex(),
            indexMetadata.getTotalNumberOfShards()
        );
    }

    /**
     * A holder that answers everything here from the descriptor and calls {@code loader} the first time
     * something needs the rest.
     */
    public IndexMetadataHolder toHolder(Index index, Supplier<IndexMetadata> loader) {
        return LazyIndexMetadata.builder(index)
            .state(state)
            .aliases(aliases)
            .system(system)
            .hidden(hidden)
            .remoteSnapshot(remoteSnapshot)
            .warm(warmIndex)
            .totalNumberOfShards(totalNumberOfShards)
            .loader(loader)
            .build();
    }

    public IndexMetadata.State getState() {
        return state;
    }

    public Map<String, AliasMetadata> getAliases() {
        return aliases;
    }

    public boolean isSystem() {
        return system;
    }

    public boolean isHidden() {
        return hidden;
    }

    public boolean isRemoteSnapshot() {
        return remoteSnapshot;
    }

    public boolean isWarmIndex() {
        return warmIndex;
    }

    public int getTotalNumberOfShards() {
        return totalNumberOfShards;
    }

    public ManifestIndexDescriptor(StreamInput in) throws IOException {
        this.state = IndexMetadata.State.fromId(in.readByte());
        int aliasCount = in.readVInt();
        Map<String, AliasMetadata> aliases = new HashMap<>(aliasCount);
        for (int i = 0; i < aliasCount; i++) {
            AliasMetadata alias = new AliasMetadata(in);
            aliases.put(alias.getAlias(), alias);
        }
        this.aliases = Collections.unmodifiableMap(aliases);
        this.system = in.readBoolean();
        this.hidden = in.readBoolean();
        this.remoteSnapshot = in.readBoolean();
        this.warmIndex = in.readBoolean();
        this.totalNumberOfShards = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(state.id());
        out.writeVInt(aliases.size());
        for (AliasMetadata alias : aliases.values()) {
            alias.writeTo(out);
        }
        out.writeBoolean(system);
        out.writeBoolean(hidden);
        out.writeBoolean(remoteSnapshot);
        out.writeBoolean(warmIndex);
        out.writeVInt(totalNumberOfShards);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STATE_FIELD, state.id());
        builder.field(SYSTEM_FIELD, system);
        builder.field(HIDDEN_FIELD, hidden);
        builder.field(REMOTE_SNAPSHOT_FIELD, remoteSnapshot);
        builder.field(WARM_FIELD, warmIndex);
        builder.field(TOTAL_SHARDS_FIELD, totalNumberOfShards);
        builder.startObject(ALIASES_FIELD);
        for (AliasMetadata alias : aliases.values()) {
            AliasMetadata.Builder.toXContent(alias, builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    /**
     * Parsed by hand rather than through {@code ConstructingObjectParser} because the aliases are a map
     * of {@link AliasMetadata} keyed by alias name, which that parser cannot express without a wrapper
     * type per alias.
     */
    public static ManifestIndexDescriptor fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        IndexMetadata.State state = IndexMetadata.State.OPEN;
        Map<String, AliasMetadata> aliases = new HashMap<>();
        boolean system = false;
        boolean hidden = false;
        boolean remoteSnapshot = false;
        boolean warmIndex = false;
        int totalNumberOfShards = 0;

        String currentFieldName = parser.currentName();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (ALIASES_FIELD.equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        AliasMetadata alias = AliasMetadata.Builder.fromXContent(parser);
                        aliases.put(alias.getAlias(), alias);
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token.isValue()) {
                switch (currentFieldName) {
                    case STATE_FIELD:
                        state = IndexMetadata.State.fromId((byte) parser.intValue());
                        break;
                    case SYSTEM_FIELD:
                        system = parser.booleanValue();
                        break;
                    case HIDDEN_FIELD:
                        hidden = parser.booleanValue();
                        break;
                    case REMOTE_SNAPSHOT_FIELD:
                        remoteSnapshot = parser.booleanValue();
                        break;
                    case WARM_FIELD:
                        warmIndex = parser.booleanValue();
                        break;
                    case TOTAL_SHARDS_FIELD:
                        totalNumberOfShards = parser.intValue();
                        break;
                    default:
                        // A field a later version added. Skipping keeps an older node able to read a
                        // newer manifest's descriptor rather than failing the whole read.
                        break;
                }
            }
        }
        return new ManifestIndexDescriptor(state, aliases, system, hidden, remoteSnapshot, warmIndex, totalNumberOfShards);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ManifestIndexDescriptor that = (ManifestIndexDescriptor) o;
        return system == that.system
            && hidden == that.hidden
            && remoteSnapshot == that.remoteSnapshot
            && warmIndex == that.warmIndex
            && totalNumberOfShards == that.totalNumberOfShards
            && state == that.state
            && Objects.equals(aliases, that.aliases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, aliases, system, hidden, remoteSnapshot, warmIndex, totalNumberOfShards);
    }

    @Override
    public String toString() {
        return "ManifestIndexDescriptor{state="
            + state
            + ", aliases="
            + aliases.keySet()
            + ", system="
            + system
            + ", hidden="
            + hidden
            + ", remoteSnapshot="
            + remoteSnapshot
            + ", warm="
            + warmIndex
            + ", totalShards="
            + totalNumberOfShards
            + "}";
    }
}
