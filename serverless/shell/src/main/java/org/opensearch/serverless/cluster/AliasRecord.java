/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.cluster;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * A name that stands for some indices.
 *
 * <p><b>Stored in the same register namespace as an index descriptor, which is the whole design.</b> A
 * separate namespace would have been easier and would have let an index be created with a name an alias
 * already had — after which the alias silently stops resolving, because something has to win and an index
 * winning is the only defensible order. Sharing the namespace makes that impossible rather than unlikely:
 * both are created with the same put-if-absent against the same key, so the object store arbitrates, and
 * whichever arrives second is refused with the name it collided with.
 *
 * <p>It also makes resolution one read. A caller naming {@code logs} gets back either an index or an alias
 * from the same register, rather than a miss on one namespace followed by a lookup in another.
 *
 * <p><b>One level, no chains.</b> An alias names indices and not other aliases. Chains would make
 * resolution a walk of unbounded length on a request path, and the cycle detection that follows is a great
 * deal of machinery for something nobody has asked for.
 */
public final class AliasRecord {

    /** The field that distinguishes an alias record from an index descriptor in the same register. */
    public static final String DISCRIMINATOR = "alias";

    private final String name;
    private final List<String> indices;
    private final boolean dataStream;
    private final long generation;
    private final String timestampField;
    private final long mutationUntil;
    private final boolean creating;
    private final List<String> additions;

    /**
     * Creates an alias.
     *
     * @param name the alias name
     * @param indices the indices it stands for, in the order given
     */
    public AliasRecord(String name, List<String> indices) {
        this(name, indices, false, 0L, null);
    }

    /**
     * Creates a record that may be a data stream.
     *
     * <p>A data stream here is an alias with a generation: its indices are its backing indices in order,
     * the last of them is the one a write goes to, and the generation is the number the next backing
     * index is named with. Nothing else about it is different from an alias, which is why it is one.
     *
     * @param name the name
     * @param indices the indices, oldest first for a data stream
     * @param dataStream whether this is a data stream
     * @param generation the data stream's generation, 0 for an alias
     * @param timestampField the data stream's timestamp field, null for an alias
     */
    public AliasRecord(String name, List<String> indices, boolean dataStream, long generation, String timestampField) {
        this(name, indices, dataStream, generation, timestampField, 0L, false, List.of());
    }

    private AliasRecord(
        String name,
        List<String> indices,
        boolean dataStream,
        long generation,
        String timestampField,
        long mutationUntil,
        boolean creating,
        List<String> additions
    ) {
        this.name = name;
        this.indices = List.copyOf(indices);
        this.dataStream = dataStream;
        this.generation = generation;
        this.timestampField = timestampField;
        this.mutationUntil = mutationUntil;
        this.creating = creating;
        this.additions = List.copyOf(additions);
    }

    /** Whether a mutation is preparing reverse hints before publishing membership. */
    public boolean mutationPending() {
        return mutationUntil != 0L;
    }

    /** Whether the pending mutation is an unpublished creation. */
    public boolean creationPending() {
        return mutationPending() && creating;
    }

    /** The recovery deadline for a pending mutation, or zero for a published record. */
    public long mutationUntil() {
        return mutationUntil;
    }

    /** The members whose hints the pending mutation is preparing. */
    public List<String> pendingAdditions() {
        return additions;
    }

    /** Published and preparing members both prevent a concurrent cleanup from removing their hint. */
    public boolean protectsHint(String index) {
        return indices.contains(index) || additions.contains(index);
    }

    /**
     * Reserves a mutation while preserving the previously published fields.
     *
     * @param additions the newly added members whose hints must be protected
     * @param until when another writer may recover an abandoned mutation
     * @param creating whether no alias has been published yet
     * @return the reserved record
     */
    public AliasRecord preparing(List<String> additions, long until, boolean creating) {
        if (until <= 0L || mutationPending()) {
            throw new IllegalArgumentException("invalid alias mutation reservation");
        }
        return new AliasRecord(name, indices, dataStream, generation, timestampField, until, creating, additions);
    }

    /** Returns the last published fields without their temporary mutation reservation. */
    public AliasRecord published() {
        return new AliasRecord(name, indices, dataStream, generation, timestampField);
    }

    /**
     * Reports whether this is a data stream rather than a plain alias.
     *
     * @return true for a data stream
     */
    public boolean dataStream() {
        return dataStream;
    }

    /**
     * Returns the data stream's generation: how many backing indices it has ever had.
     *
     * @return the generation, 0 for an alias
     */
    public long generation() {
        return generation;
    }

    /**
     * Returns the data stream's timestamp field.
     *
     * @return the field, or null for an alias
     */
    public String timestampField() {
        return timestampField;
    }

    /**
     * Returns the index a write through this name goes to: the newest backing index of a data stream.
     *
     * @return the write index, or null for a plain alias, which is not written through
     */
    public String writeIndex() {
        return dataStream ? indices.get(indices.size() - 1) : null;
    }

    /**
     * Returns the alias name.
     *
     * @return the name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the indices this alias stands for.
     *
     * @return the index names
     */
    public List<String> indices() {
        return indices;
    }

    /**
     * Renders the record for its register.
     *
     * @return the bytes to store
     * @throws IOException if it cannot be written
     */
    public BytesReference toBytes() throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field(DISCRIMINATOR, name);
            builder.startArray("indices");
            for (String index : indices) {
                builder.value(index);
            }
            builder.endArray();
            // Written only for a data stream, so an alias record is byte-identical to what this class wrote
            // before data streams existed.
            if (dataStream) {
                builder.field("data_stream", true);
                builder.field("generation", generation);
                builder.field("timestamp_field", timestampField);
            }
            if (mutationPending()) {
                builder.startObject("mutation");
                builder.field("until", mutationUntil);
                builder.field("creating", creating);
                builder.field("additions", additions);
                builder.endObject();
            }
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    /**
     * Parses a record from its register bytes.
     *
     * @param input the stored bytes
     * @return the alias
     * @throws IOException if the bytes are not a well-formed alias record
     */
    public static AliasRecord fromStream(InputStream input) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, input)
        ) {
            final var body = parser.map();
            final Object name = body.get(DISCRIMINATOR);
            if (name == null) {
                throw new IOException("not an alias record: no " + DISCRIMINATOR + " field");
            }
            final List<String> indices = new ArrayList<>();
            if (body.get("indices") instanceof List<?> listed) {
                for (Object index : listed) {
                    indices.add(String.valueOf(index));
                }
            }
            if (indices.isEmpty()) {
                throw new IOException("alias " + name + " stands for no indices");
            }
            final boolean dataStream = Boolean.TRUE.equals(body.get("data_stream"));
            final long generation = body.get("generation") instanceof Number n ? n.longValue() : 0L;
            final String timestampField = body.get("timestamp_field") == null ? null : String.valueOf(body.get("timestamp_field"));
            final AliasRecord result = new AliasRecord(name.toString(), indices, dataStream, generation, timestampField);
            if (body.containsKey("mutation") == false) {
                return result;
            }
            if (body.get("mutation") instanceof java.util.Map<?, ?> == false) {
                throw new IOException("invalid alias mutation");
            }
            final var mutation = (java.util.Map<?, ?>) body.get("mutation");
            final Object until = mutation.get("until");
            if ((until instanceof Integer || until instanceof Long) == false
                || ((Number) until).longValue() <= 0L
                || mutation.get("creating") instanceof Boolean == false
                || mutation.get("additions") instanceof List<?> == false) {
                throw new IOException("invalid alias mutation fields");
            }
            final List<String> additions = new ArrayList<>();
            for (Object added : (List<?>) mutation.get("additions")) {
                if (added instanceof String == false || ((String) added).isEmpty()) {
                    throw new IOException("invalid alias mutation member");
                }
                additions.add((String) added);
            }
            return result.preparing(additions, ((Number) until).longValue(), (Boolean) mutation.get("creating"));
        }
    }

    @Override
    public String toString() {
        return "AliasRecord[" + name + " -> " + indices + "]";
    }
}
