/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.serverless.store.CommitManifest;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What a snapshot captured: every named index's shape, and every shard's commit at the moment it was
 * taken.
 *
 * <p><b>The record is the commit, the same reason a {@link PointInTime}'s is.</b> A manifest register
 * holds one value, overwritten on every publish, so "the commit this snapshot named" is not something a
 * pointer could still mean later. Unlike a point in time, this has no expiry — a snapshot is kept until
 * deleted, which is the whole difference between a paging cursor and a backup.
 *
 * <p><b>Taking one costs no data movement.</b> Every file a captured commit names was already durable in
 * the object store before this record existed; freezing it is one metadata write per index, referencing
 * blobs that already exist rather than copying them — the same reason a {@link PointInTime} is cheap to
 * take. What is not free is <em>restoring</em> one: this shell's shard storage is keyed by the index's own
 * uuid ({@code RegisterMap#shardData}), and a manifest's blob paths are relative to that uuid's own
 * container, so a restore into a new index (a new uuid, by construction — see M32's index-deletion notes
 * for why reusing one is the bug this design specifically closed) has to copy the referenced blobs into the
 * new index's own storage. The snapshot stays shallow; the restore does the one copy the uuid-keying
 * invariant makes unavoidable.
 *
 * <p><b>An index entry with a shard count and no {@code shards} is a capture in progress</b> (see
 * {@code MetadataPlane.capturing}): the record is written naming every index before any manifest is
 * read, and rewritten with the commits once every one has been. Records written before settings were
 * captured have no {@code settings} and parse unchanged.
 */
public final class SnapshotRecord {

    private final String repo;
    private final String name;
    private final String uuid;
    private final boolean shallow;
    private final long startTimeMillis;
    private final long endTimeMillis;
    private final Map<String, SnapshottedIndex> indices;

    /**
     * Creates a record.
     *
     * @param repo the repository it belongs to
     * @param name the snapshot's name within that repository
     * @param uuid a fresh identifier, the way {@code SnapshotInfo#snapshotId} carries one on real
     *     OpenSearch — this shell's snapshots are named uniquely per repository already, so nothing here
     *     resolves by uuid; it exists so a response can carry one for a client that expects to see it
     * @param shallow whether this snapshot referenced its shards' live blobs rather than copying them —
     *     decided once, from the repository's {@link RepositoryDescriptor#shallowByDefault()} at the
     *     moment this snapshot was taken, and fixed from then on regardless of later repository changes
     * @param startTimeMillis when capture began, in the plane's clock
     * @param endTimeMillis when capture finished
     * @param indices each captured index, by the name it had when the snapshot was taken
     */
    public SnapshotRecord(
        String repo,
        String name,
        String uuid,
        boolean shallow,
        long startTimeMillis,
        long endTimeMillis,
        Map<String, SnapshottedIndex> indices
    ) {
        this.repo = repo;
        this.name = name;
        this.uuid = uuid;
        this.shallow = shallow;
        this.startTimeMillis = startTimeMillis;
        this.endTimeMillis = endTimeMillis;
        this.indices = Map.copyOf(indices);
    }

    /** One index as a snapshot captured it: its shape, and every shard's commit. */
    public static final class SnapshottedIndex {

        private final String uuid;
        private final int numberOfShards;
        private final String mapping;
        private final Map<String, Object> settings;
        private final Map<Integer, CommitManifest> shards;

        /**
         * Creates a captured index with no settings.
         *
         * @param uuid the index's uuid at the moment of capture
         * @param numberOfShards its shard count
         * @param mapping its mapping source, or null for none
         * @param shards each shard's commit at the moment of capture
         */
        public SnapshottedIndex(String uuid, int numberOfShards, String mapping, Map<Integer, CommitManifest> shards) {
            this(uuid, numberOfShards, mapping, Map.of(), shards);
        }

        /**
         * Creates a captured index.
         *
         * @param uuid the index's uuid at the moment of capture
         * @param numberOfShards its shard count
         * @param mapping its mapping source, or null for none
         * @param settings the index's settings in the structured form {@code Settings#toXContent} writes
         *     (nested objects, lists as lists), so a restore can {@code Settings.Builder#loadFromMap} them;
         *     empty for none
         * @param shards each shard's commit at the moment of capture
         */
        public SnapshottedIndex(
            String uuid,
            int numberOfShards,
            String mapping,
            Map<String, Object> settings,
            Map<Integer, CommitManifest> shards
        ) {
            this.uuid = uuid;
            this.numberOfShards = numberOfShards;
            this.mapping = mapping;
            this.settings = settings == null ? Map.of() : Map.copyOf(settings);
            this.shards = Map.copyOf(shards);
        }

        /**
         * Returns the captured settings, structured; empty for none.
         *
         * @return the settings
         */
        public Map<String, Object> settings() {
            return settings;
        }

        /**
         * Returns the index's uuid at the moment of capture.
         *
         * @return the uuid
         */
        public String uuid() {
            return uuid;
        }

        /**
         * Returns the captured shard count.
         *
         * @return the shard count
         */
        public int numberOfShards() {
            return numberOfShards;
        }

        /**
         * Returns the captured mapping source.
         *
         * @return the mapping, or null
         */
        public String mapping() {
            return mapping;
        }

        /**
         * Returns each shard's captured commit.
         *
         * @return shard number to commit
         */
        public Map<Integer, CommitManifest> shards() {
            return shards;
        }
    }

    /**
     * Returns the repository this snapshot belongs to.
     *
     * @return the repository name
     */
    public String repo() {
        return repo;
    }

    /**
     * Returns the snapshot's name within its repository.
     *
     * @return the name
     */
    public String name() {
        return name;
    }

    /**
     * Returns this snapshot's identifier.
     *
     * @return the uuid
     */
    public String uuid() {
        return uuid;
    }

    /**
     * Reports whether this snapshot referenced its shards' live blobs rather than copying them.
     *
     * @return true if shallow
     */
    public boolean shallow() {
        return shallow;
    }

    /**
     * Returns when this snapshot's capture began.
     *
     * @return the start time, in the plane's clock
     */
    public long startTimeMillis() {
        return startTimeMillis;
    }

    /**
     * Returns when this snapshot's capture finished.
     *
     * @return the end time, in the plane's clock
     */
    public long endTimeMillis() {
        return endTimeMillis;
    }

    /**
     * Returns every index this snapshot captured, by the name it had at capture time.
     *
     * @return index name to what was captured
     */
    public Map<String, SnapshottedIndex> indices() {
        return indices;
    }

    /**
     * Returns the register key one snapshot is stored under: its repository and name, joined the same way
     * a shard-head joins an index name and a shard number.
     *
     * @return the key
     */
    public String key() {
        return RegisterMap.snapshotKey(repo, name);
    }

    /**
     * Returns the blobs one shard of one captured index depends on, as {@code term/file} pairs — empty if
     * this snapshot did not capture that index's uuid, or captured it without that shard.
     *
     * <p>Keyed by uuid rather than by index name on purpose, tighter than {@link PointInTime#referencedBlobs}
     * is today: a name can be reused by an unrelated later index (M32 is the whole reason storage no longer
     * is), and a snapshot durable enough to outlive that reuse must not let a later index's shard be
     * mistaken for the one it actually captured.
     *
     * <p><b>Always empty for a standard (non-shallow) snapshot.</b> Its bytes were copied into the
     * repository's own storage at capture time ({@code RegisterMap#snapshotShardData}), not left where the
     * live shard wrote them, so nothing about a live index's own path is ever referenced by one — there is
     * nothing here for the garbage collector or index deletion to protect.
     *
     * @param indexUuid the index's uuid
     * @param shard the shard number
     * @return the referenced blobs
     */
    public List<String> referencedBlobs(String indexUuid, int shard) {
        if (shallow == false) {
            return List.of();
        }
        for (SnapshottedIndex captured : indices.values()) {
            if (captured.uuid().equals(indexUuid) == false) {
                continue;
            }
            final CommitManifest manifest = captured.shards().get(shard);
            if (manifest == null) {
                return List.of();
            }
            final List<String> referenced = new ArrayList<>(manifest.files().size());
            for (Map.Entry<String, String> file : manifest.files().entrySet()) {
                referenced.add(file.getValue() + "/" + file.getKey());
            }
            return referenced;
        }
        return List.of();
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
            builder.field("repo", repo);
            builder.field("name", name);
            builder.field("uuid", uuid);
            builder.field("shallow", shallow);
            builder.field("start_time_in_millis", startTimeMillis);
            builder.field("end_time_in_millis", endTimeMillis);
            builder.startObject("indices");
            for (Map.Entry<String, SnapshottedIndex> index : indices.entrySet()) {
                builder.startObject(index.getKey());
                final SnapshottedIndex captured = index.getValue();
                builder.field("uuid", captured.uuid());
                builder.field("number_of_shards", captured.numberOfShards());
                if (captured.mapping() != null) {
                    builder.field("mapping", captured.mapping());
                }
                if (captured.settings().isEmpty() == false) {
                    builder.field("settings", captured.settings());
                }
                builder.startArray("shards");
                for (Map.Entry<Integer, CommitManifest> shard : captured.shards().entrySet()) {
                    builder.startObject();
                    builder.field("shard", shard.getKey());
                    builder.field("term", shard.getValue().term());
                    builder.startObject("files");
                    for (Map.Entry<String, String> file : shard.getValue().files().entrySet()) {
                        builder.field(file.getKey(), file.getValue());
                    }
                    builder.endObject();
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    /**
     * Parses a record from its register bytes.
     *
     * @param input the stored bytes
     * @return the record
     * @throws IOException if the bytes are not a well-formed record
     */
    public static SnapshotRecord fromStream(InputStream input) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, input)
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw malformed("expected an object");
            }
            final var body = parser.map();
            if (parser.nextToken() != null) {
                throw malformed("trailing content");
            }
            final String repo = requiredString(body.get("repo"), "repo");
            final String name = requiredString(body.get("name"), "name");
            final String uuid = body.get("uuid") == null ? null : requiredString(body.get("uuid"), "uuid");
            final long startTime = requiredLong(body.get("start_time_in_millis"), "start_time_in_millis");
            final long endTime = requiredLong(body.get("end_time_in_millis"), "end_time_in_millis");
            final Object shallowValue = body.get("shallow");
            if (shallowValue instanceof Boolean == false && "true".equals(shallowValue) == false && "false".equals(shallowValue) == false) {
                throw malformed("shallow must be a boolean");
            }
            final boolean shallow = Boolean.TRUE.equals(shallowValue) || "true".equals(shallowValue);
            if (body.get("indices") instanceof Map<?, ?> == false) {
                throw malformed("indices must be an object");
            }
            final Map<?, ?> indexMap = (Map<?, ?>) body.get("indices");
            final Map<String, SnapshottedIndex> indices = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : indexMap.entrySet()) {
                if (entry.getValue() instanceof Map<?, ?> == false) {
                    throw malformed("an index entry is not an object");
                }
                final Map<?, ?> indexBody = (Map<?, ?>) entry.getValue();
                final String indexUuid = requiredString(indexBody.get("uuid"), "index uuid");
                final long shardCount = requiredLong(indexBody.get("number_of_shards"), "number_of_shards");
                if (shardCount < 1 || shardCount > org.opensearch.serverless.cluster.IndexDescriptor.MAX_SHARDS) {
                    throw malformed("invalid shard count");
                }
                final Object mapping = indexBody.get("mapping");
                if (mapping != null && mapping instanceof String == false) {
                    throw malformed("mapping must be a string");
                }
                if (indexBody.containsKey("settings") && indexBody.get("settings") instanceof Map<?, ?> == false) {
                    throw malformed("settings must be an object");
                }
                @SuppressWarnings("unchecked")
                final Map<String, Object> settings = indexBody.get("settings") instanceof Map<?, ?> given
                    ? (Map<String, Object>) given
                    : Map.of();
                final Map<Integer, CommitManifest> shards = new LinkedHashMap<>();
                // Older provisional records may omit shards. That means capture in progress, which
                // pins the entire index. A present field must always have the writer's list shape.
                if (indexBody.containsKey("shards")) {
                    if (indexBody.get("shards") instanceof List<?> == false) {
                        throw malformed("shards must be an array");
                    }
                    for (Object shardEntry : (List<?>) indexBody.get("shards")) {
                        if (shardEntry instanceof Map<?, ?> == false) {
                            throw malformed("a shard entry is not an object");
                        }
                        final Map<?, ?> shardBody = (Map<?, ?>) shardEntry;
                        final long shard = requiredLong(shardBody.get("shard"), "shard");
                        if (shard < 0 || shard >= shardCount || shards.containsKey((int) shard)) {
                            throw malformed("invalid or duplicate shard number");
                        }
                        final long term = requiredLong(shardBody.get("term"), "term");
                        if (term < 0 || shardBody.get("files") instanceof Map<?, ?> == false) {
                            throw malformed("a shard must contain its term and files object");
                        }
                        final Map<String, String> files = new LinkedHashMap<>();
                        for (Map.Entry<?, ?> file : ((Map<?, ?>) shardBody.get("files")).entrySet()) {
                            files.put(requiredString(file.getKey(), "file name"), requiredString(file.getValue(), "file location"));
                        }
                        shards.put((int) shard, new CommitManifest(term, files));
                    }
                    if (shards.isEmpty() == false && shards.size() != shardCount) {
                        throw malformed("a completed index must contain every shard");
                    }
                }
                indices.put(
                    requiredString(entry.getKey(), "index name"),
                    new SnapshottedIndex(indexUuid, (int) shardCount, (String) mapping, settings, shards)
                );
            }
            return new SnapshotRecord(repo, name, uuid, shallow, startTime, endTime, indices);
        }
    }

    private static IOException malformed(String reason) {
        return new IOException("malformed snapshot record: " + reason);
    }

    private static String requiredString(Object value, String field) throws IOException {
        if (value instanceof String text && text.isEmpty() == false) {
            return text;
        }
        throw malformed(field + " must be a nonempty string");
    }

    private static long requiredLong(Object value, String field) throws IOException {
        if (value instanceof Number || value instanceof String) {
            try {
                return Long.parseLong(value.toString());
            } catch (NumberFormatException e) {
                throw malformed(field + " must be an integer");
            }
        }
        throw malformed("missing integer field " + field);
    }

    @Override
    public String toString() {
        return "SnapshotRecord[" + repo + ":" + name + ", " + indices.size() + " indices]";
    }
}
