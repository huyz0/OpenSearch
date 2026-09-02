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
 */
public final class SnapshotRecord {

    private final String repo;
    private final String name;
    private final long createdAtMillis;
    private final Map<String, SnapshottedIndex> indices;

    /**
     * Creates a record.
     *
     * @param repo the repository it belongs to
     * @param name the snapshot's name within that repository
     * @param createdAtMillis when it was taken, in the plane's clock
     * @param indices each captured index, by the name it had when the snapshot was taken
     */
    public SnapshotRecord(String repo, String name, long createdAtMillis, Map<String, SnapshottedIndex> indices) {
        this.repo = repo;
        this.name = name;
        this.createdAtMillis = createdAtMillis;
        this.indices = Map.copyOf(indices);
    }

    /** One index as a snapshot captured it: its shape, and every shard's commit. */
    public static final class SnapshottedIndex {

        private final String uuid;
        private final int numberOfShards;
        private final String mapping;
        private final Map<Integer, CommitManifest> shards;

        /**
         * Creates a captured index.
         *
         * @param uuid the index's uuid at the moment of capture
         * @param numberOfShards its shard count
         * @param mapping its mapping source, or null for none
         * @param shards each shard's commit at the moment of capture
         */
        public SnapshottedIndex(String uuid, int numberOfShards, String mapping, Map<Integer, CommitManifest> shards) {
            this.uuid = uuid;
            this.numberOfShards = numberOfShards;
            this.mapping = mapping;
            this.shards = Map.copyOf(shards);
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
     * Returns when this snapshot was taken.
     *
     * @return the creation time, in the plane's clock
     */
    public long createdAtMillis() {
        return createdAtMillis;
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
     * @param indexUuid the index's uuid
     * @param shard the shard number
     * @return the referenced blobs
     */
    public List<String> referencedBlobs(String indexUuid, int shard) {
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
            builder.field("created_at", createdAtMillis);
            builder.startObject("indices");
            for (Map.Entry<String, SnapshottedIndex> index : indices.entrySet()) {
                builder.startObject(index.getKey());
                final SnapshottedIndex captured = index.getValue();
                builder.field("uuid", captured.uuid());
                builder.field("number_of_shards", captured.numberOfShards());
                if (captured.mapping() != null) {
                    builder.field("mapping", captured.mapping());
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
            final var body = parser.map();
            final Object repo = body.get("repo");
            final Object name = body.get("name");
            final Object createdAt = body.get("created_at");
            if (repo == null || name == null || createdAt == null) {
                throw new IOException("malformed snapshot record: missing a required field");
            }
            final Map<String, SnapshottedIndex> indices = new LinkedHashMap<>();
            if (body.get("indices") instanceof Map<?, ?> indexMap) {
                for (Map.Entry<?, ?> entry : indexMap.entrySet()) {
                    if ((entry.getValue() instanceof Map<?, ?>) == false) {
                        throw new IOException("malformed snapshot record: an index entry is not an object");
                    }
                    final Map<?, ?> indexBody = (Map<?, ?>) entry.getValue();
                    final Object uuid = indexBody.get("uuid");
                    final Object numberOfShards = indexBody.get("number_of_shards");
                    if (uuid == null || numberOfShards == null) {
                        throw new IOException("malformed snapshot record: an index entry is missing its uuid or shard count");
                    }
                    final Object mapping = indexBody.get("mapping");
                    final Map<Integer, CommitManifest> shards = new LinkedHashMap<>();
                    if (indexBody.get("shards") instanceof List<?> listed) {
                        for (Object shardEntry : listed) {
                            if ((shardEntry instanceof Map<?, ?>) == false) {
                                throw new IOException("malformed snapshot record: a shard entry is not an object");
                            }
                            final Map<?, ?> shardBody = (Map<?, ?>) shardEntry;
                            final Map<String, String> files = new LinkedHashMap<>();
                            if (shardBody.get("files") instanceof Map<?, ?> named) {
                                for (Map.Entry<?, ?> file : named.entrySet()) {
                                    files.put(String.valueOf(file.getKey()), String.valueOf(file.getValue()));
                                }
                            }
                            shards.put(
                                Integer.parseInt(String.valueOf(shardBody.get("shard"))),
                                new CommitManifest(Long.parseLong(String.valueOf(shardBody.get("term"))), files)
                            );
                        }
                    }
                    indices.put(
                        String.valueOf(entry.getKey()),
                        new SnapshottedIndex(
                            uuid.toString(),
                            Integer.parseInt(String.valueOf(numberOfShards)),
                            mapping == null ? null : mapping.toString(),
                            shards
                        )
                    );
                }
            }
            return new SnapshotRecord(repo.toString(), name.toString(), Long.parseLong(String.valueOf(createdAt)), indices);
        }
    }

    @Override
    public String toString() {
        return "SnapshotRecord[" + repo + ":" + name + ", " + indices.size() + " indices]";
    }
}
