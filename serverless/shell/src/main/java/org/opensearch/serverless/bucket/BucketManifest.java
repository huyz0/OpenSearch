/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.bucket;

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
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The one commit register of a bucket: for every member index, keyed by its uuid, the commit that member last
 * checkpointed.
 *
 * <p>Each member's entry is an ordinary {@link CommitManifest}, so a member's files resolve exactly as a shard's do;
 * the bucket only changes how many registers there are. Membership lives here and nowhere else: an index is in a
 * bucket when this register names it, and its descriptor's bucket field is a hint a reader checks against this.
 *
 * <p>JSON, like every other register, so it can be read in the store's own console.
 */
public final class BucketManifest {

    /** One member index: its name, for operators and sweeps, and its newest checkpointed commit, or null before its first. */
    public record Member(String indexName, CommitManifest commit) {
        /**
         * Creates a member entry.
         *
         * @param indexName the member index's name
         * @param commit its newest checkpointed commit, or null if it has none yet
         */
        public Member {
            Objects.requireNonNull(indexName);
        }
    }

    private final String bucketId;
    private final long term;
    private final Map<String, Member> members;

    /**
     * Creates a bucket manifest.
     *
     * @param bucketId the bucket
     * @param term the bucket head term the owner wrote this at
     * @param members member index uuid to its entry
     */
    public BucketManifest(String bucketId, long term, Map<String, Member> members) {
        this.bucketId = BucketIds.validate(bucketId);
        if (term < 0) {
            throw new IllegalArgumentException("term must not be negative, got " + term);
        }
        this.term = term;
        this.members = Collections.unmodifiableMap(new TreeMap<>(members));
    }

    /**
     * Returns an empty manifest for a new bucket.
     *
     * @param bucketId the bucket
     * @return a manifest with no members at term zero
     */
    public static BucketManifest empty(String bucketId) {
        return new BucketManifest(bucketId, 0L, Map.of());
    }

    /** @return the bucket */
    public String bucketId() {
        return bucketId;
    }

    /** @return the bucket head term this was written at */
    public long term() {
        return term;
    }

    /** @return member index uuid to its entry, in uuid order */
    public Map<String, Member> members() {
        return members;
    }

    /** @return how many indices this bucket holds */
    public int size() {
        return members.size();
    }

    /**
     * Returns one member's entry.
     *
     * @param indexUuid the member index's uuid
     * @return its entry, or empty if the index is not in this bucket
     */
    public Optional<Member> member(String indexUuid) {
        return Optional.ofNullable(members.get(indexUuid));
    }

    /**
     * Returns this manifest with a member added or its entry replaced.
     *
     * @param indexUuid the member index's uuid
     * @param member its entry
     * @return the new manifest
     */
    public BucketManifest withMember(String indexUuid, Member member) {
        final TreeMap<String, Member> next = new TreeMap<>(members);
        next.put(Objects.requireNonNull(indexUuid), Objects.requireNonNull(member));
        return new BucketManifest(bucketId, term, next);
    }

    /**
     * Returns this manifest without one member. Its files stay in the packs until a repack drops them.
     *
     * @param indexUuid the member index's uuid
     * @return the new manifest, or this one if the index was not a member
     */
    public BucketManifest withoutMember(String indexUuid) {
        if (members.containsKey(indexUuid) == false) {
            return this;
        }
        final TreeMap<String, Member> next = new TreeMap<>(members);
        next.remove(indexUuid);
        return new BucketManifest(bucketId, term, next);
    }

    /**
     * Returns this manifest as written at a head term.
     *
     * @param newTerm the bucket head term
     * @return the new manifest
     */
    public BucketManifest atTerm(long newTerm) {
        return new BucketManifest(bucketId, newTerm, members);
    }

    /**
     * Serializes this manifest.
     *
     * @return the register bytes
     * @throws IOException if serialization fails
     */
    public BytesReference toBytes() throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("bucket", bucketId);
            builder.field("term", term);
            builder.startObject("members");
            for (Map.Entry<String, Member> e : members.entrySet()) {
                builder.startObject(e.getKey());
                builder.field("index", e.getValue().indexName());
                if (e.getValue().commit() != null) {
                    try (InputStream commit = e.getValue().commit().toBytes().streamInput()) {
                        builder.rawField("commit", commit, XContentType.JSON);
                    }
                }
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    /**
     * Parses a manifest.
     *
     * @param input the register bytes
     * @return the manifest
     * @throws IOException if the bytes are malformed
     */
    public static BucketManifest fromStream(InputStream input) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, input)
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IOException("malformed bucket manifest: not an object");
            }
            String bucketId = null;
            long term = -1L;
            final Map<String, Member> members = new TreeMap<>();
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new IOException("malformed bucket manifest: expected a field, got " + token);
                }
                final String field = parser.currentName();
                token = parser.nextToken();
                if ("members".equals(field) && token == XContentParser.Token.START_OBJECT) {
                    while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                        final String uuid = parser.currentName();
                        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                            throw new IOException("malformed bucket manifest: member [" + uuid + "] is not an object");
                        }
                        members.put(uuid, parseMember(uuid, parser));
                    }
                } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                    // A newer node may write a structure this one does not know.
                    parser.skipChildren();
                } else if ("bucket".equals(field)) {
                    bucketId = parser.text();
                } else if ("term".equals(field)) {
                    term = parser.longValue();
                }
            }
            if (bucketId == null || term < 0) {
                throw new IOException("malformed bucket manifest: missing bucket or term");
            }
            return new BucketManifest(bucketId, term, members);
        }
    }

    private static Member parseMember(String uuid, XContentParser parser) throws IOException {
        String indexName = null;
        CommitManifest commit = null;
        while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            final String field = parser.currentName();
            final XContentParser.Token token = parser.nextToken();
            if ("commit".equals(field) && token == XContentParser.Token.START_OBJECT) {
                try (XContentBuilder copy = XContentFactory.jsonBuilder()) {
                    copy.copyCurrentStructure(parser);
                    try (InputStream bytes = BytesReference.bytes(copy).streamInput()) {
                        commit = CommitManifest.fromStream(bytes);
                    }
                }
            } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            } else if ("index".equals(field)) {
                indexName = parser.text();
            }
        }
        if (indexName == null) {
            throw new IOException("malformed bucket manifest: member [" + uuid + "] names no index");
        }
        return new Member(indexName, commit);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof BucketManifest other
            && bucketId.equals(other.bucketId)
            && term == other.term
            && members.keySet().equals(other.members.keySet())
            && members.entrySet()
                .stream()
                .allMatch(
                    e -> e.getValue().indexName().equals(other.members.get(e.getKey()).indexName())
                        && sameCommit(e.getValue().commit(), other.members.get(e.getKey()).commit())
                );
    }

    private static boolean sameCommit(CommitManifest a, CommitManifest b) {
        if (a == null || b == null) {
            return a == b;
        }
        return a.term() == b.term() && a.files().equals(b.files()) && a.lengths().equals(b.lengths());
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketId, term, members.keySet());
    }

    @Override
    public String toString() {
        return "BucketManifest[" + bucketId + " term=" + term + " members=" + members.size() + "]";
    }
}
