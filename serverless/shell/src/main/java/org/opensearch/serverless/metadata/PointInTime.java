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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
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
 * A view of an index frozen at a moment, and the promise that its bytes will still be there.
 *
 * <p><b>The record is the commit, not a reference to it.</b> A manifest register holds one value: the
 * commit that is current. Publishing overwrites it, so "the commit as it was at 10:03" is not something
 * that can be pointed at afterwards — by 10:04 the register says something else. So a point in time copies
 * each shard's manifest into itself. That is a few kilobytes for a shard with a few hundred segments, and
 * it is the only version that survives the writer continuing to work.
 *
 * <p><b>It is a promise to the garbage collector, and that is what makes it real.</b> Freezing a view is
 * easy; the hard part is that the files the view names go on existing. The sweep deletes a blob when it is
 * unreferenced by the live commit and belongs to a dead term — both true of exactly the files a point in
 * time is holding on to. Without the collector reading these, a paging caller would find their commit
 * dissolving underneath them halfway through, which is worse than not offering this at all.
 *
 * <p><b>It names the index by uuid as well as by name.</b> A name can be deleted and created again, and
 * the new index of that name has the same first segment names as the old one had. A view that knew only
 * the name would open the new index's bytes under the old view's id — a "frozen" view serving whatever
 * is there now — and the collector would pin the new index's files on the old view's behalf. The uuid is
 * what a view is <em>of</em>; the name is how a caller spelled it. A record written before the uuid was
 * recorded carries none, and is matched by name alone, which is the older, looser behaviour and not a
 * failure.
 *
 * <p><b>It expires, and expiry is enforced by time rather than by the caller.</b> A caller that goes away
 * mid-page would otherwise pin those files for the life of the deployment. The keep-alive is absolute
 * rather than sliding — a search does not renew it — because a sliding one would mean a caller paging
 * slowly could hold a commit indefinitely without ever saying they meant to.
 */
public final class PointInTime {

    private final String id;
    private final String index;
    private final String indexUuid;
    private final long expiresAtMillis;
    private final Map<Integer, CommitManifest> shards;
    /** Whether this is a freeze that has begun and not finished; see {@link #isCapturing}. */
    private final boolean capturing;

    /**
     * Creates a record that names its index by name only.
     *
     * <p>Kept for records and callers that predate the uuid; {@link #PointInTime(String, String, String,
     * long, Map)} is the form a new view should be written in.
     *
     * @param id the identifier a caller quotes back
     * @param index the index it froze
     * @param expiresAtMillis when it stops being honoured, in the plane's clock
     * @param shards each shard's commit at the moment it was taken
     */
    public PointInTime(String id, String index, long expiresAtMillis, Map<Integer, CommitManifest> shards) {
        this(id, index, null, expiresAtMillis, shards);
    }

    /**
     * Creates a record.
     *
     * @param id the identifier a caller quotes back
     * @param index the index it froze
     * @param indexUuid the uuid of that index at the moment it was frozen, or null if not recorded
     * @param expiresAtMillis when it stops being honoured, in the plane's clock
     * @param shards each shard's commit at the moment it was taken
     */
    public PointInTime(String id, String index, String indexUuid, long expiresAtMillis, Map<Integer, CommitManifest> shards) {
        this(id, index, indexUuid, expiresAtMillis, shards, false);
    }

    /**
     * Creates a record, saying whether it is a finished view or a capture in progress.
     *
     * @param id the identifier
     * @param index the index frozen
     * @param indexUuid the incarnation frozen, or null for a record written before uuids were recorded
     * @param expiresAtMillis when it stops being honoured, in the plane's clock
     * @param shards each shard's commit at the moment it was taken; empty only while capturing
     * @param capturing whether this marks a freeze that has begun and not finished
     */
    public PointInTime(
        String id,
        String index,
        String indexUuid,
        long expiresAtMillis,
        Map<Integer, CommitManifest> shards,
        boolean capturing
    ) {
        this.id = id;
        this.index = index;
        this.indexUuid = indexUuid;
        this.expiresAtMillis = expiresAtMillis;
        this.shards = Map.copyOf(shards);
        this.capturing = capturing;
    }

    /**
     * Returns the identifier.
     *
     * @return the id
     */
    public String id() {
        return id;
    }

    /**
     * Returns the index this view is of.
     *
     * @return the index name
     */
    public String index() {
        return index;
    }

    /**
     * Returns the uuid of the index this view is of, as it was when the view was taken.
     *
     * @return the uuid, or null for a record written before it was recorded
     */
    public String indexUuid() {
        return indexUuid;
    }

    /**
     * Returns when this view stops being honoured.
     *
     * @return the expiry, in the plane's clock
     */
    public long expiresAtMillis() {
        return expiresAtMillis;
    }

    /**
     * Returns each shard's frozen commit.
     *
     * @return shard number to commit
     */
    public Map<Integer, CommitManifest> shards() {
        return shards;
    }

    /**
     * Reports whether this view has expired.
     *
     * @param nowMillis the current time, in the plane's clock
     * @return true if it should no longer be honoured or held
     */
    public boolean expiredAt(long nowMillis) {
        return nowMillis >= expiresAtMillis;
    }

    /**
     * The same view with a later deadline.
     *
     * <p>Extending a view is a new record with a later expiry and everything else unchanged — the uuid
     * included, which a caller rebuilding the record by hand from its name and shards would drop.
     *
     * @param expiresAtMillis the new deadline
     * @return the extended view
     */
    public PointInTime withExpiry(long expiresAtMillis) {
        return new PointInTime(id, index, indexUuid, expiresAtMillis, shards);
    }

    /**
     * Reports whether this is the stand-in the plane substitutes for a record it could not read.
     *
     * <p>Such a record pins nothing by name — it has no shards — and the collector has to treat its
     * presence as "do not delete anything", since what it holds is unknowable. See
     * {@code GarbageCollector#sweepShard}.
     *
     * @return true if this record stands for one that could not be read
     */
    public boolean isPlaceholder() {
        return index.isEmpty();
    }

    /**
     * Reports whether this record marks a freeze that has begun and not yet finished.
     *
     * <p><b>Why this exists.</b> Freezing reads one manifest per shard and only then writes the record.
     * Between the first read and that write, the collector has no idea a view is being taken: the files
     * shard 0 froze can stop being referenced by the live commit — the writer publishes again — and be
     * swept before the record naming them exists. The wall-clock floor on unreferenced blobs makes a
     * short freeze safe, and only a short one: at {@code IndexDescriptor.MAX_SHARDS} the manifest reads
     * alone exceed the default floor at any per-read latency above about fifteen milliseconds, which is
     * ordinary for an object store under load.
     *
     * <p>So a freeze writes this record <em>first</em>, naming its index and holding no shards, and
     * overwrites it with the real one once every manifest is read. The collector deletes nothing for an
     * index one of these names, exactly as it already does for a snapshot capture that has named an index
     * and not yet recorded its commits — {@code GarbageCollector#capturing}. This is that same guard,
     * which points in time did not have.
     *
     * <p>A finished view always holds at least one shard: both callers refuse to record one that covers
     * no shard at all. So "names an index and holds nothing" is unambiguous, and is distinct from
     * {@link #isPlaceholder}, which names no index.
     *
     * <p>An abandoned capture — the node died mid-freeze — pins its index until the record expires, which
     * the reaper enforces. Bounded, and the same exposure a snapshot capture already has.
     *
     * @return true if this is a freeze in progress rather than a finished view
     */
    public boolean isCapturing() {
        return capturing;
    }

    /**
     * Reports whether this view holds files of the given index.
     *
     * <p>By uuid when both sides know one, by name otherwise. A view taken over an index that has since
     * been deleted and created again under the same name does not pin the new index's files, and the new
     * index's sweep does not have to honour it — but a record written before uuids were recorded still
     * matches by name, which errs towards pinning.
     *
     * @param indexName the index's name
     * @param uuid the index's uuid, or null if the caller does not know it
     * @return true if this view is of that index
     */
    public boolean pins(String indexName, String uuid) {
        if (index.equals(indexName) == false) {
            return false;
        }
        return indexUuid == null || uuid == null || indexUuid.equals(uuid);
    }

    /**
     * Reports whether a blob name is one this system would have written a record under.
     *
     * <p><b>Why a name check exists at all.</b> A record that cannot be read is treated as live — refusing
     * to delete data you cannot account for is the safe direction, and a half-written record must not have
     * its files swept out from under the caller who is about to use it. But "unreadable means it pins
     * everything" turns any stray object in this container into a deployment-wide off switch for the
     * garbage collector: one file left by a console, a backup tool, or a test filesystem, and nothing is
     * ever reclaimed again, silently.
     *
     * <p>So the two cases are separated by the only evidence available before parsing: the name. A
     * truncated record this system wrote still has the name this system gave it, so the conservative
     * treatment still covers the case it exists for. Something with a name we would never mint is not ours
     * and does not get to speak for our data.
     *
     * <p>The shape is {@link org.opensearch.common.UUIDs#randomBase64UUID}'s: twenty base64url characters
     * with no padding.
     *
     * @param name the blob name
     * @return true if it could be a record this system wrote
     */
    public static boolean looksLikeAnId(String name) {
        if (name == null || name.length() != 22) {
            return false;
        }
        for (int i = 0; i < name.length(); i++) {
            final char c = name.charAt(i);
            final boolean allowed = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_';
            if (allowed == false) {
                return false;
            }
        }
        return true;
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
            builder.field("id", id);
            builder.field("index", index);
            if (indexUuid != null) {
                builder.field("index_uuid", indexUuid);
            }
            builder.field("expires_at", expiresAtMillis);
            if (capturing) {
                // Only on a capture marker, so an ordinary view's bytes are byte-for-byte what they were.
                builder.field("capturing", true);
            }
            builder.startArray("shards");
            for (Map.Entry<Integer, CommitManifest> shard : shards.entrySet()) {
                builder.startObject();
                builder.field("shard", shard.getKey());
                builder.field("term", shard.getValue().term());
                builder.startObject("files");
                for (Map.Entry<String, String> file : shard.getValue().files().entrySet()) {
                    builder.field(file.getKey(), file.getValue());
                }
                builder.endObject();
                if (shard.getValue().lengths().isEmpty() == false) {
                    builder.startObject("lengths");
                    for (Map.Entry<String, Long> length : shard.getValue().lengths().entrySet()) {
                        builder.field(length.getKey(), length.getValue());
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endArray();
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
    public static PointInTime fromStream(InputStream input) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, input)
        ) {
            final var body = parser.map();
            final Object id = body.get("id");
            final Object index = body.get("index");
            final Object expiresAt = body.get("expires_at");
            if (id == null || index == null || expiresAt == null) {
                throw new IOException("malformed point in time: missing a required field");
            }
            // Optional: a record written before the uuid was recorded has none, and reads as before.
            final Object indexUuid = body.get("index_uuid");
            final Map<Integer, CommitManifest> shards = new LinkedHashMap<>();
            if (body.get("shards") instanceof List<?> listed) {
                for (Object entry : listed) {
                    if ((entry instanceof Map<?, ?>) == false) {
                        throw new IOException("malformed point in time: a shard entry is not an object");
                    }
                    final Map<?, ?> shard = (Map<?, ?>) entry;
                    final Map<String, String> files = new LinkedHashMap<>();
                    if (shard.get("files") instanceof Map<?, ?> named) {
                        for (Map.Entry<?, ?> file : named.entrySet()) {
                            files.put(String.valueOf(file.getKey()), String.valueOf(file.getValue()));
                        }
                    }
                    final Map<String, Long> lengths = new LinkedHashMap<>();
                    if (shard.get("lengths") instanceof Map<?, ?> known) {
                        for (Map.Entry<?, ?> length : known.entrySet()) {
                            lengths.put(String.valueOf(length.getKey()), Long.parseLong(String.valueOf(length.getValue())));
                        }
                    }
                    shards.put(
                        Integer.parseInt(String.valueOf(shard.get("shard"))),
                        new CommitManifest(Long.parseLong(String.valueOf(shard.get("term"))), files, null, lengths)
                    );
                }
            }
            final boolean capturing = Boolean.parseBoolean(String.valueOf(body.get("capturing")));
            if (shards.isEmpty() && capturing == false) {
                // A finished view that froze nothing is malformed, and stays so. A capture marker is the
                // one record that legitimately holds none: it is written before the manifests are read.
                throw new IOException("malformed point in time: it froze no shards");
            }
            return new PointInTime(
                id.toString(),
                index.toString(),
                indexUuid == null ? null : indexUuid.toString(),
                Long.parseLong(String.valueOf(expiresAt)),
                shards,
                capturing
            );
        }
    }

    /**
     * Returns the blobs one shard of this view depends on, as {@code term/file} pairs.
     *
     * <p>The same shape the sweep builds from the live commit, so the two sets can simply be added
     * together rather than compared through a translation nobody would notice getting wrong.
     *
     * @param shard the shard number
     * @return the referenced blobs
     */
    public List<String> referencedBlobs(int shard) {
        final CommitManifest manifest = shards.get(shard);
        if (manifest == null) {
            return List.of();
        }
        final List<String> referenced = new ArrayList<>(manifest.files().size());
        for (Map.Entry<String, String> file : manifest.files().entrySet()) {
            referenced.add(file.getValue() + "/" + file.getKey());
        }
        return referenced;
    }

    /**
     * Reads a view off the transport wire.
     *
     * <p>Carries the whole view, every shard's manifest included, rather than a reduced form with just the
     * shard being asked for. {@link org.opensearch.serverless.shell.ServerlessNode#openFrozenView} needs
     * every shard's term to build the view's {@code IndexMetadata} even when opening one shard of it — see
     * that method for why — and sending the full view lets the node this is forwarded to call the exact
     * method the coordinating node would have called locally, rather than a second implementation that
     * could drift from it.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public PointInTime(StreamInput in) throws IOException {
        this.id = in.readString();
        this.index = in.readString();
        this.expiresAtMillis = in.readVLong();
        final int count = in.readVInt();
        final Map<Integer, CommitManifest> read = new LinkedHashMap<>(count);
        for (int i = 0; i < count; i++) {
            read.put(in.readVInt(), new CommitManifest(in));
        }
        this.shards = Map.copyOf(read);
        this.indexUuid = in.readOptionalString();
        // Never on the wire: a capture marker is not forwarded anywhere. Only a finished view is sent, to
        // open a frozen shard of it, and one that arrived mid-capture would name no shard to open.
        this.capturing = false;
    }

    /**
     * Writes this view to the transport wire.
     *
     * @param out the stream
     * @throws IOException if writing fails
     */
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(index);
        out.writeVLong(expiresAtMillis);
        out.writeVInt(shards.size());
        for (Map.Entry<Integer, CommitManifest> shard : new java.util.TreeMap<>(shards).entrySet()) {
            out.writeVInt(shard.getKey());
            shard.getValue().writeTo(out);
        }
        out.writeOptionalString(indexUuid);
    }

    @Override
    public String toString() {
        return "PointInTime["
            + id
            + " of "
            + index
            + (indexUuid == null ? "" : " (" + indexUuid + ")")
            + ", "
            + shards.size()
            + " shards, expires "
            + expiresAtMillis
            + "]";
    }
}
