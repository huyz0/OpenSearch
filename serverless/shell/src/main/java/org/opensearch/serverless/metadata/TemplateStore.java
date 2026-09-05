/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Index and component templates: content-addressed blobs behind one register that names them.
 *
 * <p><b>Why this is not the enumeration the design refuses.</b> The standing objection to listing indices is
 * that their number is unbounded by design — a deployment is meant to hold a hundred million of them — so any
 * answer is either truncated or ruinously expensive. Templates are operator-authored configuration. There are
 * tens of them, and the count is bounded <em>on the way in</em>: creating one past the cap is refused, so
 * reading all of them at index creation can never be the thing that fails. That is the difference between
 * bounding an input and truncating an answer, and it is why the same rule does not apply to both.
 *
 * <p><b>The marker is the index, and that is what makes a change atomic.</b> The first version stored each
 * template under its own name and bumped a counter afterwards, so a node could tell whether its copy was
 * current. Two writes, and a crash between them left every warm cache in the fleet serving the old set —
 * the blob was there, the counter had not moved, and nothing would move it until an unrelated change
 * happened somewhere. Now the register holds {@code {version, entries: name → content hash}}, each
 * template's bytes live under their hash, and a change is one compare-and-swap of the register. A blob
 * written before a crash is simply not indexed: invisible, consistent with the failure the operator was
 * told about, and harmless. A reader that sees the new register sees the new set, and re-reads only the
 * entries whose hash moved.
 *
 * <p>Templates are stored as the JSON a client sent, verbatim. Parsing them into a typed model here would mean
 * maintaining a second account of a shape OpenSearch defines, and the only thing this shell needs to read out
 * of one is its patterns, its priority and what it composes.
 *
 * <p><b>A store written before the register carried entries still reads</b>, by listing the named blobs the
 * way it always did, and is carried over to the indexed form by the first change made to it.
 */
public final class TemplateStore {

    /**
     * The most templates one deployment may hold.
     *
     * <p>A limit on creation, not on reading. Index creation reads every template to find the matching ones,
     * so an unbounded number of templates would make index creation unboundedly slow — the cost lands on the
     * wrong operation entirely. Refusing the thousand-and-first template puts it back on the operation that
     * caused it. Checked under the register's compare-and-swap, so two puts cannot both see room for one.
     */
    public static final int MAX_TEMPLATES = 1000;

    /**
     * The prefix a template blob carried before the register indexed them, so a foreign object in this
     * container is not read as one.
     *
     * <p><b>This is not defensive coding, it is a bug that was caught.</b> The first version read every blob
     * in the container and parsed it as a template. Lucene's {@code ExtrasFS} — which the test framework runs
     * precisely to find code that assumes it owns a directory — drops a file named {@code extra0} into
     * directories, and index creation, which reads every template, began failing across the whole deployment
     * with "stored template [extra0] could not be read". An operator or another tool leaving a file here
     * would do the same in production.
     *
     * <p>The rule is {@code WalStore}'s: a name this system did not mint is somebody else's file and is left
     * alone; a name it did mint that will not parse is corruption and is reported. Absent, foreign and corrupt
     * are three different answers. The indexed form never lists at all, which closes the door for good.
     */
    private static final String LEGACY_PREFIX = "template-";

    /** The prefix a template's bytes live under, followed by the hash of those bytes. */
    private static final String CONTENT_PREFIX = "blob-";

    /** How many times a change re-reads the register and tries again when another writer moved it. */
    private static final int MARKER_ATTEMPTS = 10;

    private final BlobContainer container;
    private final Cache cache;

    /** The register that moves on every change, so a reader can tell whether its copy is current. */
    static final String VERSION_BLOB = "_version";

    /**
     * A node's copy of one store's contents, valid while the marker has not moved.
     *
     * <p>Shared across the {@code TemplateStore} instances the plane hands out for one container, which
     * is what makes reading every template on every index creation cost one small register read rather
     * than a listing and a read per template. One reference, replaced whole, so a reader never sees a
     * version labelled with another version's contents.
     */
    public static final class Cache {
        private volatile Snapshot snapshot;
    }

    /** One consistent reading of the store: the marker's version, the hashes it named, their contents. */
    private record Snapshot(long version, Map<String, String> hashes, Map<String, String> contents) {
    }

    /** The register as read: a change count, and the index of names to hashes once the store carries one. */
    private record Marker(long version, Map<String, String> entries, long generation) {
        boolean legacy() {
            return entries == null;
        }
    }

    /** A name the register indexes whose bytes are not there: the one state a reader cannot resolve alone. */
    private static final class MissingContentException extends IOException {
        MissingContentException(String name, String hash) {
            super("template [" + name + "] is indexed but its content [" + hash + "] is not in the store");
        }
    }

    /**
     * Creates a store over one container.
     *
     * @param container the register container holding these templates
     */
    public TemplateStore(BlobContainer container) {
        this(container, new Cache());
    }

    /**
     * Creates a store whose listings are served from a shared cache while the marker has not moved.
     *
     * @param container the container
     * @param cache the cache shared by every store over this container on this node
     */
    public TemplateStore(BlobContainer container, Cache cache) {
        this.container = container;
        this.cache = cache;
    }

    /**
     * Reads the marker.
     *
     * @return the number of changes ever made, or 0 when none has been
     * @throws IOException if the read fails
     */
    public long version() throws IOException {
        return readMarker().version();
    }

    private Marker readMarker() throws IOException {
        final Optional<BlobRegister> register = container.readRegister(VERSION_BLOB);
        if (register.isEmpty()) {
            return new Marker(0L, null, BlobRegister.ABSENT_GENERATION);
        }
        final String text = register.get().value().utf8ToString().trim();
        if (text.startsWith("{") == false) {
            // The counter the first version kept: a store not yet carried over to the indexed form.
            return new Marker(Long.parseLong(text), null, register.get().generation());
        }
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    register.get().value().streamInput()
                )
        ) {
            final Map<String, Object> body = parser.map();
            final long version = body.get("version") instanceof Number number ? number.longValue() : 0L;
            final Map<String, String> entries = new LinkedHashMap<>();
            if (body.get("entries") instanceof Map<?, ?> named) {
                for (Map.Entry<?, ?> entry : named.entrySet()) {
                    entries.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
                }
            }
            return new Marker(version, entries, register.get().generation());
        }
    }

    /** Swaps the register to the next version carrying the given index; false if another writer moved it first. */
    private boolean writeMarker(Marker current, Map<String, String> entries) throws IOException {
        final BytesReference bytes;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("version", current.version() + 1);
            builder.startObject("entries");
            for (Map.Entry<String, String> entry : new TreeMap<>(entries).entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
            builder.endObject();
            bytes = BytesReference.bytes(builder);
        }
        final BlobRegisterCasResult result = current.generation() == BlobRegister.ABSENT_GENERATION
            ? container.createRegisterIfAbsent(VERSION_BLOB, bytes)
            : container.compareAndSwapRegister(VERSION_BLOB, current.generation(), bytes);
        return result.applied();
    }

    /**
     * The index the register holds, or, for a store not yet carried over, the one it will hold: every named
     * blob adopted into the content-addressed form. Adoption writes blobs and nothing else; the register
     * swap that follows is what makes it count, and until then the named blobs are still the truth.
     */
    private Map<String, String> indexOf(Marker marker) throws IOException {
        if (marker.legacy() == false) {
            return marker.entries();
        }
        final Map<String, String> entries = new LinkedHashMap<>();
        for (Map.Entry<String, String> each : legacyReadAll().entrySet()) {
            entries.put(each.getKey(), writeContent(each.getValue()));
        }
        return entries;
    }

    /**
     * Stores a template, replacing any of the same name.
     *
     * <p>The bytes first, under their hash; then one compare-and-swap of the register naming them. A crash
     * between the two leaves an unindexed blob, which no reader will ever see — the same answer the caller
     * got. The old bytes, if no other name shares them, go after the swap.
     *
     * @param name the template name
     * @param source the template's JSON, as the client sent it
     * @throws TooManyTemplatesException if this would exceed {@link #MAX_TEMPLATES}
     * @throws IOException if the write fails
     */
    public void put(String name, String source) throws IOException {
        Names.validateId(name, "template");
        final String hash = writeContent(source);
        for (int attempt = 0; attempt < MARKER_ATTEMPTS; attempt++) {
            final Marker marker = readMarker();
            final Map<String, String> entries = new LinkedHashMap<>(indexOf(marker));
            if (entries.containsKey(name) == false && entries.size() >= MAX_TEMPLATES) {
                throw new TooManyTemplatesException(MAX_TEMPLATES);
            }
            final String previous = entries.put(name, hash);
            if (writeMarker(marker, entries)) {
                if (marker.legacy()) {
                    dropLegacyBlobs();
                }
                if (previous != null && previous.equals(hash) == false && entries.containsValue(previous) == false) {
                    dropContent(previous);
                }
                return;
            }
        }
        throw new IOException("could not move the store's marker: it is being changed concurrently");
    }

    /**
     * Reads one template.
     *
     * @param name the template name
     * @return its source, or empty if there is none
     * @throws IOException if the read fails
     */
    public Optional<String> get(String name) throws IOException {
        Names.validateId(name, "template");
        try {
            return get(name, readMarker());
        } catch (MissingContentException e) {
            // The register moved between our read of it and our read of the bytes it named -- a
            // concurrent change dropped what we were about to read. Once more, from the register.
            return get(name, readMarker());
        }
    }

    private Optional<String> get(String name, Marker marker) throws IOException {
        if (marker.legacy()) {
            return legacyGet(name);
        }
        final String hash = marker.entries().get(name);
        if (hash == null) {
            return Optional.empty();
        }
        final Snapshot snapshot = cache.snapshot;
        if (snapshot != null && hash.equals(snapshot.hashes().get(name))) {
            return Optional.of(snapshot.contents().get(name));
        }
        return Optional.of(readContent(name, hash));
    }

    /**
     * Removes a template.
     *
     * @param name the template name
     * @return whether one was there
     * @throws IOException if the delete fails
     */
    public boolean delete(String name) throws IOException {
        Names.validateId(name, "template");
        for (int attempt = 0; attempt < MARKER_ATTEMPTS; attempt++) {
            final Marker marker = readMarker();
            if (marker.legacy() && legacyGet(name).isEmpty()) {
                // Nothing to remove, so nothing to carry over either.
                return false;
            }
            final Map<String, String> entries = new LinkedHashMap<>(indexOf(marker));
            final String hash = entries.remove(name);
            if (hash == null) {
                return false;
            }
            if (writeMarker(marker, entries)) {
                if (marker.legacy()) {
                    dropLegacyBlobs();
                }
                if (entries.containsValue(hash) == false) {
                    dropContent(hash);
                }
                return true;
            }
        }
        throw new IOException("could not move the store's marker: it is being changed concurrently");
    }

    /**
     * Reads every template, by name.
     *
     * @return the templates, sorted by name so the answer is stable
     * @throws IOException if listing or reading fails
     */
    public Map<String, String> all() throws IOException {
        try {
            return all(readMarker());
        } catch (MissingContentException e) {
            // As in get: the register moved underneath the read of the bytes it named. Once more.
            return all(readMarker());
        }
    }

    private Map<String, String> all(Marker marker) throws IOException {
        // One register read tells whether the copy is current; the bytes are read only for entries whose
        // hash moved since the copy was taken.
        final Snapshot cached = cache.snapshot;
        if (cached != null && cached.version() == marker.version()) {
            return cached.contents();
        }
        final Map<String, String> hashes;
        final Map<String, String> contents = new TreeMap<>();
        if (marker.legacy()) {
            hashes = Map.of();
            contents.putAll(legacyReadAll());
        } else {
            hashes = marker.entries();
            for (Map.Entry<String, String> entry : hashes.entrySet()) {
                if (cached != null && entry.getValue().equals(cached.hashes().get(entry.getKey()))) {
                    contents.put(entry.getKey(), cached.contents().get(entry.getKey()));
                } else {
                    contents.put(entry.getKey(), readContent(entry.getKey(), entry.getValue()));
                }
            }
        }
        final Snapshot fresh = new Snapshot(marker.version(), Map.copyOf(hashes), Collections.unmodifiableMap(contents));
        cache.snapshot = fresh;
        return fresh.contents();
    }

    /**
     * Reads the templates whose names begin with a prefix, or all of them for an empty prefix.
     *
     * @param prefix the prefix, empty for all
     * @return the matching templates
     * @throws IOException if listing or reading fails
     */
    public Map<String, String> withPrefix(String prefix) throws IOException {
        final Map<String, String> matching = new LinkedHashMap<>();
        for (Map.Entry<String, String> each : all().entrySet()) {
            if (prefix.isEmpty() || each.getKey().startsWith(prefix)) {
                matching.put(each.getKey(), each.getValue());
            }
        }
        return matching;
    }

    /** Writes bytes under their hash and returns the hash. Writing the same bytes twice is the same blob. */
    private String writeContent(String source) throws IOException {
        final byte[] bytes = source.getBytes(StandardCharsets.UTF_8);
        final String hash = sha256(bytes);
        container.writeBlob(CONTENT_PREFIX + hash, new java.io.ByteArrayInputStream(bytes), bytes.length, false);
        return hash;
    }

    private String readContent(String name, String hash) throws IOException {
        try (InputStream in = container.readBlob(CONTENT_PREFIX + hash)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (java.nio.file.NoSuchFileException e) {
            throw new MissingContentException(name, hash);
        } catch (IOException e) {
            if (missing(e)) {
                throw new MissingContentException(name, hash);
            }
            throw e;
        }
    }

    private void dropContent(String hash) {
        try {
            container.deleteBlobsIgnoringIfNotExists(List.of(CONTENT_PREFIX + hash));
        } catch (IOException e) {
            // Storage, not correctness: a blob nothing indexes is invisible, and the next change of a
            // template with these bytes would simply find it already there.
        }
    }

    private Optional<String> legacyGet(String name) throws IOException {
        try (InputStream in = container.readBlob(LEGACY_PREFIX + name)) {
            return Optional.of(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (java.nio.file.NoSuchFileException e) {
            return Optional.empty();
        } catch (IOException e) {
            // Some stores report an absent blob as a plain IOException rather than a typed one, and an
            // absent template is not a failure to read the store.
            if (missing(e)) {
                return Optional.empty();
            }
            throw e;
        }
    }

    private Map<String, String> legacyReadAll() throws IOException {
        final Map<String, String> templates = new TreeMap<>();
        final List<String> blobs = new ArrayList<>(container.listBlobs().keySet());
        for (String blob : blobs) {
            if (blob.startsWith(LEGACY_PREFIX) == false) {
                // Somebody else's file. Not ours to interpret, and not ours to delete either.
                continue;
            }
            final String name = blob.substring(LEGACY_PREFIX.length());
            legacyGet(name).ifPresent(source -> templates.put(name, source));
        }
        return templates;
    }

    /** Removes the named blobs a carried-over store no longer reads. Best effort: they are unreferenced. */
    private void dropLegacyBlobs() {
        try {
            final List<String> stale = new ArrayList<>();
            for (String blob : container.listBlobs().keySet()) {
                if (blob.startsWith(LEGACY_PREFIX)) {
                    stale.add(blob);
                }
            }
            if (stale.isEmpty() == false) {
                container.deleteBlobsIgnoringIfNotExists(stale);
            }
        } catch (IOException e) {
            // Left behind, they cost storage and nothing else: the register no longer names them.
        }
    }

    private static String sha256(byte[] bytes) {
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the JVM specification", e);
        }
        final byte[] hash = digest.digest(bytes);
        final StringBuilder hex = new StringBuilder(hash.length * 2);
        for (byte b : hash) {
            hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return hex.toString();
    }

    private static boolean missing(IOException e) {
        final String message = e.getMessage();
        return message != null && (message.contains("NoSuchKey") || message.contains("does not exist") || message.contains("Not Found"));
    }

    /** More templates than a deployment may hold. */
    public static final class TooManyTemplatesException extends IOException {
        /**
         * Creates the exception.
         *
         * @param cap the limit that was reached
         */
        public TooManyTemplatesException(int cap) {
            super(
                "this deployment already holds "
                    + cap
                    + " templates, which is the most it will hold: index creation reads every template to find "
                    + "the ones that match, so an unbounded number of them would make index creation "
                    + "unboundedly slow. Delete one before adding another"
            );
        }
    }

    /** Renders a stored template as raw JSON bytes for a response. */
    static BytesReference bytes(String source) {
        return new BytesArray(source.getBytes(StandardCharsets.UTF_8));
    }
}
