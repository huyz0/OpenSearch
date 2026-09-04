/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Index and component templates, one register per name.
 *
 * <p><b>Why this is not the enumeration the design refuses.</b> The standing objection to listing indices is
 * that their number is unbounded by design — a deployment is meant to hold a hundred million of them — so any
 * answer is either truncated or ruinously expensive. Templates are operator-authored configuration. There are
 * tens of them, and the count is bounded <em>on the way in</em>: creating one past the cap is refused, so
 * reading all of them at index creation can never be the thing that fails. That is the difference between
 * bounding an input and truncating an answer, and it is why the same rule does not apply to both.
 *
 * <p>The old refusal for templates said "there is no cluster state for a template to live in". That was the
 * same stale reasoning M50, M51, M53 and M55 each turned up: a template needs somewhere to live, not
 * specifically cluster state, and a register on an object store is somewhere.
 *
 * <p>Templates are stored as the JSON a client sent, verbatim. Parsing them into a typed model here would mean
 * maintaining a second account of a shape OpenSearch defines, and the only thing this shell needs to read out
 * of one is its patterns, its priority and what it composes.
 */
public final class TemplateStore {

    /**
     * The most templates one deployment may hold.
     *
     * <p>A limit on creation, not on reading. Index creation reads every template to find the matching ones,
     * so an unbounded number of templates would make index creation unboundedly slow — the cost lands on the
     * wrong operation entirely. Refusing the thousand-and-first template puts it back on the operation that
     * caused it.
     */
    public static final int MAX_TEMPLATES = 1000;

    /**
     * The prefix every template blob carries, so a foreign object in this container is not read as one.
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
     * are three different answers.
     */
    private static final String PREFIX = "template-";

    private final BlobContainer container;
    private final Cache cache;

    /** The register that moves on every change, so a reader can tell whether its copy is current. */
    static final String VERSION_BLOB = "_version";

    /**
     * A node's copy of one store's contents, valid while the marker has not moved.
     *
     * <p>Shared across the {@code TemplateStore} instances the plane hands out for one container, which
     * is what makes reading every template on every index creation cost one small register read rather
     * than a listing and a read per template.
     */
    public static final class Cache {
        private volatile long version = -1L;
        private volatile Map<String, String> contents;
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
        final Optional<org.opensearch.common.blobstore.BlobRegister> register = container.readRegister(VERSION_BLOB);
        return register.isEmpty() ? 0L : Long.parseLong(register.get().value().utf8ToString().trim());
    }

    private void bump() throws IOException {
        for (int attempt = 0; attempt < 10; attempt++) {
            final Optional<org.opensearch.common.blobstore.BlobRegister> register = container.readRegister(VERSION_BLOB);
            final long current = register.isEmpty() ? 0L : Long.parseLong(register.get().value().utf8ToString().trim());
            final org.opensearch.core.common.bytes.BytesArray next = new org.opensearch.core.common.bytes.BytesArray(
                Long.toString(current + 1).getBytes(StandardCharsets.UTF_8)
            );
            final org.opensearch.common.blobstore.BlobRegisterCasResult result = register.isEmpty()
                ? container.createRegisterIfAbsent(VERSION_BLOB, next)
                : container.compareAndSwapRegister(VERSION_BLOB, register.get().generation(), next);
            if (result.applied()) {
                return;
            }
        }
        throw new IOException("could not move the store's marker: it is being changed concurrently");
    }

    /**
     * Stores a template, replacing any of the same name.
     *
     * @param name the template name
     * @param source the template's JSON, as the client sent it
     * @throws TooManyTemplatesException if this would exceed {@link #MAX_TEMPLATES}
     * @throws IOException if the write fails
     */
    public void put(String name, String source) throws IOException {
        Names.validateId(name, "template");
        final Map<String, String> existing = all();
        if (existing.containsKey(name) == false && existing.size() >= MAX_TEMPLATES) {
            throw new TooManyTemplatesException(MAX_TEMPLATES);
        }
        final byte[] bytes = source.getBytes(StandardCharsets.UTF_8);
        container.writeBlob(PREFIX + name, new java.io.ByteArrayInputStream(bytes), bytes.length, false);
        bump();
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
        try (InputStream in = container.readBlob(PREFIX + name)) {
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

    /**
     * Removes a template.
     *
     * @param name the template name
     * @return whether one was there
     * @throws IOException if the delete fails
     */
    public boolean delete(String name) throws IOException {
        Names.validateId(name, "template");
        if (get(name).isEmpty()) {
            return false;
        }
        container.deleteBlobsIgnoringIfNotExists(List.of(PREFIX + name));
        bump();
        return true;
    }

    /**
     * Reads every template, by name.
     *
     * @return the templates, sorted by name so the answer is stable
     * @throws IOException if listing or reading fails
     */
    public Map<String, String> all() throws IOException {
        // One register read tells whether the copy is current; the listing and the reads happen only
        // when something changed.
        final long version = version();
        final Map<String, String> cached = cache.contents;
        if (cached != null && cache.version == version) {
            return cached;
        }
        final Map<String, String> fresh = java.util.Collections.unmodifiableMap(readAll());
        cache.contents = fresh;
        cache.version = version;
        return fresh;
    }

    private Map<String, String> readAll() throws IOException {
        final Map<String, String> templates = new TreeMap<>();
        final List<String> blobs = new ArrayList<>(container.listBlobs().keySet());
        for (String blob : blobs) {
            if (blob.startsWith(PREFIX) == false) {
                // Somebody else's file. Not ours to interpret, and not ours to delete either.
                continue;
            }
            final String name = blob.substring(PREFIX.length());
            get(name).ifPresent(source -> templates.put(name, source));
        }
        return templates;
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
