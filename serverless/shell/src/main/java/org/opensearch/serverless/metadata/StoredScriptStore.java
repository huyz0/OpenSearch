/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobContainer;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Stored scripts, in a register namespace of their own, with a marker that moves on every change.
 *
 * <p><b>Why a marker.</b> Core hands stored scripts to every node through cluster state, so a script put
 * on one node is compiled on another the moment the state is applied. There is no cluster state here, and
 * the alternative -- every node listing the scripts container on every reconcile pass to find out whether
 * anything changed -- is the bill this deployment refused to pay for point-in-time reaping. The marker is
 * one small register: a node reads it once per pass, and re-reads the scripts only when the number has
 * moved. A put or delete bumps it under compare-and-swap after the script itself is written, so a node
 * that sees the new number sees the new script.
 *
 * <p>The scripts themselves are a {@link TemplateStore}: bounded on the way in, read by name, the same
 * shape templates and pipelines take.
 */
public final class StoredScriptStore {

    /** The marker's register name, which the template store keeps. */
    static final String VERSION_BLOB = TemplateStore.VERSION_BLOB;

    private final BlobContainer container;
    private final TemplateStore scripts;

    /**
     * Creates the store over a container.
     *
     * @param container the scripts container
     */
    public StoredScriptStore(BlobContainer container) {
        this(container, new TemplateStore.Cache());
    }

    /**
     * Creates the store over a container, with a shared listing cache.
     *
     * @param container the scripts container
     * @param cache the cache shared by every store over this container on this node
     */
    public StoredScriptStore(BlobContainer container, TemplateStore.Cache cache) {
        this.container = container;
        this.scripts = new TemplateStore(container, cache);
    }

    /**
     * Stores a script and moves the marker.
     *
     * @param id the script id
     * @param source the script, as the JSON body core's put-stored-script API takes
     * @throws IOException if the write fails
     */
    public void put(String id, String source) throws IOException {
        if (VERSION_BLOB.equals(id)) {
            throw new IllegalArgumentException("[" + id + "] is reserved");
        }
        scripts.put(id, source);
    }

    /**
     * Reads a script.
     *
     * @param id the script id
     * @return the stored JSON, or empty
     * @throws IOException if the read fails
     */
    public Optional<String> get(String id) throws IOException {
        return VERSION_BLOB.equals(id) ? Optional.empty() : scripts.get(id);
    }

    /**
     * Removes a script and moves the marker.
     *
     * @param id the script id
     * @return true if there was one
     * @throws IOException if the delete fails
     */
    public boolean delete(String id) throws IOException {
        if (VERSION_BLOB.equals(id)) {
            return false;
        }
        return scripts.delete(id);
    }

    /**
     * Reads every script.
     *
     * @return id to stored JSON
     * @throws IOException if the listing or a read fails
     */
    public Map<String, String> all() throws IOException {
        final Map<String, String> found = new java.util.LinkedHashMap<>(scripts.all());
        found.remove(VERSION_BLOB);
        return found;
    }

    /**
     * Reads the marker.
     *
     * @return the number of changes ever made, or 0 when none has been
     * @throws IOException if the read fails
     */
    public long version() throws IOException {
        return scripts.version();
    }
}
