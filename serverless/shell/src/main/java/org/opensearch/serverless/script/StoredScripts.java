/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.script;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.script.StoredScriptSource;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.StoredScriptStore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * This node's copy of the stored scripts, refreshed from the store when its marker moves.
 *
 * <p>Core's {@code ScriptService} reads stored scripts from the cluster state it last applied, which is how
 * a script put on one node is compiled on another. There is no cluster state here; this is what stands in
 * for it. A node applies the store to itself at three moments: when it serves a put or delete (so the
 * caller's next request sees it), on every reconcile pass when the marker has moved (one small register
 * read; the scripts are re-read only when the number changed), and when a script id is asked for that it
 * does not have (so a script put on another node a moment ago is found on first use rather than after the
 * next pass).
 */
public final class StoredScripts {

    private static final Logger logger = LogManager.getLogger(StoredScripts.class);

    private final Supplier<MetadataPlane> plane;
    private volatile Map<String, StoredScriptSource> scripts = Map.of();
    private volatile long appliedVersion = -1L;

    /**
     * Creates the cache.
     *
     * @param plane supplies the metadata plane, which is known only after the node adopts one
     */
    public StoredScripts(Supplier<MetadataPlane> plane) {
        this.plane = plane;
    }

    /**
     * Looks a script up, refreshing from the store once if it is not here.
     *
     * @param id the script id
     * @return the script, or empty
     */
    public Optional<StoredScriptSource> lookup(String id) {
        final StoredScriptSource known = scripts.get(id);
        if (known != null) {
            return Optional.of(known);
        }
        // The marker, not the listing: a miss re-reads the store only when something changed since this
        // node last read it, so a request naming a script that does not exist costs one register read
        // rather than a listing and a read per script.
        refresh(false);
        return Optional.ofNullable(scripts.get(id));
    }

    /**
     * Re-reads the store if its marker has moved, or unconditionally.
     *
     * @param force whether to re-read even when the marker has not moved
     * @return true if the scripts were re-read
     */
    public synchronized boolean refresh(boolean force) {
        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return false;
        }
        try {
            final StoredScriptStore store = metadata.scripts();
            final long version = store.version();
            if (force == false && version == appliedVersion) {
                return false;
            }
            final Map<String, StoredScriptSource> fresh = new LinkedHashMap<>();
            for (Map.Entry<String, String> each : store.all().entrySet()) {
                try {
                    fresh.put(each.getKey(), parse(each.getValue()));
                } catch (Exception e) {
                    // One unparseable record is not a reason to drop every other script.
                    logger.warn("stored script [" + each.getKey() + "] could not be parsed and is skipped", e);
                }
            }
            scripts = Map.copyOf(fresh);
            appliedVersion = version;
            return true;
        } catch (IOException e) {
            logger.warn("could not refresh stored scripts", e);
            return false;
        }
    }

    /**
     * Parses a stored script as core's put-stored-script API takes it: {@code {"script": {...}}}.
     *
     * @param json the stored JSON
     * @return the script
     */
    public static StoredScriptSource parse(String json) {
        return StoredScriptSource.parse(new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
    }
}
