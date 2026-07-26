/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Node-level component holding the {@link NameIndex} and keeping it fed.
 *
 * <p>This is the wiring the structure needed to become a service. It listens to cluster state and
 * applies index creates and deletes to the index, so a node's copy tracks reality without anything
 * else having to remember to update it.
 *
 * <h2>Why cluster state, when the point is to stop depending on cluster state</h2>
 *
 * Feeding from cluster state looks circular given the architecture's goal. It is a deliberate first
 * step. Today the cluster manager still publishes index metadata, so this is the source that exists and
 * is correct. What the service buys immediately is that <em>resolution</em> stops walking
 * {@code Metadata.indicesLookup}, which is the O(all indices) structure the design needs to retire.
 * When index metadata moves to the manifest, the feed changes and nothing above it does.
 *
 * <p>Disabled by default. The name index answers the same questions core already answers, so running
 * both is pure cost until the switchover, and shipping it inert is how C5 and C6 were landed too.
 */
public class NameIndexService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(NameIndexService.class);

    /**
     * Key only. The {@code Setting} itself is declared on the plugin, because that is where the
     * plugin's own guard test requires every operator-visible setting to live, and a setting declared
     * anywhere else silently can never be configured.
     */
    public static final String ENABLED_SETTING_KEY = "serverless_storage.name_index.enabled";

    private final boolean enabled;
    private final NameIndex nameIndex;
    private final NameIndexResolver resolver;

    public NameIndexService(boolean enabled) {
        this.enabled = enabled;
        this.nameIndex = new NameIndex();
        this.resolver = new NameIndexResolver(nameIndex);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public NameIndex getNameIndex() {
        return nameIndex;
    }

    public NameIndexResolver getResolver() {
        return resolver;
    }

    /** Resolves an expression list with core's semantics. */
    public List<String> resolve(IndicesOptions options, String... expressions) {
        return resolver.resolve(options, expressions);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (enabled == false || event.metadataChanged() == false) {
            return;
        }
        try {
            apply(event.previousState().metadata(), event.state().metadata());
        } catch (Exception e) {
            // A failure here must not break cluster state application. The index goes stale rather than
            // the node going down, and the next full sync corrects it.
            logger.warn("failed to apply metadata change to the name index", e);
        }
    }

    /**
     * Applies the difference between two metadata generations.
     *
     * <p>Walks the current metadata rather than diffing holders, because an index whose alias set
     * changed needs re-adding and a holder comparison would call that unchanged. This is O(all indices)
     * per change, which is exactly the cost the architecture exists to remove, and it is acceptable only
     * because it is temporary: the manifest feed that replaces it is incremental by construction.
     */
    void apply(Metadata previous, Metadata current) {
        Map<String, List<String>> aliasTargets = new HashMap<>();

        for (IndexMetadata indexMetadata : current) {
            String name = indexMetadata.getIndex().getName();
            byte status = indexMetadata.getState() == IndexMetadata.State.CLOSE ? IndexNameEntry.STATUS_CLOSED : IndexNameEntry.STATUS_OPEN;
            IndexNameEntry existing = nameIndex.lookup(name);
            byte[] uuid = uuidBytes(indexMetadata.getIndexUUID());
            if (existing == null || existing.getStatus() != status || Arrays.equals(existing.getUuid(), uuid) == false) {
                nameIndex.create(name, uuid, status);
            }
            for (AliasMetadata alias : indexMetadata.getAliases().values()) {
                aliasTargets.computeIfAbsent(alias.getAlias(), ignored -> new ArrayList<>()).add(name);
            }
        }

        for (Map.Entry<String, List<String>> alias : aliasTargets.entrySet()) {
            // Sorted, because the order targets are discovered follows metadata iteration and is not
            // stable across runs. An alias that resolves to the same set in a different order every time
            // is a difference a caller comparing two responses will see and cannot explain.
            alias.getValue().sort(NamePatterns::compareUtf8);
            IndexNameEntry existing = nameIndex.lookup(alias.getKey());
            if (existing == null || existing.isAlias() == false || existing.getTargets().equals(alias.getValue()) == false) {
                nameIndex.createAlias(alias.getKey(), uuidBytes(alias.getKey()), alias.getValue());
            }
        }

        // Deletes, taken from what the previous generation held and this one does not. Aliases that lost
        // their last index disappear the same way, because they are only ever derived from the indices
        // that name them.
        for (IndexMetadata indexMetadata : previous) {
            String name = indexMetadata.getIndex().getName();
            if (current.hasIndex(name) == false) {
                nameIndex.delete(name);
            }
            for (AliasMetadata alias : indexMetadata.getAliases().values()) {
                if (aliasTargets.containsKey(alias.getAlias()) == false) {
                    nameIndex.delete(alias.getAlias());
                }
            }
        }

        nameIndex.maybeRebuild();
    }

    /**
     * Index UUIDs are base64-ish strings of about 22 characters, not 16-byte values, so they are hashed
     * into the fixed width the compact structure uses. The name is the identity here; the UUID is
     * carried so a caller can tell a recreated index from its predecessor, and a 128-bit digest of the
     * real UUID preserves that distinction.
     */
    private static byte[] uuidBytes(String uuid) {
        byte[] source = uuid.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[CompactNameIndex.UUID_LENGTH];
        for (int i = 0; i < source.length; i++) {
            out[i % out.length] ^= source[i];
            out[(i + 7) % out.length] = (byte) (out[(i + 7) % out.length] * 31 + source[i]);
        }
        return out;
    }
}
