/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import java.util.concurrent.atomic.AtomicReference;

/**
 * The point at which a shard discovers its mapping is behind.
 *
 * <p>H6c designed refresh-on-demand around a generation stamped on the request: a shard already at or ahead
 * of it does no read, which is what makes pulling cheaper than broadcasting. W13 found that no request
 * carries such a generation and no shard entry point accepts one, so that trigger has no caller and cannot
 * acquire one without a request format change.
 *
 * <p>This is the trigger that replaces it, and it is cheaper than the original rather than a fallback. A
 * shard does not need telling that the mapping moved. It finds out when it meets a field its mapping does
 * not have, which is exactly and only when being behind matters. A stamped design refreshes whenever the
 * generation moves, whether or not this shard ever sees the new field; this one refreshes only when this
 * shard actually needs to.
 *
 * <p><b>Nothing registered means nothing changes.</b> An ordinary index, and any cluster not running gated
 * indices, takes one null check per unknown field, which is already the rare path: a field is unknown at
 * most once per shard, because handling it makes it known.
 *
 * <p>The refresher answers whether the field is known <em>after</em> refreshing, so the caller can re-check
 * rather than guess. Returning false means genuinely absent, and the caller proceeds to reject or infer
 * exactly as it did before this existed.
 */
public final class UnknownFieldRefresh {

    /** Refreshes one shard's mapping when it meets a field it does not know. */
    @FunctionalInterface
    public interface Refresher {
        /**
         * @param mapperService the shard's mapping, which a refresh must update for the field to become
         *                      usable rather than merely present in the store
         * @param indexUuid     which index's mapping to read
         * @param fieldName     the field that was missing, so a refresher can skip work when the stored
         *                      mapping does not have it either
         * @return whether {@code fieldName} is known after the refresh
         */
        boolean refresh(MapperService mapperService, String indexUuid, String fieldName);
    }

    private static final AtomicReference<Refresher> REFRESHER = new AtomicReference<>();

    private UnknownFieldRefresh() {}

    /** Installs the refresher. Registering null clears it, which is how a test restores the default. */
    public static void register(Refresher refresher) {
        REFRESHER.set(refresher);
    }

    /** Whether anything can refresh, which is what lets a test tell "not installed" from "did nothing". */
    public static boolean isRegistered() {
        return REFRESHER.get() != null;
    }

    /**
     * Gives a shard one chance to discover the field before it is rejected or dynamically inferred.
     *
     * <p>A refresher that throws is treated as having found nothing rather than being allowed to fail the
     * document. This sits on the indexing path, and a store hiccup must not turn every document carrying a
     * new field into an error: the caller's existing behaviour, rejecting or inferring, is a correct
     * outcome, while a failed write is not.
     *
     * @return whether the field is now known, so the caller should look it up again
     */
    public static boolean refreshed(MapperService mapperService, String indexUuid, String fieldName) {
        Refresher refresher = REFRESHER.get();
        if (refresher == null) {
            return false;
        }
        try {
            return refresher.refresh(mapperService, indexUuid, fieldName);
        } catch (Exception e) {
            return false;
        }
    }
}
