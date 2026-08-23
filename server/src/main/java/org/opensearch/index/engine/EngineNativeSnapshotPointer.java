/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;

/**
 * The result of a successful {@link Engine#attemptEngineNativeSnapshot} call: the opaque pointer
 * bytes plus the {@code engineId} tag identifying which engine produced them, so a
 * later delete can route release back to the same engine via {@link
 * EngineNativeSnapshotReleasers}. Both fields travel together deliberately -- core reads {@code
 * engineId} directly (for the release-registry lookup) without ever interpreting {@code payload}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class EngineNativeSnapshotPointer {

    private final String engineId;
    private final byte[] payload;

    public EngineNativeSnapshotPointer(String engineId, byte[] payload) {
        this.engineId = Objects.requireNonNull(engineId, "engineId");
        this.payload = Objects.requireNonNull(payload, "payload");
    }

    /**
     * Opaque tag identifying which engine produced this pointer -- see {@link
     * EngineNativeSnapshotReleasers}. Core only ever compares this for equality; it never
     * interprets it.
     */
    public String engineId() {
        return engineId;
    }

    /** The exact bytes handed to {@code ShardRecoveryStrategy.EngineNativeSnapshots#restore} on restore. */
    public byte[] payload() {
        return payload;
    }
}
