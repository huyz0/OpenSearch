/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.serverless.log.LogicalOwnershipSession;
import org.opensearch.serverless.log.LogicalStreamRecord;
import org.opensearch.serverless.log.LogicalStreamState;

import java.io.IOException;
import java.util.Objects;

/** Shared validation of a historical applied owner cut; never continuing authority or a native schema proof. */
final class LogicalPreparationBinding {
    private LogicalPreparationBinding() {}

    static LogicalStreamState.View require(
        LogicalStreamRecord assignment,
        LogicalOwnershipSession.Ownership ownership,
        long firstObservedIndex,
        byte[] inputDigest
    ) throws IOException {
        Objects.requireNonNull(assignment);
        Objects.requireNonNull(ownership);
        if (firstObservedIndex < -1 || Objects.requireNonNull(inputDigest).length != 32) {
            throw new IllegalArgumentException("invalid original logical request identity");
        }
        final var state = Objects.requireNonNull(ownership.state());
        final var barrier = Objects.requireNonNull(ownership.checkpoint());
        if (barrier.logId().equals(assignment.stream()) == false
            || barrier.index() < 0
            || barrier.entryTerm() < 1
            || state.owner() == null
            || state.producerEpoch() < 1
            || state.primaryTerm() < 1
            || state.retired()
            || state.schemaVersion() < 1
            || state.schemaCheckpoint() == null
            || state.schemaCheckpoint().logId().equals(assignment.stream()) == false
            || state.schemaCheckpoint().index() < 0
            || state.schemaCheckpoint().entryTerm() < 1
            || state.lastIndex() < barrier.index()
            || state.lastIndex() < state.schemaCheckpoint().index()
            || state.lastSequenceNumber() < -1) {
            throw new IOException("logical proposal differs from applied ownership, schema or activation");
        }
        return state;
    }
}
