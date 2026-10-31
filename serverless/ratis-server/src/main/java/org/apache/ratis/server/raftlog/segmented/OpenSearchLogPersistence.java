/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.ratis.server.raftlog.segmented;

import org.opensearch.common.annotation.InternalApi;

import java.io.IOException;

/**
 * Opt-in storage evidence hooks, serialized on the native log worker. Implementations require
 * safe synchronous log flush; they must not call back into the Raft server or log locks.
 */
@InternalApi
public interface OpenSearchLogPersistence {
    /**
     * Persists boundary evidence after file/directory persistence, before the native flush index
     * and any replication acknowledgement can advance.
     * @param index latest durably written physical index
     * @throws IOException when evidence cannot be persisted; the native I/O task must fail
     */
    void onLogFlushed(long index) throws IOException;

    /**
     * Lowers boundary evidence for an authorized suffix truncation before disk changes begin.
     * Unlike StateMachine.data().truncate, this runs after older queued I/O has finished.
     * @param firstIndex first physical position to discard
     * @throws IOException when the lower boundary cannot be persisted; no truncation may proceed
     */
    void beforeLogTruncate(long firstIndex) throws IOException;
}
