/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.serverless.store.WalRecord;

import java.io.IOException;

/**
 * Shard-bound durable append and streaming recovery. Activation, fencing, retention, and resource
 * ownership are intentionally separate from this data interface. Consensus checkpoints must never
 * be inferred from an object-store ordinal or from a record's maximum operation sequence number.
 */
@InternalApi
public interface DurableOperationLog extends OperationAppender {

    /**
     * Streams recoverable resolved records after the owning activation has established its recovery
     * boundary and fenced obsolete writers. This is a recovery operation, not an unfenced live-tail
     * subscription. Implementations preserve history ordering and do not retain the entire history.
     *
     * @param consumer callback, invoked synchronously for each record
     * @throws IOException on invalid history, read failure, or consumer failure
     */
    void replay(CheckedConsumer<WalRecord, IOException> consumer) throws IOException;
}
