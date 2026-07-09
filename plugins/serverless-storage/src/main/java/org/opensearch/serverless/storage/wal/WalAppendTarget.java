/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import java.io.IOException;

/**
 * The narrow write surface {@link org.opensearch.serverless.storage.translog.WalMirroringTranslog}
 * actually needs from a WAL chunk stream: append one record, flush buffered records, and report how
 * many are currently buffered. Extracted as an interface (rather than depending on {@link
 * WalChunkService} directly) so {@link EncryptingWalChunkService} -- a delegating decorator, since
 * {@link WalChunkService} is {@code final} with no interface of its own to implement -- can sit in
 * front of the real chunk service without {@code WalMirroringTranslog} needing to know the
 * difference, the same seam shape {@link
 * org.opensearch.serverless.storage.format.BundleFileReader} already gives {@code
 * LocalDiskCachingBundleStore}/{@code CachingBundleFileReader} on the read side.
 */
public interface WalAppendTarget {

    /**
     * Buffers {@code record} in memory; does not itself durably write anything -- see {@link #flush}.
     *
     * @param record the operation to buffer.
     */
    void append(WalRecord record) throws IOException;

    /**
     * Durably writes every currently-buffered record into one new chunk and clears the buffer.
     *
     * @return the chunk sequence number written, or a negative value if there was nothing to flush.
     */
    long flush() throws IOException;

    /** How many records are currently buffered, not yet durably written by a {@link #flush} call. */
    int bufferedRecordCount();
}
