/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import java.io.IOException;
import java.util.function.BooleanSupplier;

/**
 * The term a writer's open began on this node, and whether it was stopped.
 *
 * <p>Only an open begins a term, after confirming the head names this node at it. The write path asks before every append:
 * a shard open here at a term no open began -- a reader's engine on an ingest node, or a writer whose term was taken over
 * as it opened -- is not this term's writer, and an append from it would log a write nobody replays as acknowledged.
 */
public final class WriterTerm {

    private volatile long term = -1L;
    private volatile boolean stopped;

    WriterTerm() {}

    /**
     * Begins a term, then asks whether this node still owns it: a head taken and superseded before the open got here stops
     * this writer.
     *
     * @param term the term the writer holds
     * @param stillOwner asked once the term is begun; false stops this writer. Null skips the question.
     * @throws IOException if this node no longer owns the term
     */
    public synchronized void begin(long term, BooleanSupplier stillOwner) throws IOException {
        this.term = term;
        this.stopped = false;
        if (stillOwner != null && stillOwner.getAsBoolean() == false) {
            stopped = true;
            throw new IOException("term " + term + " has been taken over; this node will not write under it");
        }
    }

    /**
     * Whether an open began this term and was not stopped.
     *
     * @param term the term to ask about
     * @return true if this node writes the shard at it
     */
    public boolean begun(long term) {
        return this.term == term && stopped == false;
    }
}
