/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Every write a client attempted, and what it was told -- the record a fleet run is judged against.
 *
 * <p>Each attempt has an id of its own, never reused, so a document found later belongs to exactly one attempt.
 * An attempt ends one of three ways, by what the client can prove:
 * <ul>
 *   <li><b>acked</b> -- a 2xx. The document must be readable afterwards, unless its index was deleted by a delete
 *   that had not returned before the write began.</li>
 *   <li><b>refused</b> -- a 4xx, or the request never left the client. The document must never be readable: a
 *   refusal the system nevertheless applied is a lie in the other direction.</li>
 *   <li><b>unknown</b> -- a 5xx, a timeout, a connection broken mid-request. Either outcome is honest.</li>
 * </ul>
 * Deletes of an index are recorded with when they began and returned, which is what lets an acked write to a
 * deleted-and-recreated name be judged: one that began after a delete returned went to the new incarnation and
 * must be there.
 *
 * <p>Persisted as it goes, one line per attempt, so a run that dies still leaves its evidence.
 */
final class FleetLedger implements Closeable {

    enum Outcome {
        ACKED,
        REFUSED,
        UNKNOWN
    }

    record Write(String index, String id, String node, long startedNanos, long endedNanos, int status, long seqNo, long primaryTerm,
        Outcome outcome, String reason) {
    }

    record Delete(String index, long startedNanos, long endedNanos, boolean acked) {
    }

    private final ConcurrentLinkedQueue<Write> writes = new ConcurrentLinkedQueue<>();
    private final Map<String, List<Delete>> deletes = new ConcurrentHashMap<>();
    private final BufferedWriter out;

    FleetLedger(Path file) throws IOException {
        Files.createDirectories(file.getParent());
        this.out = Files.newBufferedWriter(file, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    void record(Write write) {
        writes.add(write);
        line(
            "W,"
                + write.index()
                + ","
                + write.id()
                + ","
                + write.node()
                + ","
                + write.startedNanos()
                + ","
                + write.endedNanos()
                + ","
                + write.status()
                + ","
                + write.seqNo()
                + ","
                + write.primaryTerm()
                + ","
                + write.outcome()
                + ","
                + (write.reason() == null ? "" : write.reason().replace(',', ';').replace('\n', ' '))
        );
    }

    void recordDelete(Delete delete) {
        deletes.computeIfAbsent(delete.index(), k -> java.util.Collections.synchronizedList(new ArrayList<>())).add(delete);
        line("D," + delete.index() + "," + delete.startedNanos() + "," + delete.endedNanos() + "," + delete.acked());
    }

    private synchronized void line(String text) {
        try {
            out.write(text);
            out.newLine();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    synchronized void flush() throws IOException {
        out.flush();
    }

    Collection<Write> writes() {
        return writes;
    }

    /**
     * Whether an acked write must still be readable: false if a delete of its index had not returned before the
     * write began -- then the write may have gone to the incarnation that delete removed -- or if a delete the client
     * never heard back about was sent before the write ended, since that one may take effect at any time after.
     */
    boolean mustBePresent(Write write) {
        final List<Delete> ofIndex = deletes.get(write.index());
        if (ofIndex == null) {
            return true;
        }
        synchronized (ofIndex) {
            for (Delete delete : ofIndex) {
                if (delete.endedNanos() >= write.startedNanos()) {
                    return false;
                }
                // A delete the client never heard back about can take effect at any time after it was sent -- the
                // client stopped waiting, the node did not. Writes after it may be removed by it, honestly.
                if (delete.acked() == false && delete.startedNanos() <= write.endedNanos()) {
                    return false;
                }
            }
        }
        return true;
    }

    /** What a check found. */
    record Verdict(long acked, long ackedChecked, long refused, long unknown, List<Write> lost, List<Write> refusedButVisible,
        long ackedExcusedByDelete) {
        boolean clean() {
            return lost.isEmpty() && refusedButVisible.isEmpty();
        }

        @Override
        public String toString() {
            return "acked "
                + acked
                + " (checked "
                + ackedChecked
                + ", excused by a later delete "
                + ackedExcusedByDelete
                + "), refused "
                + refused
                + ", unknown "
                + unknown
                + "; LOST "
                + lost.size()
                + (lost.isEmpty() ? "" : " e.g. " + lost.subList(0, Math.min(5, lost.size())))
                + "; REFUSED-BUT-VISIBLE "
                + refusedButVisible.size()
                + (refusedButVisible.isEmpty() ? "" : " e.g. " + refusedButVisible.subList(0, Math.min(5, refusedButVisible.size())));
        }
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}
