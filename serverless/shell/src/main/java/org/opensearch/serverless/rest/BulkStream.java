/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.http.HttpChunk;
import org.opensearch.rest.StreamingRestChannel;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;

/**
 * One {@code _bulk} request applied as its body arrives, a slice at a time, with an ordinary bulk response at the end.
 *
 * <p><b>Memory per request is bounded, whatever the body's size.</b> Bytes arrive in chunks the transport reads only as
 * this asks for them. Once a slice's worth has arrived ({@link BulkHandler#SLICE_BYTES_SETTING}, 8 MB by default), its
 * complete operations are parsed and applied while the next slice arrives; a third is not read until the first is done, so
 * a connection holds at most two slices of body and one slice's working set, and one slice while it waits its turn. A client sending faster than its batch can
 * be applied is slowed by TCP, as an object store slows an upload, rather than refused or buffered.
 *
 * <p><b>Applying waits its turn for memory.</b> Each slice is applied holding a share of the node's
 * {@link BulkMemoryBudget}, granted first come, first served. Waiting holds no thread, and its connection is not read
 * meanwhile. A slice refused after waiting {@link BulkHandler#WAIT_SETTING} ends the request with a 429. A shard group the
 * object store is too slow to take is sent again, the slice waiting, for as long again; only then is it answered 429.
 *
 * <p><b>What differs from a body taken whole.</b> Operations apply in order, a slice at a time, so a body that turns out
 * to be malformed part way through is refused (400) after the slices before it were applied, where a whole body would
 * have been refused with nothing applied. The response, as before, lists every operation's outcome; it holds ids and
 * outcomes, not sources.
 */
final class BulkStream extends BaseSubscriber<HttpChunk> {

    private static final Logger logger = LogManager.getLogger(BulkStream.class);

    /** The parameters every slice is applied with. */
    record Request(String defaultIndex, String routing, String defaultPipeline, boolean refresh) {
    }

    private final BulkHandler handler;
    private final StreamingRestChannel channel;
    private final ServerlessNode serving;
    private final MetadataPlane metadata;
    private final Request request;
    private final BulkMemoryBudget budget;
    private final long sliceBytes;
    private final long maxLineBytes;
    private final double memoryFactor;
    private final TimeValue wait;
    private final long startedAt = System.nanoTime();

    /** Every operation's outcome so far, sources dropped once applied. */
    private final List<BulkHandler.Item> outcomes = new ArrayList<>();

    // Guarded by this.
    private byte[] buffer = new byte[64 * 1024];
    private int length;
    private boolean complete;
    private boolean applying;
    /** Whether the slice being applied has its share of the budget; until then nothing more is read. */
    private boolean granted;
    private boolean asked;
    private boolean finished;
    private int linesBefore;
    /** How much must have arrived before the next slice is cut: more than a slice when one operation is larger than that. */
    private long cutAt;
    private boolean refreshed = true;

    BulkStream(
        BulkHandler handler,
        StreamingRestChannel channel,
        ServerlessNode serving,
        MetadataPlane metadata,
        Request request,
        BulkMemoryBudget budget,
        long sliceBytes,
        long maxLineBytes,
        double memoryFactor,
        TimeValue wait
    ) {
        this.handler = handler;
        this.channel = channel;
        this.serving = serving;
        this.metadata = metadata;
        this.request = request;
        this.budget = budget;
        this.sliceBytes = sliceBytes;
        this.maxLineBytes = maxLineBytes;
        this.memoryFactor = memoryFactor;
        this.wait = wait;
        this.cutAt = sliceBytes;
    }

    /** Starts reading the body. */
    void start() {
        channel.subscribe(this);
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
        askForMore();
    }

    @Override
    protected void hookOnNext(HttpChunk chunk) {
        try {
            final BytesRef bytes = chunk.content().toBytesRef();
            synchronized (this) {
                asked = false;
                if (finished) {
                    return;
                }
                if (chunk.isLast()) {
                    complete = true;
                }
                ensureRoom(bytes.length);
                System.arraycopy(bytes.bytes, bytes.offset, buffer, length, bytes.length);
                length += bytes.length;
            }
        } finally {
            chunk.close();
        }
        progress();
    }

    @Override
    protected void hookOnComplete() {
        synchronized (this) {
            complete = true;
        }
        progress();
    }

    @Override
    protected void hookOnError(Throwable throwable) {
        logger.debug("a streamed batch's body failed", throwable);
        finish(RestStatus.BAD_REQUEST, "body_failed", "the batch's body could not be read: " + throwable.getMessage());
    }

    private void ensureRoom(int more) {
        if (length + more > buffer.length) {
            buffer = Arrays.copyOf(buffer, (int) Math.max(length + more, Math.min(Integer.MAX_VALUE - 8L, buffer.length * 2L)));
        }
    }

    /** Cuts and applies a slice if one is ready and none is being applied, and reads on if there is room. */
    private void progress() {
        final byte[] slice;
        final int sliceLength;
        final boolean last;
        final int firstLine;
        synchronized (this) {
            if (finished) {
                return;
            }
            if (applying == false && (length >= cutAt || complete)) {
                if (complete && length == 0) {
                    slice = null;
                    sliceLength = 0;
                    last = true;
                    firstLine = linesBefore;
                } else {
                    // The slice keeps the buffer it filled; what arrives next goes to a new one, and whatever the slice
                    // leaves unparsed is put back in front of it when the slice is done.
                    slice = buffer;
                    sliceLength = length;
                    last = complete;
                    firstLine = linesBefore;
                    buffer = new byte[(int) Math.min(Integer.MAX_VALUE - 8, Math.max(64 * 1024, Math.min(sliceBytes, sliceLength)))];
                    length = 0;
                    applying = true;
                    granted = false;
                }
            } else {
                slice = null;
                sliceLength = -1;
                last = false;
                firstLine = 0;
            }
        }
        if (slice == null && sliceLength == 0) {
            respond();
            return;
        }
        if (slice != null) {
            final long charge = (long) (sliceLength * memoryFactor);
            budget.acquire(charge, wait, () -> {
                synchronized (this) {
                    granted = true;
                }
                // Only now read ahead: a batch waiting its turn holds one slice, not two.
                askForMore();
                apply(slice, sliceLength, last, firstLine, budget.granted(charge));
            }, e -> finish(RestStatus.TOO_MANY_REQUESTS, "rejected_execution_exception", e.getMessage()));
        }
        askForMore();
    }

    /** Asks for the next chunk unless one is already asked for, the request is over, or two slices are held. */
    private void askForMore() {
        synchronized (this) {
            if (asked
                || complete
                || finished
                || (applying && (granted == false || length >= sliceBytes))
                || (applying == false && length >= cutAt)) {
                return;
            }
            asked = true;
        }
        request(1);
    }

    /** Parses and applies a slice's complete operations, holding its share of the budget. */
    private void apply(byte[] slice, int sliceLength, boolean last, int firstLine, long held) {
        final Releasable pressure;
        try {
            pressure = serving.services().indexingPressure().markCoordinatingOperationStarted(sliceLength, true);
        } catch (Exception e) {
            budget.release(held);
            fail(e);
            return;
        }
        // Held until the slice is answered, retries included, and given back once.
        final Releasable done = org.opensearch.common.lease.Releasables.releaseOnce(() -> {
            pressure.close();
            budget.release(held);
        });
        final BulkBodyParser.Prefix prefix;
        try {
            prefix = BulkBodyParser.parsePrefix(slice, sliceLength, last, firstLine, request.defaultIndex(), request.routing());
            final List<BulkHandler.Item> items = prefix.items();
            if (items.isEmpty() == false) {
                for (BulkHandler.Item item : items) {
                    if (item.pipeline == null) {
                        item.pipeline = request.defaultPipeline();
                    }
                }
                handler.dispatcher().route(serving, metadata, items);
                handler.dispatcher().shape(serving, metadata, items);
            }
        } catch (BulkBodyParser.BadRequest e) {
            done.close();
            finish(RestStatus.BAD_REQUEST, "bad_bulk_body", e.getMessage());
            return;
        } catch (Exception e) {
            done.close();
            logger.debug("a streamed batch's slice failed", e);
            fail(e);
            return;
        }
        write(slice, sliceLength, firstLine, prefix, prefix.items(), System.nanoTime() + wait.nanos(), 0, done);
    }

    /**
     * Writes a slice's operations. A shard group refused because the object store is slow applied nothing, so rather
     * than answer it 429 the slice waits and sends it again, until {@link BulkHandler#WAIT_SETTING} has passed: the
     * next slice is not cut meanwhile and the connection is not read, so the client is slowed instead of refused.
     */
    private void write(
        byte[] slice,
        int sliceLength,
        int firstLine,
        BulkBodyParser.Prefix prefix,
        List<BulkHandler.Item> pending,
        long deadline,
        int attempt,
        Releasable done
    ) {
        final List<BulkHandler.Item> refused = new ArrayList<>();
        try {
            if (pending.isEmpty() == false) {
                final boolean sliceRefreshed = handler.dispatcher().dispatch(serving, pending, request.refresh());
                synchronized (this) {
                    refreshed &= sliceRefreshed;
                }
                for (BulkHandler.Item item : pending) {
                    if (BulkDispatch.THROTTLED.equals(item.errorType)) {
                        refused.add(item);
                    }
                }
            }
        } catch (Exception e) {
            done.close();
            logger.debug("a streamed batch's slice failed", e);
            fail(e);
            return;
        }
        final long now = System.nanoTime();
        if (refused.isEmpty() == false && now < deadline) {
            synchronized (this) {
                if (finished) {
                    done.close();
                    return;
                }
            }
            for (BulkHandler.Item item : refused) {
                item.clearRefusal();
            }
            // 50 ms doubling to a second: soon after the store catches up, without spinning while it does not.
            final long delayNanos = Math.min(
                deadline - now,
                TimeValue.timeValueMillis(Math.min(1_000L, 50L << Math.min(attempt, 5))).nanos()
            );
            serving.threadPool()
                .schedule(
                    () -> write(slice, sliceLength, firstLine, prefix, refused, deadline, attempt + 1, done),
                    TimeValue.timeValueNanos(Math.max(1L, delayNanos)),
                    org.opensearch.threadpool.ThreadPool.Names.GENERIC
                );
            return;
        }
        final List<BulkHandler.Item> items = prefix.items();
        for (BulkHandler.Item item : items) {
            // The outcome stays for the response; the source does not.
            item.operation = item.operation.withSource("");
        }
        synchronized (this) {
            outcomes.addAll(items);
        }
        done.close();
        sliceDone(slice, sliceLength, firstLine, prefix.consumedBytes(), prefix.consumedLines());
    }

    /** Puts back what the slice left unparsed, and moves on to the next. */
    private void sliceDone(byte[] slice, int sliceLength, int firstLine, int consumed, int lines) {
        if (sliceLength - consumed > maxLineBytes) {
            finish(
                RestStatus.REQUEST_ENTITY_TOO_LARGE,
                "operation_too_large",
                "an operation in this batch is longer than " + maxLineBytes + " bytes, the most one may be"
            );
            return;
        }
        synchronized (this) {
            // What the slice left, an operation not yet whole, goes back in front of what has arrived since.
            final int left = sliceLength - consumed;
            if (left > 0) {
                final byte[] joined = new byte[Math.max(left + length, buffer.length)];
                System.arraycopy(slice, consumed, joined, 0, left);
                System.arraycopy(buffer, 0, joined, left, length);
                buffer = joined;
                length = left + length;
            }
            linesBefore = firstLine + lines;
            // No operation was whole: wait for a slice more than what is here, rather than cutting the same bytes again.
            cutAt = consumed == 0 && left > 0 ? (long) left + sliceBytes : sliceBytes;
            applying = false;
        }
        progress();
    }

    private void respond() {
        final List<BulkHandler.Item> all;
        final boolean allRefreshed;
        synchronized (this) {
            if (finished) {
                return;
            }
            finished = true;
            all = new ArrayList<>(outcomes);
            allRefreshed = refreshed;
        }
        try {
            if (all.isEmpty()) {
                send(IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "empty_bulk", "the body contained no operations"));
                return;
            }
            handler.respond(channel, all, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt), allRefreshed);
        } catch (Exception e) {
            logger.error("failed to send a streamed batch's response", e);
        }
    }

    /**
     * Sends a response with its own status. Once the body is being read the streaming channel has its headers prepared
     * as 200 unless told otherwise, so a refusal names its status first.
     */
    private void send(org.opensearch.rest.RestResponse response) {
        channel.prepareResponse(response.status(), java.util.Map.of("Content-Type", List.of(response.contentType())));
        channel.sendResponse(response);
    }

    private void finish(RestStatus status, String type, String reason) {
        synchronized (this) {
            if (finished) {
                return;
            }
            finished = true;
        }
        cancel();
        try {
            send(IndexAdminHandler.error(channel, status, type, reason));
        } catch (Exception e) {
            logger.error("failed to report a streamed batch's failure", e);
        }
    }

    private void fail(Exception failure) {
        synchronized (this) {
            if (finished) {
                return;
            }
            finished = true;
        }
        cancel();
        try {
            send(IndexAdminHandler.failure(channel, failure));
        } catch (Exception e) {
            logger.error("failed to report a streamed batch's failure", e);
        }
    }
}
