/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.split;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.routing.UnitLayout;
import org.opensearch.serverless.shell.BulkOperation;
import org.opensearch.serverless.shell.BulkOutcome;
import org.opensearch.serverless.shell.NodeWritePath;
import org.opensearch.serverless.shell.WriteHold;
import org.opensearch.serverless.shell.WriteOutcome;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Sends the writes a split unit still receives to the half each document's id routes to, on this node.
 *
 * <p>A coordinator that routed with the layout from before the split sends its writes to the old unit. Its writes are
 * held while the halves open, then handed here, so every write the old unit is sent after its final tail was read
 * lands in a half and none is applied to the old unit. A batch is split by half, each half applies its part in
 * request order, and the outcomes are put back in the order they were asked.
 */
final class HalvesTarget implements WriteHold.Target {

    private final NodeWritePath writes;
    private final UnitLayout after;
    private final UnitLayout.Split split;
    private final ShardId left;
    private final ShardId right;

    HalvesTarget(NodeWritePath writes, UnitLayout after, UnitLayout.Split split, ShardId left, ShardId right) {
        this.writes = writes;
        this.after = after;
        this.split = split;
        this.left = left;
        this.right = right;
    }

    /** The half a document id routes to. */
    ShardId halfOf(String id) {
        final int unit = after.unitForRouting(id).id();
        if (unit == split.left().id()) {
            return left;
        }
        if (unit == split.right().id()) {
            return right;
        }
        // Only the old unit's writes are forwarded, and every id it covered routes to one of its halves.
        throw new IllegalStateException("[" + id + "] routes to unit " + unit + ", not to a half of " + split.retired());
    }

    @Override
    public WriteOutcome index(
        ShardId shardId,
        String id,
        String source,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent,
        boolean mayGrowMapping
    ) throws IOException {
        return writes.index(halfOf(id), id, source, ifSeqNo, ifPrimaryTerm, requireAbsent, mayGrowMapping);
    }

    @Override
    public WriteOutcome delete(ShardId shardId, String id, long ifSeqNo, long ifPrimaryTerm) throws IOException {
        return writes.delete(halfOf(id), id, ifSeqNo, ifPrimaryTerm);
    }

    @Override
    public List<BulkOutcome> bulk(ShardId shardId, List<BulkOperation> batch) throws IOException {
        final List<BulkOperation> toLeft = new ArrayList<>();
        final List<BulkOperation> toRight = new ArrayList<>();
        final boolean[] isLeft = new boolean[batch.size()];
        for (int i = 0; i < batch.size(); i++) {
            final BulkOperation operation = batch.get(i);
            isLeft[i] = halfOf(operation.record().id()).equals(left);
            (isLeft[i] ? toLeft : toRight).add(operation);
        }
        final List<BulkOutcome> fromLeft = toLeft.isEmpty() ? List.of() : writes.bulkOperations(left, toLeft);
        final List<BulkOutcome> fromRight = toRight.isEmpty() ? List.of() : writes.bulkOperations(right, toRight);
        final BulkOutcome[] outcomes = new BulkOutcome[batch.size()];
        int l = 0;
        int r = 0;
        for (int i = 0; i < outcomes.length; i++) {
            outcomes[i] = isLeft[i] ? fromLeft.get(l++) : fromRight.get(r++);
        }
        return Arrays.asList(outcomes);
    }
}
