/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.documents;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.VersionType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** A bulk's items as the native {@link BulkRequest} filters are given, and back; and its outcomes as a native {@link BulkResponse}. */
public final class BulkNative {

    private BulkNative() {}

    public record NativeBatch(BulkRequest request, Map<DocWriteRequest<?>, BulkItem> rejected) {
    }

    public record NativeResult(BulkResponse response, Map<BulkItemResponse, BulkItem> extensions) {
    }

    /** Marks every item with whether its writes are pushed to search nodes. */
    public static void pushTo(List<BulkItem> items, boolean tailPush) {
        for (BulkItem item : items) {
            item.tailPush = tailPush;
        }
    }

    public static NativeBatch bulkRequestFor(List<BulkItem> items, WriteRequest.RefreshPolicy refreshPolicy, String pipeline) {
        final var request = new BulkRequest().pipeline(pipeline).setRefreshPolicy(refreshPolicy);
        final Map<DocWriteRequest<?>, BulkItem> rejected = new java.util.IdentityHashMap<>();
        for (BulkItem item : items) {
            final DocWriteRequest<?> write;
            if ("update".equals(item.action)) {
                // Update is explicitly refused by this surface; it must still have the correct native type.
                write = new UpdateRequest(item.index, item.operation.id());
            } else if (item.operation.isDeletion()) {
                final var delete = new DeleteRequest(item.index, item.operation.id()).routing(item.operation.routing());
                if (item.ifSeqNo >= 0L && item.ifPrimaryTerm > 0L) {
                    delete.setIfSeqNo(item.ifSeqNo).setIfPrimaryTerm(item.ifPrimaryTerm);
                }
                write = delete;
            } else {
                final var index = new IndexRequest(item.index).id(item.operation.id())
                    .routing(item.operation.routing())
                    .source(item.operation.source(), XContentType.JSON)
                    .opType(item.requireAbsent ? DocWriteRequest.OpType.CREATE : DocWriteRequest.OpType.INDEX)
                    .setPipeline(item.pipeline);
                if (item.ifSeqNo >= 0L && item.ifPrimaryTerm > 0L) {
                    index.setIfSeqNo(item.ifSeqNo).setIfPrimaryTerm(item.ifPrimaryTerm);
                }
                write = index;
            }
            request.add(write);
            if (item.failed()) {
                rejected.put(write, item);
            }
        }
        return new NativeBatch(request, rejected);
    }

    public static List<BulkItem> admittedItems(BulkRequest request, Map<DocWriteRequest<?>, BulkItem> rejected) throws IOException {
        final List<BulkItem> items = new ArrayList<>(request.numberOfActions());
        for (DocWriteRequest<?> write : request.requests()) {
            final BulkItem refused = rejected.get(write);
            if (refused != null) {
                // Parsing failures cannot turn into writes merely because a filter changes another item.
                items.add(refused);
                continue;
            }
            final BulkBodyParser.Action action = new BulkBodyParser.Action(write.opType().getLowercase(), write.index());
            action.id = write.id();
            action.ifSeqNo = write.ifSeqNo();
            action.ifPrimaryTerm = write.ifPrimaryTerm();
            action.routing = write.routing();
            action.requireAlias = write.isRequireAlias();
            if (write instanceof IndexRequest index) {
                action.source = index.source() == null
                    ? ""
                    : org.opensearch.common.xcontent.XContentHelper.convertToJson(index.source(), false, false, index.getContentType());
                action.pipeline = index.getPipeline() == null ? request.pipeline() : index.getPipeline();
            }
            final long version = write.version();
            final boolean defaultVersion = version == org.opensearch.common.lucene.uid.Versions.MATCH_ANY
                || (write.opType() == DocWriteRequest.OpType.CREATE && version == org.opensearch.common.lucene.uid.Versions.MATCH_DELETED);
            if (write.versionType() != VersionType.INTERNAL || defaultVersion == false) {
                action.conditional = "version";
            }
            final BulkItem item = action.toItem();
            if (write instanceof IndexRequest index) {
                if (index.getFinalPipeline() != null) {
                    item.fail(
                        RestStatus.NOT_IMPLEMENTED,
                        "unsupported_action",
                        "an explicit native final pipeline is not supported; configure index.final_pipeline"
                    );
                }
                if (index.id() == null) {
                    index.id(item.operation.id());
                }
            }
            items.add(item);
        }
        return items;
    }

    public static NativeResult nativeResult(List<BulkItem> items, long took, boolean refreshed) {
        final var responses = new BulkItemResponse[items.size()];
        final Map<BulkItemResponse, BulkItem> extensions = new java.util.IdentityHashMap<>();
        for (int i = 0; i < items.size(); i++) {
            final BulkItem item = items.get(i);
            final var op = DocWriteRequest.OpType.fromString("index".equals(item.action) && item.requireAbsent ? "create" : item.action);
            final String index = item.writtenIndex == null ? item.index : item.writtenIndex;
            if (item.failed()) {
                responses[i] = new BulkItemResponse(
                    i,
                    op,
                    new BulkItemResponse.Failure(
                        index,
                        item.operation.id(),
                        new org.opensearch.OpenSearchStatusException(item.errorReason, item.status),
                        item.status
                    )
                );
            } else {
                final var shardId = new ShardId(index, item.writtenUuid, item.shard);
                final DocWriteResponse response;
                if ("noop".equals(item.result)) {
                    // Core uses UpdateResponse for an ingest drop even when the input operation is index/create.
                    response = new UpdateResponse(
                        shardId,
                        item.operation.id(),
                        item.seqNo,
                        item.primaryTerm,
                        item.version,
                        DocWriteResponse.Result.NOOP
                    );
                } else if (item.operation.isDeletion()) {
                    response = new DeleteResponse(
                        shardId,
                        item.operation.id(),
                        item.seqNo,
                        item.primaryTerm,
                        item.version,
                        "deleted".equals(item.result)
                    );
                } else {
                    response = new IndexResponse(
                        shardId,
                        item.operation.id(),
                        item.seqNo,
                        item.primaryTerm,
                        item.version,
                        "created".equals(item.result)
                    );
                }
                if ("noop".equals(item.result) == false) {
                    response.setShardInfo(new org.opensearch.action.support.replication.ReplicationResponse.ShardInfo(1, 1));
                    response.setForcedRefresh(refreshed);
                }
                responses[i] = new BulkItemResponse(i, op, response);
            }
            extensions.put(responses[i], item);
        }
        return new NativeResult(new BulkResponse(responses, took), extensions);
    }
}
