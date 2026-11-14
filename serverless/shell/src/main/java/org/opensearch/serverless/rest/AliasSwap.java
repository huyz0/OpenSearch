/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.serverless.cluster.AliasRecord;
import org.opensearch.serverless.metadata.MetadataPlane;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Adds or removes indices on an alias by compare-and-swap on its register, refusing what an alias cannot hold.
 * Split out of {@link AliasHandler}.
 */
final class AliasSwap {

    private AliasSwap() {}

    /** A refusal, with the status and type the caller should see rather than one status for everything. */
    record Refusal(RestStatus status, String type, String reason) {
    }

    /** Core's refusal when an alias operation names a data stream. */
    static String dataStreamNotAlias(String name) {
        return "The provided expression [" + name + "] matches a data stream, specify the corresponding concrete indices instead";
    }

    /**
     * Applies one add-or-remove to an alias under a compare-and-swap, retrying a lost race.
     *
     * <p>The record and the generation it is swapped under come from one read; see the class comment for
     * why two reads are not a token at all.
     *
     * @param metadata the metadata plane
     * @param name the alias
     * @param indices the indices to add or remove
     * @param adding whether to add or remove
     * @param isWriteIndex for an add, true to mark the index added as the write index, false to unmark it, null to leave it
     * @return a refusal to report, or null on success
     * @throws IOException if the store cannot be read or written
     */
    static Refusal swap(MetadataPlane metadata, String name, List<String> indices, boolean adding, Boolean isWriteIndex)
        throws IOException {
        if (adding && Boolean.TRUE.equals(isWriteIndex) && indices.size() != 1) {
            return new Refusal(
                RestStatus.BAD_REQUEST,
                "illegal_argument_exception",
                "is_write_index marks one index; this request names " + indices.size()
            );
        }
        for (int attempt = 0; attempt < 4; attempt++) {
            final var resolved = metadata.resolveForAliasMutation(name);
            if (resolved.index() != null) {
                return new Refusal(
                    RestStatus.BAD_REQUEST,
                    "invalid_alias_name_exception",
                    "[" + name + "] is an index, not an alias; a name cannot be both"
                );
            }
            final AliasRecord before = resolved.alias();
            if (before != null && before.dataStream()) {
                return new Refusal(RestStatus.BAD_REQUEST, "illegal_argument_exception", dataStreamNotAlias(name));
            }
            final List<String> now = new ArrayList<>(before == null ? List.of() : before.indices());
            for (String index : indices) {
                if (adding) {
                    if (now.contains(index) == false) {
                        now.add(index);
                    }
                } else {
                    now.remove(index);
                }
            }
            String writeMember = before == null ? null : before.writeMember();
            if (adding && Boolean.TRUE.equals(isWriteIndex)) {
                writeMember = indices.get(0);
            } else if (adding && Boolean.FALSE.equals(isWriteIndex) && indices.contains(writeMember)) {
                writeMember = null;
            }
            if (writeMember != null && now.contains(writeMember) == false) {
                writeMember = null;
            }
            if (before == null) {
                if (adding == false) {
                    return new Refusal(RestStatus.NOT_FOUND, "aliases_not_found_exception", "aliases [" + name + "] missing");
                }
                try {
                    metadata.createAlias(new AliasRecord(name, now).withWriteMember(writeMember));
                    return null;
                } catch (org.opensearch.serverless.metadata.IndexAlreadyExistsException e) {
                    continue; // somebody created it between the read and the write; re-read and merge
                }
            }
            if (now.equals(before.indices()) && java.util.Objects.equals(writeMember, before.writeMember())) {
                if (adding) {
                    return null; // already named; nothing to swap
                }
                // Core's answer when a remove matched nothing: 404, not a silent 200 that reads as done.
                return new Refusal(RestStatus.NOT_FOUND, "aliases_not_found_exception", "aliases [" + name + "] missing on " + indices);
            }
            if (now.isEmpty()) {
                // The last index left. An alias over nothing is worse than no alias, so it goes -- under the
                // generation this read saw, so a concurrent add is a lost swap here rather than a lost add.
                if (metadata.deleteAlias(before, resolved.generation())) {
                    return null;
                }
                continue;
            }
            if (metadata.updateAlias(before, MetadataPlane.withIndices(before, now).withWriteMember(writeMember), resolved.generation())
                .isPresent()) {
                return null;
            }
        }
        return new Refusal(RestStatus.CONFLICT, "version_conflict_engine_exception", "[" + name + "] is being changed concurrently; retry");
    }
}
