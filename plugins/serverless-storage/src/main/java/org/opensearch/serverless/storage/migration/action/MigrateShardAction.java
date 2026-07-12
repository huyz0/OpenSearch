/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.migration.action;

import org.opensearch.action.ActionType;

/**
 * Adopts a real, currently-open, locally-recovered shard's current Lucene commit into serverless
 * storage in place (rfc-serverless-opensearch.md &sect;16 Phase 6), closing the gap {@link
 * org.opensearch.serverless.storage.migration.ClassicIndexMigrator}'s own javadoc named: "resolve a
 * real, currently-open {@code IndexShard}'s local {@code Store} from a shard id alone." {@link
 * MigrateShardRequest} names the shard; {@link TransportMigrateShardAction} does the actual work,
 * reusing {@code ClassicIndexMigrator#migrate} unchanged once it has the {@code Directory}/{@code
 * SegmentInfos} pair in hand.
 */
public class MigrateShardAction extends ActionType<MigrateShardResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final MigrateShardAction INSTANCE = new MigrateShardAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/migration/migrate_shard";

    private MigrateShardAction() {
        super(NAME, MigrateShardResponse::new);
    }
}
