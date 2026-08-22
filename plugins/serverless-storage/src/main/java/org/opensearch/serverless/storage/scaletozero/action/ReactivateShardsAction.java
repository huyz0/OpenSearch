/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero.action;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;

/** Reactivates every suspended shard of one index -- see {@link ReactivateShardsRequest}'s own javadoc. */
public final class ReactivateShardsAction extends ActionType<AcknowledgedResponse> {

    /** The singleton instance every caller uses. */
    public static final ReactivateShardsAction INSTANCE = new ReactivateShardsAction();

    /** The transport action name this is registered under. */
    public static final String NAME = "cluster:admin/serverless_storage/scale_to_zero/reactivate";

    private ReactivateShardsAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
