/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex.action;

import org.opensearch.action.ActionType;

/** Resolves index expressions against a node's name index rather than against cluster state. */
public class ResolveIndexNamesAction extends ActionType<ResolveIndexNamesResponse> {

    public static final ResolveIndexNamesAction INSTANCE = new ResolveIndexNamesAction();
    public static final String NAME = "cluster:admin/serverless_storage/name_index/resolve";

    private ResolveIndexNamesAction() {
        super(NAME, ResolveIndexNamesResponse::new);
    }
}
