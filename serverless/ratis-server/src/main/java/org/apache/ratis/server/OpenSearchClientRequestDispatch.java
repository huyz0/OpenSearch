/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.ratis.server;

import org.opensearch.common.annotation.InternalApi;

/**
 * Opt-in contract for an embedding host that already submits client requests on a bounded executor
 * and detaches application completions from consensus locks. The proxy can execute its submission
 * directly there, avoiding the SDK's client-executor handoff and join. This is necessary when a
 * removed leader closes its division under the consensus monitor: waiting for a client worker
 * that needs that same monitor deadlocks. Non-opted-in state machines retain upstream dispatch.
 */
@InternalApi
public interface OpenSearchClientRequestDispatch {}
