/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * REST and transport action exposing a cluster-wide, read-only scale-up candidate policy
 * evaluation -- the "policy" half of the RFC's scale-up autoscaling subsection, joining every
 * node's reader-engine query-rate signal with cluster metadata's current search-replica count.
 * Mirrors {@code org.opensearch.serverless.storage.scaletozero.action}'s own "policy, not
 * mechanism" split; see {@link org.opensearch.serverless.storage.scaleup.ReaderReplicaExpansionCoordinator}
 * for the mechanism half.
 */
package org.opensearch.serverless.storage.scaleup.action;
