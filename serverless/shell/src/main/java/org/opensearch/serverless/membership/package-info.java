/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Membership as a derived view over node leases.
 *
 * <p>Nothing here is authoritative about shard ownership; that is answered per-shard by compare-and-swap
 * on the shard-head. Two nodes disagreeing about who exists is the normal state of the system.
 */
package org.opensearch.serverless.membership;
