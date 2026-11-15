/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Warming a search node's block cache from its peers: on join, on a hand-off from a draining member, and the
 * drain that hands its hot set on. Above {@code node}, which only knows that a node can be warmed.
 */
package org.opensearch.serverless.warm;
