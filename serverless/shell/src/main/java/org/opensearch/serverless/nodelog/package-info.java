/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * The per-node write log: one log per node, never per shard, written in epochs of numbered blobs.
 *
 * <p>The committer groups whatever the node logs in one window into one PUT, records each new epoch in the node's
 * lease before writing it, confirms a shard's ownership before its first records go into an epoch, and tracks what
 * each shard has not yet published so complete epochs can be deleted without a listing. A successor fences and
 * replays a dead or departed node's epochs through the takeover reader. The blob format carries every shard's runs
 * and the trim point, so cleanup is derived from the header rather than discovered.
 */
package org.opensearch.serverless.nodelog;
