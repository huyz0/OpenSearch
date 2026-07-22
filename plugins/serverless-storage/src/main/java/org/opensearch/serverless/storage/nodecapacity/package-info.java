/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Node-level (fleet size) autoscaling signal -- deliberately separate from {@code scaletozero}/{@code
 * scaleup}, which redistribute shard copies among nodes that already exist. This package only ever
 * computes and reports; it never adds, removes, or drains a node itself (see
 * docs-site/src/content/docs/design/node-autoscaling.md for the full design and why the actual
 * scaling decision belongs to an external control plane, not this plugin).
 */
package org.opensearch.serverless.storage.nodecapacity;
