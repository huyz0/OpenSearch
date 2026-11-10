/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * The shell's REST surface.
 *
 * <p>This is an explicit allowlist, not whatever happens to route: an endpoint that is not implemented
 * returns 501 saying so, and never
 * an empty success. A "confident empty answer" from a node that
 * cannot see the data is what that rule exists to prevent.
 */
package org.opensearch.serverless.rest;
