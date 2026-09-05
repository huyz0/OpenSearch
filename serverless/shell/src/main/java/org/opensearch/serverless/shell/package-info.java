/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * The serverless node shell: lifecycle, wiring and node-local cluster state.
 *
 * <p>This package constructs the OpenSearch data plane without a control plane, and drives it from a
 * view each node computes for itself rather than from a consensus-published one. See
 * {@code rfc-serverless-shell.md}.
 */
package org.opensearch.serverless.shell;
