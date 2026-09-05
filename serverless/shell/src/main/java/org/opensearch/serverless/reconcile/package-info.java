/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Background reconciliation: the small idempotent loops that replace a privileged serializer.
 *
 * <p>{@code rfc-serverless-metadata-plane.md} section 6 describes control logic here as many stateless
 * workers acting through the same compare-and-swap protocol as everyone else, rather than one node with
 * the authority to decide. Nothing in this package is privileged; every loop is safe to run on any
 * number of nodes at once, and safe to not run at all.
 */
package org.opensearch.serverless.reconcile;
