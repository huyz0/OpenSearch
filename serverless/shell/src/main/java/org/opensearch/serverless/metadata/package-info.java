/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * The metadata plane: object-store registers, arbitrated by compare-and-swap.
 *
 * <p>This is the truth layer of {@code rfc-serverless-shell.md} section 9. Every mutation is a CAS on
 * one small blob, and there is no consensus process anywhere in the system. A CAS register has
 * consensus number infinity, so this gives up no safety relative to Raft; what it gives up is change
 * notification, multi-object atomicity and cheap linearizable reads, which sections 9.5 and 9.6 cost
 * out rather than wave away.
 *
 * <p><b>Decision D5.</b> The R11 conformance suite — concurrent CAS contenders against real S3 and GCS,
 * asserting exactly one winner — is deferred. Everything here is exercised against
 * {@code FsBlobContainer} only, and passing there proves nothing about a provider's conditional write.
 * Until R11 runs, this package carries no durability claim on S3, GCS or Azure.
 */
package org.opensearch.serverless.metadata;
