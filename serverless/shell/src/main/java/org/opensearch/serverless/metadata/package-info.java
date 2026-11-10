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
 * <p>This is the shell's truth layer. Every mutation is a CAS on
 * one small blob, and there is no consensus process anywhere in the system. A CAS register has
 * consensus number infinity, so this gives up no safety relative to Raft; what it gives up is change
 * notification, multi-object atomicity and cheap linearizable reads, which sections 9.5 and 9.6 cost
 * out rather than wave away.
 *
 * <p><b>Which stores this is verified on.</b> The blob-store conformance suite runs concurrent CAS
 * contenders and asserts exactly one winner. It has run against {@code FsBlobContainer} and S3-compatible
 * emulators, not against AWS S3, GCS or Azure. Passing on an emulator proves nothing about a provider's
 * conditional write, so this package carries no durability claim on S3, GCS or Azure.
 */
package org.opensearch.serverless.metadata;
