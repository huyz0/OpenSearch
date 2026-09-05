/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Segment publication to the object store.
 *
 * <p>This is where "the object store is the only source of truth" stops being a slogan. A shard's
 * committed segments are uploaded under a term-scoped prefix and named by a manifest register; a node
 * taking over the shard restores from that manifest before opening it.
 *
 * <p>The term-scoped prefix is the fencing mechanism of {@code rfc-serverless-shell.md} section 9.6:
 * a writer at term T writes under {@code t=T/}, so a zombie at an older term writes to a prefix no
 * reader consults. Its bytes are inert rather than corrupting, and garbage collection reclaims them.
 */
package org.opensearch.serverless.store;
