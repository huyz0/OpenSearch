/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Engine-independent identity, framing, persistence, and replication for activated operation logs.
 * The shell owns document semantics; this module must not depend on the shell or search engine.
 */
package org.opensearch.serverless.log;
