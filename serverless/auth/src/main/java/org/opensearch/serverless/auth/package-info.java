/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Authentication for the serverless shell, written as a plugin against the shell's plugin host.
 *
 * <p>This module depends on {@code :server} and not on {@code :serverless:shell}, which is the whole of
 * its argument: everything it uses is API that any third-party plugin has. If it needed more than that,
 * the host would not be a host.
 */
package org.opensearch.serverless.auth;
