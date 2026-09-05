/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

/**
 * The shard-head names an owner that holds no live lease.
 *
 * <p>Precisely a dead writer nobody has noticed yet, and the request that hit this is the first thing in
 * the system to prove it. Nothing was sent, so the request is safe to forward to whoever the register
 * names next; {@link ForwardFailure#classify(Throwable)} files it under {@link ForwardFailure#UNREACHABLE}.
 */
public final class OwnerUnreachableException extends RuntimeException {

    /**
     * Creates the failure.
     *
     * @param message which shard, and which owner
     */
    public OwnerUnreachableException(String message) {
        super(message);
    }
}
