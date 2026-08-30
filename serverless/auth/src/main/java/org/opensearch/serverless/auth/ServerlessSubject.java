/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.auth;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.UserSubject;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.identity.tokens.BasicAuthToken;

import java.security.Principal;

/**
 * Who is making the current request, as core's identity API describes it.
 *
 * <p>This is what any plugin gets from {@code IdentityService#getCurrentSubject()}, and it is the reason
 * the shell's authentication is expressed through core's SPI rather than as something of its own: a plugin
 * asking who the caller is should not have to know which authentication plugin is installed.
 *
 * <p><b>The principal is read at the moment it is asked for, not captured.</b> A subject built once and
 * handed around would answer with whoever was making a request when it happened to be constructed, which
 * on a shared thread pool is a different person from the one asking.
 *
 * <p><b>No identity is {@code UNAUTHENTICATED}, and that is the same answer core gives.</b> Work that did
 * not come in through the REST layer — a reconciler tick, a forwarded shard operation — has no caller, and
 * saying so is more useful than inventing a system identity that authorization would later have to
 * special-case.
 */
public final class ServerlessSubject implements UserSubject {

    private final ThreadContext threadContext;
    private final CredentialStore store;

    /**
     * Creates the subject.
     *
     * @param threadContext where the request's principal was left, which may be null before the node has
     *     finished starting
     * @param store the accounts, for {@link #authenticate(AuthToken)}
     */
    ServerlessSubject(ThreadContext threadContext, CredentialStore store) {
        this.threadContext = threadContext;
        this.store = store;
    }

    @Override
    public Principal getPrincipal() {
        if (threadContext == null) {
            return NamedPrincipal.UNAUTHENTICATED;
        }
        final String principal = threadContext.getTransient(ServerlessAuthPlugin.PRINCIPAL);
        return principal == null ? NamedPrincipal.UNAUTHENTICATED : new NamedPrincipal(principal);
    }

    /**
     * Checks a credential and, if it is good, makes it this thread's identity.
     *
     * <p>Core's contract is that this throws on failure rather than returning a verdict, so a caller who
     * ignores the result cannot proceed as though it had passed. That shape is worth keeping exactly:
     * authentication that can be accidentally ignored is authentication that will be.
     *
     * @param token the credential, which must be a {@link BasicAuthToken}
     */
    @Override
    public void authenticate(AuthToken token) {
        if (token instanceof BasicAuthToken basic) {
            final char[] password = basic.getPassword() == null ? new char[0] : basic.getPassword().toCharArray();
            final CredentialStore.Verdict verdict;
            try {
                verdict = store.verify(basic.getUser(), password);
            } finally {
                java.util.Arrays.fill(password, '\0');
            }
            if (verdict.unavailable() != null) {
                throw new OpenSearchSecurityException(verdict.unavailable(), RestStatus.SERVICE_UNAVAILABLE);
            }
            if (verdict.authenticated() == false) {
                throw new OpenSearchSecurityException("the credentials offered were not accepted", RestStatus.UNAUTHORIZED);
            }
            if (threadContext != null && threadContext.getTransient(ServerlessAuthPlugin.PRINCIPAL) == null) {
                threadContext.putTransient(ServerlessAuthPlugin.PRINCIPAL, verdict.principal());
            }
            return;
        }
        throw new OpenSearchSecurityException(
            "this node authenticates with HTTP Basic; ["
                + (token == null ? "null" : token.getClass().getSimpleName())
                + "] is not a credential it can check",
            RestStatus.UNAUTHORIZED
        );
    }
}
