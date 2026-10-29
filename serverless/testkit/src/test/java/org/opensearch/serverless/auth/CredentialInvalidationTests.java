/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.auth;

import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CredentialInvalidationTests extends OpenSearchTestCase {
    private static class Accounts {
        final AtomicReference<String> record = new AtomicReference<>(PasswordHash.encode("old-password".toCharArray(), 10_000));
        final AtomicReference<Runnable> afterRead = new AtomicReference<>();
        final AtomicBoolean loseWriteResponse = new AtomicBoolean();
        final AtomicLong clock = new AtomicLong(1_000L);
        final AtomicLong marker = new AtomicLong();
        final CredentialStore store;

        Accounts() {
            final Client client = mock(Client.class);
            final MockSecureSettings secure = new MockSecureSettings();
            secure.setString("serverless.auth.bootstrap.password", "configured-password");
            final Settings settings = Settings.builder().put("serverless.auth.hash.iterations", 10_000).setSecureSettings(secure).build();
            store = new CredentialStore(settings, () -> client, clock::get);
            when(client.get(any(GetRequest.class))).thenAnswer(invocation -> {
                final GetRequest request = invocation.getArgument(0);
                final String snapshot = record.get();
                final GetResponse response = mock(GetResponse.class);
                final long markerSnapshot = marker.get();
                when(response.isExists()).thenReturn(
                    CredentialStore.MARKER.equals(request.id()) ? markerSnapshot != 0L : "user".equals(request.id()) && snapshot != null
                );
                when(response.getSource()).thenAnswer(ignored -> {
                    if (CredentialStore.MARKER.equals(request.id())) {
                        return Map.of("marker", markerSnapshot);
                    }
                    final Runnable hook = afterRead.getAndSet(null);
                    if (hook != null) {
                        hook.run();
                    }
                    return Map.of("hash", snapshot);
                });
                final PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
                future.onResponse(response);
                return future;
            });
            when(client.delete(any(DeleteRequest.class))).thenAnswer(invocation -> {
                record.set(null);
                final DeleteResponse response = mock(DeleteResponse.class);
                when(response.getResult()).thenReturn(org.opensearch.action.DocWriteResponse.Result.DELETED);
                final PlainActionFuture<DeleteResponse> future = PlainActionFuture.newFuture();
                future.onResponse(response);
                return future;
            });
            when(client.index(any(IndexRequest.class))).thenAnswer(invocation -> {
                final IndexRequest request = invocation.getArgument(0);
                final PlainActionFuture<IndexResponse> future = PlainActionFuture.newFuture();
                if ("user".equals(request.id())) {
                    record.set(request.sourceAsMap().get("hash").toString());
                    if (loseWriteResponse.get()) {
                        future.onFailure(new OpenSearchTimeoutException("the write landed but its response was lost"));
                        return future;
                    }
                }
                future.onResponse(null);
                return future;
            });
        }
    }

    public void testAnOldVerificationCannotRepopulateARemovedAccountsCache() {
        final Accounts accounts = new Accounts();
        accounts.afterRead.set(() -> {
            try {
                assertTrue(accounts.store.remove("user"));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        assertTrue(accounts.store.verify("user", "old-password".toCharArray()).authenticated());
        assertFalse(
            "an in-flight check must not cache a record removed during its lookup",
            accounts.store.isKnown("user", "old-password".toCharArray())
        );
        assertFalse(accounts.store.verify("user", "old-password".toCharArray()).authenticated());
    }

    public void testAnOldVerificationCannotRepopulateAfterAMarkerChange() {
        final Accounts accounts = new Accounts();
        accounts.afterRead.set(() -> {
            accounts.record.set(null);
            accounts.marker.set(2L);
            accounts.clock.addAndGet(CredentialStore.MARKER_INTERVAL_MILLIS);
            assertTrue(accounts.store.verify("admin", "configured-password".toCharArray()).authenticated());
        });
        assertTrue(accounts.store.verify("user", "old-password".toCharArray()).authenticated());
        assertFalse(accounts.store.isKnown("user", "old-password".toCharArray()));
        assertFalse(accounts.store.verify("user", "old-password".toCharArray()).authenticated());
    }

    public void testAnAmbiguousPasswordChangeEvictsTheOldCredential() {
        final Accounts accounts = new Accounts();
        assertTrue(accounts.store.verify("user", "old-password".toCharArray()).authenticated());
        accounts.loseWriteResponse.set(true);
        expectThrows(OpenSearchTimeoutException.class, () -> accounts.store.put("user", "new-password".toCharArray()));
        assertFalse(
            "a failed response does not prove the password stayed unchanged",
            accounts.store.isKnown("user", "old-password".toCharArray())
        );
        assertFalse(accounts.store.verify("user", "old-password".toCharArray()).authenticated());
        assertTrue(accounts.store.verify("user", "new-password".toCharArray()).authenticated());
    }
}
