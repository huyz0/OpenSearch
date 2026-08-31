/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TLS on the node's transport, supplied by a plugin.
 *
 * <p><b>Its own class because it needs its own task.</b> Netty's SSL handler opens the connection from
 * netty-common's own protection domain, which the test framework's security manager grants nothing — so
 * the handshake fails with a {@code SecurityException} about connecting to localhost, in a build where a
 * plain netty4 connection is fine. That is the framework's policy rather than anything about the shell,
 * and one test loses the manager rather than the whole module. The same trade {@code pluginTest} makes.
 */
public class ServerlessTlsTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-tls")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .build();
    }

    /**
     * A plugin that turns TLS on for the node's transport.
     *
     * <p>Core's netty4 module already ships {@code SecureNetty4Transport}; what it needs is somewhere to
     * get an {@code SSLEngine} from, and the only way a plugin can supply one is a
     * {@link org.opensearch.plugins.SecureSettingsFactory}. This is that, over a self-signed certificate
     * committed beside this test — the same shape the OpenSearch security plugin uses, with its
     * configuration replaced by one key.
     *
     * <p>The keystore is a fixture and nothing else: it is generated for {@code CN=localhost}, its password
     * is in this file, and it is exactly as secret as core's own {@code netty4-server-keystore}. Generating
     * one at test time would be better and does not work here — netty's self-signed generator needs
     * BouncyCastle, which this module does not have outside FIPS builds.
     */
    public static final class TlsPlugin extends Plugin implements org.opensearch.plugins.SecureSettingsFactory {

        private static final char[] PASSWORD = "testonly".toCharArray();

        private final io.netty.handler.ssl.ClientAuth clientAuth;
        private final boolean presentsACertificate;

        /** A plugin that encrypts and does not authenticate the peer. */
        public TlsPlugin() {
            this(io.netty.handler.ssl.ClientAuth.NONE, false);
        }

        /**
         * Creates the plugin.
         *
         * @param clientAuth what this node demands of a peer connecting to it
         * @param presentsACertificate whether this node offers one when it connects out
         */
        public TlsPlugin(io.netty.handler.ssl.ClientAuth clientAuth, boolean presentsACertificate) {
            this.clientAuth = clientAuth;
            this.presentsACertificate = presentsACertificate;
        }

        @Override
        public Settings additionalSettings() {
            return Settings.builder()
                .put("transport.type", org.opensearch.transport.Netty4ModulePlugin.NETTY_SECURE_TRANSPORT_NAME)
                .build();
        }

        @Override
        public java.util.Optional<org.opensearch.plugins.SecureSettingsFactory> getSecureSettingFactory(Settings settings) {
            return java.util.Optional.of(this);
        }

        private static javax.net.ssl.KeyManagerFactory keys() throws Exception {
            final java.security.KeyStore store = java.security.KeyStore.getInstance("PKCS12");
            try (java.io.InputStream in = TlsPlugin.class.getResourceAsStream("tls-test-keystore.p12")) {
                store.load(in, PASSWORD);
            }
            final var factory = javax.net.ssl.KeyManagerFactory.getInstance(javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
            factory.init(store, PASSWORD);
            return factory;
        }

        private static javax.net.ssl.TrustManagerFactory trust() throws Exception {
            final java.security.KeyStore store = java.security.KeyStore.getInstance("PKCS12");
            try (java.io.InputStream in = TlsPlugin.class.getResourceAsStream("tls-test-truststore.p12")) {
                store.load(in, PASSWORD);
            }
            final var factory = javax.net.ssl.TrustManagerFactory.getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
            factory.init(store);
            return factory;
        }

        @Override
        public java.util.Optional<org.opensearch.plugins.SecureTransportSettingsProvider> getSecureTransportSettingsProvider(
            Settings settings
        ) {
            return java.util.Optional.of(new org.opensearch.plugins.SecureTransportSettingsProvider() {
                @Override
                public java.util.Optional<org.opensearch.plugins.TransportExceptionHandler> buildServerTransportExceptionHandler(
                    Settings settings,
                    org.opensearch.transport.Transport transport
                ) {
                    return java.util.Optional.empty();
                }

                @Override
                public java.util.Optional<javax.net.ssl.SSLEngine> buildSecureServerTransportEngine(
                    Settings settings,
                    org.opensearch.transport.Transport transport
                ) throws javax.net.ssl.SSLException {
                    try {
                        // REQUIRE, not NONE: the peer has to present a certificate this node trusts, which
                        // is what turns "the traffic is encrypted" into "the peer is a node of this
                        // deployment". Without it the transport port is still open to anyone who can reach
                        // it, and encryption protects the wire from a bystander rather than the node from a
                        // caller.
                        final var context = io.netty.handler.ssl.SslContextBuilder.forServer(keys())
                            .trustManager(trust())
                            .clientAuth(clientAuth)
                            .build();
                        final javax.net.ssl.SSLEngine engine = context.newEngine(io.netty.buffer.ByteBufAllocator.DEFAULT);
                        engine.setUseClientMode(false);
                        return java.util.Optional.of(engine);
                    } catch (javax.net.ssl.SSLException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new javax.net.ssl.SSLException(e);
                    }
                }

                @Override
                public java.util.Optional<javax.net.ssl.SSLEngine> buildSecureClientTransportEngine(
                    Settings settings,
                    String hostname,
                    int port
                ) throws javax.net.ssl.SSLException {
                    try {
                        final var builder = io.netty.handler.ssl.SslContextBuilder.forClient().trustManager(trust());
                        if (presentsACertificate) {
                            builder.keyManager(keys());
                        }
                        final var context = builder.build();
                        final javax.net.ssl.SSLEngine engine = context.newEngine(io.netty.buffer.ByteBufAllocator.DEFAULT, hostname, port);
                        engine.setUseClientMode(true);
                        return java.util.Optional.of(engine);
                    } catch (javax.net.ssl.SSLException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new javax.net.ssl.SSLException(e);
                    }
                }
            });
        }

        @Override
        public java.util.Optional<org.opensearch.plugins.SecureHttpTransportSettingsProvider> getSecureHttpTransportSettingsProvider(
            Settings settings
        ) {
            return java.util.Optional.empty();
        }

        @Override
        public java.util.Optional<org.opensearch.plugins.SecureAuxTransportSettingsProvider> getSecureAuxTransportSettingsProvider(
            Settings settings
        ) {
            return java.util.Optional.empty();
        }
    }

    /**
     * A plugin can turn on TLS for the node's transport, and the nodes then talk over it.
     *
     * <p><b>This was impossible rather than undemonstrated.</b> The shell built its network module with an
     * empty collection of secure-settings factories, so core never called {@code getSecureTransports} and a
     * plugin naming the secure transport was told the type did not exist. That is the largest single piece
     * of what hosting OpenSearch Security requires — the part that protects the wire — and it was one
     * argument.
     *
     * <p>The test forwards a write between two nodes, because a transport that was selected and then
     * carried nothing would be a worse outcome than one that was never selected. Encryption itself is
     * core's {@code SecureNetty4Transport} doing its job; what is being proved here is that the shell lets
     * a plugin reach it.
     */
    public void testAPluginCanTurnOnTlsForTheTransport() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode owner = new ServerlessNode(nodeSettings("tls-owner"), List.of(new TlsPlugin()));
            ServerlessNode other = new ServerlessNode(nodeSettings("tls-other"), List.of(new TlsPlugin()))
        ) {
            owner.start();
            other.start();
            owner.setMetadataPlane(plane);
            other.setMetadataPlane(plane);
            owner.activateWriter(plane, "alpha", 0);
            other.syncFrom(plane);
            assertTrue("the other node must not own the shard", other.reconciler().openShards().isEmpty());

            // Forwarded over TLS: this node does not own the shard, so the write crosses the wire.
            other.client().index(new IndexRequest("alpha").id("k").source("{\"msg\":\"over tls\",\"n\":1}", XContentType.JSON)).actionGet();
            assertTrue("and it must actually carry traffic", other.client().get(new GetRequest("alpha", "k")).actionGet().isExists());
        }
    }

    /**
     * With mutual TLS the peer is authenticated, not merely encrypted to.
     *
     * <p><b>Why this is a different claim from the test above.</b> Encryption protects the wire from a
     * bystander. It does nothing about a caller who can reach the transport port — and the shell forwards
     * writes over that port and applies them without re-deciding, so anyone who can connect can write
     * anything. That is why the status document called the transport port trusted infrastructure.
     *
     * <p>Requiring a client certificate is what changes it, and it is a deployment's decision expressed
     * entirely in the provider a plugin supplies. The shell's part is only to let the plugin's provider
     * reach the transport, which is what M39 fixed; this shows the decision is actually available.
     */
    public void testMutualTlsAuthenticatesThePeer() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode owner = new ServerlessNode(nodeSettings("mtls-owner"), List.of(mutual()));
            ServerlessNode other = new ServerlessNode(nodeSettings("mtls-other"), List.of(mutual()))
        ) {
            owner.start();
            other.start();
            owner.setMetadataPlane(plane);
            other.setMetadataPlane(plane);
            owner.activateWriter(plane, "alpha", 0);
            other.syncFrom(plane);

            other.client().index(new IndexRequest("alpha").id("k").source("{\"msg\":\"mutual\",\"n\":1}", XContentType.JSON)).actionGet();
            assertTrue(
                "a node that presents a certificate is served",
                other.client().get(new GetRequest("alpha", "k")).actionGet().isExists()
            );
        }
    }

    /**
     * And a peer that presents none is refused, which is the half that makes the other half mean anything.
     *
     * <p>A test that only shows mutual TLS working would pass just as well against a server that had never
     * asked for a certificate.
     */
    public void testAPeerWithNoCertificateIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode owner = new ServerlessNode(nodeSettings("mtls-strict"), List.of(mutual()));
            ServerlessNode stranger = new ServerlessNode(nodeSettings("mtls-stranger"), List.of(new TlsPlugin()))
        ) {
            owner.start();
            stranger.start();
            owner.setMetadataPlane(plane);
            stranger.setMetadataPlane(plane);
            owner.activateWriter(plane, "alpha", 0);
            stranger.syncFrom(plane);

            final Exception refused = expectThrows(
                Exception.class,
                () -> stranger.client()
                    .index(new IndexRequest("alpha").id("k").source("{\"msg\":\"stranger\",\"n\":1}", XContentType.JSON))
                    .actionGet()
            );
            assertNotNull("the write must not have been accepted", refused);
            // And nothing was written: a refusal that still applied the write would be the worst outcome.
            assertFalse(
                "a refused peer must not have written anything",
                owner.client().get(new GetRequest("alpha", "k")).actionGet().isExists()
            );
        }
    }

    private static TlsPlugin mutual() {
        return new TlsPlugin(io.netty.handler.ssl.ClientAuth.REQUIRE, true);
    }
}
