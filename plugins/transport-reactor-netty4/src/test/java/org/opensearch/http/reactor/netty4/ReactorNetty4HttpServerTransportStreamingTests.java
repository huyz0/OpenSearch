/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.reactor.SharedGroupFactory;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@link ReactorNetty4HttpServerTransport} class with streaming support.
 */
public class ReactorNetty4HttpServerTransportStreamingTests extends AbstractReactorNetty4HttpServerTransportStreamingTests {
    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;
    private ClusterSettings clusterSettings;

    @Before
    public void setup() {
        networkService = new NetworkService(Collections.emptyList());
        threadPool = new TestThreadPool("test");
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    }

    @After
    public void shutdown() {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        threadPool = null;
        networkService = null;
        bigArrays = null;
        clusterSettings = null;
    }

    public void testRequestResponseStreaming() throws InterruptedException {
        final String responseString = randomAlphaOfLength(4 * 1024);
        final String url = "/stream/";

        final ToXContent[] chunks = newChunks(responseString);
        final HttpServerTransport.Dispatcher dispatcher = createStreamingDispatcher(threadPool, url, responseString);

        try (
            ReactorNetty4HttpServerTransport transport = new ReactorNetty4HttpServerTransport(
                Settings.EMPTY,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                clusterSettings,
                new SharedGroupFactory(Settings.EMPTY),
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            try (ReactorHttpClient client = ReactorHttpClient.create(false, Settings.EMPTY)) {
                HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url);
                final FullHttpResponse response = client.stream(remoteAddress.address(), request, Arrays.stream(chunks));
                try {
                    assertThat(response.status(), equalTo(HttpResponseStatus.OK));
                    byte[] bytes = new byte[response.content().readableBytes()];
                    response.content().readBytes(bytes);
                    assertThat(new String(bytes, StandardCharsets.UTF_8), equalTo(Arrays.stream(newChunks(responseString)).map(s -> {
                        try (XContentBuilder builder = XContentType.JSON.contentBuilder()) {
                            return s.toXContent(builder, ToXContent.EMPTY_PARAMS).toString();
                        } catch (final IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    }).collect(Collectors.joining("\r\n", "", "\r\n"))));
                } finally {
                    response.release();
                }
            }
        }
    }

    public void testConnectionsGettingClosedForStreamingRequests() throws InterruptedException {
        final String responseString = randomAlphaOfLength(4 * 1024);
        final String url = "/stream/";

        final ToXContent[] chunks = newChunks(responseString);
        final HttpServerTransport.Dispatcher dispatcher = createStreamingDispatcher(threadPool, url, responseString);

        try (
            ReactorNetty4HttpServerTransport transport = new ReactorNetty4HttpServerTransport(
                Settings.EMPTY,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                clusterSettings,
                new SharedGroupFactory(Settings.EMPTY),
                NoopTracer.INSTANCE
            );
            ReactorHttpClient client = ReactorHttpClient.create(false, Settings.EMPTY)
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());
            HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url);
            long numRequests = randomLongBetween(5L, 15L);
            for (int i = 0; i < numRequests; i++) {
                logger.info("Sending request {}/{}", i + 1, numRequests);
                final FullHttpResponse response = client.stream(remoteAddress.address(), request, Arrays.stream(chunks));
                try {
                    assertThat(response.status(), equalTo(HttpResponseStatus.OK));
                } finally {
                    response.release();
                }
            }
            assertThat(transport.stats().getServerOpen(), equalTo(0L));
            assertThat(transport.stats().getTotalOpen(), equalTo(numRequests));
        }
    }

    /**
     * With demand-driven reads, a handler that asks for one chunk at a time, slowly, still gets every byte of a large body
     * in order, and answers with an ordinary response.
     */
    public void testDemandDrivenReadsDeliverEveryChunkToASlowHandler() throws Exception {
        slowHandler(true, org.opensearch.core.rest.RestStatus.OK);
    }

    /** With demand-driven reads, a streaming handler that has read the body can still answer with a status of its own. */
    public void testAStreamedRequestCanBeAnsweredWithAnError() throws Exception {
        slowHandler(true, org.opensearch.core.rest.RestStatus.BAD_REQUEST);
    }

    private void slowHandler(boolean demandDriven, org.opensearch.core.rest.RestStatus answer) throws Exception {
        final String url = "/stream/slow";
        final byte[] body = randomAlphaOfLength(4 * 1024 * 1024 + randomIntBetween(0, 1024)).getBytes(StandardCharsets.UTF_8);
        final java.util.concurrent.atomic.AtomicLong received = new java.util.concurrent.atomic.AtomicLong();
        final java.util.concurrent.atomic.AtomicInteger outstanding = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger maxOutstanding = new java.util.concurrent.atomic.AtomicInteger();
        final java.io.ByteArrayOutputStream copy = new java.io.ByteArrayOutputStream();
        final HttpServerTransport.Dispatcher dispatcher = org.opensearch.http.TestDispatcherBuilder.dispatcherBuilderWithDefaults()
            .withDispatchHandler((uri, rawPath, method, params) -> java.util.Optional.of(new org.opensearch.rest.RestHandler() {
                @Override
                public boolean supportsStreaming() {
                    return true;
                }

                @Override
                public void handleRequest(
                    org.opensearch.rest.RestRequest request,
                    org.opensearch.rest.RestChannel channel,
                    org.opensearch.transport.client.node.NodeClient client
                ) {
                    throw new AssertionError();
                }
            }))
            .withDispatchRequest((request, channel, threadContext) -> {
                final org.opensearch.rest.StreamingRestChannel streaming = (org.opensearch.rest.StreamingRestChannel) channel;
                streaming.subscribe(new reactor.core.publisher.BaseSubscriber<org.opensearch.http.HttpChunk>() {
                    @Override
                    protected void hookOnSubscribe(org.reactivestreams.Subscription subscription) {
                        outstanding.incrementAndGet();
                        subscription.request(1);
                    }

                    @Override
                    protected void hookOnNext(org.opensearch.http.HttpChunk chunk) {
                        maxOutstanding.accumulateAndGet(outstanding.decrementAndGet() + 1, Math::max);
                        try {
                            final org.apache.lucene.util.BytesRef ref = chunk.content().toBytesRef();
                            synchronized (copy) {
                                copy.write(ref.bytes, ref.offset, ref.length);
                            }
                            received.addAndGet(ref.length);
                        } finally {
                            chunk.close();
                        }
                        // Slow: the next chunk is asked for later, from another thread.
                        threadPool.schedule(() -> {
                            outstanding.incrementAndGet();
                            request(1);
                        }, org.opensearch.common.unit.TimeValue.timeValueMillis(1), ThreadPool.Names.GENERIC);
                    }

                    @Override
                    protected void hookOnComplete() {
                        streaming.sendResponse(
                            new org.opensearch.rest.BytesRestResponse(answer, "text/plain; charset=UTF-8", Long.toString(received.get()))
                        );
                    }
                });
            })
            .build();
        final Settings settings = Settings.builder()
            .put(ReactorNetty4HttpServerTransport.SETTING_STREAMING_DEMAND_DRIVEN.getKey(), true)
            .build();
        try (
            ReactorNetty4HttpServerTransport transport = new ReactorNetty4HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                clusterSettings,
                new SharedGroupFactory(settings),
                NoopTracer.INSTANCE
            );
            ReactorHttpClient client = ReactorHttpClient.create(false, Settings.EMPTY)
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());
            final io.netty.handler.codec.http.FullHttpRequest request = new io.netty.handler.codec.http.DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.POST,
                url
            );
            request.headers().add(io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE, "application/json");
            request.headers().add(io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH, body.length);
            final FullHttpResponse response = client.send(
                remoteAddress.address(),
                request,
                new io.netty.handler.codec.http.DefaultHttpContent(io.netty.buffer.Unpooled.wrappedBuffer(body))
            );
            try {
                assertThat(response.status().code(), equalTo(answer.getStatus()));
                assertThat(response.content().toString(StandardCharsets.UTF_8), equalTo(Integer.toString(body.length)));
            } finally {
                response.release();
            }
            assertArrayEquals("every byte, in order", body, copy.toByteArray());
            if (demandDriven) {
                assertThat("never more than the one chunk asked for", maxOutstanding.get(), equalTo(1));
            }
        }
    }
}
