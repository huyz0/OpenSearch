/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.common.concurrent.CompletableContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.http.HttpChunk;
import org.opensearch.http.HttpResponse;
import org.opensearch.http.StreamingHttpChannel;
import org.opensearch.transport.netty4.Netty4Utils;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

class ReactorNetty4StreamingHttpChannel implements StreamingHttpChannel {
    private final HttpServerRequest request;
    private final HttpServerResponse response;
    private final CompletableContext<Void> closeContext = new CompletableContext<>();
    private final Publisher<HttpChunk> receiver;
    private final StreamingHttpContentSender sender;
    private volatile FluxSink<HttpChunk> producer;
    private volatile boolean lastChunkReceived = false;

    ReactorNetty4StreamingHttpChannel(HttpServerRequest request, HttpServerResponse response, StreamingHttpContentSender sender) {
        this(request, response, sender, null);
    }

    /**
     * Creates a channel whose request chunks are read from the connection only as the handler asks for them.
     *
     * @param content the request's content, or null to have chunks pushed through {@link #receiveChunk} as they arrive.
     *     Read on demand, a handler that has not asked for more stops the socket being read, and TCP slows the client,
     *     rather than chunks queuing in memory without bound.
     */
    ReactorNetty4StreamingHttpChannel(
        HttpServerRequest request,
        HttpServerResponse response,
        StreamingHttpContentSender sender,
        Flux<HttpContent> content
    ) {
        this.request = request;
        this.response = response;
        this.sender = sender;
        if (content == null) {
            this.receiver = Flux.create(producer -> this.producer = producer);
            this.demandDriven = false;
        } else {
            // The chunk copies its bytes as it is made, inside onNext, so the buffer Reactor Netty releases after it is
            // no longer referenced. The last chunk ends the stream, whether it is the request's own or the one appended.
            this.receiver = content.concatWith(Mono.just(DefaultLastHttpContent.EMPTY_LAST_CONTENT))
                .map(chunk -> (HttpChunk) new ReactorNetty4HttpChunk(chunk.content(), chunk instanceof LastHttpContent))
                .takeUntil(HttpChunk::isLast);
            this.demandDriven = true;
        }
        this.request.withConnection(connection -> Netty4Utils.addListener(connection.channel().closeFuture(), closeContext));
    }

    private final boolean demandDriven;

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void close() {
        request.withConnection(connection -> {
            if (closeContext.isDone() == false) {
                Netty4Utils.addListener(connection.channel().close(), closeContext);
            } else {
                connection.channel().close();
            }
        });
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeContext.addListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public void sendChunk(HttpChunk chunk, ActionListener<Void> listener) {
        sender.send(createContent(chunk), listener, chunk.isLast());
    }

    @Override
    public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
        sender.send(createContent(response), listener, true);
    }

    @Override
    public void prepareResponse(int status, Map<String, List<String>> headers) {
        if (this.response.hasSentHeaders() == false) {
            this.response.status(status);
            headers.forEach((k, vs) -> vs.forEach(v -> this.response.addHeader(k, v)));
        }
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) response.remoteAddress();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) response.hostAddress();
    }

    @Override
    public void receiveChunk(HttpChunk message) {
        try {
            if (lastChunkReceived) {
                return;
            }

            producer.next(message);
            if (message.isLast()) {
                lastChunkReceived = true;
                producer.complete();
            }
        } catch (final Exception ex) {
            producer.error(ex);
        } finally {
            message.close();
        }
    }

    @Override
    public boolean isReadable() {
        return demandDriven || producer != null;
    }

    @Override
    public boolean isWritable() {
        return sender.isReady();
    }

    @Override
    public void subscribe(Subscriber<? super HttpChunk> subscriber) {
        receiver.subscribe(subscriber);
    }

    @Override
    public <T> Optional<T> get(String name, Class<T> clazz) {
        return ReactorNetty4BaseHttpChannel.get(request, name, clazz);
    }

    private static HttpContent createContent(HttpResponse response) {
        final FullHttpResponse fullHttpResponse = (FullHttpResponse) response;
        return new DefaultHttpContent(fullHttpResponse.content());
    }

    private static HttpContent createContent(HttpChunk chunk) {
        return new DefaultHttpContent(Unpooled.copiedBuffer(BytesReference.toByteBuffers(chunk.content())));
    }
}
