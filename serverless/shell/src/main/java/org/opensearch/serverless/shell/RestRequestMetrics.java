/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.ExceptionsHelper;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.http.HttpChunk;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.StreamingRestChannel;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;

import org.reactivestreams.Subscriber;

/** Measures requests that reach a registered REST handler, including its authentication wrapper. */
final class RestRequestMetrics {
    private final Counter responses;
    private final Histogram responseTime;

    RestRequestMetrics(MetricsRegistry registry) {
        responses = registry.createCounter("opensearch.serverless.rest.responses", "Outcomes of dispatched REST requests", "1");
        responseTime = registry.createHistogram(
            "opensearch.serverless.rest.response_time",
            "Elapsed time from REST handler dispatch to response headers or a handler exception",
            "ms"
        );
    }

    RestHandler wrap(RestHandler handler, UnaryOperator<RestHandler> authWrapper) {
        RestHandler authenticated = authWrapper == null ? handler : authWrapper.apply(handler);
        return new RestHandler.Wrapper(authenticated) {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                ResponseObservation observation = new ResponseObservation(request.method());
                RestChannel measured = channel instanceof StreamingRestChannel streaming
                    ? new MeasuredStreamingChannel(streaming, observation)
                    : new MeasuredChannel(channel, observation);
                try {
                    super.handleRequest(request, measured, client);
                } catch (Exception exception) {
                    observation.record(ExceptionsHelper.status(exception));
                    throw exception;
                }
            }
        };
    }

    private final class ResponseObservation {
        private final RestRequest.Method method;
        private final long started = System.nanoTime();
        private final AtomicBoolean recorded = new AtomicBoolean();

        private ResponseObservation(RestRequest.Method method) {
            this.method = method;
        }

        private void record(RestStatus status) {
            if (recorded.compareAndSet(false, true)) {
                Tags tags = Tags.of("method", method.name()).addTag("status", status.getStatus());
                responses.add(1, tags);
                responseTime.record((System.nanoTime() - started) / (double) TimeUnit.MILLISECONDS.toNanos(1), tags);
            }
        }
    }

    private static class MeasuredChannel implements RestChannel {
        private final RestChannel delegate;
        private final ResponseObservation observation;

        private MeasuredChannel(RestChannel delegate, ResponseObservation observation) {
            this.delegate = delegate;
            this.observation = observation;
        }

        @Override
        public XContentBuilder newBuilder() throws IOException {
            return delegate.newBuilder();
        }

        @Override
        public XContentBuilder newErrorBuilder() throws IOException {
            return delegate.newErrorBuilder();
        }

        @Override
        public XContentBuilder newBuilder(MediaType mediaType, boolean useFiltering) throws IOException {
            return delegate.newBuilder(mediaType, useFiltering);
        }

        @Override
        public XContentBuilder newBuilder(MediaType mediaType, MediaType responseContentType, boolean useFiltering) throws IOException {
            return delegate.newBuilder(mediaType, responseContentType, useFiltering);
        }

        @Override
        public BytesStreamOutput bytesOutput() {
            return delegate.bytesOutput();
        }

        @Override
        public RestRequest request() {
            return delegate.request();
        }

        @Override
        public boolean detailedErrorsEnabled() {
            return delegate.detailedErrorsEnabled();
        }

        @Override
        public boolean detailedErrorStackTraceEnabled() {
            return delegate.detailedErrorStackTraceEnabled();
        }

        @Override
        public void sendResponse(RestResponse response) {
            observation.record(response.status());
            delegate.sendResponse(response);
        }
    }

    private static final class MeasuredStreamingChannel extends MeasuredChannel implements StreamingRestChannel {
        private final StreamingRestChannel streaming;
        private final ResponseObservation observation;

        private MeasuredStreamingChannel(StreamingRestChannel streaming, ResponseObservation observation) {
            super(streaming, observation);
            this.streaming = streaming;
            this.observation = observation;
        }

        @Override
        public void sendChunk(HttpChunk chunk) {
            streaming.sendChunk(chunk);
        }

        @Override
        public void prepareResponse(RestStatus status, Map<String, List<String>> headers) {
            observation.record(status);
            streaming.prepareResponse(status, headers);
        }

        @Override
        public boolean isReadable() {
            return streaming.isReadable();
        }

        @Override
        public boolean isWritable() {
            return streaming.isWritable();
        }

        @Override
        public void subscribe(Subscriber<? super HttpChunk> subscriber) {
            streaming.subscribe(subscriber);
        }
    }
}
