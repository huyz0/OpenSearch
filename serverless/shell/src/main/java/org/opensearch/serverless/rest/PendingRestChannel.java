/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/** Holds a response until action filters finish, using the HTTP channel's accounted output buffers. */
final class PendingRestChannel implements RestChannel {
    private final RestChannel delegate;
    private final AtomicReference<RestResponse> pending = new AtomicReference<>();

    PendingRestChannel(RestChannel delegate) {
        this.delegate = delegate;
    }

    RestResponse response() {
        final RestResponse response = pending.get();
        if (response == null) {
            throw new IllegalStateException("the operation did not produce a response");
        }
        return response;
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
        if (pending.compareAndSet(null, response) == false) {
            throw new IllegalStateException("the operation produced more than one response");
        }
    }
}
