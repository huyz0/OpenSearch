/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.SuppressForbidden;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * An HTTP/1.1 proxy in front of an S3 API that can make the store slow, throttling, or unreachable -- for one
 * node, since each node gets its own.
 *
 * <p>Requests pass through byte for byte, so a signed request stays valid: the {@code Host} header the client
 * signed is the one the store receives. Three faults, switchable at any time:
 * <ul>
 *   <li><b>latency</b>: every request waits before it is forwarded;</li>
 *   <li><b>slowdown</b>: a fraction of requests are answered {@code 503 SlowDown} with S3's own error body, and
 *   never reach the store -- what a throttled bucket does;</li>
 *   <li><b>partition</b>: every connection is closed and every new one refused -- the node can reach its peers
 *   and its clients, and not its store.</li>
 * </ul>
 * And it counts what passes, by operation, which is how a fleet's request mix is measured per node without
 * trusting the node's own accounting.
 */
final class FaultProxy implements Closeable {

    /** A generator per connection thread; these are not test threads, so the framework's randomness is out of reach. */
    private final ThreadLocal<java.util.Random> random = ThreadLocal.withInitial(() -> new java.util.Random(System.nanoTime()));

    private final String name;
    private final InetSocketAddress upstream;
    private final ServerSocket server;
    private final Thread acceptor;
    private final Set<Socket> open = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    private volatile long latencyMillis;
    private volatile double slowdownFraction;
    private volatile boolean partitioned;

    private final Map<String, AtomicLong> requests = new ConcurrentHashMap<>();
    /** The same requests by operation and by what they touch -- descriptors, heads, the log -- for a cost breakdown. */
    private final Map<String, AtomicLong> byArea = new ConcurrentHashMap<>();
    private final AtomicLong injected = new AtomicLong();
    private final AtomicLong refused = new AtomicLong();

    FaultProxy(String name, String upstreamHost, int upstreamPort) throws IOException {
        this.name = name;
        this.upstream = new InetSocketAddress(InetAddress.getByName(upstreamHost), upstreamPort);
        this.server = new ServerSocket(0, 256, InetAddress.getLoopbackAddress());
        this.acceptor = new Thread(this::accept, "fault-proxy-" + name);
        this.acceptor.setDaemon(true);
        this.acceptor.start();
    }

    /** The endpoint a node should be pointed at instead of the store. */
    String endpoint() {
        return "http://127.0.0.1:" + server.getLocalPort();
    }

    void setLatencyMillis(long millis) {
        this.latencyMillis = Math.max(0L, millis);
    }

    void setSlowdownFraction(double fraction) {
        this.slowdownFraction = Math.max(0d, Math.min(1d, fraction));
    }

    /** Cuts this node off from its store: every connection now open is closed, and every new one refused. */
    void partition() {
        partitioned = true;
        for (Socket socket : open) {
            closeQuietly(socket);
        }
    }

    void heal() {
        partitioned = false;
    }

    /** Requests forwarded, by operation, since the last {@link #resetCounts}. */
    Map<String, Long> counts() {
        final Map<String, Long> snapshot = new TreeMap<>();
        requests.forEach((op, n) -> snapshot.put(op, n.get()));
        return snapshot;
    }

    /** Requests forwarded, by operation and area, since the last {@link #resetCounts}. */
    Map<String, Long> countsByArea() {
        final Map<String, Long> snapshot = new TreeMap<>();
        byArea.forEach((op, n) -> snapshot.put(op, n.get()));
        return snapshot;
    }

    long injected() {
        return injected.get();
    }

    long refused() {
        return refused.get();
    }

    void resetCounts() {
        requests.clear();
        byArea.clear();
        injected.set(0);
        refused.set(0);
    }

    private void accept() {
        while (closed == false) {
            final Socket client;
            try {
                client = server.accept();
            } catch (IOException e) {
                if (closed) {
                    return;
                }
                continue;
            }
            if (partitioned) {
                refused.incrementAndGet();
                closeQuietly(client);
                continue;
            }
            open.add(client);
            final Thread worker = new Thread(() -> serve(client), "fault-proxy-" + name + "-conn");
            worker.setDaemon(true);
            worker.start();
        }
    }

    /** One client connection, paired with one upstream connection, request after request. */
    @SuppressForbidden(reason = "a proxy in front of the object store is a socket client by definition")
    private void serve(Socket client) {
        Socket store = null;
        try {
            client.setTcpNoDelay(true);
            final InputStream fromClient = new BufferedInputStream(client.getInputStream(), 65_536);
            final OutputStream toClient = client.getOutputStream();
            InputStream fromStore = null;
            OutputStream toStore = null;
            while (closed == false && partitioned == false) {
                final Head request = Head.read(fromClient);
                if (request == null) {
                    return;
                }
                final String op = classify(request);
                byArea.computeIfAbsent(op + " " + area(request.target()), k -> new AtomicLong()).incrementAndGet();
                final long delay = latencyMillis;
                if (delay > 0) {
                    LockSupport.parkNanos(delay * 1_000_000L);
                }
                if (partitioned) {
                    return;
                }
                if (slowdownFraction > 0 && random.get().nextDouble() < slowdownFraction) {
                    injected.incrementAndGet();
                    slowDown(toClient);
                    return;
                }
                requests.computeIfAbsent(op, k -> new AtomicLong()).incrementAndGet();
                if (store == null) {
                    store = new Socket();
                    store.connect(upstream, 10_000);
                    store.setTcpNoDelay(true);
                    open.add(store);
                    fromStore = new BufferedInputStream(store.getInputStream(), 65_536);
                    toStore = store.getOutputStream();
                }
                toStore.write(request.raw);
                final boolean expectsContinue = "100-continue".equalsIgnoreCase(request.header("expect"));
                Head response;
                if (expectsContinue) {
                    toStore.flush();
                    response = Head.read(fromStore);
                    if (response == null) {
                        return;
                    }
                    if (response.status() == 100) {
                        toClient.write(response.raw);
                        toClient.flush();
                        copyBody(request, fromClient, toStore, false);
                        toStore.flush();
                        response = Head.read(fromStore);
                    } else {
                        // The store answered without the body: relay it and give up the connection, since the
                        // client may or may not send the body now.
                        relay(request, response, fromStore, toClient);
                        return;
                    }
                } else {
                    copyBody(request, fromClient, toStore, false);
                    toStore.flush();
                    response = Head.read(fromStore);
                }
                while (response != null && response.status() >= 100 && response.status() < 200) {
                    toClient.write(response.raw);
                    response = Head.read(fromStore);
                }
                if (response == null) {
                    return;
                }
                if (relay(request, response, fromStore, toClient) == false) {
                    return;
                }
            }
        } catch (IOException e) {
            // A closed socket at either end is how a connection ends here, fault or not.
        } finally {
            closeQuietly(client);
            open.remove(client);
            if (store != null) {
                closeQuietly(store);
                open.remove(store);
            }
        }
    }

    /** Relays a response; false when the connection cannot carry another request. */
    private static boolean relay(Head request, Head response, InputStream fromStore, OutputStream toClient) throws IOException {
        toClient.write(response.raw);
        final int status = response.status();
        final boolean bodiless = "HEAD".equals(request.method()) || status == 204 || status == 304;
        boolean reusable = "close".equalsIgnoreCase(response.header("connection")) == false;
        if (bodiless == false) {
            reusable &= copyBody(response, fromStore, toClient, true);
        }
        toClient.flush();
        return reusable;
    }

    /** Copies a message's body; returns false if it was delimited by the connection closing. */
    private static boolean copyBody(Head head, InputStream in, OutputStream out, boolean untilCloseAllowed) throws IOException {
        if ("chunked".equalsIgnoreCase(head.header("transfer-encoding"))) {
            while (true) {
                final String sizeLine = readLine(in);
                out.write((sizeLine + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
                final String hex = sizeLine.contains(";") ? sizeLine.substring(0, sizeLine.indexOf(';')) : sizeLine;
                final long size = Long.parseLong(hex.trim(), 16);
                if (size == 0) {
                    // Trailers, then the blank line.
                    while (true) {
                        final String trailer = readLine(in);
                        out.write((trailer + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
                        if (trailer.isEmpty()) {
                            return true;
                        }
                    }
                }
                copyExactly(in, out, size);
                final String crlf = readLine(in);
                out.write((crlf + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
            }
        }
        final String length = head.header("content-length");
        if (length != null) {
            copyExactly(in, out, Long.parseLong(length.trim()));
            return true;
        }
        if (untilCloseAllowed && head.isResponse()) {
            in.transferTo(out);
            return false;
        }
        return true;
    }

    private static void copyExactly(InputStream in, OutputStream out, long length) throws IOException {
        final byte[] buffer = new byte[65_536];
        long left = length;
        while (left > 0) {
            final int n = in.read(buffer, 0, (int) Math.min(buffer.length, left));
            if (n < 0) {
                throw new EOFException("body ended " + left + " bytes early");
            }
            out.write(buffer, 0, n);
            left -= n;
        }
    }

    private static String readLine(InputStream in) throws IOException {
        final ByteArrayOutputStream line = new ByteArrayOutputStream(64);
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') {
                break;
            }
            if (b != '\r') {
                line.write(b);
            }
        }
        if (b == -1 && line.size() == 0) {
            throw new EOFException();
        }
        return line.toString(StandardCharsets.ISO_8859_1);
    }

    private static void slowDown(OutputStream toClient) throws IOException {
        final byte[] body = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>SlowDown</Code>"
            + "<Message>Please reduce your request rate.</Message><RequestId>fault-proxy</RequestId></Error>").getBytes(
                StandardCharsets.UTF_8
            );
        final String head = "HTTP/1.1 503 Slow Down\r\nContent-Type: application/xml\r\nContent-Length: "
            + body.length
            + "\r\nConnection: close\r\n\r\n";
        toClient.write(head.getBytes(StandardCharsets.ISO_8859_1));
        toClient.write(body);
        toClient.flush();
    }

    /** An operation name for counting: the method, a listing told apart from a read, a batch delete from a write. */
    private static String classify(Head request) {
        final String target = request.target();
        final String method = request.method();
        if ("GET".equals(method) && (target.contains("list-type=") || target.contains("?prefix=") || target.contains("&prefix="))) {
            return "LIST";
        }
        if ("POST".equals(method) && target.contains("delete")) {
            return "DELETE_BATCH";
        }
        return method;
    }

    /**
     * What a request touches, from its key or its listing prefix: the first segment of the key under the bucket,
     * and for a shard's segments whether it is the manifest, the log or segment data.
     */
    static String area(String target) {
        String key;
        final int query = target.indexOf('?');
        final String path = query < 0 ? target : target.substring(0, query);
        final java.util.regex.Matcher prefix = java.util.regex.Pattern.compile("[?&]prefix=([^&]*)").matcher(target);
        if (prefix.find()) {
            key = java.net.URLDecoder.decode(prefix.group(1), StandardCharsets.UTF_8);
        } else {
            // Path style: /bucket/key...
            final String trimmed = path.startsWith("/") ? path.substring(1) : path;
            final int slash = trimmed.indexOf('/');
            key = slash < 0 ? "" : java.net.URLDecoder.decode(trimmed.substring(slash + 1), StandardCharsets.UTF_8);
        }
        final String[] parts = key.split("/");
        final String top = parts.length == 0 || parts[0].isEmpty() ? "(bucket)" : parts[0];
        if ("segments".equals(top) && parts.length > 2) {
            if ("manifest".equals(parts[2])) {
                return "segments/manifest";
            }
            if ("wal".equals(parts[2])) {
                return "segments/wal";
            }
            return "segments/data";
        }
        if ("cluster".equals(top) && parts.length > 1) {
            return "cluster/" + parts[1];
        }
        return top;
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {}
    }

    @Override
    public void close() {
        closed = true;
        try {
            server.close();
        } catch (IOException ignored) {}
        for (Socket socket : open) {
            closeQuietly(socket);
        }
    }

    /** A request or response head: its bytes as received, and its first line and headers parsed. */
    private record Head(byte[] raw, String firstLine, Map<String, String> headers) {

        static Head read(InputStream in) throws IOException {
            final ByteArrayOutputStream raw = new ByteArrayOutputStream(512);
            String firstLine = null;
            final Map<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            while (true) {
                final ByteArrayOutputStream line = new ByteArrayOutputStream(128);
                int b;
                while ((b = in.read()) != -1) {
                    raw.write(b);
                    if (b == '\n') {
                        break;
                    }
                    if (b != '\r') {
                        line.write(b);
                    }
                }
                if (b == -1) {
                    return null;
                }
                final String text = line.toString(StandardCharsets.ISO_8859_1);
                if (firstLine == null) {
                    if (text.isEmpty()) {
                        continue;
                    }
                    firstLine = text;
                    continue;
                }
                if (text.isEmpty()) {
                    return new Head(raw.toByteArray(), firstLine, headers);
                }
                final int colon = text.indexOf(':');
                if (colon > 0) {
                    headers.put(text.substring(0, colon).trim().toLowerCase(Locale.ROOT), text.substring(colon + 1).trim());
                }
            }
        }

        String header(String name) {
            return headers.get(name);
        }

        boolean isResponse() {
            return firstLine.startsWith("HTTP/");
        }

        int status() {
            final String[] parts = firstLine.split(" ", 3);
            return Integer.parseInt(parts[1]);
        }

        String method() {
            return firstLine.split(" ", 2)[0];
        }

        String target() {
            final String[] parts = firstLine.split(" ", 3);
            return parts.length > 1 ? parts[1] : "";
        }
    }
}
