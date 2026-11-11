/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.store;

import org.opensearch.common.blobstore.BlobContainer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * How the write log names and parses its blobs: record and seal names, term directories, and the records a batch
 * blob holds. Split out of {@link WalStore}.
 */
final class WalLayout {

    private WalLayout() {}

    /** A record's blob name: a zero-padded ordinal, and nothing else in the container is ours. */
    static final java.util.regex.Pattern RECORD_NAME = java.util.regex.Pattern.compile("\\d{20}");

    /** A seal's blob name. Deliberately not a bare ordinal, so nothing counting records can count a seal. */
    static final java.util.regex.Pattern SEAL_NAME = java.util.regex.Pattern.compile("seal-\\d{20}");

    static byte[] readOne(BlobContainer container, String name) throws IOException {
        try (InputStream in = container.readBlob(name)) {
            return in.readAllBytes();
        } catch (IOException e) {
            throw new IOException("unreadable WAL record at " + container.path().buildAsString() + name, e);
        }
    }

    static String name(long ordinal) {
        return String.format(java.util.Locale.ROOT, "%020d", ordinal);
    }

    /** The seal blobs among a listing's names, in the order the listing gave them. */
    static List<String> sealNamesIn(java.util.Collection<String> names) {
        final List<String> seals = new ArrayList<>();
        for (String name : names) {
            if (SEAL_NAME.matcher(name).matches()) {
                seals.add(name);
            }
        }
        return seals;
    }

    /** Merges the named seals, taking the lowest recorded position per term. Caller supplies the listing. */
    static Map<Long, String> mergeSeals(BlobContainer seals, List<String> sealNames) throws IOException {
        final Map<Long, String> merged = new java.util.HashMap<>();
        for (String sealName : sealNames) {
            final byte[] bytes;
            try (InputStream in = seals.readBlob(sealName)) {
                bytes = in.readAllBytes();
            }
            for (String line : new String(bytes, java.nio.charset.StandardCharsets.UTF_8).split("\n")) {
                if (line.isEmpty()) {
                    continue;
                }
                final int space = line.indexOf(' ');
                if (space < 0) {
                    throw new IOException("unreadable WAL seal at " + seals.path().buildAsString() + sealName);
                }
                final Long term = parseTermNumber(line.substring(0, space));
                if (term == null) {
                    throw new IOException("unreadable WAL seal at " + seals.path().buildAsString() + sealName);
                }
                final String position = line.substring(space + 1);
                merged.merge(term, position, (a, b) -> a.compareTo(b) <= 0 ? a : b);
            }
        }
        return merged;
    }

    /**
     * The highest term any seal was written at. Every term below it has been sealed, whether or not that
     * seal mentions it — a seal that does not mention a term is saying the term was empty, which is a
     * stronger statement than saying nothing.
     *
     * <p>A pure function of the listing the caller already has, so asking this costs nothing on top of
     * the merge it is asked alongside.
     *
     * @param sealNames the seal blob names, from one listing
     * @return the highest sealing term, or {@link Long#MIN_VALUE} if the log has never been sealed
     */
    static long highestSealedTerm(List<String> sealNames) {
        long highest = Long.MIN_VALUE;
        for (String name : sealNames) {
            final Long term = parseTermNumber(name.substring("seal-".length()));
            if (term != null && term > highest) {
                highest = term;
            }
        }
        return highest;
    }

    static Long parseTermNumber(String text) {
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    static Map<Long, BlobContainer> termsIn(Map<String, BlobContainer> children) {
        final Map<Long, BlobContainer> byTerm = new TreeMap<>();
        for (Map.Entry<String, BlobContainer> child : children.entrySet()) {
            final Long term = parseTerm(child.getKey());
            if (term != null) {
                byTerm.put(term, child.getValue());
            }
        }
        return byTerm;
    }

    /**
     * Parses one blob, which holds a single record or a batch of them, one per line.
     *
     * <p>Splitting on the newline byte is safe on UTF-8 without decoding first: {@code 0x0A} cannot
     * appear inside a multi-byte sequence, so a byte-level split cannot land mid-character.
     *
     * @param blob the blob's bytes
     * @return the records it holds, in order
     * @throws IOException if a line does not parse
     */
    static List<WalRecord> parseBatch(byte[] blob) throws IOException {
        final List<WalRecord> records = new ArrayList<>();
        parseBatch(blob, records::add);
        return records;
    }

    static void parseBatch(byte[] blob, org.opensearch.common.CheckedConsumer<WalRecord, IOException> consumer) throws IOException {
        int start = 0;
        for (int i = 0; i <= blob.length; i++) {
            if (i != blob.length && blob[i] != '\n') {
                continue;
            }
            if (i > start) {
                consumer.accept(WalRecord.fromStream(new java.io.ByteArrayInputStream(blob, start, i - start)));
            }
            start = i + 1;
        }
    }

    static Long parseTerm(String containerName) {
        if (containerName.startsWith("t=") == false) {
            return null;
        }
        try {
            return Long.parseLong(containerName.substring(2));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
