/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

/** One document, or the fact that there is none. */
public final class Document {

    private final String id;
    private final boolean found;
    private final String source;
    private final long seqNo;
    private final long primaryTerm;
    private final long version;
    private String routing;

    /**
     * Creates a found document with no sequence identity.
     *
     * @param id the document id
     * @param found whether it exists
     * @param source its source, or null when it does not
     */
    public Document(String id, boolean found, String source) {
        this(
            id,
            found,
            source,
            org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO,
            org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            org.opensearch.common.lucene.uid.Versions.NOT_FOUND
        );
    }

    /**
     * Creates a found document, carrying the sequence identity it currently holds.
     *
     * @param id the document id
     * @param found whether it exists
     * @param source its source, or null when it does not
     * @param seqNo the sequence number of the operation that last wrote it
     * @param primaryTerm the primary term that operation ran at
     * @param version its current version
     */
    public Document(String id, boolean found, String source, long seqNo, long primaryTerm, long version) {
        this.id = id;
        this.found = found;
        this.source = source;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.version = version;
    }

    /**
     * Returns the custom routing value the document was written with.
     *
     * @return the routing value, or null when it is placed by its id
     */
    public String routing() {
        return routing;
    }

    /**
     * Sets the custom routing value the document was written with, as read from its {@code _routing} field.
     *
     * @param value the routing value, or null
     * @return this
     */
    public Document withRouting(String value) {
        this.routing = value;
        return this;
    }

    /**
     * Returns the sequence number of the operation that last wrote this document.
     *
     * @return the sequence number, unassigned when the document was not found
     */
    public long seqNo() {
        return seqNo;
    }

    /**
     * Returns the primary term that operation ran at.
     *
     * @return the primary term, unassigned when the document was not found
     */
    public long primaryTerm() {
        return primaryTerm;
    }

    /**
     * Returns the document's current version.
     *
     * @return the version
     */
    public long version() {
        return version;
    }

    /**
     * Records that no such document exists.
     *
     * @param id the document id
     * @return the absence
     */
    public static Document absent(String id) {
        return new Document(id, false, null);
    }

    /**
     * Returns the document id.
     *
     * @return the id
     */
    public String id() {
        return id;
    }

    /**
     * Reports whether the document exists.
     *
     * @return true when it does
     */
    public boolean found() {
        return found;
    }

    /**
     * Returns the document source, or null when there is none.
     *
     * @return the source
     */
    public String source() {
        return source;
    }
}
