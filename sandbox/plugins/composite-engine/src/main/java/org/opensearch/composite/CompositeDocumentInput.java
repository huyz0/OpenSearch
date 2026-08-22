/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.OpenSearchException;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.MappedFieldType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A composite {@link DocumentInput} that wraps one {@link DocumentInput} per registered
 * data format and broadcasts all field additions to every per-format input.
 * <p>
 * Metadata operations ({@code setRowId}, {@code setVersion}, {@code setSeqNo},
 * {@code setPrimaryTerm}) and field additions are broadcast to all per-format inputs.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeDocumentInput implements DocumentInput<List<? extends DocumentInput<?>>> {

    private final DocumentInput<?> primaryDocumentInput;
    private final DataFormat primaryFormat;
    private final Map<DataFormat, DocumentInput<?>> secondaryDocumentInputs;
    private long rowId = -1L;

    /**
     * Constructs a CompositeDocumentInput with a primary format input and secondary format inputs.
     *
     * @param primaryFormat the primary data format
     * @param primaryDocumentInput the document input for the primary format
     * @param secondaryDocumentInputs a map of secondary data formats to their corresponding document inputs
     */
    public CompositeDocumentInput(
        DataFormat primaryFormat,
        DocumentInput<?> primaryDocumentInput,
        Map<DataFormat, DocumentInput<?>> secondaryDocumentInputs
    ) {
        this.primaryFormat = Objects.requireNonNull(primaryFormat, "primaryFormat must not be null");
        this.primaryDocumentInput = Objects.requireNonNull(primaryDocumentInput, "primaryDocumentInput must not be null");
        this.secondaryDocumentInputs = Collections.unmodifiableMap(
            Objects.requireNonNull(secondaryDocumentInputs, "secondaryDocumentInputs must not be null")
        );
    }

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        try {
            primaryDocumentInput.addField(fieldType, value);
        } catch (Exception e) {
            throw wrapAddFieldFailure(e, fieldType, "primary", primaryFormat);
        }
        for (Map.Entry<DataFormat, DocumentInput<?>> entry : secondaryDocumentInputs.entrySet()) {
            try {
                entry.getValue().addField(fieldType, value);
            } catch (Exception e) {
                throw wrapAddFieldFailure(e, fieldType, "secondary", entry.getKey());
            }
        }
    }

    /**
     * Decides how a sub-format's {@code addField} failure leaves this method.
     * <p>
     * A sub-format reports a mapping/parse problem as an {@link OpenSearchException} subtype that
     * carries the right HTTP status — {@code ParquetDocumentInput}, for instance, throws
     * {@link org.opensearch.index.mapper.MapperParsingException}, a 400. Those are rethrown
     * untouched: rewrapping them in {@link IllegalStateException}, which has no {@code status()}
     * override, laundered every client-side mapping error in every sub-format into a 500. The
     * originating exception already names the offending field, so no context is lost.
     * <p>
     * Anything else really is unexpected here, so it keeps the wrapper that names the failing
     * role and format.
     */
    private static RuntimeException wrapAddFieldFailure(Exception e, MappedFieldType fieldType, String role, DataFormat format) {
        if (e instanceof OpenSearchException openSearchException) {
            return openSearchException;
        }
        String message = "Failed to add field [" + fieldType.name() + "] in " + role + " format [" + format.name() + "]";
        return new IllegalStateException(message, e);
    }

    @Override
    public void setRowId(String rowIdFieldName, long rowId) {
        primaryDocumentInput.setRowId(rowIdFieldName, rowId);
        for (DocumentInput<?> input : secondaryDocumentInputs.values()) {
            input.setRowId(rowIdFieldName, rowId);
        }
        this.rowId = rowId;
    }

    /** Returns the row ID assigned via {@link #setRowId}, or {@code -1} if none. */
    public long getRowId() {
        return rowId;
    }

    public long getFieldCount(String fieldName) {
        // Return the field count from the primary document input
        return primaryDocumentInput.getFieldCount(fieldName);
    }

    @Override
    public List<? extends DocumentInput<?>> getFinalInput() {
        return null;
    }

    @Override
    public void close() {
        // No-op: document input lifecycle is independent of writer pool
    }

    /**
     * Returns the primary format's document input.
     *
     * @return the primary document input
     */
    public DocumentInput<?> getPrimaryInput() {
        return primaryDocumentInput;
    }

    /**
     * Returns the primary data format.
     *
     * @return the primary data format
     */
    public DataFormat getPrimaryFormat() {
        return primaryFormat;
    }

    /**
     * Returns an unmodifiable map of secondary data formats to their document inputs.
     *
     * @return the secondary inputs
     */
    public Map<DataFormat, DocumentInput<?>> getSecondaryInputs() {
        return secondaryDocumentInputs;
    }
}
