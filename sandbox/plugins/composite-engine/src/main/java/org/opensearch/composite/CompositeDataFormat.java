/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A composite {@link DataFormat} that wraps multiple per-format {@link DataFormat} instances.
 * Each constituent format retains its own {@link FieldTypeCapabilities} — field routing is
 * handled per-format by {@link CompositeDocumentInput}, not by this class.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeDataFormat extends DataFormat {

    /** Canonical format name for the composite engine. */
    public static final String COMPOSITE_FORMAT_NAME = "composite";

    private final DataFormat primaryDataFormat;
    private final List<DataFormat> dataFormats;

    /**
     * Constructs a CompositeDataFormat with a designated primary format and a list of all constituent formats.
     *
     * @param primaryDataFormat the authoritative data format used for merge operations
     * @param dataFormats       all constituent data formats (including the primary)
     */
    public CompositeDataFormat(DataFormat primaryDataFormat, List<DataFormat> dataFormats) {
        this.primaryDataFormat = Objects.requireNonNull(primaryDataFormat, "primaryDataFormat must not be null");
        this.dataFormats = List.copyOf(Objects.requireNonNull(dataFormats, "dataFormats must not be null"));
    }

    /**
     * Constructs an empty CompositeDataFormat with no constituent formats.
     */
    public CompositeDataFormat() {
        this.primaryDataFormat = null;
        this.dataFormats = List.of();
    }

    /**
     * Returns the list of constituent data formats.
     *
     * @return the data formats
     */
    public List<DataFormat> getDataFormats() {
        return dataFormats;
    }

    /**
     * Returns the primary data format used for merge operations.
     *
     * @return the primary data format
     */
    public DataFormat getPrimaryDataFormat() {
        return primaryDataFormat;
    }

    @Override
    public String name() {
        return COMPOSITE_FORMAT_NAME;
    }

    /**
     * {@inheritDoc}
     * <p>
     * {@code priority()} is a <em>precedence rank</em>: both places that order formats —
     * {@code DataFormatRegistry.supportsCapability} and
     * {@link CompositeDataFormatPlugin#assignCapabilities} — sort <b>ascending</b> and take the
     * earliest match, so a <b>lower</b> number means the format is preferred <b>sooner</b>
     * (parquet {@code 0} is consulted before lucene {@code 50}).
     * <p>
     * The composite format is a last-resort fallback for any field that a concrete format can
     * serve on its own, so it returns {@link Long#MAX_VALUE} and sorts last. It previously
     * returned {@link Long#MIN_VALUE} while its comment claimed "lowest priority" — under an
     * ascending sort that put the composite <em>first</em>, the exact opposite of the intent.
     */
    @Override
    public long priority() {
        return Long.MAX_VALUE;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The union — per field type — of every constituent format's capabilities, matching what
     * {@link CompositeIndexingExecutionEngine} documents this format as exposing. It previously
     * returned only {@code dataFormats.get(0).supportedFields()}, which under-reported any
     * capability that only a secondary format could serve.
     * <p>
     * The instance the registry stores comes from {@link #CompositeDataFormat() the no-arg
     * constructor} and therefore has no constituents, so it correctly reports an empty set: that
     * instance is a name/priority placeholder, and per-field routing for a composite index is
     * decided by {@link CompositeDataFormatPlugin#assignCapabilities}, which resolves the
     * configured formats from index settings rather than reading this method.
     */
    @Override
    public Set<FieldTypeCapabilities> supportedFields() {
        if (dataFormats.isEmpty()) {
            return Set.of();
        }
        Map<String, EnumSet<FieldTypeCapabilities.Capability>> byFieldType = new LinkedHashMap<>();
        for (DataFormat dataFormat : dataFormats) {
            for (FieldTypeCapabilities ftc : dataFormat.supportedFields()) {
                byFieldType.computeIfAbsent(ftc.fieldType(), k -> EnumSet.noneOf(FieldTypeCapabilities.Capability.class))
                    .addAll(ftc.capabilities());
            }
        }
        Set<FieldTypeCapabilities> union = new LinkedHashSet<>(byFieldType.size());
        byFieldType.forEach((fieldType, capabilities) -> union.add(new FieldTypeCapabilities(fieldType, capabilities)));
        return Collections.unmodifiableSet(union);
    }

    @Override
    public String toString() {
        return "CompositeDataFormat{dataFormats=" + dataFormats + '}';
    }
}
