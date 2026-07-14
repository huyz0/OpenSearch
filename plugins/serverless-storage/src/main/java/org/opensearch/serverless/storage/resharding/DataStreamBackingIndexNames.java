/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Reverses {@code org.opensearch.cluster.metadata.DataStream#getDefaultBackingIndexName}'s fixed
 * {@code .ds-<streamName>-<generation:%06d>} naming scheme -- the piece {@link
 * DataStreamShardCountAdvisorCache} needs to turn a new backing index's own name (all {@code
 * IndexSettingProvider#getAdditionalIndexSettings} is ever given) back into the data stream name
 * the cache is keyed by.
 *
 * <p>Reliable specifically because the generation suffix is always all-digits, zero-padded to at
 * least 6 wide (core's own {@code String.format(Locale.ROOT, ".ds-%s-%06d", ...)} -- {@code %06d}
 * is a <em>minimum</em> width, so a generation &gt;= 1,000,000 produces a wider, still all-digit
 * suffix, never a truncated one) -- unlike a variable-width, non-digit suffix, this can be
 * stripped unambiguously even if {@code streamName} itself contains hyphens or digits.
 */
public final class DataStreamBackingIndexNames {

    private static final String PREFIX = ".ds-";
    private static final Pattern GENERATION_SUFFIX = Pattern.compile("-\\d{6,}$");

    private DataStreamBackingIndexNames() {}

    /**
     * Extracts the data stream name from one of its own backing index names, if {@code
     * backingIndexName} matches core's own {@code .ds-<streamName>-<generation:%06d>} format.
     *
     * @param backingIndexName a real or about-to-be-created backing index name.
     * @return the data stream name, or empty if {@code backingIndexName} doesn't match the expected format.
     */
    public static Optional<String> parseDataStreamName(String backingIndexName) {
        if (backingIndexName == null || backingIndexName.startsWith(PREFIX) == false) {
            return Optional.empty();
        }
        String withoutPrefix = backingIndexName.substring(PREFIX.length());
        if (GENERATION_SUFFIX.matcher(withoutPrefix).find() == false) {
            return Optional.empty();
        }
        String withoutSuffix = GENERATION_SUFFIX.matcher(withoutPrefix).replaceFirst("");
        return withoutSuffix.isEmpty() ? Optional.empty() : Optional.of(withoutSuffix);
    }
}
