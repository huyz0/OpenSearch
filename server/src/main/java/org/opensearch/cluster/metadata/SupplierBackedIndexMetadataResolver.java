/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Phase C5 of {@code core-pluggability-refactor-plan.md}: an {@link IndexMetadataResolver} that answers
 * from whatever a plugin has already registered with {@link AbsentIndexDescriptorSuppliers} -- the
 * pre-existing static registry -- rather than duplicating that plugin's lookup/caching logic in a
 * plugin-owned adapter.
 *
 * <p><b>Why this lives in {@code server/} instead of the plugin.</b> A plugin that calls {@code
 * AbsentIndexDescriptorSuppliers.register(...)} today (the only real one, as of this phase, is
 * {@code plugins/serverless-storage}'s {@code DescriptorGate}) already has everything this class needs
 * registered: this is pure glue with zero plugin-specific logic, so it belongs beside the registry it
 * forwards to, fully unit-testable with {@code :server:test} alone, and reusable by any future plugin that
 * populates the same registry instead of each one writing its own copy.
 *
 * <p><b>Additive, not a replacement.</b> A plugin using this still calls {@code
 * AbsentIndexDescriptorSuppliers.register(...)} exactly as before -- that registration is what this class
 * reads from. Returning {@code Optional.of(new SupplierBackedIndexMetadataResolver())} from {@link
 * org.opensearch.plugins.ClusterPlugin#getIndexMetadataResolver()} only adds a second path to the same
 * answer ({@link Metadata#index(String)} on a miss, instead of a caller explicitly calling {@link
 * AbsentIndexDescriptorSuppliers#metadataOrDescriptor}); it changes nothing about any existing call site,
 * which is what makes it safe to land ahead of Phase C4's call-site migration -- see that phase's own
 * status-log entry for why the ordering matters.
 *
 * <p>Delegates to {@link AbsentIndexDescriptorSuppliers#synthesisedMetadata(String)} specifically (not
 * {@link AbsentIndexDescriptorSuppliers#supply}, its lower-level primitive), which is the exact call {@code
 * metadataOrDescriptor} itself makes on a miss -- so this class's answer for a given index name is
 * identical to what every existing call site already gets, cache and all.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class SupplierBackedIndexMetadataResolver implements IndexMetadataResolver {

    @Override
    public IndexMetadata resolve(Metadata metadata, String indexName) {
        return AbsentIndexDescriptorSuppliers.synthesisedMetadata(indexName);
    }
}
