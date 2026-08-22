/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.opensearch.common.settings.Settings;
import org.opensearch.rest.action.admin.indices.RestCreateIndexAction;
import org.opensearch.rest.action.admin.indices.RestDeleteIndexAction;
import org.opensearch.rest.action.admin.indices.RestGetIndicesAction;
import org.opensearch.rest.action.document.RestDeleteAction;
import org.opensearch.rest.action.document.RestMultiGetAction;
import org.opensearch.rest.action.document.RestUpdateAction;
import org.opensearch.rest.action.search.RestCountAction;
import org.opensearch.rest.action.search.RestExplainAction;
import org.opensearch.rest.action.search.RestMultiSearchAction;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Phase 5 (rfc-serverless-opensearch.md, REST API gating audit): a real serverless deployment with
 * {@code serverless_storage.rest_gating.enabled} turned on needs the ordinary CRUD/read surface to
 * keep working -- {@code RestHandler#apiAvailabilityScope()} defaults to {@code UNAVAILABLE}, so every
 * handler needs an explicit opt-in, and nothing previously enumerated the set that opts in.
 *
 * <p>Auditing the whole REST surface found create-index, delete-index, delete-document,
 * update-document, multi-get, multi-search, count, explain, and get-indices all still defaulting to
 * {@code UNAVAILABLE} -- meaning a deployment with gating on could not create an index at all, the
 * most severe possible instance of this gap. Fixed by declaring each {@code AVAILABLE}, alongside
 * the create/get/search/bulk/get-mapping/get-settings/get-aliases/cluster-health/cluster-get-settings
 * set that already was.
 *
 * <p>This pins the fix for the nine handlers this pass touched, so a future core change reverting one
 * of them (or removing the override entirely) fails a test immediately rather than silently
 * reintroducing an unusable deployment. It intentionally does not re-verify the handlers that already
 * declared {@code AVAILABLE} before this pass -- those were presumably reviewed when each was added.
 */
public class CrudSurfaceAvailabilityScopeTests extends OpenSearchTestCase {

    public void testCreateIndexIsAvailable() {
        assertAvailable(new RestCreateIndexAction());
    }

    public void testDeleteIndexIsAvailable() {
        assertAvailable(new RestDeleteIndexAction());
    }

    public void testDeleteDocumentIsAvailable() {
        assertAvailable(new RestDeleteAction());
    }

    public void testUpdateDocumentIsAvailable() {
        assertAvailable(new RestUpdateAction());
    }

    public void testMultiGetIsAvailable() {
        assertAvailable(new RestMultiGetAction(Settings.EMPTY));
    }

    public void testMultiSearchIsAvailable() {
        assertAvailable(new RestMultiSearchAction(Settings.EMPTY));
    }

    public void testCountIsAvailable() {
        assertAvailable(new RestCountAction());
    }

    public void testExplainIsAvailable() {
        assertAvailable(new RestExplainAction());
    }

    public void testGetIndicesIsAvailable() {
        assertAvailable(new RestGetIndicesAction());
    }

    private static void assertAvailable(RestHandler handler) {
        assertEquals(
            handler.getClass().getSimpleName()
                + " must be AVAILABLE under serverless mode, or a gated "
                + "deployment loses this operation entirely -- the default is UNAVAILABLE, so this is an "
                + "explicit opt-in a future change could silently drop",
            RestHandler.ApiAvailabilityScope.AVAILABLE,
            handler.apiAvailabilityScope()
        );
    }
}
