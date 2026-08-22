/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.rest.RestHandler.ApiAvailabilityScope;
import org.opensearch.test.OpenSearchTestCase;

public class RestGetSettingsActionTests extends OpenSearchTestCase {

    public void testApiAvailabilityScopeIsAvailable() {
        assertEquals(ApiAvailabilityScope.AVAILABLE, new RestGetSettingsAction().apiAvailabilityScope());
    }
}
