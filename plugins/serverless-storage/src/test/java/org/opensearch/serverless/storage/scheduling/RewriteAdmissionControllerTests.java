/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scheduling;

import org.opensearch.test.OpenSearchTestCase;

public class RewriteAdmissionControllerTests extends OpenSearchTestCase {

    public void testConstructorRejectsNonPositiveCap() {
        expectThrows(IllegalArgumentException.class, () -> new RewriteAdmissionController(0));
        expectThrows(IllegalArgumentException.class, () -> new RewriteAdmissionController(-1));
    }

    public void testTryAcquireSucceedsUpToTheCapThenFails() {
        RewriteAdmissionController controller = new RewriteAdmissionController(2);
        assertTrue(controller.tryAcquire());
        assertTrue(controller.tryAcquire());
        assertFalse("a third concurrent tick must be refused once the cap of 2 is reached", controller.tryAcquire());
    }

    public void testReleaseFreesAPermitForTheNextTryAcquire() {
        RewriteAdmissionController controller = new RewriteAdmissionController(1);
        assertTrue(controller.tryAcquire());
        assertFalse("no permit left until the first one is released", controller.tryAcquire());

        controller.release();

        assertTrue("releasing the held permit must free it up for the next tick", controller.tryAcquire());
    }
}
