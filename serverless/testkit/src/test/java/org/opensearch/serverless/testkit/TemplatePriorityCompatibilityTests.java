/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.serverless.metadata.TemplateResolver;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class TemplatePriorityCompatibilityTests extends OpenSearchTestCase {
    private static String template(String priority) {
        return "{\"index_patterns\":[\"logs-*\"],\"priority\":" + priority + ",\"template\":{}}";
    }

    private static Long nativePriority(String priority) throws Exception {
        try (
            var parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, template(priority))
        ) {
            return ComposableIndexTemplate.parse(parser).priority();
        }
    }

    public void testExistingNumericStringPriorityControlsSelection() throws Exception {
        assertEquals(Long.valueOf(20L), nativePriority("\"20\""));
        assertEquals(
            "higher",
            TemplateResolver.resolve(Map.of("lower", template("10"), "higher", template("\"20\"")), Map.of(), "logs-new").from()
        );
    }

    public void testExistingDecimalStringUsesNativeLongCoercion() throws Exception {
        assertEquals(Long.valueOf(20L), nativePriority("\"20.8\""));
        assertEquals(
            "higher",
            TemplateResolver.resolve(Map.of("lower", template("10"), "higher", template("\"20.8\"")), Map.of(), "logs-new").from()
        );
    }

    public void testNegativePriorityCannotSilentlyDisappearAtTheSentinel() throws Exception {
        expectThrows(
            IllegalArgumentException.class,
            () -> TemplateResolver.resolve(Map.of("invalid", template(Long.toString(Long.MIN_VALUE))), Map.of(), "logs-new")
        );
    }

    public void testMalformedPriorityCannotActLikeAnOmittedPriority() throws Exception {
        expectThrows(
            IllegalArgumentException.class,
            () -> TemplateResolver.resolve(Map.of("invalid", template("true")), Map.of(), "logs-new")
        );
    }

    public void testOutOfRangePriorityCannotWrapOrSaturate() throws Exception {
        expectThrows(
            IllegalArgumentException.class,
            () -> TemplateResolver.resolve(Map.of("invalid", template("1e40")), Map.of(), "logs-new")
        );
    }

    public void testNumericFractionsKeepNativeLongCoercion() throws Exception {
        assertEquals(Long.valueOf(20L), nativePriority("20.8"));
        assertEquals("a", TemplateResolver.resolve(Map.of("a", template("20.8"), "b", template("20")), Map.of(), "logs-new").from());
    }

    public void testOmittedAndZeroPrioritiesStillTieByName() throws Exception {
        final String absent = "{\"index_patterns\":[\"logs-*\"],\"template\":{}}";
        assertEquals("a", TemplateResolver.resolve(Map.of("a", absent, "b", template("0")), Map.of(), "logs-new").from());
    }

    public void testAnUnrelatedInvalidLegacyTemplateDoesNotBlockCreation() throws Exception {
        final String unrelated = template("-1").replace("logs-*", "unrelated-*");
        assertEquals("valid", TemplateResolver.resolve(Map.of("invalid", unrelated, "valid", template("10")), Map.of(), "logs-new").from());
    }
}
