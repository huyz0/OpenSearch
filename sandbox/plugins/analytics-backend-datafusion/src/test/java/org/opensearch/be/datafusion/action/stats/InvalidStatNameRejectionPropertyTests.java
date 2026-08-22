/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Randomized tests for invalid stat name rejection.
 *
 * <p>Feature: datafusion-cluster-stats, Property 4: Invalid stat name rejection
 *
 * <p>For any string that is not one of the 9 valid stat section names, when included
 * in the {@code stat} path parameter, the REST handler SHALL return an HTTP 400 response
 * whose body lists all valid stat names.
 *
 * <p><b>Validates: Requirements 3.3</b>
 */
public class InvalidStatNameRejectionPropertyTests extends OpenSearchTestCase {

    private static final int TRIES = 150;

    /** All 9 valid stat section names, mirroring {@code RestDataFusionStatsAction.VALID_STAT_NAMES}. */
    private static final Set<String> VALID_STAT_NAMES = Set.of(
        "io_runtime",
        "cpu_runtime",
        "coordinator_reduce",
        "query_execution",
        "stream_next",
        "plan_setup",
        "fragment_executor_gate",
        "adaptive_budget",
        "disk_spill"
    );

    /** Characters used to build "close to valid, but not valid" stat names. */
    private static final String SNAKE_CASE_CHARS = "abcdefghijklmnopqrstuvwxyz_0123456789";

    /**
     * Printable ASCII minus the comma. The comma is excluded deliberately: the handler treats
     * the {@code stat} parameter as a comma-separated <em>list</em> of section names
     * ({@code Strings.splitStringByCommaToArray}), so a string containing a comma is not a
     * single stat name at all — and {@code ","} alone splits to the empty array, which is
     * legitimately not an error.
     */
    private static final String PRINTABLE_ASCII_NO_COMMA =
        "!\"#$%&'()*+-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~ ";

    // ---- Generators ----

    /**
     * Produces one arbitrary non-empty string that is NOT in the valid stat names set.
     * Mixes alphanumeric strings, snake_case strings (similar to valid names but different),
     * and strings with special characters.
     */
    private String invalidStatName() {
        while (true) {
            String candidate;
            switch (randomIntBetween(0, 2)) {
                case 0:
                    candidate = randomAlphaOfLengthBetween(1, 50);
                    break;
                case 1:
                    candidate = randomStringFromPool(SNAKE_CASE_CHARS, randomIntBetween(1, 30));
                    break;
                default:
                    candidate = randomStringFromPool(PRINTABLE_ASCII_NO_COMMA, randomIntBetween(1, 30));
                    break;
            }
            if (candidate.isEmpty() == false && VALID_STAT_NAMES.contains(candidate) == false) {
                return candidate;
            }
        }
    }

    private String randomStringFromPool(String pool, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(pool.charAt(randomIntBetween(0, pool.length() - 1)));
        }
        return sb.toString();
    }

    // ---- Property 4: Invalid stat name rejection ----

    /**
     * Feature: datafusion-cluster-stats, Property 4: Invalid stat name rejection
     *
     * <p>For any string not in the 9 valid stat section names, the REST handler
     * returns HTTP 400 listing valid names.
     *
     * <p><b>Validates: Requirements 3.3</b>
     */
    public void testInvalidStatNameReturnsHttp400WithValidNamesList() throws Exception {
        RestDataFusionStatsAction handler = new RestDataFusionStatsAction();

        // Validation happens before client.execute() is ever reached, so a no-op client is enough.
        try (NodeClient client = new NoOpNodeClient(getTestName())) {
            for (int i = 0; i < TRIES; i++) {
                String invalidStat = invalidStatName();

                // Build a RestRequest with the invalid stat as the "stat" path parameter
                Map<String, String> params = new HashMap<>();
                params.put("stat", invalidStat);
                RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(
                    "/_plugins/_analytics_backend_datafusion/stats"
                ).withParams(params).build();

                FakeRestChannel channel = new FakeRestChannel(request, true, 1);
                handler.handleRequest(request, channel, client);

                // Verify HTTP 400 status
                RestResponse response = channel.capturedResponse();
                assertNotNull("Channel must have received a response", response);
                assertEquals("Invalid stat '" + invalidStat + "' must produce HTTP 400", RestStatus.BAD_REQUEST, response.status());

                // Verify the response body lists valid stat names
                String responseBody = response.content().utf8ToString();
                assertTrue(
                    "Response must contain 'Invalid stat sections' message. Got: " + responseBody,
                    responseBody.contains("Invalid stat sections")
                );
                assertTrue(
                    "Response must list all valid stat names. Got: " + responseBody,
                    responseBody.contains(RestDataFusionStatsAction.VALID_STATS)
                );
            }
        }
    }
}
