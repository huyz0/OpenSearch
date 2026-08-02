/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * The node-stats extension point exists and a plugin can fill it.
 *
 * <h2>Why this test rather than trusting the compiler</h2>
 *
 * This branch deleted {@link PluginNodeStats} and {@code Plugin.nodeStats()} and put a concrete stats class
 * in core in their place. Nothing failed, because nothing in this repository overrode the method: the only
 * consumer of an extension point is, by definition, outside the codebase that offers it. A plugin elsewhere
 * simply stopped compiling.
 *
 * <p>So the compiler cannot defend this and neither can any existing test. What defends it is a test that
 * behaves like the absent consumer: it implements the interface, overrides the method, and asserts the
 * contract holds. Deleting either piece again fails here rather than somewhere nobody can see.
 *
 * <p>The requirement it guards is that core changes are extension points rather than behaviour. Removing an
 * extension point and naming a concrete implementation in core is the exact inverse, and it is a regression
 * against upstream independent of anything this branch is trying to build.
 */
public class PluginNodeStatsExtensionPointTests extends OpenSearchTestCase {

    /** What a plugin's contribution looks like: named, serialisable, renderable. */
    private static final class ExampleStats implements PluginNodeStats {

        private final long value;

        ExampleStats(long value) {
            this.value = value;
        }

        ExampleStats(StreamInput in) throws IOException {
            this.value = in.readVLong();
        }

        @Override
        public String getWriteableName() {
            return "example_plugin_stats";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(value);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field("value", value);
        }
    }

    private static final class ExamplePlugin extends Plugin {
        @Override
        public List<PluginNodeStats> nodeStats() {
            return List.of(new ExampleStats(42));
        }
    }

    /** A plugin that says nothing contributes nothing, which is what every existing plugin does. */
    public void testTheDefaultContributesNothing() {
        assertTrue("a plugin that does not override this must contribute no stats", new Plugin() {
        }.nodeStats().isEmpty());
    }

    public void testAPluginCanContributeNodeStats() {
        List<PluginNodeStats> contributed = new ExamplePlugin().nodeStats();

        assertEquals(1, contributed.size());
        assertEquals(
            "the name is what the payload renders under and what the coordinator deserialises by",
            "example_plugin_stats",
            contributed.get(0).getWriteableName()
        );
    }

    /**
     * The contribution has to survive the wire, because stats are gathered per node and rendered by whichever
     * node coordinated the request.
     */
    public void testAContributionRoundTripsOverTheWire() throws IOException {
        ExampleStats original = new ExampleStats(7);

        ExampleStats restored;
        try (org.opensearch.common.io.stream.BytesStreamOutput out = new org.opensearch.common.io.stream.BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new ExampleStats(in);
            }
        }

        assertEquals(original.value, restored.value);
    }
}
