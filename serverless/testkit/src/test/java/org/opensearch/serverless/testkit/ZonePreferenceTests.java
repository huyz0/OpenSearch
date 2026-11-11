/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.lease.Releasable;
import org.opensearch.serverless.cluster.HotShardSpread;
import org.opensearch.serverless.cluster.ReaderPlacement;
import org.opensearch.serverless.cluster.ZonePreference;
import org.opensearch.serverless.cluster.Zones;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A hot shard's search goes to a reader in the coordinator's zone first; a cold one keeps placement's reader; and the
 * fleet-wide order always follows, so a zone's reader is a preference and never the only choice.
 */
public class ZonePreferenceTests extends OpenSearchTestCase {

    private static final String ROLE = "search";

    private final AtomicLong nanos = new AtomicLong(1_000_000_000L);

    private List<NodeLease> fleet() {
        final List<NodeLease> members = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            final String zone = "zone-" + (char) ('a' + i % 3);
            members.add(new NodeLease("node-" + i, "e" + i, "127.0.0.1:9300", Set.of(ROLE), Long.MAX_VALUE).withZone(zone));
        }
        return members;
    }

    private HotShardSpread spread() {
        return new HotShardSpread(50.0, 4, nanos::get);
    }

    /** A shard whose placement-preferred reader is not in {@code zone}. */
    private int shardRankedOutside(List<NodeLease> members, String zone) {
        for (int shard = 0;; shard++) {
            final String first = ReaderPlacement.candidatesFor("idx", shard, members, ROLE, 1).get(0);
            if (Zones.same(zone, Zones.zoneOf(members, first)) == false) {
                return shard;
            }
        }
    }

    public void testAHotShardGoesToAReaderInTheCoordinatorsZoneFirst() {
        final List<NodeLease> members = fleet();
        final HotShardSpread spread = spread();
        final ZonePreference preference = new ZonePreference(true, 1.0, 8);
        final int shard = shardRankedOutside(members, "zone-a");
        final List<String> global = ReaderPlacement.candidatesFor("idx", shard, members, ROLE, 4);
        List<String> ordered = null;
        for (int i = 0; i < 20; i++) {
            ordered = preference.order("idx", shard, spread.order("idx", shard, global), members, ROLE, "zone-a", spread);
        }
        assertEquals("zone-a", Zones.zoneOf(members, ordered.get(0)));
        assertTrue("the fleet-wide order follows, whole", ordered.subList(1, ordered.size()).containsAll(global));
        assertEquals("with nothing repeated", ordered.size(), Set.copyOf(ordered).size());
        // Stable: every coordinator in the zone picks the same reader while the shard is not spread wider.
        final List<String> again = preference.order("idx", shard, spread.order("idx", shard, global), members, ROLE, "zone-a", spread);
        assertEquals(ordered.get(0), again.get(0));
    }

    public void testAColdShardKeepsPlacementsReader() {
        final List<NodeLease> members = fleet();
        final HotShardSpread spread = spread();
        final ZonePreference preference = new ZonePreference(true, 1.0, 8);
        final int shard = shardRankedOutside(members, "zone-a");
        final List<String> global = ReaderPlacement.candidatesFor("idx", shard, members, ROLE, 4);
        // One search, ten seconds after the last: well under a search a second.
        final List<String> ordered = spread.order("idx", shard, global);
        nanos.addAndGet(60_000_000_000L);
        assertEquals(global, preference.order("idx", shard, ordered, members, ROLE, "zone-a", spread));
    }

    public void testNoZoneOrDisabledChangesNothing() {
        final List<NodeLease> members = fleet();
        final HotShardSpread spread = spread();
        final int shard = shardRankedOutside(members, "zone-a");
        final List<String> global = ReaderPlacement.candidatesFor("idx", shard, members, ROLE, 4);
        for (int i = 0; i < 20; i++) {
            spread.order("idx", shard, global);
        }
        assertEquals(global, new ZonePreference(true, 0.0, 8).order("idx", shard, global, members, ROLE, null, spread));
        assertEquals(global, new ZonePreference(false, 0.0, 8).order("idx", shard, global, members, ROLE, "zone-a", spread));
        assertEquals(
            "a zone with no readers",
            global,
            new ZonePreference(true, 0.0, 8).order("idx", shard, global, members, ROLE, "zone-z", spread)
        );
    }

    public void testABusyZoneReaderGivesWay() {
        final List<NodeLease> members = fleet();
        final HotShardSpread spread = spread();
        final ZonePreference preference = new ZonePreference(true, 0.0, 2);
        final int shard = shardRankedOutside(members, "zone-a");
        final List<String> global = ReaderPlacement.candidatesFor("idx", shard, members, ROLE, 4);
        final List<String> inZone = preference.order("idx", shard, global, members, ROLE, "zone-a", spread);
        final String zoneReader = inZone.get(0);
        final List<Releasable> busy = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            busy.add(spread.begin(zoneReader));
        }
        assertEquals(
            "three more in flight than the margin of two: placement's reader",
            global,
            preference.order("idx", shard, global, members, ROLE, "zone-a", spread)
        );
        busy.forEach(Releasable::close);
        assertEquals(zoneReader, preference.order("idx", shard, global, members, ROLE, "zone-a", spread).get(0));
    }
}
