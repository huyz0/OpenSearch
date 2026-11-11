/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.cluster;

import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.membership.NodeLease;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Sends a hot shard's searches to a reader in the coordinator's own zone.
 *
 * <p><b>Why.</b> A shard's answer is the largest thing a search moves between nodes, and a reader chosen by placement
 * alone is in another zone two times in three, which is the traffic the clouds bill. Ranking the readers of the
 * coordinator's zone the same way placement ranks the whole fleet gives each zone its own stable, warm reader for the
 * shard, so the answer stays inside the zone.
 *
 * <p><b>Only hot shards.</b> A reader per zone for every shard would hold up to one copy per zone of every index's
 * cache, for shards that are searched once an hour. A shard searched at least {@link #MIN_PER_SECOND} times a second by
 * this coordinator moves enough bytes to be worth a reader in each zone; a colder one keeps the single reader placement
 * names, wherever it is, and its cache affinity is unchanged.
 *
 * <p><b>Never a requirement.</b> The zone's chosen reader goes first and the fleet-wide order follows it, so the next
 * attempt after a zone's reader fails is the reader placement would have used anyway. A zone's reader with
 * {@link #SPILL_IN_FLIGHT} more searches in flight than the fleet-wide choice gives way to it, so a zone with fewer
 * nodes than the others does not collect a queue for the money it saves.
 */
public final class ZonePreference {

    /** Whether hot shards prefer a reader in the coordinator's zone; true by default, and a no-op without zones. */
    public static final String ENABLED = "serverless.search.zone_local.enabled";
    /** Searches a second, from one coordinator, above which a shard gets a reader in the coordinator's zone. */
    public static final String MIN_PER_SECOND = "serverless.search.zone_local.min_per_second";
    /** How many more searches in flight a zone's reader may have than the fleet-wide choice before it gives way. */
    public static final String SPILL_IN_FLIGHT = "serverless.search.zone_local.spill_in_flight";

    static final double DEFAULT_MIN_PER_SECOND = 1.0;
    static final int DEFAULT_SPILL_IN_FLIGHT = 8;

    private final boolean enabled;
    private final double minPerSecond;
    private final int spillInFlight;

    private static final java.util.Map<Object, ZonePreference> BY_NODE = new java.util.WeakHashMap<>();

    /**
     * Returns a node's preference, built from its settings on first use. The value holds no reference to the node, so
     * the weak key is the only thing keeping the entry.
     *
     * @param node the node, held weakly
     * @param settings the node's settings
     * @return its preference
     */
    public static ZonePreference of(Object node, Settings settings) {
        synchronized (BY_NODE) {
            return BY_NODE.computeIfAbsent(node, n -> new ZonePreference(settings));
        }
    }

    /**
     * Builds the preference from a node's settings.
     *
     * @param settings the node's settings
     */
    public ZonePreference(Settings settings) {
        this(
            settings.getAsBoolean(ENABLED, true),
            settings.getAsDouble(MIN_PER_SECOND, DEFAULT_MIN_PER_SECOND),
            settings.getAsInt(SPILL_IN_FLIGHT, DEFAULT_SPILL_IN_FLIGHT)
        );
    }

    /**
     * Builds a preference.
     *
     * @param enabled whether it applies at all
     * @param minPerSecond the rate above which a shard counts as hot; zero makes every shard hot
     * @param spillInFlight the in-flight margin before a zone's reader gives way
     */
    public ZonePreference(boolean enabled, double minPerSecond, int spillInFlight) {
        if (minPerSecond < 0.0 || spillInFlight < 0) {
            throw new IllegalArgumentException("invalid zone preference [" + minPerSecond + "][" + spillInFlight + "]");
        }
        this.enabled = enabled;
        this.minPerSecond = minPerSecond;
        this.spillInFlight = spillInFlight;
    }

    /**
     * Puts the coordinator's zone's reader first, when the shard is hot enough to have one.
     *
     * @param index the index
     * @param shard the shard number
     * @param ordered the fleet-wide readers as the spread ordered them for this search
     * @param members the live leases
     * @param role the role a reader advertises
     * @param localZone the coordinator's zone, or null
     * @param spread the coordinator's spread, which has already counted this search
     * @return the readers to try, in order: the zone's choice, then {@code ordered}
     */
    public List<String> order(
        String index,
        int shard,
        List<String> ordered,
        Collection<NodeLease> members,
        String role,
        String localZone,
        HotShardSpread spread
    ) {
        if (enabled == false || localZone == null || ordered.isEmpty() || spread.perSecond(index, shard) < minPerSecond) {
            return ordered;
        }
        if (Zones.same(localZone, Zones.zoneOf(members, ordered.get(0)))) {
            // Placement's choice is already in this zone.
            return ordered;
        }
        final List<NodeLease> inZone = new ArrayList<>();
        for (NodeLease lease : members) {
            if (Zones.same(localZone, lease.zone())) {
                inZone.add(lease);
            }
        }
        final List<String> ranked = ReaderPlacement.candidatesFor(index, shard, inZone, role, spread.maxReaders());
        if (ranked.isEmpty()) {
            return ordered;
        }
        final String chosen = spread.orderCounted(index, shard, ranked).get(0);
        if (spread.inFlight(chosen) > spread.inFlight(ordered.get(0)) + spillInFlight) {
            return ordered;
        }
        final LinkedHashSet<String> out = new LinkedHashSet<>();
        out.add(chosen);
        out.addAll(ordered);
        return List.copyOf(out);
    }
}
