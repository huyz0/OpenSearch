/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.usage;

import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.store.IndexUsage;
import org.opensearch.serverless.store.StoreUsage;
import org.opensearch.serverless.usage.UsageSettings.Mode;
import org.opensearch.serverless.usage.UsageSettings.Tier;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * One node's per-tenant cost guardrails: what each tenant's indices cost here, and the level that puts it at against its
 * budget.
 *
 * <p><b>Levels.</b> A tenant is the index's bucket group, or the index itself. Every {@link #TICK_MILLIS} the node adds up
 * each tenant's cost since the last tick, from the store's per-index usage, in cost units (one PUT each). Over its budget
 * for a minute it goes up a level, and up another after two more minutes still over, to three at most; under it for five
 * minutes, down one. A tenant's budget is its tier's, {@link UsageSettings#INDEX_BUDGET} overriding it, and when its
 * indices disagree, the largest.
 *
 * <p>This node sees only its own share of a tenant, so a level can also be set from outside, by a collector that sees
 * every node ({@link UsageSettings#TENANT_LEVELS}); the higher of the two holds.
 *
 * <p><b>What each level does</b>, only in {@link Mode#ENFORCE} and only to that tenant's indices: at one and above, its
 * shards publish {@link #PUBLISH_STRETCH} times less often. Writes are still pushed to search nodes as they are
 * acknowledged, which costs no store request, so searches see them as soon as before. In {@link Mode#OBSERVE}, the
 * default, levels are only computed and reported. Nothing here sends a request to the store or another node.
 */
public final class Guardrails {

    /** How often levels are recomputed. */
    public static final long TICK_MILLIS = 10_000L;

    /** How much longer a tenant at level one or above waits to publish. */
    public static final int PUBLISH_STRETCH = 4;

    private static final long LEVEL_UP_AFTER_MILLIS = 60_000L;
    private static final long NEXT_LEVEL_AFTER_MILLIS = 120_000L;
    private static final long LEVEL_DOWN_AFTER_MILLIS = 300_000L;

    private final Mode nodeMode;
    private final Tier defaultTier;
    private final long[] tierBudgets;
    private final Function<Settings, String> groupOf;
    private final Supplier<StoreUsage> usage;
    private final LongSupplier clock;

    private final ConcurrentHashMap<String, IndexInfo> indices = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Tenant> tenants = new ConcurrentHashMap<>();
    private volatile Map<String, Integer> external = Map.of();
    private long lastTickMillis = -1L;

    /**
     * Creates a node's guardrails.
     *
     * @param settings the node's settings
     * @param groupOf an index's bucket group from its settings, empty or null for none
     * @param usage the store's per-index usage, or a supplier of null when metering is off
     * @param clock milliseconds
     */
    public Guardrails(Settings settings, Function<Settings, String> groupOf, Supplier<StoreUsage> usage, LongSupplier clock) {
        this.nodeMode = Mode.parse(UsageSettings.MODE.get(settings));
        this.defaultTier = Tier.parse(UsageSettings.DEFAULT_TIER.get(settings));
        this.tierBudgets = new long[] {
            UsageSettings.SMALL_BUDGET.get(settings),
            UsageSettings.STANDARD_BUDGET.get(settings),
            UsageSettings.LARGE_BUDGET.get(settings) };
        this.groupOf = groupOf;
        this.usage = usage;
        this.clock = clock;
    }

    /** What the guardrails know of an index: its tenant, and how the tenant is budgeted. */
    public record IndexInfo(String tenant, Tier tier, long budgetPerMinute, Mode mode, Settings settings) {
    }

    private static final class Tenant {
        long lastCostMilli = -1L;
        long lastDocs = -1L;
        double costPerMinute;
        double docsPerMinute;
        int level;
        long overSinceMillis = -1L;
        long underSinceMillis = -1L;
        long changedAtMillis = -1L;
    }

    // ---- what an index is

    /**
     * Learns an index's tenant and budget from its settings. Cheap when the settings are the ones it already holds.
     *
     * @param index the index name
     * @param indexSettings its settings
     */
    public void note(String index, Settings indexSettings) {
        final IndexInfo known = indices.get(index);
        if (known != null && known.settings() == indexSettings) {
            return;
        }
        final String group = groupOf.apply(indexSettings);
        final String tierName = UsageSettings.INDEX_TIER.get(indexSettings);
        final Tier tier = tierName.isEmpty() ? defaultTier : Tier.parse(tierName);
        final long override = UsageSettings.INDEX_BUDGET.get(indexSettings);
        final String modeName = UsageSettings.INDEX_MODE.get(indexSettings);
        indices.put(
            index,
            new IndexInfo(
                group == null || group.isEmpty() ? index : group,
                tier,
                override > 0 ? override : tierBudgets[tier.ordinal()],
                modeName.isEmpty() ? nodeMode : Mode.parse(modeName),
                indexSettings
            )
        );
    }

    /**
     * Returns what is known of an index: what its settings said, or the node's defaults with the index as its own tenant.
     *
     * @param index the index name
     * @return the index's tenant and budget
     */
    public IndexInfo info(String index) {
        final IndexInfo known = indices.get(index);
        return known != null ? known : new IndexInfo(index, defaultTier, tierBudgets[defaultTier.ordinal()], nodeMode, null);
    }

    /**
     * Returns a tenant's level: the higher of this node's and the one set from outside.
     *
     * @param tenant the tenant
     * @return 0 to 4
     */
    public int level(String tenant) {
        final Tenant state = tenants.get(tenant);
        return Math.max(state == null ? 0 : state.level, external.getOrDefault(tenant, 0));
    }

    private boolean enforcedAt(IndexInfo info, int minLevel) {
        return info.mode() == Mode.ENFORCE && level(info.tenant()) >= minLevel;
    }

    /**
     * Applies the cluster's settings: the levels set from outside.
     *
     * @param cluster the {@code cluster/config} register's settings
     */
    public void applyCluster(Settings cluster) {
        external = parseLevels(UsageSettings.TENANT_LEVELS.get(cluster));
    }

    /**
     * Parses {@code tenant=level,...}, leaving out pairs that do not parse.
     *
     * @param value the setting's value
     * @return tenant to level, 0 to 4
     */
    public static Map<String, Integer> parseLevels(String value) {
        if (value == null || value.isBlank()) {
            return Map.of();
        }
        final Map<String, Integer> levels = new HashMap<>();
        for (String pair : value.split(",")) {
            final int eq = pair.lastIndexOf('=');
            if (eq <= 0) {
                continue;
            }
            try {
                final int level = Integer.parseInt(pair.substring(eq + 1).trim());
                final String tenant = pair.substring(0, eq).trim();
                if (tenant.isEmpty() == false && level >= 0 && level <= 4) {
                    levels.put(tenant, level);
                }
            } catch (NumberFormatException e) {
                // Left out: a bad pair must not stop the others.
            }
        }
        return Map.copyOf(levels);
    }

    // ---- level one

    /**
     * Returns how long an index's oldest unpublished write may get: stretched for a tenant at level one or above.
     *
     * @param index the index
     * @param ageMillis the age it would otherwise publish at; less than zero (never by age) is kept
     * @return the age
     */
    public long publishAge(String index, long ageMillis) {
        return ageMillis >= 0 && enforcedAt(info(index), 1) ? ageMillis * PUBLISH_STRETCH : ageMillis;
    }

    // ---- levels

    /** Recomputes every tenant's level from the usage since the last tick; a no-op when the node's mode is off. */
    public synchronized void tick() {
        final StoreUsage store = usage.get();
        if (store == null || nodeMode == Mode.OFF) {
            return;
        }
        final long now = clock.getAsLong();
        final long elapsed = lastTickMillis < 0 ? -1L : Math.max(1L, now - lastTickMillis);
        lastTickMillis = now;
        final Map<String, long[]> sums = new HashMap<>();
        for (Map.Entry<String, IndexUsage> line : store.lines().entrySet()) {
            if (line.getKey().startsWith("_")) {
                continue;
            }
            final IndexInfo info = info(line.getKey());
            if (info.mode() == Mode.OFF) {
                continue;
            }
            final long[] sum = sums.computeIfAbsent(info.tenant(), k -> new long[2]);
            sum[0] += line.getValue().costMilliUnits();
            sum[1] += line.getValue().get(IndexUsage.Kind.DOCS);
        }
        final Map<String, Long> budgets = new HashMap<>();
        for (IndexInfo info : indices.values()) {
            budgets.merge(info.tenant(), info.budgetPerMinute(), Math::max);
        }
        for (Map.Entry<String, long[]> sum : sums.entrySet()) {
            final Tenant tenant = tenants.computeIfAbsent(sum.getKey(), k -> new Tenant());
            final long budget = budgets.getOrDefault(sum.getKey(), tierBudgets[defaultTier.ordinal()]);
            if (elapsed > 0 && tenant.lastCostMilli >= 0) {
                tenant.costPerMinute = (sum.getValue()[0] - tenant.lastCostMilli) / 1000d * 60_000d / elapsed;
                tenant.docsPerMinute = (sum.getValue()[1] - tenant.lastDocs) * 60_000d / elapsed;
                step(tenant, tenant.costPerMinute > budget, now);
            }
            tenant.lastCostMilli = sum.getValue()[0];
            tenant.lastDocs = sum.getValue()[1];
        }
    }

    private static void step(Tenant tenant, boolean over, long now) {
        if (over) {
            tenant.underSinceMillis = -1L;
            if (tenant.overSinceMillis < 0) {
                tenant.overSinceMillis = now;
            }
            final long wait = tenant.level == 0 ? LEVEL_UP_AFTER_MILLIS : NEXT_LEVEL_AFTER_MILLIS;
            final long since = tenant.level == 0 ? tenant.overSinceMillis : Math.max(tenant.overSinceMillis, tenant.changedAtMillis);
            if (tenant.level < 3 && now - since >= wait) {
                tenant.level++;
                tenant.changedAtMillis = now;
            }
        } else {
            tenant.overSinceMillis = -1L;
            if (tenant.underSinceMillis < 0) {
                tenant.underSinceMillis = now;
            }
            final long since = Math.max(tenant.underSinceMillis, tenant.changedAtMillis);
            if (tenant.level > 0 && now - since >= LEVEL_DOWN_AFTER_MILLIS) {
                tenant.level--;
                tenant.changedAtMillis = now;
            }
        }
    }

    // ---- counting

    /**
     * Counts documents written to an index.
     *
     * @param index the index
     * @param docs how many
     */
    public void wrote(String index, int docs) {
        final StoreUsage store = usage.get();
        if (store != null && docs > 0) {
            store.record(index, IndexUsage.Kind.DOCS, docs);
        }
    }

    /**
     * Counts bytes of an index's writes kept to push to search nodes.
     *
     * @param index the index
     * @param bytes how many
     */
    public void pushed(String index, long bytes) {
        final StoreUsage store = usage.get();
        if (store != null && bytes > 0) {
            store.record(index, IndexUsage.Kind.PUSH_BYTES, bytes);
        }
    }

    /**
     * Starts counting a search over indices: one query for each, and its time shared between them when it ends.
     *
     * @param searched the indices it covers
     * @return ends the search
     */
    public Releasable searching(Collection<String> searched) {
        final StoreUsage store = usage.get();
        if (store == null || searched.isEmpty()) {
            return () -> {};
        }
        final long started = System.nanoTime();
        return () -> {
            final long each = (System.nanoTime() - started) / searched.size();
            for (String index : searched) {
                store.record(index, IndexUsage.Kind.QUERIES, 1L);
                store.record(index, IndexUsage.Kind.SEARCH_NANOS, each);
            }
        };
    }

    // ---- reporting

    /**
     * Describes what the guardrails know of each tenant this node has seen.
     *
     * @return tenant to its rate, budget, level and counts
     */
    public synchronized Map<String, Map<String, Object>> tenants() {
        final Map<String, Long> budgets = new HashMap<>();
        final Map<String, String> tiers = new HashMap<>();
        for (IndexInfo info : indices.values()) {
            budgets.merge(info.tenant(), info.budgetPerMinute(), Math::max);
            tiers.putIfAbsent(info.tenant(), info.tier().name().toLowerCase(Locale.ROOT));
        }
        final Map<String, Map<String, Object>> out = new TreeMap<>();
        for (Map.Entry<String, Tenant> entry : tenants.entrySet()) {
            final Tenant tenant = entry.getValue();
            final Map<String, Object> line = new LinkedHashMap<>();
            line.put("level", level(entry.getKey()));
            line.put("local_level", tenant.level);
            line.put("set_level", external.getOrDefault(entry.getKey(), 0));
            line.put("tier", tiers.getOrDefault(entry.getKey(), defaultTier.name().toLowerCase(Locale.ROOT)));
            line.put("budget_per_minute", budgets.getOrDefault(entry.getKey(), tierBudgets[defaultTier.ordinal()]));
            line.put("cost_per_minute", Math.round(tenant.costPerMinute * 100d) / 100d);
            line.put("docs_per_minute", Math.round(tenant.docsPerMinute * 100d) / 100d);
            out.put(entry.getKey(), line);
        }
        return out;
    }

    /**
     * Returns the store's per-index usage these guardrails read.
     *
     * @return the usage, or null when metering is off or the store is not open yet
     */
    public StoreUsage usage() {
        return usage.get();
    }

    /**
     * Returns the node's mode.
     *
     * @return the mode
     */
    public Mode mode() {
        return nodeMode;
    }
}
