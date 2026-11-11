/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.routing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * An index's range map: which unit covers which range of the 64-bit routing hash space.
 *
 * <p>Immutable, and held by the index descriptor, so it is cached exactly as long as the descriptor is and
 * replaced with it by one compare-and-swap. Lookup is a binary search over the units' first hashes, which
 * for the few hundred units an index can have is a handful of comparisons per document.
 *
 * <p><b>The shape rule.</b> Every unit is either a run of whole declared shards or a piece of exactly one.
 * Splits keep it by cutting along declared-shard boundaries first, and only halving inside a declared
 * shard once a unit holds just one. Because of it, a declared shard always maps to the units that cover its
 * range, and a unit never holds part of one declared shard together with any of another, so a search
 * routed to one declared shard needs at most a range filter on the unit it lands on.
 *
 * <p>Unit ids are never reused. A split retires its unit and creates two new ones, so a unit id names
 * one Lucene index's content for its whole life, which is what lets the old unit keep serving while its
 * successors are built.
 */
public final class UnitLayout {

    /** The largest hash, unsigned. */
    static final long LAST_HASH = -1L;

    /**
     * One unit and the hash range it covers, both ends inclusive and unsigned.
     *
     * @param id the unit id, unique for the index's life
     * @param first the first hash it covers
     * @param last the last hash it covers
     */
    public record Unit(int id, long first, long last) {

        /**
         * Whether this unit covers a hash.
         *
         * @param hash an unsigned hash
         * @return true when {@code first <= hash <= last}
         */
        public boolean covers(long hash) {
            return Long.compareUnsigned(first, hash) <= 0 && Long.compareUnsigned(hash, last) <= 0;
        }

        @Override
        public String toString() {
            return "unit " + id + " [" + Long.toHexString(first) + ", " + Long.toHexString(last) + "]";
        }
    }

    /**
     * A planned split: the unit it retires, the two that replace it, and where it cuts.
     *
     * @param retired the unit being split
     * @param left the lower half, which ends at {@code cut - 1}
     * @param right the upper half, which starts at {@code cut}
     */
    public record Split(Unit retired, Unit left, Unit right) {
    }

    private final ShardMode mode;
    private final int declaredShards;
    private final List<Unit> units;
    private final long[] firsts;
    private final int nextUnitId;
    private final Split pending;

    /**
     * Creates a layout, checking that it covers the whole hash space and keeps the shape rule.
     *
     * @param mode the index's shard mode
     * @param declaredShards the user's {@code number_of_shards}
     * @param units the units, in any order
     * @param nextUnitId the id the next new unit gets; greater than every id in {@code units}
     */
    public UnitLayout(ShardMode mode, int declaredShards, List<Unit> units, int nextUnitId) {
        this(mode, declaredShards, units, nextUnitId, null);
    }

    /**
     * Creates a layout with a split planned but not yet applied.
     *
     * @param mode the index's shard mode
     * @param declaredShards the user's {@code number_of_shards}
     * @param units the live units, in any order
     * @param nextUnitId the id the next new unit gets; greater than every id in {@code units} and in {@code pending}
     * @param pending the planned split whose halves have ids reserved but serve nothing yet, or null
     */
    public UnitLayout(ShardMode mode, int declaredShards, List<Unit> units, int nextUnitId, Split pending) {
        this.mode = Objects.requireNonNull(mode);
        if (declaredShards < 1) {
            throw new IllegalArgumentException("declared shards must be positive, got " + declaredShards);
        }
        this.declaredShards = declaredShards;
        final List<Unit> sorted = new ArrayList<>(units);
        sorted.sort((a, b) -> Long.compareUnsigned(a.first(), b.first()));
        if (sorted.isEmpty()) {
            throw new IllegalArgumentException("a layout needs at least one unit");
        }
        final Set<Integer> ids = new HashSet<>();
        long expected = 0L;
        for (int i = 0; i < sorted.size(); i++) {
            final Unit unit = sorted.get(i);
            if (unit.id() < 0 || unit.id() >= nextUnitId || ids.add(unit.id()) == false) {
                throw new IllegalArgumentException(unit + " has a duplicate or out-of-range id (next id " + nextUnitId + ")");
            }
            if (unit.first() != expected || Long.compareUnsigned(unit.first(), unit.last()) > 0) {
                throw new IllegalArgumentException(
                    unit + " leaves a gap or overlaps: expected it to start at " + Long.toHexString(expected)
                );
            }
            checkShape(unit);
            if (i < sorted.size() - 1) {
                if (unit.last() == LAST_HASH) {
                    throw new IllegalArgumentException(unit + " ends the hash space but is not the last unit");
                }
                expected = unit.last() + 1L;
            } else if (unit.last() != LAST_HASH) {
                throw new IllegalArgumentException(unit + " is the last unit but does not reach the end of the hash space");
            }
        }
        this.units = Collections.unmodifiableList(sorted);
        this.firsts = new long[sorted.size()];
        for (int i = 0; i < sorted.size(); i++) {
            firsts[i] = sorted.get(i).first();
        }
        this.nextUnitId = nextUnitId;
        if (pending != null) {
            final Split p = pending;
            if (this.units.contains(p.retired()) == false
                || p.left().first() != p.retired().first()
                || p.right().last() != p.retired().last()
                || p.left().last() == LAST_HASH
                || p.left().last() + 1L != p.right().first()
                || ids.contains(p.left().id())
                || ids.contains(p.right().id())
                || p.left().id() == p.right().id()
                || Math.max(p.left().id(), p.right().id()) >= nextUnitId) {
                throw new IllegalArgumentException("pending split " + p + " does not split a unit of this layout into two new halves");
            }
            checkShape(p.left());
            checkShape(p.right());
        }
        this.pending = pending;
    }

    /**
     * The layout a new index starts with.
     *
     * @param mode fixed: one unit per declared shard, ids {@code 0..N-1}; adaptive: one unit, id 0
     * @param declaredShards the user's {@code number_of_shards}
     * @return the initial layout
     */
    public static UnitLayout initial(ShardMode mode, int declaredShards) {
        if (mode == ShardMode.ADAPTIVE) {
            return new UnitLayout(mode, declaredShards, List.of(new Unit(0, 0L, LAST_HASH)), 1);
        }
        final List<Unit> units = new ArrayList<>(declaredShards);
        for (int shard = 0; shard < declaredShards; shard++) {
            units.add(new Unit(shard, RoutingHash.firstHashOf(shard, declaredShards), RoutingHash.lastHashOf(shard, declaredShards)));
        }
        return new UnitLayout(mode, declaredShards, units, declaredShards);
    }

    private void checkShape(Unit unit) {
        final int firstShard = RoutingHash.declaredShard(unit.first(), declaredShards);
        final int lastShard = RoutingHash.declaredShard(unit.last(), declaredShards);
        if (firstShard == lastShard) {
            return; // a piece of one declared shard, or exactly one
        }
        // Several declared shards: they must be whole, so it starts and ends on their boundaries.
        if (unit.first() != RoutingHash.firstHashOf(firstShard, declaredShards)
            || unit.last() != RoutingHash.lastHashOf(lastShard, declaredShards)) {
            throw new IllegalArgumentException(unit + " holds part of a declared shard together with another declared shard");
        }
    }

    /**
     * Returns the index's shard mode.
     *
     * @return the index's shard mode
     */
    public ShardMode mode() {
        return mode;
    }

    /**
     * Returns the user's {@code number_of_shards}, which is what every user-facing shard count reports.
     *
     * @return the user's {@code number_of_shards}, which is what every user-facing shard count reports
     */
    public int declaredShards() {
        return declaredShards;
    }

    /**
     * Returns the units, ordered by the hash range they cover.
     *
     * @return the units, ordered by the hash range they cover
     */
    public List<Unit> units() {
        return units;
    }

    /**
     * Returns the number of units.
     *
     * @return the number of units
     */
    public int unitCount() {
        return units.size();
    }

    /**
     * Returns the id the next new unit gets.
     *
     * @return the id the next new unit gets
     */
    public int nextUnitId() {
        return nextUnitId;
    }

    /**
     * Returns the unit covering a hash.
     *
     * @param hash an unsigned hash
     * @return the unit
     */
    public Unit unitFor(long hash) {
        int low = 0;
        int high = firsts.length - 1;
        // The last unit whose first hash is at or below the hash. The first unit starts at 0, so one exists.
        while (low < high) {
            final int mid = (low + high + 1) >>> 1;
            if (Long.compareUnsigned(firsts[mid], hash) <= 0) {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        return units.get(low);
    }

    /**
     * Returns the unit a routing value goes to.
     *
     * @param routing the document's routing, or its id when it has none
     * @return the unit
     */
    public Unit unitForRouting(String routing) {
        return unitFor(RoutingHash.of(routing));
    }

    /**
     * Returns the units that hold a declared shard's documents.
     *
     * <p>One unit when the declared shard is whole inside it, more once it has been halved.
     *
     * @param declaredShard the declared shard, in {@code [0, declaredShards)}
     * @return the units, in hash order
     */
    public List<Unit> unitsOf(int declaredShard) {
        final long first = RoutingHash.firstHashOf(declaredShard, declaredShards);
        final long last = RoutingHash.lastHashOf(declaredShard, declaredShards);
        final List<Unit> covering = new ArrayList<>();
        for (Unit unit : units) {
            if (Long.compareUnsigned(unit.last(), first) >= 0 && Long.compareUnsigned(unit.first(), last) <= 0) {
                covering.add(unit);
            }
        }
        return covering;
    }

    /**
     * Whether a search for one declared shard needs a hash filter on this unit: true when the unit holds
     * documents of other declared shards too.
     *
     * @param unit a unit of this layout
     * @param declaredShard the declared shard searched
     * @return true when a range filter is needed to return exactly that declared shard's documents
     */
    public boolean needsFilter(Unit unit, int declaredShard) {
        return RoutingHash.declaredShard(unit.first(), declaredShards) != declaredShard
            || RoutingHash.declaredShard(unit.last(), declaredShards) != declaredShard;
    }

    /**
     * Plans splitting a unit in two, without changing this layout.
     *
     * <p>A unit holding several declared shards is cut at the declared-shard boundary nearest its middle,
     * so both halves stay runs of whole declared shards. A unit holding one declared shard, or a piece of
     * one, is halved by hash.
     *
     * @param unitId the unit to split
     * @return the plan
     * @throws IllegalArgumentException when there is no such unit, or it covers a single hash
     */
    public Split planSplit(int unitId) {
        if (pending != null) {
            throw new IllegalStateException("a split is already pending: " + pending);
        }
        final Unit unit = unit(unitId);
        final int firstShard = RoutingHash.declaredShard(unit.first(), declaredShards);
        final int lastShard = RoutingHash.declaredShard(unit.last(), declaredShards);
        final long cut;
        if (lastShard > firstShard) {
            cut = RoutingHash.firstHashOf(firstShard + (lastShard - firstShard + 1) / 2, declaredShards);
        } else {
            if (unit.first() == unit.last()) {
                throw new IllegalArgumentException(unit + " covers a single hash and cannot be split");
            }
            cut = unit.first() + ((unit.last() - unit.first()) >>> 1) + 1L;
        }
        return new Split(unit, new Unit(nextUnitId, unit.first(), cut - 1L), new Unit(nextUnitId + 1, cut, unit.last()));
    }

    /**
     * Returns the split planned but not yet applied, or null.
     *
     * <p>Its halves' ids are reserved, so they are shard slots of the index and can be built, opened and caught
     * up, but they are not live units: nothing routes to them and no search covers them until the split is
     * applied.
     *
     * @return the pending split, or null
     */
    public Split pending() {
        return pending;
    }

    /**
     * Returns this layout with a split recorded as pending, its halves' ids reserved.
     *
     * @param split a plan made from this layout
     * @return the new layout
     */
    public UnitLayout withPending(Split split) {
        return new UnitLayout(
            mode,
            declaredShards,
            units,
            Math.max(nextUnitId, Math.max(split.left().id(), split.right().id()) + 1),
            split
        );
    }

    /**
     * Returns this layout without its pending split; the reserved ids stay used, so they are never handed out again.
     *
     * @return the new layout
     */
    public UnitLayout withoutPending() {
        return new UnitLayout(mode, declaredShards, units, nextUnitId, null);
    }

    /**
     * Returns this layout with a planned split applied. A pending split, if it is this one, is cleared.
     *
     * @param split a plan made from this layout
     * @return the new layout
     */
    public UnitLayout apply(Split split) {
        if (units.contains(split.retired()) == false) {
            throw new IllegalArgumentException("the split's " + split.retired() + " is not in this layout");
        }
        final List<Unit> next = new ArrayList<>(units);
        next.remove(split.retired());
        next.add(split.left());
        next.add(split.right());
        final Split stillPending = split.equals(pending) ? null : pending;
        return new UnitLayout(
            mode,
            declaredShards,
            next,
            Math.max(nextUnitId, Math.max(split.left().id(), split.right().id()) + 1),
            stillPending
        );
    }

    /**
     * Returns this layout with two neighbouring units merged into one new unit.
     *
     * <p>Refused when the result would break the shape rule, which happens when one is a piece of a
     * declared shard and the other holds a different one.
     *
     * @param lowerId the unit covering the lower range
     * @param upperId the unit covering the range right after it
     * @return the new layout
     * @throws IllegalArgumentException when the units are not neighbours or cannot merge
     */
    public UnitLayout merge(int lowerId, int upperId) {
        final Unit lower = unit(lowerId);
        final Unit upper = unit(upperId);
        if (lower.last() == LAST_HASH || lower.last() + 1L != upper.first()) {
            throw new IllegalArgumentException(lower + " and " + upper + " are not neighbours");
        }
        final List<Unit> next = new ArrayList<>(units);
        next.remove(lower);
        next.remove(upper);
        next.add(new Unit(nextUnitId, lower.first(), upper.last()));
        if (pending != null && (pending.retired().equals(lower) || pending.retired().equals(upper))) {
            throw new IllegalStateException("unit " + pending.retired().id() + " has a split pending");
        }
        return new UnitLayout(mode, declaredShards, next, nextUnitId + 1, pending);
    }

    /**
     * Returns a unit by id.
     *
     * @param unitId the id
     * @return the unit
     * @throws IllegalArgumentException when this layout has no such unit
     */
    public Unit unit(int unitId) {
        for (Unit unit : units) {
            if (unit.id() == unitId) {
                return unit;
            }
        }
        throw new IllegalArgumentException("no unit " + unitId + " in this layout");
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof UnitLayout other
            && mode == other.mode
            && declaredShards == other.declaredShards
            && nextUnitId == other.nextUnitId
            && units.equals(other.units)
            && Objects.equals(pending, other.pending);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode, declaredShards, nextUnitId, units, pending);
    }

    @Override
    public String toString() {
        return "UnitLayout["
            + mode.settingValue()
            + ", declared="
            + declaredShards
            + ", units="
            + units
            + (pending == null ? "" : ", pending=" + pending)
            + "]";
    }
}
