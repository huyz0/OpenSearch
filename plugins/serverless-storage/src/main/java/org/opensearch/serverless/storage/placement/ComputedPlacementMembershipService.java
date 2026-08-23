/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.Lifecycle;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps the published placement membership up to date: adding nodes as they appear, and removing one
 * only once it has been absent for both a sustained run of observations and a real stretch of wall-clock
 * time.
 *
 * <p>The asymmetry is the design. Adding a node has to happen automatically, because a cluster that
 * grows should start using the new capacity without an operator ceremony. Removing one must not happen
 * eagerly, because a node is absent for two very different reasons that look identical from here:
 * it is restarting and will be back with its data, or it is gone for good. Guessing wrong in the second
 * direction is harmless and self-correcting; guessing wrong in the first direction moves every shard
 * that node owned to a node with none of its data, which then recovers empty while looking healthy.
 * That is the failure C13 hit, and it is silent.
 *
 * <p><b>This javadoc used to say "decommission is not implemented", and that was dangerously stale.</b>
 * It is implemented, it runs automatically, and it moves shards -- so a reader who trusted this paragraph
 * would have been reasoning about a class that could not lose data while looking at one that could. See
 * {@link #decommissionable} for what now has to be true before a member is dropped, and why one of those
 * two conditions on its own was not enough.
 *
 * <p>Runs only on the elected cluster manager, because it writes cluster state. Every other node reads
 * the result.
 *
 * <p><b>Deliberately still built on {@link AbsentIndexRoutingSuppliers} directly, out of scope for Phase
 * C4b of {@code core-pluggability-refactor-plan.md}</b> (its own {@code addRegistrationListener}/{@code
 * removeRegistrationListener}/{@code isRegistered} calls, in the constructor and {@link #publishIfElected}/
 * {@link #clusterChanged}). This service's whole reason to exist is reacting to a supplier's registration
 * <em>changing</em> during a node's lifetime -- publishing immediately once placement is enabled rather than
 * waiting for the next unrelated cluster-state change, and self-unregistering on node shutdown. The SPI
 * ({@code ClusterPlugin#getIndexCatalog()}) has no equivalent concept: a catalog is registered once,
 * unconditionally, at node startup (confirmed against the real registration:
 * {@code ServerlessStoragePlugin#getIndexCatalog()} always returns a catalog, regardless of
 * whether the underlying feature is on), and never re-registered or unregistered afterward. Migrating this
 * service would mean either inventing a registration-change-notification concept the SPI doesn't have (a
 * real design question of its own, not attempted here) or dropping the "publish immediately on enable"
 * optimization and relying solely on {@link #clusterChanged}'s own organic triggering -- a real, if narrow,
 * behavior change deliberately not made without explicit sign-off. {@code IndexCatalog#isActive()} is the
 * closest the SPI comes, and it is the answer to the closely related finding that "a catalog is registered"
 * and "the underlying feature is currently active" are not the same question -- but this service needs a
 * third thing neither answers: notice of the <em>moment</em> the feature turns on.
 */
public class ComputedPlacementMembershipService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ComputedPlacementMembershipService.class);

    private final ClusterService clusterService;

    /**
     * Held as a field rather than passed as a method reference twice, because adding and removing have to
     * refer to the same object for the removal to find anything.
     */
    private final Runnable onPlacementRegistered = this::publishIfElected;

    /**
     * How many consecutive elected-cluster-manager observations a member may be missing before it is
     * eligible to be treated as gone.
     *
     * <p>Not a tuned number so much as a stated one. Every cluster state change this node applies is an
     * observation, so this is "absent across a sustained run of cluster activity" rather than a duration.
     * Ten is chosen to be clearly on the safe side, and should be re-derived against a real restart
     * profile before anyone relies on the exact value.
     *
     * <p><b>And on its own it was not a delay at all.</b> A node leaving generates its own burst of
     * cluster state updates -- node-left, the shard failures that follow it, the reroutes those trigger --
     * and this plugin keeps generating more on top of that, so ten observations routinely elapse inside a
     * few seconds, well within an ordinary node restart. Counting events measured how busy the cluster
     * was, not how long the node had been away, and it was busiest precisely because the node had just
     * gone. That is why {@link #DECOMMISSION_ABSENCE_SETTING} exists beside it.
     */
    static final long ABSENT_OBSERVATIONS_BEFORE_DECOMMISSION = 10;

    /**
     * How long a member must have been continuously absent, in wall-clock time, before it may be
     * decommissioned.
     *
     * <p>The condition the observation count was silently failing to be. A restart takes as long as it
     * takes regardless of how many cluster state updates happen meanwhile, so the thing that has to be
     * measured is time. Fifteen minutes is comfortably longer than a node restart, including one that
     * replays a large translog, and short enough that a node genuinely gone under autoscaling does not
     * hold rendezvous weight for the rest of the day.
     *
     * <p>Both conditions are required, and neither is redundant. The clock alone would decommission a node
     * across a cluster-manager election that simply was not watching; the observations alone are what the
     * incident above showed is not a duration at all.
     *
     * <p>Declared on {@code ServerlessStoragePlugin} and only aliased here, for the reason that class's own
     * setting javadoc gives: the plugin's declared settings are asserted to be exactly the set it registers
     * in {@code getSettings()}, so a setting declared anywhere else is one an operator can never configure.
     */
    public static final org.opensearch.common.settings.Setting<org.opensearch.common.unit.TimeValue> DECOMMISSION_ABSENCE_SETTING =
        org.opensearch.serverless.storage.ServerlessStoragePlugin.PLACEMENT_DECOMMISSION_ABSENCE_SETTING;

    /** One member's absence: how many times it has been observed missing, and when it first was. */
    private record Absence(long observations, long firstAbsentMillis) {
    }

    /**
     * Per-member absence, held only on the elected cluster manager.
     *
     * <p>Deliberately not persisted. See {@link #decommissionable}: losing this restarts both the count
     * and the clock, which delays a decommission and can never cause one, and that is the direction that
     * fails safely.
     */
    private final java.util.Map<String, Absence> absences = new java.util.concurrent.ConcurrentHashMap<>();

    private final long decommissionAfterAbsenceMillis;

    /** The wall clock, injectable so a test can move time without waiting for it. */
    private final java.util.function.LongSupplier clock;

    public ComputedPlacementMembershipService(ClusterService clusterService) {
        this(clusterService, decommissionAfterAbsenceMillis(clusterService), System::currentTimeMillis);
    }

    /**
     * The configured absence window, falling back to the setting's own default when the cluster service
     * carries no settings. A {@link ClusterService} built for a construction test has none, and reading
     * through it unconditionally turned that into an NPE while wiring the plugin -- for a value whose
     * default is the right answer in exactly that case.
     */
    private static long decommissionAfterAbsenceMillis(ClusterService clusterService) {
        org.opensearch.common.settings.Settings settings = clusterService == null ? null : clusterService.getSettings();
        return (settings == null
            ? DECOMMISSION_ABSENCE_SETTING.getDefault(org.opensearch.common.settings.Settings.EMPTY)
            : DECOMMISSION_ABSENCE_SETTING.get(settings)).millis();
    }

    ComputedPlacementMembershipService(
        ClusterService clusterService,
        long decommissionAfterAbsenceMillis,
        java.util.function.LongSupplier clock
    ) {
        this.decommissionAfterAbsenceMillis = decommissionAfterAbsenceMillis;
        this.clock = clock;
        this.clusterService = clusterService;
        // Publish as soon as placement is enabled rather than waiting for the next unrelated cluster
        // state change. Without this, a cluster that installs a supplier and then goes idle keeps
        // computing placement against the live node list, which is the input this whole mechanism exists
        // to stop using, and it does so silently.
        AbsentIndexRoutingSuppliers.addRegistrationListener(onPlacementRegistered);
    }

    private void publishIfElected() {
        // The registry is static and outlives any single node, so a node that has shut down is still on
        // its listener list. Leaving would mean submitting cluster state updates against a closed service,
        // which in a test JVM means every node of every earlier suite waking up on the next registration.
        if (clusterService.lifecycleState() != Lifecycle.State.STARTED) {
            AbsentIndexRoutingSuppliers.removeRegistrationListener(onPlacementRegistered);
            return;
        }
        ClusterState state = clusterService.state();
        if (state.nodes().isLocalNodeElectedClusterManager() == false) {
            return;
        }
        List<String> dataNodes = dataNodeIds(state);
        if (dataNodes.isEmpty() == false && get(state).withNodes(dataNodes) != get(state)) {
            submitUpdate(dataNodes, List.of());
        }
    }

    /** The membership published in this state, or empty when nothing has been published yet. */
    public static ComputedPlacementMembership get(ClusterState state) {
        ComputedPlacementMembership membership = (ComputedPlacementMembership) state.metadata().custom(ComputedPlacementMembership.TYPE);
        return membership == null ? ComputedPlacementMembership.EMPTY : membership;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().nodes().isLocalNodeElectedClusterManager() == false) {
            return;
        }
        // Nothing to maintain until a placement supplier is installed. An unconfigured cluster must not
        // acquire this metadata at all, so that turning the feature off leaves no trace behind.
        if (AbsentIndexRoutingSuppliers.isRegistered() == false) {
            return;
        }
        List<String> dataNodes = dataNodeIds(event.state());
        if (dataNodes.isEmpty()) {
            return;
        }
        List<String> decommission = decommissionable(get(event.state()), dataNodes);
        if (get(event.state()).withNodes(dataNodes) == get(event.state()) && decommission.isEmpty()) {
            return; // already a superset with nothing to drop, and withNodes returning the same instance is how we know
        }
        submitUpdate(dataNodes, decommission);
    }

    /**
     * Members that have been absent long enough to be treated as gone rather than restarting.
     *
     * <p><b>Two conditions, because the first one alone was not what it claimed to be.</b> This used to
     * require only a run of consecutive observations, reasoning that "a wall clock says how long a node has
     * been away and not whether anyone was watching, so a cluster manager that was itself restarting would
     * see a long absence for a node that never left". That half is still true, and it is why observations
     * are still required. What it missed is that observations are not a delay: the departure itself
     * generates node-left, shard-failed and reroute updates, and this plugin generates more, so the ten
     * observations elapsed <em>inside</em> an ordinary restart -- decommissioning a node that was coming
     * back and re-placing its shards onto nodes holding none of its data, which is exactly the C13 failure
     * the counter existed to prevent, produced by the mechanism meant to prevent it.
     *
     * <p>So a member is decommissionable only when it has been missed across a sustained run of
     * observations <em>and</em> continuously absent for {@link #DECOMMISSION_ABSENCE_SETTING}. The clock
     * starts at the first observed absence and is thrown away the moment the node is seen again, so an
     * intermittent node accumulates nothing.
     *
     * <p><b>Both are in memory, and losing them is the safe direction.</b> A cluster manager election
     * resets the count and the clock, which delays a decommission and never causes one. That asymmetry is
     * deliberate: decommissioning a node that was merely restarting moves its shards to nodes holding none
     * of its data, and those recover empty while looking healthy, which is silent. Waiting longer costs
     * requests that fail and retry, which is loud and self-correcting.
     */
    private List<String> decommissionable(ComputedPlacementMembership membership, List<String> presentDataNodes) {
        for (String present : presentDataNodes) {
            absences.remove(present);
        }
        long now = clock.getAsLong();
        List<String> gone = new ArrayList<>();
        for (String member : membership.nodeIds()) {
            if (presentDataNodes.contains(member)) {
                continue;
            }
            Absence absence = absences.merge(
                member,
                new Absence(1L, now),
                (existing, fresh) -> new Absence(existing.observations() + 1, existing.firstAbsentMillis())
            );
            if (absence.observations() >= ABSENT_OBSERVATIONS_BEFORE_DECOMMISSION
                && now - absence.firstAbsentMillis() >= decommissionAfterAbsenceMillis) {
                gone.add(member);
            }
        }
        return gone;
    }

    /**
     * The decommission decision for one observation of this state -- test-only visibility.
     *
     * <p>Exposed because the property worth pinning is the decision itself, not the cluster state update it
     * eventually produces: the version of this code that decommissioned a restarting node submitted a
     * perfectly well-formed update, and what was wrong was what it had decided to put in it.
     */
    List<String> decommissionableForTesting(ClusterState state) {
        return decommissionable(get(state), dataNodeIds(state));
    }

    private void submitUpdate(List<String> dataNodes, List<String> decommission) {
        clusterService.submitStateUpdateTask("computed-placement-membership", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                ComputedPlacementMembership current = get(currentState);
                // The decommission list was captured against the state that was current when this task was
                // submitted, and a queued task can wait behind others long enough for the node to come
                // back -- a restart finishing is exactly the event that produces a burst of cluster state
                // work for this task to queue behind. Applying the captured list unconditionally removed a
                // node that was present again in this very state and then re-added it from dataNodes,
                // bouncing every shard it owned through two epochs for nothing. So presence is re-checked
                // here, against the state actually being transformed, which is the only state whose answer
                // matters.
                List<String> stillAbsent = new ArrayList<>();
                List<String> presentNow = dataNodeIds(currentState);
                for (String candidate : decommission) {
                    if (presentNow.contains(candidate) == false) {
                        stillAbsent.add(candidate);
                    } else {
                        logger.info("skipping decommission of [{}]: it is present again in the state being updated", candidate);
                        absences.remove(candidate);
                    }
                }
                ComputedPlacementMembership updated = current.withNodes(dataNodes).withoutNodes(stillAbsent);
                if (updated == current) {
                    return currentState;
                }
                logger.info("computed placement membership now {}", updated);
                return ClusterState.builder(currentState)
                    .metadata(Metadata.builder(currentState.metadata()).putCustom(ComputedPlacementMembership.TYPE, updated))
                    .build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("failed to update computed placement membership", e);
            }
        });
    }

    private static List<String> dataNodeIds(ClusterState state) {
        List<String> nodeIds = new ArrayList<>();
        for (DiscoveryNode node : state.nodes()) {
            if (node.isDataNode()) {
                nodeIds.add(node.getId());
            }
        }
        return nodeIds;
    }
}
