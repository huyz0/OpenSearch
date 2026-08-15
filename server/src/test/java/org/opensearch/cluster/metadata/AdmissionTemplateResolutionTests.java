/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.indices.SystemIndices;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.Collections;

import static java.util.Collections.singletonList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * T52. {@code settingsForAdmission} is meant to mirror {@code applyCreateIndexRequest}'s template
 * resolution exactly, so that the admission decision and the gate creation itself meets agree. It used
 * to resolve a template against {@code request.index()} unconditionally, which is three ways wrong:
 * creation skips templates entirely for a resize target and for a system index, and resolves against the
 * data stream name rather than the backing index's own name when there is one. Getting any of the three
 * wrong over-admits: the request is sent off the cluster state thread on the strength of a template
 * creation itself will not apply, builds and discards a descriptor, is refused by {@code
 * DescriptorOnlyCreation#skipsClusterState}, and falls back to the road it should have taken directly --
 * paying the whole creation twice.
 *
 * <p>The resize case is the one with a cost today, per the round plan, because a resize target is the one
 * of the three most likely to share a name pattern with an ordinary gated template (a shrink or split
 * target is typically named after its source). It gets the dedicated test; the other two divergences are
 * covered alongside it since the fix is one function.
 */
public class AdmissionTemplateResolutionTests extends OpenSearchTestCase {

    private static final Settings GATED_TEMPLATE = Settings.builder().put("index.serverless_storage.enabled", true).build();

    @After
    public void clearRegistrations() {
        DescriptorOnlyCreation.registerAdmissionCheck(null);
        DescriptorOnlyCreation.register(null);
    }

    /**
     * Same shape as {@code GatedMappingOffClusterStateThreadIT}'s admission check: gating turns on the
     * setting a resolved template can carry, so a request that is admitted despite carrying nothing itself
     * proves the template was what did it.
     */
    private static void registerGatedOnTheSetting() {
        DescriptorOnlyCreation.registerAdmissionCheck(settings -> settings.getAsBoolean("index.serverless_storage.enabled", false));
    }

    private MetadataCreateIndexService serviceWithTemplate(String pattern, boolean systemIndex) {
        final Metadata.Builder metadataBuilder = Metadata.builder()
            .put(IndexTemplateMetadata.builder("gated-by-template").patterns(singletonList(pattern)).settings(GATED_TEMPLATE).build());
        final ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadataBuilder)
            .build();

        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        lastState = clusterState;

        final SystemIndices systemIndices = systemIndex
            ? new SystemIndices(
                Collections.singletonMap(
                    "test-feature",
                    singletonList(new SystemIndexDescriptor(pattern, "a descriptor that owns the pattern under test"))
                )
            )
            : new SystemIndices(Collections.emptyMap());

        return new MetadataCreateIndexService(
            Settings.EMPTY,
            clusterService,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            systemIndices,
            true,
            null,
            null,
            null
        );
    }

    private MetadataCreateIndexService serviceWithAliasCarryingTemplate(String pattern) {
        final Metadata.Builder metadataBuilder = Metadata.builder()
            .put(
                IndexTemplateMetadata.builder("gated-by-template-with-alias")
                    .patterns(singletonList(pattern))
                    .settings(GATED_TEMPLATE)
                    .putAlias(AliasMetadata.builder("an-alias"))
                    .build()
            );
        final ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadataBuilder)
            .build();
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        lastState = clusterState;
        return new MetadataCreateIndexService(
            Settings.EMPTY,
            clusterService,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new SystemIndices(Collections.emptyMap()),
            true,
            null,
            null,
            null
        );
    }

    /** The state the last service was built over, which is what {@code localExecute} passes in. */
    private ClusterState lastState;

    private ClusterState stateOf(MetadataCreateIndexService ignored) {
        return lastState;
    }

    /**
     * The condition that lets a creation run on the node that received it rather than the cluster manager.
     *
     * <p>Certain rather than probable, and the tests below are all one property: an admitted creation that
     * the real gate may still decline must not be routed away from the cluster manager, because the road it
     * declines onto submits a cluster state update task and only the cluster manager can publish one.
     */
    public void testAnAdmittedCreationWithNothingToDeclineOnRunsLocally() {
        registerGatedOnTheSetting();
        final MetadataCreateIndexService service = serviceWithTemplate("serverless_gated-*", false);

        final CreateIndexClusterStateUpdateRequest request = new CreateIndexClusterStateUpdateRequest(
            "cause",
            "serverless_gated-index",
            "serverless_gated-index"
        ).settings(Settings.EMPTY);

        assertTrue(
            "a name in the serverless namespace says which plane the index belongs in, with no template to "
                + "resolve and no road to fall back from",
            service.certainlyGated(request, stateOf(service))
        );
    }

    public void testAnAliasOnTheRequestKeepsItOnTheClusterManager() {
        registerGatedOnTheSetting();
        final MetadataCreateIndexService service = serviceWithTemplate("serverless_gated-*", false);

        final CreateIndexClusterStateUpdateRequest request = new CreateIndexClusterStateUpdateRequest(
            "cause",
            "serverless_gated-index",
            "serverless_gated-index"
        ).settings(Settings.EMPTY).aliases(java.util.Set.of(new Alias("an-alias")));

        assertFalse(
            "an index in the namespace cannot have an alias -- validate refuses this creation outright now, "
                + "and this is the second line of defence: were it ever admitted, the road it declines onto "
                + "only works on the cluster manager",
            service.certainlyGated(request, stateOf(service))
        );
    }

    /**
     * The one a request cannot see, which is why this is not simply a check on the request.
     *
     * <p>A template's alias reaches the finished metadata and makes the index non-representable exactly as
     * the request's own would, and nothing in the request says so. Reading only the request here would
     * route the commonest template-configured deployment to the wrong node.
     */
    public void testATemplatesAliasKeepsItOnTheClusterManagerToo() {
        registerGatedOnTheSetting();
        final MetadataCreateIndexService service = serviceWithAliasCarryingTemplate("serverless_gated-*");

        final CreateIndexClusterStateUpdateRequest request = new CreateIndexClusterStateUpdateRequest(
            "cause",
            "serverless_gated-index",
            "serverless_gated-index"
        ).settings(Settings.EMPTY);

        assertTrue(
            "the template must still gate it, or this passes for the wrong reason",
            DescriptorOnlyCreation.mayBypassClusterState(service.settingsForAdmission(request))
        );
        assertFalse(
            "an alias the template contributes is as good a reason to decline as one the request carries",
            service.certainlyGated(request, stateOf(service))
        );
    }

    public void testAnOrdinaryCreationIsNeverRunLocally() {
        registerGatedOnTheSetting();
        final MetadataCreateIndexService service = serviceWithTemplate("serverless_gated-*", false);

        final CreateIndexClusterStateUpdateRequest request = new CreateIndexClusterStateUpdateRequest(
            "cause",
            "ordinary-index",
            "ordinary-index"
        ).settings(Settings.EMPTY);

        assertFalse(
            "an ordinary index needs a cluster state update, and creating one anywhere else loses it",
            service.certainlyGated(request, stateOf(service))
        );
    }

    public void testWithNoGateInstalledNothingRunsLocally() {
        // No registerGatedOnTheSetting(), so this is an ordinary cluster.
        final MetadataCreateIndexService service = serviceWithTemplate("serverless_gated-*", false);

        final CreateIndexClusterStateUpdateRequest request = new CreateIndexClusterStateUpdateRequest(
            "cause",
            "serverless_gated-index",
            "serverless_gated-index"
        ).settings(GATED_TEMPLATE);

        assertFalse(
            "a cluster that never installed the gate must be untouched by this, whatever the name says",
            service.certainlyGated(request, stateOf(service))
        );
    }

    public void testResizeTargetIsNotAdmittedOnATemplateCreationWouldNotResolve() {
        registerGatedOnTheSetting();
        final MetadataCreateIndexService service = serviceWithTemplate("resize-target-*", false);

        final CreateIndexClusterStateUpdateRequest request = new CreateIndexClusterStateUpdateRequest(
            "cause",
            "resize-target-index",
            "resize-target-index"
        ).recoverFrom(new Index("resize-source-index", "someUUID")).settings(Settings.EMPTY);

        assertFalse(
            "applyCreateIndexRequest resolves no template for a resize target -- source metadata is used "
                + "instead -- so admission must not resolve one either, or a resize whose target name "
                + "happens to match a gated template is sent off-thread only to be refused and fall back, "
                + "paying the whole creation twice",
            DescriptorOnlyCreation.mayBypassClusterState(service.settingsForAdmission(request))
        );
    }

    public void testOrdinaryCreationMatchingTheSameTemplateIsStillAdmitted() {
        // The control for the resize test above: the same template, on a request with no recoverFrom,
        // must still gate -- otherwise the resize test would pass because template resolution is broken
        // altogether, not because the resize skip is correct.
        registerGatedOnTheSetting();
        final MetadataCreateIndexService service = serviceWithTemplate("resize-target-*", false);

        final CreateIndexClusterStateUpdateRequest request = new CreateIndexClusterStateUpdateRequest(
            "cause",
            "resize-target-index",
            "resize-target-index"
        ).settings(Settings.EMPTY);

        assertTrue(
            "an ordinary creation matching the same template must still be admitted, or this proves "
                + "nothing about the resize skip specifically",
            DescriptorOnlyCreation.mayBypassClusterState(service.settingsForAdmission(request))
        );
    }

    public void testSystemIndexIsNotAdmittedOnATemplateCreationWouldNotResolve() {
        registerGatedOnTheSetting();
        final MetadataCreateIndexService service = serviceWithTemplate(".system-index", true);

        final CreateIndexClusterStateUpdateRequest request = new CreateIndexClusterStateUpdateRequest(
            "cause",
            ".system-index",
            ".system-index"
        ).settings(Settings.EMPTY);

        assertFalse(
            "applyCreateIndexRequest never resolves a template for a system index, so admission must not " + "either",
            DescriptorOnlyCreation.mayBypassClusterState(service.settingsForAdmission(request))
        );
    }

    public void testDataStreamBackingIndexResolvesAgainstTheDataStreamNameNotItsOwn() {
        // The data-stream-name substitution only matters for v2 template lookup: that is the one place
        // applyCreateIndexRequest itself resolves against dataStreamName() rather than request.index() (its
        // v1 fallback resolves against request.index() even for a data stream backing index, and admission
        // must match that too -- see the pattern below chosen not to match request.index()).
        registerGatedOnTheSetting();
        final Template template = new Template(GATED_TEMPLATE, null, null);
        final ComposableIndexTemplate v2Template = new ComposableIndexTemplate(
            singletonList("logs-app"),
            template,
            Collections.emptyList(),
            1L,
            null,
            null,
            null
        );
        final Metadata.Builder metadataBuilder = Metadata.builder().put("gated-by-v2-template", v2Template);
        final ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadataBuilder)
            .build();
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        final MetadataCreateIndexService service = new MetadataCreateIndexService(
            Settings.EMPTY,
            clusterService,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new SystemIndices(Collections.emptyMap()),
            true,
            null,
            null,
            null
        );

        final CreateIndexClusterStateUpdateRequest request = new CreateIndexClusterStateUpdateRequest(
            "cause",
            ".ds-logs-app-000001",
            ".ds-logs-app-000001"
        ).dataStreamName("logs-app").settings(Settings.EMPTY);

        assertTrue(
            "applyCreateIndexRequest resolves a data stream backing index's v2 template against the data "
                + "stream's own name, not the generated backing index name, so admission must too or it "
                + "misses a template that gates the whole stream",
            DescriptorOnlyCreation.mayBypassClusterState(service.settingsForAdmission(request))
        );
    }
}
