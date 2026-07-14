/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Proves {@link ServerlessStorageLegacySnapshotActionFilter} enforces rfc-serverless-opensearch.md
 * &sect;7.1.1's own policy decision directly against a real {@link ClusterState}/{@link
 * IndexNameExpressionResolver} (the same kind of merged state core itself builds), not through any
 * mocked collaborator.
 */
public class ServerlessStorageLegacySnapshotActionFilterTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        // Not `new ClusterService(...)` directly: that leaves the underlying ClusterApplierService
        // unstarted, so ClusterServiceUtils#setState (which waits on a real apply-listener latch)
        // would hang forever -- confirmed the hard way, not assumed: an earlier version of this
        // test did exactly that and every test hung until the suite's own 20-minute timeout fired.
        // ClusterServiceUtils#createClusterService returns one that's already properly started.
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        clusterService.close();
        ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS);
        super.tearDown();
    }

    private void setClusterState(String... indexNamesAndServerlessFlags) {
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 0; i < indexNamesAndServerlessFlags.length; i += 2) {
            String indexName = indexNamesAndServerlessFlags[i];
            boolean serverlessEnabled = Boolean.parseBoolean(indexNamesAndServerlessFlags[i + 1]);
            Settings.Builder indexSettings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
            if (serverlessEnabled) {
                indexSettings.put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true);
            }
            metadata.put(IndexMetadata.builder(indexName).settings(indexSettings));
        }
        ClusterState state = ClusterState.builder(new ClusterName("test-cluster")).metadata(metadata).build();
        // ClusterService normally only accepts a new state via its own publish path (through a
        // cluster-manager election this unit test never runs) -- ClusterServiceUtils#setState is
        // core's own established test seam for directly injecting one.
        ClusterServiceUtils.setState(clusterService, state);
    }

    private ServerlessStorageLegacySnapshotActionFilter newFilter() {
        ServerlessStorageLegacySnapshotActionFilter filter = new ServerlessStorageLegacySnapshotActionFilter();
        filter.setDependencies(clusterService, new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)));
        return filter;
    }

    private <Request extends ActionRequest, Response extends ActionResponse> boolean[] apply(
        ServerlessStorageLegacySnapshotActionFilter filter,
        String action,
        Request request
    ) {
        AtomicBoolean proceeded = new AtomicBoolean(false);
        AtomicBoolean failed = new AtomicBoolean(false);
        AtomicReference<Exception> failure = new AtomicReference<>();
        ActionFilterChain<Request, Response> chain = (task, chainAction, chainRequest, listener) -> proceeded.set(true);
        ActionListener<Response> listener = ActionListener.wrap(response -> {}, e -> {
            failed.set(true);
            failure.set(e);
        });
        filter.apply(null, action, request, ActionRequestMetadata.empty(), listener, chain);
        if (failed.get()) {
            assertTrue(
                "a rejection must be an IllegalArgumentException naming the offending index/mechanism",
                failure.get() instanceof IllegalArgumentException
            );
        }
        return new boolean[] { proceeded.get(), failed.get() };
    }

    public void testRejectsLegacySnapshotAgainstAServerlessStorageIndex() {
        setClusterState("plain-idx", "false", "serverless-idx", "true");
        ServerlessStorageLegacySnapshotActionFilter filter = newFilter();

        CreateSnapshotRequest request = new CreateSnapshotRequest("some-repo", "some-snapshot").indices("serverless-idx");
        boolean[] result = apply(filter, CreateSnapshotAction.NAME, request);

        assertFalse("must never proceed to the real snapshot machinery", result[0]);
        assertTrue("must reject with a failure", result[1]);
    }

    public void testAllowsLegacySnapshotAgainstAnOrdinaryIndex() {
        setClusterState("plain-idx", "false", "serverless-idx", "true");
        ServerlessStorageLegacySnapshotActionFilter filter = newFilter();

        CreateSnapshotRequest request = new CreateSnapshotRequest("some-repo", "some-snapshot").indices("plain-idx");
        boolean[] result = apply(filter, CreateSnapshotAction.NAME, request);

        assertTrue("an index that never opted into serverless storage must proceed normally", result[0]);
        assertFalse(result[1]);
    }

    public void testWildcardIndexExpressionIsResolvedNotTreatedLiterally() {
        setClusterState("plain-idx", "false", "serverless-idx", "true");
        ServerlessStorageLegacySnapshotActionFilter filter = newFilter();

        // A bare "*" is not itself a real index name -- a literal (unresolved) check against
        // cluster metadata would find nothing and let this straight through, letting an operator
        // trivially route around the check. Resolving it must still catch the serverless index.
        CreateSnapshotRequest request = new CreateSnapshotRequest("some-repo", "some-snapshot").indices("*");
        boolean[] result = apply(filter, CreateSnapshotAction.NAME, request);

        assertFalse("wildcard resolution must still catch the serverless-storage index it expands to", result[0]);
        assertTrue(result[1]);
    }

    public void testOtherActionsAreNeverIntercepted() {
        setClusterState("serverless-idx", "true");
        ServerlessStorageLegacySnapshotActionFilter filter = newFilter();

        SearchRequest request = new SearchRequest("serverless-idx");
        boolean[] result = apply(filter, SearchAction.NAME, request);

        assertTrue("this filter is scoped to CreateSnapshotAction only -- every other action must pass through untouched", result[0]);
        assertFalse(result[1]);
    }
}
