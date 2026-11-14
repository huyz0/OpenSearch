/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.rest.RestController;
import org.opensearch.serverless.rest.NotImplementedHandler;

/**
 * The endpoints a classic OpenSearch node serves that this shell refuses, each with the reason it is refused for.
 *
 * <p>Registered as explicit 501 handlers so a client reaching for one gets a statement of why it is absent, rather than
 * core's default 400 "no handler found for uri", which reads like a typo.
 */
final class RefusedEndpoints {

    private RefusedEndpoints() {}

    /**
     * Registers a refusal, unless a plugin already serves the route.
     *
     * <p>The plugins' handlers are registered first, so a route a plugin serves is taken by the time its refusal
     * comes. The refusal yields: it states why the shell itself does not serve an endpoint, and once a plugin
     * serves it that statement is no longer true. The case that found it is the painless module, loaded as a
     * bundle for k-NN and alerting, whose script-execution endpoint used to fail the node's start against the
     * shell's refusal of the same path.
     */
    private static void refuse(RestController controller, org.opensearch.rest.RestHandler refusal) {
        try {
            controller.registerHandler(refusal);
        } catch (IllegalArgumentException e) {
            if (e.getMessage() == null || e.getMessage().startsWith("Cannot replace existing handler") == false) {
                throw e;
            }
        }
    }

    /**
     * Registers a refusal for every endpoint this shell does not serve.
     *
     * @param controller the controller the shell's own handlers are already registered on
     */
    static void register(RestController controller) {
        // These exist in classic OpenSearch and are absent here by design. Registering an explicit
        // refusal turns "no handler for uri" — which reads like a typo — into a statement.
        final String noGlobalState = "there is no cluster-wide state in a serverless cluster; "
            + "no node can answer this, and a node-local answer would be misleading";

        // The allowlist and the enumeration rule together: listing a deployment's indices is an inventory
        // operation, not a serving one. Offering it here -- even paginated -- invites a caller to walk
        // a hundred million indices through a request path, so the request path does not offer it.
        refuse(
            controller,
            new NotImplementedHandler(
                "/_serverless/indices",
                "enumerating indices is a maintenance operation, not a serving one; "
                    + "look an index up by name, or run an offline inventory"
            )
        );
        // What is left after the answerable ones moved out. Each of these needs a snapshot across every
        // index, or an allocator, or a cluster-manager task queue -- three things this design does not have
        // and is not going to grow. The endpoints that only needed the lease registry or one index's shard
        // heads are served instead, above.
        for (String path : new String[] { "/_cluster/state", "/_cluster/stats", "/_cat" }) {
            refuse(controller, new NotImplementedHandler(path, noGlobalState));
        }

        // Refusals that used to share the "no cluster-wide state" reason and are not actually refused for
        // it. A caller reading that a listing is unavailable because there is no cluster state would
        // reasonably conclude the data does not exist -- when in fact it exists, is readable one index at a
        // time, and is refused because reading all of it at once is unbounded. Three of these are refused
        // because there is no allocator, which is a different thing again. A refusal must give the
        // reason that is actually true.
        final String noClosedState = "an index here is being served or it does not exist: there is no closed state "
            + "for it to be opened from or closed into";
        final String noClusterManager = "there is no cluster manager, so there is no node to name as one";
        final String reshaping = "reshaping an index means rewriting every document into a different shard count, "
            + "and there is no _reindex here to do it with. An index that needs a different shard count is a new "
            + "index, written by the caller";
        final String simulate = "simulating template resolution is not routed; create the index and read back "
            + "GET /{index}, which reports exactly what it inherited";
        final String indexEnumeration = "aggregating across every index in the deployment is unbounded, and this "
            + "design refuses an answer it would have to truncate";
        final String noAllocator = "there is no allocator here: a shard is activated by the write that needs it, "
            + "not by a placement decision, so there is nothing to explain, retry or rebalance";
        final String enumeration = "listing every index in the deployment is unbounded, and this design refuses "
            + "an answer it would have to truncate. Use GET /_list/indices/{prefix}*, which resolves through "
            + "one bounded listing and refuses a prefix that matches more than the cap rather than cutting it "
            + "off -- OpenSearch added _list for this reason and left _cat alone";
        for (String[] refusal : new String[][] {
            {
                "/_cat/shards",
                "the same unbounded listing as _cat/indices, one row per shard. GET "
                    + "/_cluster/health/{index}?level=shards reports one index's shards, bounded" },
            { "/_cat/aliases", "aliases are found by name here, not enumerated; GET /_alias/{name} answers for one" },
            {
                "/_cat/count",
                "counting every document in the deployment means visiting every index; GET "
                    + "/{index}/_count answers for one, and takes a prefix pattern" },
            {
                "/_cat/segments",
                "per-shard Lucene detail across every index, which is both unbounded and not " + "exposed for a single index either" },
            { "/_cluster/reroute", noAllocator },
            { "/_cluster/allocation/explain", noAllocator },
            { "/_cat/allocation", noAllocator },
            { "/_cat/recovery", noAllocator },
            { "/_cat/pending_tasks", "there is no cluster manager and no task queue, so there is nothing pending" },
            {
                "/_cat/thread_pool",
                "this reports per-node thread pools, which are not among the numbers this shell reports "
                    + "about itself; GET /_serverless/stats answers for the node it is sent to, and "
                    + "?nodes=_all for the fleet" },
            { "/_list/shards", "the same unbounded listing as _cat/shards" } }) {
            refuse(controller, new NotImplementedHandler(refusal[0], refusal[1]));
        }

        // Refused for a reason of its own, and the reason has changed. The fan-out this used to be waiting
        // for exists: GET /_serverless/stats?nodes=_all asks every live node and reports which ones did not
        // answer. What is still missing is not the reach, it is the schema -- this endpoint means indices,
        // os, jvm, fs, transport and thread_pool, and none of those numbers are produced here. Serving the
        // shell's own document under this name would hand a client that parses core's schema something that
        // looks right and answers nothing, which is worse than a refusal that points at the real endpoint.
        refuse(
            controller,
            new NotImplementedHandler(
                "/_nodes/stats",
                "this endpoint's body is core's node-statistics schema -- indices, os, jvm, fs, transport, "
                    + "thread_pool -- and this shell does not produce those numbers, so there is nothing "
                    + "honest to put under this name. GET /_serverless/stats answers for the node it is sent "
                    + "to, and GET /_serverless/stats?nodes=_all asks every live node and names the ones that "
                    + "did not answer"
            )
        );
        refuse(
            controller,
            new NotImplementedHandler(
                "/_tasks",
                "there is no cluster-wide task registry: work here belongs to the node doing it and does not "
                    + "outlive it, so there is no task to look up by id from anywhere else"
            )
        );

        // Unimplemented endpoints return 501 with a reason. Without this list only explicitly refused
        // paths did; everything else a normal client reaches for fell through to core's default handler
        // and came back as
        // 400 {"error":"no handler found for uri ..."} -- a bare string, the wrong status, and
        // indistinguishable from a typo. These are the endpoints a client actually tries, each refused with
        // the reason it is refused for rather than with a generic one, because "not here" and "not here
        // *because*" are different answers and only the second is useful.
        final String noReindex = "bulk reindexing runs for hours and needs a task that survives the node "
            + "that started it; there is no cluster-wide task registry here. Read with search_after and "
            + "write with _bulk, which is the same work under the caller's own control";
        for (String[] refusal : new String[][] {
            {
                "/{index}/_refresh",
                "refresh is per-write here: pass refresh=true on the write that must be visible. A "
                    + "deployment-wide refresh would have to reach every node holding a shard of this index" },
            {
                "/{index}/_flush",
                "there is no flush to ask for: durability is the write-ahead log, which a write is in "
                    + "before it is acknowledged, and publication is the background reconciler's job" },
            {
                "/{index}/_forcemerge",
                "merging is the shard writer's own decision; there is no cluster-wide operation to trigger "
                    + "it, and forcing one on a shard this node may not own is not something this node can do" },
            { "/{index}/_termvectors/{id}", "term vectors are a per-shard Lucene detail this surface does not expose" },
            { "/_reindex", noReindex },
            { "/{index}/_update_by_query", noReindex },
            {
                "/_search/scroll",
                "scroll holds a search context open on a node; use a point in time (POST /{index}/_pit) with "
                    + "search_after, which is held in the object store and is not tied to one node" },
            { "/_ilm/policy/{name}", "index lifecycle management is not part of this surface" },
            // Found by driving a running node rather than by reading this list: twenty-two endpoints a real
            // client reaches for were answering core's default 400 "no handler found for uri", which reads
            // as a typo. Each has a reason under one of the four the rest of this list already gives -- no
            // allocator, no cluster-wide state, no modules, no closed index -- and none of them said so.
            { "/_alias", "aliases are found by name here, not enumerated; GET /_alias/{name} answers for one" },
            { "/_stats", indexEnumeration },
            {
                "/{index}/_stats",
                "per-shard statistics are not exposed; GET /_serverless/stats answers for the node "
                    + "it is sent to, and GET /_cluster/health/{index}?level=shards reports one index's shards" },
            { "/{index}/_segments", "per-shard Lucene detail is not exposed" },
            { "/{index}/_recovery", noAllocator },
            { "/{index}/_shard_stores", noAllocator },
            {
                "/{index}/_cache/clear",
                "there is no cluster-wide operation to clear a cache, and clearing one on a "
                    + "shard this node may not own is not something this node can do" },
            { "/{index}/_open", noClosedState },
            { "/{index}/_close", noClosedState },
            { "/{index}/_shrink/{target}", reshaping },
            { "/{index}/_split/{target}", reshaping },
            { "/{index}/_clone/{target}", reshaping },
            { "/_cluster/pending_tasks", "there is no cluster manager and no task queue, so there is nothing pending" },
            { "/_cat/master", noClusterManager },
            { "/_cat/cluster_manager", noClusterManager },
            {
                "/_cat/plugins",
                "plugins are per node; GET /_nodes lists the fleet and GET /_serverless/stats answers " + "for the node it is sent to" },
            {
                "/_cat/nodeattrs",
                "node attributes are not part of this deployment's node model; GET /_nodes reports " + "the roles a node does have" },
            {
                "/_cat/repositories",
                "a repository here is a namespace in this deployment's own object store; " + "GET /_snapshot lists them" },
            { "/_cat/snapshots", "GET /_snapshot/{repo} lists a repository's snapshots" },
            { "/_cat/tasks", "there is no cluster-wide task registry" },
            { "/_cat/fielddata", "fielddata is per-shard memory this surface does not report" },
            { "/_remote/info", "there are no remote clusters: cross-cluster search and replication are not part of " + "this design" },
            {
                "/_dangling",
                "a dangling index is one whose data outlived the cluster state naming it. There is no "
                    + "cluster state here, so an index exists exactly when its descriptor does" },
            {
                "/_scripts/painless/_execute",
                "the script-execution sandbox endpoint is not routed; inline scripts run " + "in a query, an aggregation or an update" },
            { "/_index_template/_simulate_index/{name}", simulate },
            { "/_index_template/_simulate/{name}", simulate },
            // The nameless form is the one in the spec, and it used to match POST /_index_template/{name}
            // and store a live template called "_simulate".
            { "/_index_template/_simulate", simulate },
            { "/_component_template/_simulate/{name}", simulate },
            { "/_component_template/_simulate", simulate },
            // Found by comparing this surface against AWS OpenSearch Serverless and Elastic Cloud
            // Serverless: every one of these is an endpoint a real client reaches for, and every one of them
            // fell through to core's default 400 rather than the promised 501. Being absent is a
            // decision; looking like a typo is not.
            {
                "/_field_caps",
                "name an index or a prefix: GET /{index}/_field_caps answers for what it can reach, and "
                    + "asking every index in the deployment what fields it has is the inventory operation "
                    + "this design refuses" },
            {
                "/_cat/templates",
                "listing templates as a table is not routed; GET /_index_template returns them all, and "
                    + "GET /_index_template/{prefix}* narrows by name" },
            // Found by walking every path in rest-api-spec against a running node (ServerlessSpecCoverageTests)
            // rather than by hand: each of these is a sibling of a path already refused above -- the
            // index-less form, the {name} form, the {metric} form -- that the refusal did not cover, so it
            // fell through to core's default 400 "no handler found for uri" while its sibling answered 501
            // with a reason. Same reason, same answer, every spelling.
            { "/_cat/templates/{name}", "GET /_index_template/{name} answers for one template" },
            { "/_cat/aliases/{name}", "aliases are found by name here, not enumerated; GET /_alias/{name} answers for one" },
            { "/_cat/allocation/{nodeId}", noAllocator },
            { "/_cat/count/{index}", "GET /{index}/_count answers for one index, and takes a prefix pattern" },
            { "/_cat/fielddata/{fields}", "fielddata is per-shard memory this surface does not report" },
            { "/_cat/recovery/{index}", noAllocator },
            { "/_cat/segments/{index}", "per-shard Lucene detail is not exposed" },
            { "/_cat/shards/{index}", "GET /_cluster/health/{index}?level=shards reports one index's shards, bounded" },
            { "/_cat/snapshots/{repo}", "GET /_snapshot/{repo}/_all lists a repository's snapshots" },
            { "/_cat/thread_pool/{thread_pool_patterns}", "GET /_serverless/stats answers for the node it is sent to" },
            { "/_cat/segment_replication", "there is no segment replication here: a shard's redundancy is the object store" },
            { "/_cat/segment_replication/{index}", "there is no segment replication here: a shard's redundancy is the object store" },
            { "/_cluster/state/{metric}", noGlobalState },
            { "/_cluster/state/{metric}/{index}", noGlobalState },
            { "/_cluster/stats/nodes/{nodeId}", noGlobalState },
            { "/_cluster/decommission/awareness", noAllocator },
            { "/_cluster/decommission/awareness/{name}/_status", noAllocator },
            { "/_cluster/decommission/awareness/{name}/{value}", noAllocator },
            { "/_cluster/voting_config_exclusions", noClusterManager },
            { "/_cluster/routing/awareness/weights", noAllocator },
            { "/_cluster/routing/awareness/{attribute}/weights", noAllocator },
            { "/_dangling/{index_uuid}", "there is no cluster state here, so an index exists exactly when its descriptor does" },
            { "/_script_context", "the context catalogue is not served; PUT /_scripts/{id}/{context} checks a script against one" },
            { "/_script_language", "the language catalogue is not served; painless and mustache are the languages here" },
            { "/{index}/_block/{block}", noClosedState },
            { "/_cache/clear", "there is no cluster-wide operation to clear a cache" },
            {
                "/_data_stream",
                "listing every data stream is the enumeration this design refuses; GET /_data_stream/{name} answers for "
                    + "one, and takes a prefix pattern" },
            {
                "/_data_stream/_modify",
                "a data stream here is an alias with a generation, so its backing indices are the alias's members "
                    + "and are changed by rolling it over, not by reassigning them underneath it" },
            { "/_data_stream/_stats", "store sizes are not reported here; GET /_data_stream/{name} lists a stream's backing indices" },
            {
                "/_data_stream/{name}/_stats",
                "store sizes are not reported here; GET /_data_stream/{name} lists a stream's backing indices" },
            { "/_template", "legacy templates are not served; composable templates are, at /_index_template" },
            { "/_template/{name}", "legacy templates are not served; composable templates are, at /_index_template/{name}" },
            { "/_flush", "there is no flush to ask for: durability is the write-ahead log" },
            { "/_refresh", "refresh is per-write here: pass refresh=true on the write that must be visible" },
            { "/_forcemerge", "merging is the shard writer's own decision" },
            { "/_recovery", noAllocator },
            { "/_segments", "per-shard Lucene detail is not exposed" },
            { "/_shard_stores", noAllocator },
            { "/_upgrade", "there is no upgrade to run: an index is served by whatever version serves it" },
            { "/{index}/_upgrade", "there is no upgrade to run: an index is served by whatever version serves it" },
            { "/_mapping", enumeration },
            { "/_mapping/field/{fields}", enumeration },
            { "/_settings", enumeration },
            { "/_settings/{name}", enumeration },
            { "/_stats/{metric}", indexEnumeration },
            {
                "/{index}/_stats/{metric}",
                "per-shard statistics are not exposed; GET /_serverless/stats answers for the node it is sent to" },
            { "/_validate/query", "name an index: GET /{index}/_validate/query answers for what it can reach" },
            {
                "/_ingest/processor/grok",
                "the grok pattern catalogue is not served; a pipeline naming a grok processor is compiled at PUT" },
            { "/_mtermvectors", "term vectors are a per-shard Lucene detail this surface does not expose" },
            { "/{index}/_mtermvectors", "term vectors are a per-shard Lucene detail this surface does not expose" },
            { "/{index}/_termvectors", "term vectors are a per-shard Lucene detail this surface does not expose" },
            { "/_nodes/hot_threads", "per-node diagnostics are not served; GET /_serverless/stats answers for the node it is sent to" },
            {
                "/_nodes/{nodeId}/hot_threads",
                "per-node diagnostics are not served; GET /_serverless/stats answers for the node it is sent to" },
            { "/_nodes/reload_secure_settings", "secure settings are read at start; there is no reload" },
            { "/_nodes/{nodeId}/reload_secure_settings", "secure settings are read at start; there is no reload" },
            { "/_nodes/usage", "feature usage is not counted here" },
            { "/_nodes/{nodeId}/usage", "feature usage is not counted here" },
            { "/_nodes/usage/{metric}", "feature usage is not counted here" },
            { "/_nodes/{nodeId}/usage/{metric}", "feature usage is not counted here" },
            { "/_nodes/{nodeId}/stats", "GET /_serverless/stats answers for the node it is sent to" },
            { "/_nodes/stats/{metric}", "GET /_serverless/stats answers for the node it is sent to" },
            { "/_nodes/{nodeId}/stats/{metric}", "GET /_serverless/stats answers for the node it is sent to" },
            { "/_nodes/stats/{metric}/{index_metric}", "GET /_serverless/stats answers for the node it is sent to" },
            { "/_nodes/{nodeId}/stats/{metric}/{index_metric}", "GET /_serverless/stats answers for the node it is sent to" },
            { "/_rank_eval", "name an index: ranking evaluation belongs in the harness doing the evaluating" },
            { "/_reindex/{task_id}/_rethrottle", noReindex },
            { "/_update_by_query/{task_id}/_rethrottle", noReindex },
            { "/_delete_by_query/{task_id}/_rethrottle", "delete_by_query runs inline here and is not throttled" },
            { "/_remotestore/_restore", "the object store is the store; there is no separate remote store to restore from" },
            { "/_remotestore/stats/{index}", "the object store is the store; there is no separate remote store to report on" },
            { "/_remotestore/stats/{index}/{shard_id}", "the object store is the store; there is no separate remote store to report on" },
            { "/_search_shards", "shards here are placed by id and served by whichever node holds them; there is no allocation to report" },
            {
                "/{index}/_search_shards",
                "shards here are placed by id and served by whichever node holds them; there is no allocation to report" },
            {
                "/_snapshot/{repo}/_cleanup",
                "a repository here is a namespace in this deployment's own object store, swept by its own collector" },
            { "/_snapshot/{repo}/{snapshot}/_clone/{target}", "cloning a snapshot is not served; take a new one" },
            { "/_snapshot/_status", "every snapshot here is taken synchronously, so there is no in-progress status to report" },
            { "/_snapshot/{repo}/_status", "every snapshot here is taken synchronously, so there is no in-progress status to report" },
            {
                "/_snapshot/{repo}/{snapshot}/_status",
                "every snapshot here is taken synchronously, so there is no in-progress status to report" },
            {
                "/_snapshot/{repo}/{snapshot}/{index}/_status",
                "every snapshot here is taken synchronously, so there is no in-progress status to report" },
            { "/_tasks/_cancel", "there is no cluster-wide task registry" },
            { "/_tasks/{task_id}", "there is no cluster-wide task registry" },
            { "/_tasks/{task_id}/_cancel", "there is no cluster-wide task registry" },
            { "/_list/wlm_stats", "workload management is not part of this surface" },
            {
                "/{index}/_mapping/field/{fields}",
                "GET /{index}/_mapping returns the whole mapping, which is one register read either way" } }) {
            refuse(controller, new NotImplementedHandler(refusal[0], refusal[1]));
        }
    }
}
