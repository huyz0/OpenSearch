/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * What every REST handler shares: refusals and failures in OpenSearch's own error shape, the action filters each
 * request runs under, a channel answered later, cat tables, response budgets, index patterns, and the handler that
 * refuses what this surface does not implement.
 */
package org.opensearch.serverless.restcore;
