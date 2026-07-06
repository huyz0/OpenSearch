/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.plugins.Plugin;

/**
 * Entry point for the object-store-native serverless storage format: segment bundles, commit
 * manifests, and the bundle garbage collector described in {@code rfc-serverless-opensearch.md}.
 * This plugin currently contributes no wiring into engines or REST handlers (Phase 1 of the
 * roadmap is storage-format-only, testable standalone) &mdash; it exists so the module has a
 * valid plugin descriptor and can be loaded into an integration-test cluster in later phases.
 */
public class ServerlessStoragePlugin extends Plugin {}
