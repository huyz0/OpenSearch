/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Snapshot repositories and the snapshot records taken in them: the repository descriptors, each snapshot's record
 * and name register, and where the data a standard snapshot copies lives.
 *
 * <p>A snapshot record pins the shard files it names until it is deleted, and an unfinished capture pins its indices,
 * which is how the metadata plane and the garbage collector read these records. Nothing here reads index descriptors
 * or shard heads; the metadata plane composes these stores and keeps the public entry points.
 */
package org.opensearch.serverless.repository;
