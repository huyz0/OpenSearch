/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The object-store request budgets of {@code serverless/request-budgets.md}, read from the test classpath, and the one check
 * a gate makes against them.
 *
 * <p>The table is the single place a pinned number lives: a gate test names its row by key and never repeats the number,
 * so lowering a budget is a one-line change to the table, and a change that raises a count fails until the table is
 * changed too, in the same diff, where a reviewer sees it.
 */
public final class RequestBudgets {

    /** Where the build copies the table on the test classpath. */
    static final String RESOURCE = "request-budgets.md";

    /** How a measurement is held to its budget. */
    enum Bound {
        /** The measurement equals the budget: more is a regression, less is an improvement the table must record. */
        EXACT,
        /** The measurement is at most the budget. */
        MAX
    }

    /** Whether a row is held by a gate or waits on something named. */
    enum Status {
        /** A gate test checks it on every run. */
        ENFORCED,
        /** Not yet met; the row names what blocks it. */
        BLOCKED
    }

    /**
     * One row of the table.
     *
     * @param key the objective's name, as a gate passes it
     * @param what the case in words
     * @param budget the pinned number
     * @param bound how the measurement is held to it
     * @param status enforced or blocked
     * @param gate for an enforced row the test that checks it, {@code Class#method}; for a blocked row what blocks it
     * @param line where the row is in the table
     */
    record Row(String key, String what, long budget, Bound bound, Status status, String gate, int line) {
    }

    private static final Pattern KEY = Pattern.compile("`([a-z0-9_.]+)`");

    private static volatile Map<String, Row> rows;

    private RequestBudgets() {}

    /**
     * Parses the table. A malformed row is an error naming its line, never a row silently skipped.
     *
     * @param in the table
     * @return its rows by key, in table order
     * @throws IOException if it cannot be read
     */
    static Map<String, Row> parse(InputStream in) throws IOException {
        final Map<String, Row> parsed = new LinkedHashMap<>();
        final List<String> errors = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String text;
            int n = 0;
            while ((text = reader.readLine()) != null) {
                n++;
                final String line = text.trim();
                if (line.startsWith("|") == false || line.startsWith("|---") || line.startsWith("| Objective")) {
                    continue;
                }
                final String[] cells = line.substring(1, line.endsWith("|") ? line.length() - 1 : line.length()).split("\\|", -1);
                if (cells.length != 6) {
                    errors.add(n + ": expected 6 cells, found " + cells.length);
                    continue;
                }
                final Matcher key = KEY.matcher(cells[0].trim());
                if (key.matches() == false) {
                    errors.add(n + ": the objective is not a `lower.case_key`: " + cells[0].trim());
                    continue;
                }
                final long budget;
                final Bound bound;
                final Status status;
                try {
                    budget = Long.parseLong(cells[2].trim().replace(",", ""));
                    bound = Bound.valueOf(cells[3].trim().toUpperCase(Locale.ROOT));
                    status = Status.valueOf(cells[4].trim().toUpperCase(Locale.ROOT));
                } catch (IllegalArgumentException e) {
                    errors.add(n + ": budget must be a number, bound exact or max, status enforced or blocked: " + line);
                    continue;
                }
                final Row row = new Row(key.group(1), cells[1].trim(), budget, bound, status, cells[5].trim().replace("`", ""), n);
                if (parsed.putIfAbsent(row.key(), row) != null) {
                    errors.add(n + ": objective " + row.key() + " appears twice");
                }
            }
        }
        if (errors.isEmpty() == false) {
            throw new IllegalStateException(RESOURCE + " is malformed:\n  " + String.join("\n  ", errors));
        }
        return parsed;
    }

    /**
     * Returns every row of the table on the classpath.
     *
     * @return the rows by key, in table order
     */
    static Map<String, Row> rows() {
        Map<String, Row> loaded = rows;
        if (loaded == null) {
            try (InputStream in = RequestBudgets.class.getResourceAsStream(RESOURCE)) {
                if (in == null) {
                    throw new IllegalStateException(RESOURCE + " is not on the test classpath; the build copies it there");
                }
                loaded = parse(in);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            rows = loaded;
        }
        return loaded;
    }

    /**
     * Whether this run records measurements instead of checking them: {@code -Dtests.request_budgets.record=true}, for
     * filling in a new row or reading where a changed count's requests went. The gate tests then log what they measured.
     *
     * @return true when recording
     */
    public static boolean recording() {
        return Boolean.parseBoolean(System.getProperty("tests.request_budgets.record", "false"));
    }

    /**
     * Holds a measurement to its row's budget, failing with what to do about it.
     *
     * @param key the objective
     * @param measured what the gate measured
     * @param detail where the requests went, shown on failure so a regression names its cause
     */
    public static void check(String key, long measured, String detail) {
        if (recording()) {
            return;
        }
        final Row row = rows().get(key);
        if (row == null) {
            throw new AssertionError("no objective " + key + " in serverless/" + RESOURCE);
        }
        if (row.status() != Status.ENFORCED) {
            throw new AssertionError("objective " + key + " is " + row.status() + "; a gate checks only enforced rows");
        }
        verdict(row, measured).ifPresent(failure -> { throw new AssertionError(failure + "\n" + detail); });
    }

    /**
     * Judges a measurement against a row.
     *
     * @param row the row
     * @param measured the measurement
     * @return the failure, or empty if the measurement meets the budget
     */
    static java.util.Optional<String> verdict(Row row, long measured) {
        if (measured > row.budget()) {
            return java.util.Optional.of(
                String.format(
                    Locale.ROOT,
                    "%s regressed: measured %d, budget %d (%s). Find the new requests below; if they are worth paying, raise the "
                        + "budget in serverless/%s in the same change and say why in its description.",
                    row.key(),
                    measured,
                    row.budget(),
                    row.what(),
                    RESOURCE
                )
            );
        }
        if (row.bound() == Bound.EXACT && measured < row.budget()) {
            return java.util.Optional.of(
                String.format(
                    Locale.ROOT,
                    "%s improved: measured %d, budget %d (%s). Lower the budget in serverless/%s to %d so the saving cannot be "
                        + "lost silently.",
                    row.key(),
                    measured,
                    row.budget(),
                    row.what(),
                    RESOURCE,
                    measured
                )
            );
        }
        return java.util.Optional.empty();
    }
}
