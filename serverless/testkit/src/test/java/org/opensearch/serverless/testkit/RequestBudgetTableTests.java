/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Every row of {@code serverless/request-budgets.md} is {@code enforced} by a gate that exists and checks it, or
 * {@code blocked} with its blocker named. There is no third status: a third status is how "we aim for zero" gets written
 * by someone who measured nothing.
 *
 * <p>What this cannot check: that the named gate measures what the row's words say. It checks that the test method
 * exists and that its class passes the row's key to {@link RequestBudgets#check}; a gate that passes the key with the
 * wrong number satisfies it.
 */
public class RequestBudgetTableTests extends OpenSearchTestCase {

    private static final String PACKAGE = RequestBudgetTableTests.class.getPackageName();

    public void testEveryRowIsEnforcedByAGateOrNamesItsBlocker() throws Exception {
        final Map<String, RequestBudgets.Row> rows = RequestBudgets.rows();
        assertFalse("the table has no rows", rows.isEmpty());
        final List<String> errors = new ArrayList<>();
        for (RequestBudgets.Row row : rows.values()) {
            final String where = "request-budgets.md:" + row.line() + " " + row.key() + ": ";
            if (row.status() == RequestBudgets.Status.BLOCKED) {
                if (row.gate().isBlank() || row.gate().equals("-") || row.gate().equalsIgnoreCase("none")) {
                    errors.add(where + "blocked, but names no blocker");
                }
                continue;
            }
            final String problem = gateProblem(row);
            if (problem != null) {
                errors.add(where + problem);
            }
        }
        assertTrue("request budgets:\n  " + String.join("\n  ", errors), errors.isEmpty());
    }

    /** Why an enforced row's gate does not hold it, or null if it does. */
    private static String gateProblem(RequestBudgets.Row row) throws IOException {
        final String[] gate = row.gate().split("#", -1);
        if (gate.length != 2 || gate[0].isBlank() || gate[1].isBlank()) {
            return "enforced, but the gate is not Class#method: " + row.gate();
        }
        final Class<?> type;
        try {
            type = Class.forName(PACKAGE + "." + gate[0]);
        } catch (ClassNotFoundException e) {
            return "enforced, but no test class " + gate[0] + " exists in " + PACKAGE;
        }
        final Method method;
        try {
            method = type.getMethod(gate[1]);
        } catch (NoSuchMethodException e) {
            return "enforced, but " + gate[0] + " has no public method " + gate[1];
        }
        if (method.getName().startsWith("test") == false || Modifier.isStatic(method.getModifiers())) {
            return "enforced, but " + row.gate() + " is not a test method the runner runs";
        }
        // The key as a string constant in the class file: the gate passes it to RequestBudgets.check.
        try (InputStream in = type.getResourceAsStream(type.getSimpleName() + ".class")) {
            final String classFile = new String(in.readAllBytes(), StandardCharsets.ISO_8859_1);
            if (classFile.contains(row.key()) == false) {
                return "enforced, but " + gate[0] + " never names the key, so it cannot check it";
            }
        }
        return null;
    }

    public void testARegressionFailsAndNamesTheBudget() {
        final RequestBudgets.Row exact = row(RequestBudgets.Bound.EXACT, 10);
        final String failure = RequestBudgets.verdict(exact, 11).orElseThrow();
        assertTrue(failure, failure.contains("regressed: measured 11, budget 10"));
        assertTrue(RequestBudgets.verdict(row(RequestBudgets.Bound.MAX, 10), 11).isPresent());
    }

    public void testAnExactSavingFailsUntilTheTableRecordsIt() {
        final String failure = RequestBudgets.verdict(row(RequestBudgets.Bound.EXACT, 10), 7).orElseThrow();
        assertTrue(failure, failure.contains("improved: measured 7, budget 10") && failure.contains("to 7"));
        assertTrue(RequestBudgets.verdict(row(RequestBudgets.Bound.EXACT, 10), 10).isEmpty());
        assertTrue("a ceiling is met from below", RequestBudgets.verdict(row(RequestBudgets.Bound.MAX, 10), 7).isEmpty());
    }

    public void testAMalformedRowIsAnErrorNotASkip() {
        final String table = "| Objective | Case | Budget | Bound | Status | Gate |\n"
            + "|---|---|---|---|---|---|\n"
            + "| `a.b` | fine | 1 | exact | enforced | `X#testY` |\n"
            + "| `a.c` | no status | 1 | exact | aspirational | `X#testY` |\n"
            + "| `a.b` | twice | 1 | max | blocked | something |\n"
            + "| a.d | not a key | 1 | max | blocked | something |\n";
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> RequestBudgets.parse(new ByteArrayInputStream(table.getBytes(StandardCharsets.UTF_8)))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("4: budget must be a number"));
        assertTrue(e.getMessage(), e.getMessage().contains("5: objective a.b appears twice"));
        assertTrue(e.getMessage(), e.getMessage().contains("6: the objective is not"));
    }

    public void testCheckRefusesAnUnknownOrBlockedObjective() {
        expectThrows(AssertionError.class, () -> RequestBudgets.check("no.such.objective", 0, ""));
        final String blocked = RequestBudgets.rows()
            .values()
            .stream()
            .filter(r -> r.status() == RequestBudgets.Status.BLOCKED)
            .map(RequestBudgets.Row::key)
            .findFirst()
            .orElseThrow(() -> new AssertionError("the table should keep at least one blocked target"));
        expectThrows(AssertionError.class, () -> RequestBudgets.check(blocked, 0, ""));
    }

    private static RequestBudgets.Row row(RequestBudgets.Bound bound, long budget) {
        return new RequestBudgets.Row("x.y", "a case", budget, bound, RequestBudgets.Status.ENFORCED, "X#testY", 1);
    }
}
