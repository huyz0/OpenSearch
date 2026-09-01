# M43 — the flake from the last session, run down

`ServerlessContentionTests.testAFrozenOwnerDoesNotWedgeANodeAlreadyConnectedToIt` failed once during a
five-suite parallel run (`java.net.http.HttpTimeoutException: request timed out`), then passed 3/3 alone
and 3/3 on the unchanged tree. That is the signature of a harness timing margin, not a defect — but
"probably a margin" is a guess, and the discipline here is not to ship guesses.

## Measuring the mechanism with nothing else in the way

`ServerlessContentionTests` freezes a real OS process with SIGSTOP, across two forked JVMs, behind an
HTTP client with its own timeout, asserting on a wall clock. That is the right end-to-end reproduction
and the wrong instrument for isolating *which* of four moving parts was slow.

`ServerlessForwardBoundTests` measures the same thing with one JVM, a warm transport connection, and an
owner whose handler simply never answers — no process control, no HTTP timeout racing the thing under
test. Run alone:

```
a warm forward to a silent owner returned 503 after 8033ms (lease ttl 8000ms)
```

The forward bound is the lease TTL, exactly, plus 33ms of overhead. The mechanism is not slow. It has no
slack to give up under load, which is a different fact and the one that mattered.

## Where the slack actually was

`ServerlessContentionTests`'s own assertion allows up to 20s (2.5× the 8s TTL) for the response. Its HTTP
client gave up at 25s — a 5s margin over the *assertion* bound, and no margin at all over the *measured*
bound once real scheduling delay is added on top of an already-tight 8033ms. Under a five-suite parallel
run plus two Docker containers on twenty cores, that 5s window is exactly where a forked JVM can lose a
scheduling slot. When it does, the client's own timeout fires first, and the failure is an exception with
nothing in it to measure — which is what made this look uninvestigable from the report alone.

The fix widens the client's request timeout to 90s. This does not weaken what the test checks: the
assertion `elapsedMillis < 20_000L` is unchanged, evaluated after `send` returns, exactly as before. Only
the outer client margin — which was never protecting against a slow server, since the server is
measurably not slow — is loosened, so a legitimately delayed scheduler now produces a number to look at
instead of an exception in its place. The exception path also now reports elapsed time, for the same
reason.

## Confirming the fix does not turn this into a test that cannot fail

Widening a timeout that guards a test is exactly the kind of change that can quietly make a real hang
pass. Canary 101: the forward timeout multiplied by ten, so a frozen owner really is waited on far past
the 20s assertion bound. Caught — the widened client margin does not touch what the test actually checks.

## Result

Four consecutive runs of `processTest` afterward, all green, plus the full five-suite sweep. Not proof a
scheduling-delay flake cannot recur — nothing but continuous running is proof of that for a timing test —
but the number that was missing is present now, and next time it happens there will be a millisecond
count instead of a stack trace to start from.

## Canaries

- **101 — the forward timeout is far longer than the assertion bound.** Caught: the widened client
  margin has not removed the test's ability to catch an actual hang.
