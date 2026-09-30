# Closing the swap/seal fencing window

**Status:** decided, option B, implemented alongside this note.
**Problem:** `STATUS.md`'s one open correctness item. A takeover is two object-store operations -- the
shard-head compare-and-swap that moves ownership, and the log seal that records where the predecessor's
history ends -- and nothing spans them. A predecessor's append landing between them is inside the cutoff:
replayed, though its writer refused to acknowledge it. The seal's cutoff is taken from a listing, so the
window is "from the swap until that listing".

## What must hold

1. **Nothing acknowledged is lost.** A write whose caller was told it is durable is replayed by every
   successor.
2. **Nothing refused is replayed.** A write whose caller was told it failed never reappears.

Both today rest on a clock assumption: a predecessor stops acknowledging at its own lease deadline less a
skew margin, and a successor takes over only after the lease has expired on *its* clock. Within the skew
budget, (1) holds and (2) fails only in the window above; beyond it, (1) can fail too.

## The code, as it stands

- A record is a blob `wal/t=<term>/<20-digit ordinal>`, written put-if-absent (`writeBlob(..., true)`;
  `If-None-Match: *` on S3, `CREATE_NEW` on a filesystem).
- The ordinal is an in-memory counter per `WalStore` instance, seeded from a listing on first use.
- `WalGroupCommitter` issues one PUT at a time per shard, but a failed group does not stop the next one, and
  a close/reopen can briefly leave two instances writing one term.
- Truncation (`onPublished`) deletes names without listing and can empty a term; `dropOlderTerms` empties
  older terms. So a listing does **not** reveal a writer's next ordinal: names are not dense, and can be
  reused after a same-term reopen of an emptied term.
- The seal is `wal/seals/seal-<term>`, per-term cutoffs from a listing, written by overwrite and merged by
  minimum; it is taken at activation and again at open (the same-term reopen case).
- The manifest CAS checks the manifest's own term and writer, never the head's.
- A write is acknowledged only if the node's lease is valid after the PUT (`appendOrRelease`).

## Options

### A. Seal in the head, taken after the predecessor's deadline

Revoke the lease (as `fad3afb` does), wait until the predecessor's self-fence deadline has passed on the
taker's clock plus a rate-drift margin, then list the log and CAS the head to `{term+1, owner, sealed}`.

- Turns the skew assumption into a clock-*rate* one, and makes takeover up to one TTL slower.
- **Does not close (2).** A PUT the predecessor sent before its deadline can land at any later time -- the
  store applies a request whenever it arrives -- so no waiting period guarantees the listing sees every
  landed append. The window narrows to "PUTs in flight at the deadline" and stays open.
- Obstacle (b), the same-term reopen, has no head CAS to carry its seal; it would need a same-term head
  write.

### B. Fence through the log itself -- occupy the predecessor's next slot

The successor writes a *fence* at the predecessor's next ordinal, put-if-absent. The store admits exactly one
of {the predecessor's append at that slot, the fence}. If the fence wins, the predecessor's append is
refused by the store and can never be acknowledged; if the append wins, it is before the fence, the successor
sees it and fences the next slot. Everything that landed is before the fence and is replayed; nothing after
the fence can exist.

- **Closes both (1) and (2) with no clock at all**, provided the predecessor's names are contiguous and the
  successor can find the next one. Neither is true of the current code (above), so B needs four changes:
  1. **Poison on failure.** Any failed or ambiguous append ends the writer's use of that term: every later
     append throws. A writer's successful names are then contiguous from where it started.
  2. **Every writer begins by fencing its own term**, then writes from the fence onwards. No seeding from a
     listing: a same-term reopen fences the instance before it (obstacle b is the same mechanism), and a
     stale instance that re-seeded would otherwise write past a fence. A writer that finds, after placing its
     own fence, that the head no longer names it at that term, poisons itself -- which covers a predecessor
     that had won a head but not yet written anything when it was superseded.
  3. **The next slot is discoverable.** Truncation keeps the highest record of each term; replaying a record
     already in the commit is already routine (truncation lags a publish) and is skipped by sequence number.
     So `max(listing) + 1` is the predecessor's next slot, or a slot it has already lost to a collision.
  4. **Fences are never deleted.** A fence is a zero-length blob -- no record is ever empty -- so truncation,
     older-term drops and replay recognise one from a listing's lengths without reading it. A deleted fence
     would free the slot a paused predecessor's next PUT is aimed at.
- Takeover fences every older term whose highest blob is not already a fence, so a term left unfenced by a
  failed activation is fenced by the next one.
- With a fence in place, a record that landed is by construction replayed, so **a successful PUT is
  acknowledged**; the lease check after the PUT no longer decides acknowledgement. That is what closes (2):
  the refused-but-replayed case came from refusing writes that had in fact landed ahead of the cutoff.
- Remaining assumptions: that the store's put-if-absent on one key is linearizable (the conformance suite
  checks it; R11 is still open for real S3, GCS and Azure), and that an append's outcome is known -- an
  ambiguous result (a timeout) is reported as a failure and poisons the writer, and such a record, if it
  landed, is replayed: the one case where a refused write can reappear, which is the ordinary meaning of
  "unknown outcome" and not a fencing hole.
- Costs: **per append, unchanged** (one conditional PUT). Per takeover: the seal's GET/LIST/PUT become one
  listing of the log's terms, one listing per unfenced older term and one fence PUT each, plus the head
  read a new writer makes after fencing its own term. Per publish: unchanged -- and the seal blobs, their
  merge and their deletes go.
- Legacy: seals already written are still read and still bound their terms on replay; no new ones are
  written. Nothing needs migrating.

### C. The manifest pointer in the head

The head holds `{term, owner, manifestGeneration}` and a publish is one CAS on the head, so a stale owner's
publish fails on the term by construction.

- Closes obstacle (c) -- a stale owner's publish -- which is already fenced today (the manifest CAS refuses a
  lower term, `StaleWriterException`) and whose delete-during-publish leak is now closed by the reclaim
  path (`ee1bd3b`). It does nothing for the append window, which is the open item.
- Every publish becomes a CAS on the head, the hottest register a shard has, and every reader resolves head
  then manifest. Rejected: it pays per publish for a problem already solved.

## Decision

**B.** It is the only option that closes the window rather than narrowing it, it needs no primitive the
stores lack -- put-if-absent is what every register create already depends on, and a store without it cannot
run this shell safely at all -- and it costs nothing per append. MinIO and RustFS honour put-if-absent; the
conformance suite gains a test that concurrent put-if-absent writes of one blob admit exactly one.

## Canaries (each fails with its safeguard removed)

- A predecessor paused past its deadline, resuming appends: its append either lands before the fence (and is
  acknowledged and replayed) or is refused by the store (and never replayed); never acknowledged-and-lost,
  never refused-and-replayed.
- An append landing between the swap and the fence -- the case `STATUS.md` names -- is replayed *and*
  acknowledged, where it used to be replayed and refused.
- Two concurrent takers: both fence, one head wins, no record is lost or doubled.
- A same-term reopen racing a stale instance's append.
- Poisoning removed; fences deleted by truncation; own-term fence skipped: each makes a canary fail.
