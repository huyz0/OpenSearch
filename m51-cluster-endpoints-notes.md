# M51 (finished) — the cluster surface this design can answer honestly

The blanket refusal on `/_cluster/*`, `/_nodes` and `/_cat/*` read: *"there is no cluster-wide state in a
serverless cluster; no node can answer this, and a node-local answer would be misleading."* One reason,
fourteen endpoints. The first clause is true. The second stopped being true some time ago, and nobody
noticed — the same stale-refusal shape M50 found in `_bulk`.

## Why "no node can answer this" had become false

Every node publishes a lease to a register in the object store and reads the others' in order to know where
to forward a write. **That registry is the membership list.** A node refusing to say who is in the fleet is
refusing to report something it computes on every heartbeat in order to route traffic. If the leases are
trustworthy enough to send a customer's write to, they are trustworthy enough to list.

`GET /_serverless/nodes` had in fact been answering exactly this question all along, under a different name.

## The line that actually holds is cost, not locality

| Question | Cost | Verdict |
| --- | --- | --- |
| Who is in the fleet | one listing + one read per member — what the heartbeat already pays | answerable |
| What is the state of one index's shards | one descriptor + one head per shard | answerable |
| Anything aggregated over every index | enumeration | refused |
| The cluster state object | — | it *is* the thing that does not exist |

Nothing added here grows with the number of indices, which is the property that separates the endpoints now
served from the ones still refused.

## The colours had to be redefined, and that is the design

Classic green/yellow/red is an assertion about **replica placement**: green means every primary and replica is
assigned, yellow means replicas are missing, red means a primary is. This architecture has no replicas, so
**yellow is unreachable by construction** — and, far more importantly, "primary unassigned" is the *normal
resting state* of a shard nobody is currently writing to.

Mapping a dormant shard onto classic's yellow or red would be the inverse of the usual lie: reporting a fault
where there is none. And it would not be academic. A client calling `wait_for_status=green&timeout=30s` at
startup — which is what test harnesses and clients do — would block until every shard happened to be
activated, in a system where shards activate on demand and may never activate without the traffic the client
is waiting to send. **A compatibility gesture that deadlocks the caller is worse than the 501 it replaced.**

So:

- **green** — every shard asked about can be served: it has a live owner, or it can be recovered from the
  object store on demand, or it is new and empty. A dormant shard is a healthy shard.
- **red** — some shard can be served by nobody, which in practice means the control plane could not be read.
  That is a *useful* red: it is the failure that actually matters in this architecture, and it is reported
  rather than thrown, because a health endpoint that 500s when things are unhealthy is the one shape a health
  endpoint must not have.
- **yellow** — never returned, and `yellow_reachable: false` says so on every response rather than leaving it
  as an unexplained absence.

`dormant_shards` is the additive field that carries the real information: how many shards would pay an
activation on their next write. `unassigned_shards` keeps its classic meaning — nobody can serve this — and is
therefore zero unless something is actually broken, which keeps `active_shards_percent_as_number` and the
colour consistent with each other the way a client expects.

## What green does not mean, said on every response

`replication: "object-store"` is on every health answer. Redundancy here is not a replica, and a customer's
tooling silently reading green as "copies exist" would be worse than a 501 precisely because it is silent. An
additive field is ignored by clients that do not know it and visible to the person debugging.

## Scope, and the field that reports it

Unscoped `/_cluster/health` answers the node half and omits the shard half. It does **not** report
`active_shards: 0`, because zero is a claim about how many shards exist and the true answer is that none were
examined. `complete: false` says which — the same field search responses already use to say "this answer does
not cover everything it might have", so the answer borrows an idiom rather than inventing one.

Naming an index makes it `complete: true` and the counters appear.

## What is honoured, and what is refused, among the wait parameters

- **`wait_for_status`** — honoured. Retrying is meaningful rather than ceremonial: status is computed from a
  live read of the control plane, so a red can genuinely become green between polls. `yellow` is accepted and
  satisfied by green, because a caller asking for yellow is asking for "at least yellow".
- **`wait_for_nodes`** — honoured, and answerable for the same reason the node list is. `>=3`, `3` and `ge(3)`
  all mean the same thing to a client waiting for a fleet to come up.
- **`wait_for_no_relocating_shards` / `wait_for_no_initializing_shards`** — nothing here ever relocates or
  initialises, so a caller waiting for neither is already satisfied. Answering immediately is truthful;
  refusing would be pedantry.
- **`wait_for_active_shards`** — refused. It counts shards an allocator has placed, and there is no allocator:
  a shard is activated by the write that needs it, not by a placement decision.
- **`wait_for_events`** — refused. It waits on a cluster manager's pending-task queue, and there is neither.

## `_cat`

`_cat` exists for a person at a terminal, so `_cat/nodes` and `_cat/health` answer as aligned text by default
and as JSON with `?format=json`. **`h=` and `s=` are refused rather than accepted and ignored**: returning the
default columns to a caller who asked for specific ones hands back a table that is not the one they asked for,
which on this surface is the failure that matters more than the missing feature.

One trap worth recording, because it bit twice in one milestone: `BaseRestHandler` rejects a request whose
parameters were not all *consumed*, and it does so after the handler has already decided to refuse. A refusal
that only inspected `h` came back as `"unrecognized parameter: [h]"` instead of as itself. Same root cause as
M50's `/{index}/_count`.

## Two fields added to the lease

`NodeLease` now carries `name` and `version`. The alternatives were worse: asking each peer over the transport
turns a listing into a fan-out, and reporting the local node's version for a peer would be a guess that is
wrong during exactly the rolling upgrade where the answer matters. A node knows its own name and version, so
it publishes them. Both are optional — a lease written before they existed parses with them absent, the name
falling back to the node id and the version reported as unknown rather than invented.

Roles are reported verbatim: `ingest` and `search`, not classic's `data`/`cluster_manager`. Translating them
would invent a distinction this architecture does not make, and both roles hold shards, which is why
`number_of_data_nodes` equals `number_of_nodes`.

## What stays refused, each for a reason of its own

The shared reason was retired with the endpoints it had stopped being true of. What is left needs one of three
things this design does not have and is not going to grow: a snapshot across every index (`_cluster/state`,
`_cluster/stats`, `_cat/indices`, `_cat/shards`, `_cat/segments`, …), an allocator (`_cluster/reroute`,
`_cluster/allocation/explain`, `_cat/allocation`, `_cat/recovery`), or a cluster-manager task queue (`_tasks`,
`_cat/pending_tasks`).

**`_nodes/stats` is the interesting one**, and it got a reason of its own rather than the general one, because
the general one would now be wrong: *"each node can answer for itself at `GET /_serverless/stats`, and this
node knows where the others are; what does not exist yet is the fan-out that would ask them and the accounting
that would report which ones did not answer."* That is a scoping decision, not an impossibility — see below.

## Canaries

- **166 — a dormant shard is treated as unassigned.** Caught: the index goes red and the percentage drops.
- **167 — unscoped health claims it counted the shards.** Caught: `complete` is true with a fabricated zero.
- **168 — a node answers only for itself.** Caught: the peer disappears from `_nodes`.
- **169 — a lease stops carrying the node name.** Caught: the id comes back where the name should be.
- **170 — `h=` is accepted and the default columns returned anyway.** Caught.
- **171 — an allocation-shaped wait is silently satisfied.** Caught.
- **172 — green stops disclosing the replication model.** Caught.

## What this does NOT establish

- **`_nodes/stats` was deliberately not built.** It needs a sixth transport action, a fan-out over leases, and
  failure accounting in the shape search responses already use (`total`/`successful`/`failed`, never a silent
  drop). That is a milestone's worth of work with its own correctness questions, and it is scoped out rather
  than forgotten. The refusal now says exactly this.
- **`_cat/indices` could be served under the existing wildcard cap and is not.** The shell already resolves
  prefix patterns with one bounded listing and refuses past a cap rather than truncating, so `_cat/indices/logs-*`
  is answerable under a rule that already exists. Bare `_cat/indices` would stay refused by the same rule. Not
  built here; recorded because the objection to it is weaker than the current refusal implies.
- **Health is one node's view, and says so by construction rather than in a field.** The node list is this
  node's observation of live leases at this instant; a node that died a moment ago is listed until its lease
  expires. That staleness is bounded by the lease TTL and is the same staleness every routing decision in this
  shell already runs on. The classic answer is also one node's view — the cluster manager's — so this is a
  different source for the same kind of claim, not a weaker kind of claim.
- **`wait_for_status` polls.** It sleeps on a generic-pool thread up to the caller's timeout. Classic uses a
  cluster-state observer; there is no state to observe here, so a poll is the honest equivalent, but a very
  large number of simultaneous waiters would hold threads.
- **Nothing here is proven against a real object store** (D5/R11), unchanged.

M51 is done.
