# Global Streak Leaderboard Design

## 1. Overview

The clean design is to treat the global leaderboard as a **near-real-time projection of authoritative streak state**.

The Streak Service remains responsible for deciding whether a user's streak is 0, 25, or 1,000 days. The Leaderboard Service only maintains an ordered view of those results.

```text
Qualifying Learning Activity
            |
            v
       Streak Service
   calculate authoritative streak
            |
            | transaction
            v
      Streak DB + Outbox
            |
            v
Kafka: streak-state-changed
keyed by user_id
            |
            v
Leaderboard Projection Pipeline
      /             |              \
     v              v               v
Shard rankings   Rank counts    Top-K aggregator
     |              |               |
     v              v               v
Redis sorted     streak-value    Global Top-K
sets             histogram       cache
      \             |              /
       \            |             /
        +-----------+------------+
                    |
                    v
             Leaderboard API
                    |
                    +--> HTTP reads
                    +--> WebSocket/SSE updates
```

A streak change event might be:

```json
{
  "event_id": "evt-123",
  "user_id": "user-456",
  "old_streak": 216,
  "new_streak": 217,
  "streak_version": 9182,
  "changed_at": "2026-11-11T20:14:32Z",
  "leaderboard_eligible": true
}
```

The `streak_version` is important for handling duplicate or out-of-order events.

---

## 2. Do Not Update the Leaderboard Directly From Lesson Services

The Leaderboard Service should consume only authoritative streak changes:

```text
Lesson completed
      |
      v
Streak Service decides:
user's streak changed from 216 to 217
      |
      v
Leaderboard receives StreakStateChanged
```

Avoid:

```text
Lesson Service
      |
      v
increment leaderboard score
```

because the lesson event may:

- be duplicated,
- not qualify,
- belong to an already-qualified day,
- arrive late,
- repair an older streak day,
- consume a freeze,
- cause a larger historical correction.

The leaderboard should receive the final streak value, not infer it independently.

---

## 3. A Single Global Redis Sorted Set Is Simple but Limited

The simplest implementation is:

```redis
ZADD global:streak-leaderboard 217 user:456
```

Retrieve the global top 100:

```redis
ZRANGE global:streak-leaderboard 0 99 REV WITHSCORES
```

Retrieve one user's rank:

```redis
ZREVRANK global:streak-leaderboard user:456
```

This works well at modest scale.

However, one Redis sorted set belongs to one Redis Cluster shard. With hundreds of millions of users, it becomes:

- a single hot write key,
- a single hot read key,
- limited by one Redis primary's memory,
- limited by one primary's CPU,
- difficult to rebalance horizontally.

At Duolingo scale, shard the leaderboard.

---

## 4. Shard the Leaderboard by `user_id`

Choose a fixed number of logical leaderboard shards:

```text
shard_id = hash(user_id) % 256
```

Create one sorted set per shard:

```text
streak:leaderboard:shard:000
streak:leaderboard:shard:001
...
streak:leaderboard:shard:255
```

For `user-456`:

```text
hash(user-456) % 256 = 42
```

Update:

```redis
ZADD streak:leaderboard:shard:042 217 user:456
```

Every user exists in exactly one shard.

The pipeline becomes:

```text
StreakStateChanged
        |
        v
hash(user_id) % 256
        |
        v
appropriate shard sorted set
```

This distributes:

- memory,
- writes,
- rank calculations,
- top-K reads,
- recovery work.

---

## 5. Make Leaderboard Updates Idempotent

Kafka can redeliver an event, and events can arrive out of order.

Suppose the consumer sees:

```text
version 9182 -> streak 217
version 9181 -> streak 216
```

The stale second event must not move the user backward.

For each leaderboard shard, retain the latest processed version:

```text
user_id -> latest streak_version
```

Conceptually:

```text
if incoming_version <= stored_version:
    ignore

otherwise:
    update score
    store incoming_version
```

In Redis, each logical shard could use:

```text
streak:{042}:ranking
streak:{042}:versions
streak:{042}:counts
```

The Redis hash tag `{042}` ensures these keys land in the same Redis Cluster slot. A Lua script can atomically:

1. Check the stored version.
2. Read the previous streak.
3. Update the user's sorted-set score.
4. Update streak-count buckets.
5. Store the new version.

Pseudo-flow:

```text
Incoming:
user-456
old=216
new=217
version=9182

Atomic shard update:
- verify version 9182 is newer
- ZADD score 217
- decrement count for 216
- increment count for 217
- save version 9182
```

Because the leaderboard is reconstructible from the Streak database or event stream, Redis remains a projection rather than the source of truth.

---

## 6. Calculating the Exact Global Top K

Each shard has a local sorted set.

To find the global top 100:

```text
1. Read the local top 100 from every shard.
2. Merge the candidates.
3. Keep the global top 100.
```

With 256 shards:

```text
256 × 100 = 25,600 candidates
```

Merging 25,600 candidates in memory is inexpensive.

```text
Shard 0 top 100 ──┐
Shard 1 top 100 ──┤
Shard 2 top 100 ──┤
...                ├──> Top-K merge -> global top 100
Shard 255 top 100 ─┘
```

This produces an **exact**, not approximate, global top K.

Why is reading only K from each shard sufficient?

If a user is not in their own shard's top K, at least K users in that shard outrank them. Therefore, that user cannot be in the global top K.

### Materialize the Result

Do not perform a 256-shard scatter-gather on every client request.

Run a Top-K Aggregator:

```text
every 1–5 seconds
or
when a shard's local top K changes
```

It writes a small cached result:

```text
streak:leaderboard:global:top100
```

Then:

```http
GET /v1/leaderboards/streak/global?limit=100
```

is served from one small cached snapshot.

Example response:

```json
{
  "as_of": "2026-11-11T20:14:35Z",
  "version": 78342,
  "entries": [
    {
      "rank": 1,
      "user_id": "user-1",
      "display_name": "LinguaMaster",
      "streak": 4127
    }
  ]
}
```

---

## 7. Calculating a User's Global Rank

A sharded sorted set cannot directly answer:

```text
What is Alice's exact global rank?
```

`ZREVRANK` gives only the rank within Alice's shard.

There are two approaches.

### Option A: Query Every Shard

Suppose Alice's streak is 217.

For each shard, count users with a score greater than 217:

```redis
ZCOUNT streak:leaderboard:shard:042 (217 +inf
```

Run this in parallel across all shards:

```text
global rank =
1
+
sum(users in every shard with streak > 217)
```

If users with equal streaks share the same rank, that gives the exact competition rank.

This is exact but requires a scatter-gather across all shards.

It can be acceptable for occasional “show my rank” requests, but not necessarily for every feed render.

### Option B: Maintain a Streak Histogram

Streak lengths are integers and are usually bounded to a few thousand or tens of thousands.

Maintain counts such as:

```text
streak length 0   -> 12,304,281 users
streak length 1   -> 19,204,552 users
streak length 2   -> 14,992,100 users
...
streak length 217 -> 82,341 users
streak length 218 -> 80,772 users
```

Then:

```text
rank for streak 217 =
1 + SUM(counts for streak > 217)
```

This is much cheaper than querying hundreds of sorted sets.

The histogram can be maintained:

- globally by a Flink aggregation,
- per leaderboard shard and summed,
- in a small Redis structure,
- in a database table.

For each streak change:

```text
216 -> 217
```

emit:

```text
count[216] -= 1
count[217] += 1
```

If equal streaks share the same displayed rank, the histogram provides a fast exact rank.

---

## 8. Tie-Breaking

Many users will have the same streak length.

The product needs an explicit rule.

### Option A: Tied Users Share the Same Rank

```text
User A: streak 217 -> rank 1,000
User B: streak 217 -> rank 1,000
User C: streak 216 -> rank 1,083
```

This is the easiest to explain and makes the histogram approach exact.

For display order among tied users, use a deterministic secondary field such as:

```text
user_id
or
time the current streak was reached
```

### Option B: Every User Gets a Unique Rank

Then the sorting key might be:

```text
streak DESC
current_streak_reached_at ASC
user_id ASC
```

This makes exact global rank harder because the system must count not only greater streak values but also tie-break positions.

A global leaderboard often works well with shared rank for equal streak lengths.

---

## 9. Near-Real-Time Updates to Clients

The storage pipeline may update within a few seconds:

```text
Streak DB commit
      |
      v
Outbox publication
      |
      v
Kafka
      |
      v
Leaderboard projection update
      |
      v
Top-K refresh
```

Target:

```text
P95 streak-to-leaderboard latency < 3–5 seconds
```

There are two common client-update models.

### Polling

The client polls every few seconds:

```http
GET /v1/leaderboards/streak/global?since_version=78342
```

This is simple and reliable, but creates repeated traffic.

### WebSocket or Server-Sent Events

```text
Leaderboard Top-K Aggregator
        |
        v
LeaderboardChanged event
        |
        v
Realtime Gateway
        |
        v
WebSocket / SSE clients
```

Example update:

```json
{
  "type": "LEADERBOARD_CHANGED",
  "leaderboard_version": 78343,
  "changed_entries": [
    {
      "user_id": "user-456",
      "old_rank": 102,
      "new_rank": 97,
      "streak": 217
    }
  ]
}
```

A practical model is:

- REST for the initial leaderboard snapshot,
- WebSocket/SSE for live changes,
- periodic full refresh to repair missed events.

The realtime update should contain enough version information to detect gaps:

```text
client has version 78342
receives version 78344
        |
        v
version 78343 was missed
        |
        v
refresh full snapshot
```

---

## 10. Update Only When the Streak Value Changes

Do not update the leaderboard for every lesson.

A user may complete 20 lessons on the same day, but their current streak changes at most once that day.

```text
Lesson 1:
streak 216 -> 217
emit leaderboard change

Lesson 2:
streak remains 217
no leaderboard update

Lesson 3:
streak remains 217
no leaderboard update
```

This greatly reduces write volume.

Other updates occur when:

- a freeze is consumed,
- a streak expires,
- a late offline event repairs the streak,
- support repairs a streak,
- a user opts into or out of public ranking.

---

## 11. Late and Corrective Streak Events

Suppose a user temporarily appears as:

```text
current streak = 1
```

Then an offline event repairs a missing historical day:

```text
current streak = 101
```

The Streak Service emits:

```json
{
  "user_id": "user-456",
  "old_streak": 1,
  "new_streak": 101,
  "streak_version": 9190
}
```

The leaderboard pipeline changes the sorted-set score:

```redis
ZADD streak:leaderboard:shard:042 101 user:456
```

The next Top-K refresh incorporates the corrected ranking.

This is another reason the leaderboard should receive absolute current streak values rather than “increment by one” commands.

---

## 12. Privacy and Eligibility

Not every user should necessarily appear publicly.

The leaderboard should respect:

- explicit opt-in or opt-out,
- child-account policies,
- regional privacy rules,
- blocked or suspended accounts,
- employee/test accounts,
- cheating or abuse status,
- anonymized display-name requirements.

A streak event can include:

```json
{
  "leaderboard_eligible": false
}
```

The projection removes the member:

```redis
ZREM streak:leaderboard:shard:042 user:456
```

The public leaderboard should store or return only limited profile information:

```text
display name
avatar
country or language, if allowed
streak length
rank
```

Avoid exposing email addresses or other internal identifiers.

---

## 13. Abuse and Cheating

A global leaderboard creates incentives to manipulate streaks.

The leaderboard pipeline should consume only streak values that have passed the Streak Service's integrity checks.

Possible controls:

- detect impossible activity volume,
- validate device timestamps for offline events,
- limit timezone changes,
- flag suspicious streak repairs,
- exclude suspended accounts,
- delay visibility for high-risk corrections.

The public projection may use:

```text
leaderboard_status =
ELIGIBLE
UNDER_REVIEW
EXCLUDED
```

Do not place fraud rules directly inside the Redis leaderboard consumer. The Streak or Integrity Service should own the decision.

---

## 14. Failure and Recovery

### Redis Shard Failure

A replica is promoted.

If recent projection updates are lost, replay the Kafka streak stream or rebuild from the authoritative Streak database.

### Leaderboard Consumer Failure

Kafka retains unprocessed events. The consumer restores its checkpoint or offset and continues.

### Top-K Aggregator Failure

The existing cached global snapshot remains available but becomes stale. A restarted aggregator recomputes from all shards.

### Duplicate Event

Ignored using `streak_version`.

### Out-of-Order Event

Ignored if its version is older than the stored version.

### Missed or Corrupted Projection

Run periodic reconciliation:

```text
sample or scan Streak DB
        |
        v
compare authoritative streak with Redis score
        |
        v
repair differences
```

---

## 15. Capacity Observations

Streak leaderboard writes are much less frequent than raw learning events.

Even with 100 million daily active users:

```text
100M streak changes/day
÷ 86,400
≈ 1,157 updates/second average
```

There will be bursts around local-time activity windows, but this is manageable across many shards.

The bigger challenge is storing hundreds of millions of leaderboard members, not the average write rate.

Sharding gives:

```text
256 shards
100M users
≈ 390,625 users per shard
```

That is much more manageable than one 100-million-member sorted set.

---

## 16. Time-Zone Fairness

A live global streak leaderboard has an unavoidable product nuance.

Users in different time zones qualify for their next local day at different global times. Someone in New Zealand may increment today's streak before someone in New York has entered the same local day.

Two choices:

### Live Leaderboard

Display each user's current authoritative streak immediately.

Advantages:

- truly near real time,
- simple,
- responsive.

Tradeoff:

- temporary timezone differences can affect rank.

### Snapshot Leaderboard

Calculate standings at a fixed UTC cutoff or on completed local days.

Advantages:

- more stable comparisons.

Tradeoff:

- less real-time,
- more complex semantics.

For a general global streak leaderboard, use the live current streak and accept short-term timezone-driven movement. If the leaderboard becomes a competitive weekly contest, a snapshot-based scoring system may be fairer.

---

## 17. Recommended Complete Architecture

```text
Learning Activities
        |
        v
Streak Service
authoritative daily-state calculation
        |
        v
Streak DB + Transactional Outbox
        |
        v
Kafka: streak-state-changed
key = user_id
        |
        v
Leaderboard Consumer / Flink Job
- deduplicate by user/version
- apply eligibility
- calculate shard
        |
        +-----------------------+
        |                       |
        v                       v
Redis Sharded Rankings     Streak Histograms
ZSET per user shard        count by streak value
        |                       |
        +-----------+-----------+
                    |
                    v
             Top-K Aggregator
        read top K from every shard
                    |
                    v
          Global Top-K Snapshot Cache
                    |
             +------+------+
             |             |
             v             v
       REST API       Realtime Gateway
                     WebSocket / SSE
```

---

## 18. Interview Summary

> Keep current streak state in the Streak Service as the source of truth and emit versioned `StreakStateChanged` events through a transactional outbox. A leaderboard projection consumes those events and updates a sharded set of Redis sorted sets, with users assigned to shards by a hash of `user_id`.
>
> A single global sorted set would become a memory and throughput bottleneck, so the global top K is computed by reading the local top K from every shard, merging the candidates, and caching the resulting global snapshot. This produces an exact top K. For an individual user's global rank, maintain a count histogram by streak length, making rank calculation fast when tied streaks share one rank.
>
> Each update is idempotent using the user's monotonic streak version, and the pipeline handles late streak repairs by replacing the user's absolute score rather than incrementing it. REST serves the initial leaderboard, while WebSockets or server-sent events publish versioned changes within a few seconds. Redis is a rebuildable projection; Kafka replay and periodic reconciliation against the Streak database provide recovery and correctness.
