# Duolingo Streak System Design

## 1. Problem Statement

Design a streak system for a Duolingo-like learning platform at large scale.

A user maintains a streak by completing qualifying learning activity on consecutive days. The system must:

- determine whether a user has preserved their streak for a given day,
- display the current streak across devices,
- support streak freezes,
- handle offline activity and late-arriving events,
- tolerate duplicate event delivery,
- support users in different time zones,
- trigger reminders when a streak is at risk,
- remain reliable at large scale.

Assumptions for this design:

- ~100M daily active users
- mobile and web clients
- users worldwide
- a user may use multiple devices
- users can complete learning activity while offline
- events can be duplicated
- events can arrive late or out of order
- streak state should update within seconds
- correctness matters more than strict synchronous processing

---

## 2. Streak Definition

A streak is the number of consecutive **local calendar days** on which a user completes at least one qualifying learning activity.

Example:

```text
Mon: lesson completed              ACTIVE
Tue: practice completed            ACTIVE
Wed: no activity, freeze consumed  FREEZE
Thu: lesson completed              ACTIVE
Fri: story completed               ACTIVE
```

Assuming a freeze preserves the streak, the streak above is five days.

Important properties:

- One qualifying activity is enough to preserve a day.
- Multiple qualifying activities on the same day still count as only one streak day.
- A user's streak is calculated against a stable streak timezone.
- A streak freeze can preserve a missed day.
- Offline activity can count toward the day when it actually occurred, subject to product-defined limits.
- The streak is derived from daily state; it is not fundamentally just an integer counter.

A useful conceptual model is:

```text
(user_id, local_date) -> ACTIVE | FREEZE | MISSED
```

---

## 3. Local Timezone vs. Streak Timezone

The user's current local or device timezone and the user's streak timezone should be treated as different concepts.

### Local/device timezone

This is the timezone reported by the user's current environment.

It can change frequently, especially when the user travels.

Example:

```text
Home:
America/New_York

Travel:
Europe/London
```

### Streak timezone

This is the authoritative timezone used by the streak system to define the user's streak-day boundaries.

For example:

```text
streak_timezone = America/New_York
```

Even if the user's phone temporarily changes to:

```text
Europe/London
```

the streak system may continue to evaluate the streak using the persisted streak timezone.

This avoids pathological behavior where users gain unusually long or short streak days due to travel or repeated timezone switching.

A production system may allow the streak timezone to change when a user permanently relocates, but such changes should be controlled. Possible rules include:

- changes become effective on the next streak day,
- limit how frequently the streak timezone can change,
- retain historical timezone information when needed for audit or repair.

---

## 4. What Counts as Streak Activity?

Not every action inside the app should qualify.

The business concept should be **meaningful learning activity**, not generic engagement.

Examples that may qualify:

- lesson completed
- practice session completed
- speaking exercise completed
- listening exercise completed
- story completed
- another product-defined learning module completed

Examples that should probably not qualify:

- app opened
- profile viewed
- leaderboard viewed
- shop viewed
- item purchased
- ad watched
- notification opened
- settings changed

The Streak Service should own the qualification rule rather than trusting every producer to decide whether an event qualifies.

Conceptually:

```text
isQualifying(activity_type, metadata) -> true | false
```

Example:

```text
LESSON_COMPLETED       -> true
PRACTICE_COMPLETED     -> true
STORY_COMPLETED        -> true

LESSON_STARTED         -> false
APP_OPENED             -> false
LEADERBOARD_VIEWED     -> false
SHOP_PURCHASED         -> false
```

Avoid defining streak eligibility purely in terms of XP. XP is a reward/accounting concept and may evolve independently from streak policy.

---

## 5. Core APIs and Events

### Read API

```http
GET /users/{user_id}/streak
```

Example response:

```json
{
  "current_streak": 417,
  "longest_streak": 621,
  "today_completed": true,
  "streak_freezes": 2
}
```

### Activity event

Learning systems emit a canonical event after meaningful activity completes.

```json
{
  "event_id": "evt_abc123",
  "user_id": "user_123",
  "activity_id": "lesson_456_attempt_789",
  "activity_type": "LESSON_COMPLETED",
  "completed_at": "2026-10-01T22:14:32Z",
  "received_at": "2026-10-01T22:14:34Z"
}
```

The streak system should consume canonical learning events rather than having every learning service update streak state directly.

---

## 6. High-Level Architecture

```text
Lesson Service
Practice Service
Speaking Service
Story Service
      |
      | LearningActivityCompleted
      v
 Durable Event Stream
      |
      v
  Streak Service
      |
      +--------------------+
      |                    |
      v                    v
Daily Streak State    User Streak State
      |                    |
      +---------+----------+
                |
                v
             Cache
                |
                v
           Streak API
```

The separation of responsibilities should be:

```text
Learning systems:
"Did learning activity happen?"

Streak system:
"Does that activity preserve the user's streak?"
```

---

## 7. Why Use Kafka?

Kafka is a strong fit because learning activity is a durable domain event that can be consumed by multiple downstream systems.

Potential consumers include:

```text
              LearningActivityCompleted
                        |
                      Kafka
                        |
       +----------------+----------------+
       |                |                |
       v                v                v
 Streak Service     XP Service     Achievement Service
       |
       v
Notification Service
```

Kafka is useful for:

- durable event delivery,
- at-least-once processing,
- replay,
- decoupling producers from consumers,
- scaling consumers independently,
- supporting multiple downstream products.

The design should not rely on end-to-end exactly-once semantics.

A stronger approach is:

```text
at-least-once delivery
        +
idempotent consumers
        =
reliable processing
```

---

## 8. Reliable Event Publication: Transactional Outbox

A learning service should not update its own database and then independently attempt to publish to Kafka.

Without an outbox:

```text
1. Lesson completion written to DB
2. Service attempts to publish event
3. Service crashes before Kafka publish
```

Result:

```text
Lesson completion exists
Streak event is missing
```

Instead, write the business state and an outbox record in the same database transaction:

```text
BEGIN TRANSACTION

INSERT lesson_completion(...)

INSERT outbox_event(
    event_id,
    event_type,
    payload
)

COMMIT
```

An asynchronous publisher reads the outbox and publishes to Kafka.

```text
Lesson Service
     |
     v
Lesson DB + Outbox
     |
     v
Outbox Publisher
     |
     v
Kafka
```

The outbox publisher itself may publish an event more than once, so consumers still need idempotency.

---

# 9. Idempotency — Critical Design Section

Idempotency is not one single problem in this system.

There are at least **two distinct levels of idempotency**:

1. **transport/event idempotency**
2. **business/domain idempotency**

These solve different failure modes.

---

## 9.1 Event-Level Idempotency

Kafka consumers normally operate with at-least-once semantics.

A common processing sequence is:

```text
Read Kafka event
      |
      v
Update database
      |
      v
Commit Kafka offset
```

Suppose processing succeeds but the consumer crashes immediately before committing the Kafka offset:

```text
1. Receive event abc
2. Update streak database successfully
3. Crash before offset commit
4. Consumer restarts
5. Kafka redelivers event abc
```

The same event is therefore processed again.

This is normal and expected.

Each event should have a globally unique immutable identifier:

```text
event_id = evt_abc123
```

The consumer can maintain a deduplication record such as:

```text
ProcessedEvents

event_id        PRIMARY KEY
processed_at
```

Then processing can conceptually become:

```text
BEGIN TRANSACTION

IF event_id already exists:
    return success

apply streak state change

INSERT INTO ProcessedEvents(event_id)

COMMIT
```

This ensures that replaying exactly the same Kafka event does not apply the same logical operation twice.

Depending on the database and data model, event deduplication may also be embedded directly into the business write rather than stored in a separate table.

---

## 9.2 Why Event-ID Deduplication Is Not Enough

Suppose a user legitimately completes two different learning activities on the same day.

```text
Event A:
event_id = evt_001
activity = LESSON_COMPLETED
date = Oct 1

Event B:
event_id = evt_002
activity = PRACTICE_COMPLETED
date = Oct 1
```

Both events are legitimate.

They have different event IDs, so neither is a duplicate transport event.

However, the streak business rule says that October 1 should count only once.

If the system merely performs:

```text
current_streak++
```

for every unique event, it incorrectly increments twice.

Therefore event-level deduplication alone does not enforce the business rule.

---

## 9.3 Business-Level Idempotency

The business invariant is:

> A user can earn at most one streak day for a given streak-local calendar date.

Represent daily state using a key such as:

```text
(user_id, streak_date)
```

The database should enforce uniqueness:

```sql
UNIQUE(user_id, streak_date)
```

or use that pair as the primary key.

Example:

```text
DailyStreakActivity

user_id    streak_date    status
---------------------------------
user_123   2026-10-01     ACTIVE
```

If another qualifying activity arrives for October 1:

```text
user_123 + 2026-10-01
```

the system detects that the streak day is already active and does not create another streak day.

This handles:

- multiple lessons in one day,
- lesson + practice on the same day,
- events emitted by different learning services,
- reordering across different activity types,
- independent valid activity events.

---

## 9.4 Two Idempotency Layers Together

The design therefore needs both:

```text
Duplicate delivery of SAME event
        |
        v
Deduplicate by event_id
```

and:

```text
Different legitimate events
for SAME user and SAME streak day
        |
        v
Enforce UNIQUE(user_id, streak_date)
```

They solve different problems:

```text
event_id uniqueness
    protects against transport duplication

(user_id, streak_date) uniqueness
    protects the business invariant
```

This distinction is extremely important in a distributed system.

---

## 9.5 Example: Duplicate Kafka Delivery

Suppose the event is:

```text
event_id = evt_001
user_id = user_123
completed_at = Oct 1, 9:30 PM
```

Kafka delivers:

```text
evt_001
evt_001
evt_001
```

Processing:

```text
First delivery:
    ProcessedEvents does not contain evt_001
    DailyStreakActivity(user_123, Oct 1) created
    evt_001 stored as processed

Second delivery:
    ProcessedEvents contains evt_001
    no-op

Third delivery:
    ProcessedEvents contains evt_001
    no-op
```

Result:

```text
Oct 1 counted once
```

---

## 9.6 Example: Multiple Legitimate Activities

Now suppose the user completes two separate activities:

```text
evt_001 = lesson completed
evt_002 = practice completed
```

Both occur on Oct 1.

Processing:

```text
evt_001:
    event is new
    create (user_123, Oct 1) = ACTIVE

evt_002:
    event is new
    (user_123, Oct 1) already ACTIVE
    no additional streak day
```

Both events are successfully processed, but the user's streak advances only once.

---

## 9.7 Avoid Depending on Exactly-Once Processing

Trying to achieve exactly-once semantics across:

```text
Producer DB
Kafka
Consumer
Streak DB
Cache
```

would add substantial complexity.

Instead, design for retries:

```text
Producer may retry
Kafka may redeliver
Consumer may restart
Database operation may be retried
```

Each state transition should remain safe under repetition.

The design principle is:

> Prefer at-least-once delivery plus idempotent state transitions over distributed exactly-once guarantees.

---

## 10. Data Model

### UserStreak

Stores the current materialized state for fast reads.

```text
UserStreak

user_id                 PK
current_streak
longest_streak
last_qualified_date
streak_timezone
freeze_count
version
updated_at
```

Example:

```text
user_id              user_123
current_streak       417
longest_streak       621
last_qualified_date  2026-10-01
streak_timezone      America/New_York
freeze_count         2
```

### DailyStreakActivity

Stores daily history.

```text
DailyStreakActivity

user_id
streak_date
status
source_activity_id
updated_at

PRIMARY KEY (user_id, streak_date)
```

Possible statuses:

```text
ACTIVE
FREEZE
MISSED
```

Example:

```text
user_123 | 2026-09-28 | ACTIVE
user_123 | 2026-09-29 | ACTIVE
user_123 | 2026-09-30 | FREEZE
user_123 | 2026-10-01 | ACTIVE
```

The daily table is important because it allows the system to reconstruct why a user has a particular streak value.

It supports:

- late events,
- historical corrections,
- streak repair,
- customer-support investigation,
- bug recovery,
- reconciliation,
- timezone-related corrections.

---

## 11. Normal Write Flow

Suppose Alice completes a lesson.

```text
Lesson Service
      |
      | LearningActivityCompleted
      v
    Kafka
      |
      v
Streak Consumer
      |
      +----------------------+
      |                      |
      v                      v
event_id dedup         timezone lookup
      |                      |
      +----------+-----------+
                 |
                 v
     Determine streak-local date
                 |
                 v
     DailyStreakActivity write
                 |
                 v
        UserStreak update
                 |
                 v
             Cache
```

For each event:

1. Receive the event.
2. Check whether the event itself was already processed.
3. Determine whether the activity qualifies.
4. Fetch the user's streak timezone.
5. Convert `completed_at` into the correct streak-local date.
6. Upsert the daily streak state.
7. If that day was newly activated, update or recompute the materialized streak.
8. Commit transaction.
9. Commit Kafka offset.

---

## 12. Offline and Late-Arriving Events

Suppose Alice completes a lesson while offline.

```text
11:50 PM  lesson completed offline
12:00 AM  calendar day changes
12:20 AM  device reconnects
```

The event might contain:

```text
completed_at = Oct 1 11:50 PM
received_at  = Oct 2 12:20 AM
```

The streak date should be derived from:

```text
completed_at
```

rather than:

```text
server received_at
```

after converting through the user's streak timezone.

Conceptually:

```text
streak_date =
    date(convert(completed_at, streak_timezone))
```

Otherwise offline users could incorrectly lose streaks simply because synchronization was delayed.

---

## 13. Late Events Can Repair Historical State

Suppose the system initially sees:

```text
Sep 30  ACTIVE
Oct 1   MISSED
Oct 2   ACTIVE
```

and temporarily calculates:

```text
current_streak = 1
```

Later, an offline event arrives proving the user completed qualifying activity on Oct 1.

The historical state changes to:

```text
Sep 30  ACTIVE
Oct 1   ACTIVE
Oct 2   ACTIVE
```

The streak should be repaired.

This is why:

```text
current_streak++
```

cannot be the sole source of truth.

The streak count is a **materialized derived value**.

The durable basis for calculation is the daily streak history.

A useful hierarchy is:

```text
Source learning events
        |
        v
Daily streak state
        |
        v
Materialized current streak
```

---

## 14. Why a Streak Is Not Just a Counter

A naive implementation might store only:

```text
current_streak = 417
```

and increment it after each qualifying event.

That fails under:

- duplicate messages,
- multiple activities per day,
- late events,
- offline completion,
- historical corrections,
- streak freezes,
- timezone changes,
- out-of-order processing.

A more robust mental model is:

> A streak is a derived value over an ordered sequence of streak-local calendar-day states.

This makes repair and reconciliation possible.

---

## 15. Streak Freeze

A freeze preserves a streak when a calendar day contains no qualifying activity.

Example:

```text
Sep 28   ACTIVE
Sep 29   ACTIVE
Sep 30   FREEZE
Oct 1    ACTIVE
```

The streak remains continuous.

Potential implementation:

```text
DailyStreakActivity(
    user_id,
    streak_date,
    status = FREEZE
)
```

Freeze consumption itself should also be idempotent.

If two workers independently detect the same missed day, the system must not consume two freezes.

The daily record provides a natural concurrency boundary:

```text
PRIMARY KEY(user_id, streak_date)
```

Freeze inventory updates may additionally use:

- transactions,
- optimistic locking,
- conditional updates,
- version numbers.

---

## 16. Read Path

Reads should not reconstruct a user's streak from all historical activity every time.

Instead, maintain a materialized record:

```text
UserStreak.current_streak
```

The read path can be:

```text
Client
   |
   v
Streak API
   |
   +----> Cache
   |
   +----> UserStreak DB
```

The daily history remains the authoritative basis for correction and recomputation.

---

## 17. Consistency Model

The system does not require strict global strong consistency.

A practical target:

- lesson completion should reflect in streak state within seconds,
- temporary eventual consistency is acceptable,
- duplicate processing must not corrupt state,
- historical state must be repairable.

Per-user ordering can simplify processing.

Kafka can partition activity events by:

```text
key = user_id
```

This tends to route events for the same user to the same partition and preserve Kafka ordering for that user's events within the stream.

However, ordering alone should not be relied upon for correctness because:

- events can arrive from different upstream systems,
- producers may have delays,
- offline events can arrive much later,
- historical events may require reprocessing.

The daily-state model must remain correct even under late arrival.

---

## 18. Reconciliation

A production system should periodically compare:

```text
daily streak state
```

with:

```text
materialized UserStreak
```

and repair inconsistencies.

This can be done asynchronously.

For example:

```text
Reconciliation Job
      |
      v
Read recent DailyStreakActivity
      |
      v
Recompute expected streak
      |
      v
Compare with UserStreak
      |
      +--> equal -> no-op
      |
      +--> mismatch -> repair + alert/metric
```

Replay from Kafka or another durable event store may also be useful when recovering from bugs.

---

## 19. Important Failure Cases

### Duplicate Kafka message

Solution:

```text
event_id deduplication
```

### Multiple valid activities on the same day

Solution:

```text
PRIMARY KEY / UNIQUE(user_id, streak_date)
```

### Producer DB commit succeeds but event publication fails

Solution:

```text
transactional outbox
```

### Consumer DB update succeeds but Kafka offset commit fails

Solution:

```text
at-least-once delivery + event idempotency
```

### Offline event arrives after midnight

Solution:

```text
use completed_at + streak_timezone
```

### Late event repairs a previously missed day

Solution:

```text
retain daily state and recompute materialized streak
```

### Two workers try to consume a freeze

Solution:

```text
transaction / optimistic concurrency / conditional update
```

### User changes devices

Solution:

```text
server-side streak state is authoritative
```

### User travels across time zones

Solution:

```text
use persisted streak_timezone rather than raw device timezone
```

---

## 20. Interview-Level Design Principles

A strong system-design answer should emphasize the following:

### 1. Define product semantics before infrastructure

Clarify:

- what counts as learning activity,
- what constitutes a day,
- how timezone changes work,
- how freezes behave,
- how offline events are treated.

### 2. Separate learning activity from streak calculation

Learning services report what happened.

The Streak Service decides what it means for streaks.

### 3. Use durable asynchronous events

Kafka is appropriate because activity events are important domain events with many downstream consumers.

### 4. Prefer at-least-once + idempotency

Avoid trying to force exactly-once semantics across multiple distributed systems.

### 5. Model daily state explicitly

Use:

```text
(user_id, streak_date)
```

as a core business key.

### 6. Distinguish transport idempotency from domain idempotency

This is one of the most important parts of the design.

```text
event_id
    handles duplicate delivery

(user_id, streak_date)
    handles duplicate business effect
```

### 7. Make derived state recomputable

The materialized `current_streak` should be repairable from daily state.

---

## 21. Compact Interview Summary

A concise architecture summary could be:

> Learning services emit canonical `LearningActivityCompleted` events through a transactional outbox into Kafka. The Streak Service consumes these events with at-least-once semantics. Each event has an immutable event ID for transport-level deduplication, while the database enforces one streak state per `(user_id, streak_date)` to guarantee business-level idempotency. The service converts each activity's completion timestamp using the user's persisted streak timezone, writes daily streak state, and updates a materialized current-streak record for fast reads. Because daily streak history is retained, late and offline events can repair historical days and the current streak can be recomputed if necessary.

---

## 22. Key Insight

The central design insight is:

> A streak is not a mutable counter. It is a derived projection over a sequence of user-local calendar-day states.

Once that is recognized, idempotency, offline activity, replay, freezes, historical repair, and distributed-event processing become much easier to model correctly.
