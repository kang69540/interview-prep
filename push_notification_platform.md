# Duolingo Push Notification Platform

## 1. Problem Statement

Design a global push-notification platform for Duolingo that can send personalized and localized notifications to hundreds of millions of users.

Notifications may be:

- immediate
- scheduled for a specific time
- scheduled according to the user’s local time
- triggered by product events such as streak risk, leaderboard changes, social interactions, or re-engagement campaigns

The platform must:

- support many product teams
- respect user notification preferences
- support iOS through APNs
- support Android through FCM
- avoid unnecessary duplicate notifications
- handle provider outages and throttling
- support retries safely
- operate at very large scale
- prevent bulk campaigns from overwhelming higher-priority notifications

---

# 2. Scale Assumptions

Assume:

```text
Registered users:              500M
Push-eligible active users:    200M/day
Average notifications/user:    2/day
```

Therefore:

```text
200M × 2 = 400M notifications/day
```

Average throughput:

```text
400M / 86,400
≈ 4,600 notifications/sec
```

Traffic will not be uniform.

Large numbers of users may receive reminders around:

```text
7 PM
8 PM
9 PM
```

in their respective local time zones.

Design for approximately:

```text
50K–100K notifications/sec peak
```

with additional burst capacity.

---

# 3. Functional Requirements

## 3.1 Notification submission

Any approved product feature should be able to request a notification.

Examples:

```text
Streak Service
Leaderboard Service
Friends Service
Course Service
Campaign Service
Experimentation Platform
```

However, product services should **not send directly to APNs or FCM**.

Instead:

```text
Feature Service
      |
      v
Notification Platform
      |
      v
APNs / FCM
```

Feature teams specify the intent:

> User 123 should receive a STREAK_AT_RISK notification.

The platform decides:

- whether it may be sent
- when it should be sent
- how it should be rendered
- which device should receive it
- which provider should deliver it

---

# 4. Notification Request Model

Use registered notification types rather than arbitrary free-form messages.

Example:

```json
{
  "notification_id": "notif_123",
  "user_id": "user_456",
  "notification_type": "STREAK_AT_RISK",
  "send_at": "2026-10-02T20:00:00-04:00",
  "template_data": {
    "streak_days": 217
  },
  "priority": "HIGH"
}
```

Important fields:

```text
notification_id
user_id
notification_type
send_at
priority
template_data
```

The `notification_id` is a logical identifier for idempotency.

---

# 5. Core Responsibilities

The platform has two broad responsibilities.

## 5.1 Notification policy and preferences

Determine:

```text
Is this user allowed to receive this notification?
```

This includes:

- global notification opt-out
- notification-type preference
- channel preference
- quiet hours
- timezone
- frequency caps
- campaign limits
- account state
- possibly locale/language

Example:

```text
User 123

push_enabled = true

streak_reminders = true
leaderboard_updates = false

quiet_hours:
    22:00–07:00

timezone:
    America/New_York
```

---

## 5.2 Notification delivery

Responsible for:

- ingesting notification requests
- deduplicating
- scheduling
- template rendering
- localization
- selecting devices
- delivering to APNs/FCM
- throttling
- retries
- delivery-attempt tracking
- provider feedback
- recording final disposition

---

# 6. High-Level Architecture

```text
                    Feature Services
                           |
                           |
                 NotificationRequest
                           |
                           v
             Kafka: notification-requests
                           |
                           v
                  Notification Workers
                           |
              +------------+------------+
              |                         |
              v                         v
       Validate / Dedup            Policy Check
              |                         |
              +------------+------------+
                           |
                    immediate?
                     /      \
                   yes       no
                    |         |
                    |         v
                    |   Scheduling Store
                    |         |
                    |         v
                    |     Scheduler
                    |         |
                    +---------+
                           |
                           v
               Kafka: ready-to-send
                           |
                           v
                   Delivery Workers
                           |
                           v
                  Final Policy Check
                           |
                           v
                      Rate Limiter
                    /              \
                   v                v
                 APNs              FCM
```

---

# 7. Why Kafka?

Kafka is useful here primarily for:

- decoupling feature services from notification delivery
- buffering large bursts
- durable asynchronous processing
- retries
- independent scaling
- backpressure
- replay
- preventing APNs/FCM availability from affecting user-facing product requests

Without asynchronous delivery:

```text
Feature Service
     |
     v
Notification Service
     |
     v
APNs
```

a slow notification dependency can affect the feature itself.

Instead:

```text
Feature Service
     |
     v
Kafka
     |
     v
Notification Platform
```

The product request can complete independently of notification delivery.

---

# 8. Why Not Let Feature Services Call the Notification Service Directly?

Direct HTTP calls are a valid simple starting point:

```text
Lesson Service ----\
Streak Service -----+--> Notification Service
Friends Service ----/
```

The problem is failure semantics.

Suppose:

```text
Feature action succeeds

Notification call fails
```

Options:

```text
1. Fail the feature request
2. Lose the notification
3. Implement retries
```

Option 3 requires:

```text
persistent retry queues
backoff
dead-letter handling
deduplication
monitoring
```

As the number of producers grows, each producer begins rebuilding messaging infrastructure.

Kafka centralizes that responsibility.

---

# 9. Policy Evaluation

There should be two policy checks.

## 9.1 Initial policy check

When the request enters the system:

```text
Notification Request
        |
        v
Initial eligibility
```

This can eliminate requests that are already clearly invalid.

For example:

```text
user disabled all push notifications
```

---

## 9.2 Authoritative policy check at delivery time

This is more important.

Suppose:

```text
Monday:
schedule notification for Tuesday 8 PM

Tuesday 4 PM:
user disables streak notifications

Tuesday 8 PM:
notification becomes due
```

If preferences were evaluated only on Monday, the platform would incorrectly send it.

Therefore:

```text
Scheduled Notification
        |
        v
Becomes due
        |
        v
Re-check current preferences
        |
      allowed?
       /    \
     yes     no
      |       |
      v       v
    send    suppress
```

The final policy check should include:

```text
current opt-in status
quiet hours
frequency limits
account state
campaign suppression
```

---

# 10. Scheduling

Scheduled notifications may number in the hundreds of millions or billions.

A naive query such as:

```sql
SELECT *
FROM scheduled_notifications
WHERE scheduled_at <= NOW();
```

against a giant global table is undesirable.

---

# 11. Partitioning Strategy

The primary access pattern is:

> What notifications need to be sent now?

Therefore partitioning primarily by `user_id` is not ideal.

With user-based partitioning:

```text
Shard A -> Alice notification due now
Shard B -> Bob due next week
Shard C -> Carol due now
Shard D -> Dan due tomorrow
```

The scheduler must search many shards.

Instead, partition primarily by **scheduled-time bucket**.

For example:

```text
partition_key =
    minute_bucket + hash(user_id) % N
```

Conceptually:

```text
2026-10-02 20:00 / shard-0
2026-10-02 20:00 / shard-1
2026-10-02 20:00 / shard-2
...
2026-10-02 20:01 / shard-0
...
```

The scheduler knows precisely which partitions correspond to the current time.

---

# 12. Scheduled Notification Schema

Example:

```text
ScheduledNotification

notification_id
user_id
notification_type
scheduled_at
priority
template_id
template_data
status
created_at
```

Possible statuses:

```text
SCHEDULED
QUEUED
SENDING
SENT
SUPPRESSED
FAILED
UNKNOWN
```

Useful index within a time bucket:

```text
(scheduled_at, status)
```

---

# 13. Hot vs Long-Term Scheduling

Do not keep notifications scheduled months into the future in the hottest execution structure.

Use:

```text
Long-term scheduling store
          |
          | becomes near-term
          v
Near-term scheduling window
          |
          v
ready-to-send Kafka
```

For example:

```text
Scheduled DB
      |
      | notifications due in next 5 minutes
      v
Scheduler Workers
      |
      v
Kafka: ready-to-send
```

Kafka then becomes the execution queue.

The database answers:

> What work should become runnable?

Kafka answers:

> What work is ready to execute?

---

# 14. Avoiding Hot Time Buckets

At 8 PM local time, millions of users may become eligible simultaneously.

Two mechanisms help.

## 14.1 Shard each time bucket

Instead of:

```text
20:00 -> one giant partition
```

use:

```text
20:00/shard-0
20:00/shard-1
20:00/shard-2
...
```

where the shard may be:

```text
hash(user_id) % N
```

---

## 14.2 Add delivery jitter

Many notifications do not require delivery at an exact second.

Instead of:

```text
10M notifications at 20:00:00
```

spread delivery across:

```text
19:55–20:05
```

or:

```text
20:00–20:15
```

depending on business requirements.

This distinction is useful:

```text
jitter
    prevents unnecessary bursts

rate limiting
    protects downstream systems
```

---

# 15. Delivery Pipeline

```text
Kafka: ready-to-send
        |
        v
Delivery Workers
        |
        v
Final Policy Check
        |
        v
Template / Localization
        |
        v
Device Lookup
        |
        v
Priority Queue
        |
        v
Rate Limiter
        |
   +----+----+
   |         |
   v         v
 APNs       FCM
```

---

# 16. Priority Classes

Not all notifications should compete equally.

Example:

```text
HIGH
- security/account notifications
- streak-at-risk reminders

MEDIUM
- social notifications
- leaderboard changes

LOW
- promotional campaigns
- re-engagement messages
```

This prevents:

```text
100M marketing notifications
```

from starving:

```text
time-sensitive streak reminders
```

Possible architecture:

```text
             Delivery Pipeline
                    |
        +-----------+-----------+
        |           |           |
       HIGH       MEDIUM        LOW
        |           |           |
        +-----------+-----------+
                    |
                    v
               Rate Limiter
```

Capacity can be reserved per priority.

---

# 17. Rate Limiting

Rate limiting should be multi-dimensional.

Possible limits:

```text
per provider
per campaign
per notification type
per tenant/product
per user
global
```

For example:

```text
APNs:
    max X requests/sec

FCM:
    max Y requests/sec
```

A token bucket is a reasonable algorithm because it supports:

```text
controlled bursts
+
sustained rate enforcement
```

---

# 18. User Frequency Caps

Provider rate limiting protects infrastructure.

User frequency caps protect the user experience.

Example:

```text
maximum:
3 non-critical push notifications/day
```

or:

```text
STREAK_AT_RISK:
max 1 per streak day
```

This is separate from provider throttling.

---

# 19. Backpressure

Kafka naturally provides buffering.

Suppose APNs begins throttling:

```text
APNs slows down
      |
      v
Delivery Workers slow
      |
      v
Kafka consumer lag grows
```

That is acceptable temporarily.

The system should monitor:

```text
consumer lag
oldest queued notification age
provider response latency
provider throttling rate
```

Autoscaling can increase workers until the external-provider rate limit becomes the bottleneck.

---

# 20. Idempotency

This system has multiple different idempotency concerns.

They should not be conflated.

---

## 20.1 Request-Level Idempotency

Kafka may redeliver:

```text
notif_123
notif_123
notif_123
```

The system should recognize them as the same logical request.

Use:

```text
notification_id
```

as an immutable idempotency key.

Conceptually:

```text
ProcessedNotificationRequests

notification_id PRIMARY KEY
```

or enforce idempotency directly in the notification-state table.

---

## 20.2 Business-Level Deduplication

Two different notification requests may still represent the same user-visible business event.

Example:

```text
notif_A:
STREAK_AT_RISK
user_123

notif_B:
STREAK_AT_RISK
user_123
```

They have different IDs but may both be generated for the same logical streak risk.

A possible business deduplication key is:

```text
(user_id,
 notification_type,
 logical_window)
```

Example:

```text
(user_123,
 STREAK_AT_RISK,
 2026-10-02)
```

This can enforce:

> At most one streak-at-risk reminder for the same user per streak day.

---

# 21. Delivery-Level Ambiguity

External push delivery does not provide true end-to-end exactly-once semantics.

Consider:

```text
Delivery Worker             APNs

send notif_123 -----------> accepted

                  notification may be delivered

response        <---------- lost

worker times out
```

The worker cannot determine whether APNs accepted the request.

The state is now:

```text
UNKNOWN
```

---

# 22. Delivery State Machine

```text
READY
  |
  v
SENDING
  |
  +--> provider success ------> SENT
  |
  +--> permanent failure -----> FAILED
  |
  +--> policy rejected -------> SUPPRESSED
  |
  +--> timeout ---------------> UNKNOWN
```

`UNKNOWN` is important because it accurately reflects distributed-system reality.

---

# 23. Retry Policy

Not every failure should be retried.

Examples:

```text
Transient:
timeout
provider 5xx
throttling

=> retry
```

```text
Permanent:
invalid device token
bad request
user no longer eligible

=> do not retry
```

Retries should use exponential backoff:

```text
1 sec
2 sec
4 sec
8 sec
...
```

with jitter to avoid retry storms.

---

# 24. APNs Duplicate Handling

APNs provides identifiers useful for tracking and notification collapsing, but the platform should not assume true idempotent delivery.

The platform should track:

```text
notification_id
provider
provider_request_id
attempt_count
last_attempt_at
status
```

For notification types where it is appropriate, collapse identifiers can reduce duplicate visible notifications.

However:

> The notification platform can guarantee that it logically intends to send a notification once, but cannot guarantee that the user's device displays it exactly once.

This is a normal distributed-system limitation.

---

# 25. Delivery Attempt Model

Example:

```text
Notification

notification_id
user_id
type
logical_dedupe_key
scheduled_at
status
priority
```

and:

```text
DeliveryAttempt

attempt_id
notification_id
provider
provider_request_id
started_at
completed_at
result
error_code
```

This separates the logical notification from individual transport attempts.

---

# 26. Device Registration

The system also requires device-token management.

Example:

```text
Device

device_id
user_id
platform
push_token
locale
app_version
enabled
last_seen_at
```

A user may have:

```text
iPhone
iPad
Android tablet
```

The product must define whether a notification should be sent to:

```text
all active devices
```

or:

```text
most recently active device
```

depending on notification type.

Invalid tokens returned by APNs/FCM should be disabled.

---

# 27. Localization

Feature services should send structured data:

```text
notification_type = STREAK_AT_RISK

template_data = {
    streak_days: 217
}
```

rather than finalized English text.

The Notification Platform determines:

```text
locale
template
translation
variables
```

Example:

```text
template:
"Don't lose your {streak_days}-day streak!"
```

Localized rendering occurs close to delivery.

This makes:

```text
language changes
template experiments
copy updates
```

independent from producer services.

---

# 28. Data Retention

Completed notifications do not need to remain indefinitely in the hot scheduling store.

After some period:

```text
Scheduling DB
      |
      v
Archive / Analytics Store
      |
      v
purge operational record
```

Keep enough history for:

```text
delivery analytics
customer support
campaign analysis
debugging
abuse investigation
```

without bloating the scheduler's primary indexes.

---

# 29. Observability

Important metrics include:

### Ingestion

```text
notification requests/sec
request rejection rate
duplicate request rate
```

### Scheduling

```text
scheduler lag
due notifications
time-to-ready
hot partition load
```

### Delivery

```text
notifications/sec
queue depth
Kafka consumer lag
delivery latency
retry rate
```

### Provider

```text
APNs latency
FCM latency
provider error rates
provider throttling
invalid-token rate
```

### Product

```text
delivered
opened
suppressed
conversion
unsubscribe rate
```

---

# 30. Failure Scenarios

## Kafka redelivery

Solution:

```text
notification_id idempotency
```

---

## Same business notification generated twice

Solution:

```text
logical dedupe key
```

---

## Scheduler worker crashes

Another worker resumes from persistent scheduling state.

---

## APNs/FCM unavailable

```text
Kafka buffers work
+
delivery workers retry
+
rate limiter/backoff protects provider
```

---

## Provider slows down

Allow consumer lag to increase temporarily.

Monitor SLA and oldest-message age.

---

## Preferences change after scheduling

Re-check preferences immediately before delivery.

---

## Campaign creates a huge traffic spike

Use:

```text
time-based partitioning
sharding
jitter
priority queues
rate limiting
```

---

## APNs accepted request but response was lost

Mark:

```text
UNKNOWN
```

then follow notification-specific retry policy.

Do not assume exactly-once delivery.

---

# 31. End-to-End Flow Example

Suppose the Streak Service determines:

```text
Alice has not practiced today.
Her streak expires tonight.
Send reminder around 8 PM.
```

### Step 1

Streak Service publishes:

```text
NotificationRequest
```

to:

```text
notification-requests
```

---

### Step 2

Notification worker:

```text
validates
deduplicates
performs coarse policy check
```

---

### Step 3

Because it is scheduled:

```text
scheduled_at = 20:00 local time
```

store it in the correct time bucket.

---

### Step 4

Near 8 PM, the scheduler moves it into:

```text
ready-to-send
```

---

### Step 5

Delivery worker re-checks:

```text
push preference
streak notification preference
quiet hours
frequency caps
account state
```

---

### Step 6

Platform renders localized content.

---

### Step 7

Worker chooses:

```text
APNs
```

because Alice's active device is an iPhone.

---

### Step 8

Rate limiter obtains capacity.

---

### Step 9

Notification is sent.

---

### Step 10

Delivery state is recorded.

```text
SENT
```

or:

```text
FAILED
UNKNOWN
SUPPRESSED
```

depending on outcome.

---

# 32. Final Architecture

```text
                    PRODUCT SERVICES
          Streak / Social / Leaderboard / Campaign
                           |
                           |
                     NotificationRequest
                           |
                           v
               +------------------------+
               | notification-requests  |
               |         Kafka          |
               +-----------+------------+
                           |
                           v
                 Notification Workers
                           |
                +----------+----------+
                |                     |
                v                     v
          Deduplication          Policy Check
                |                     |
                +----------+----------+
                           |
                    +------+------+
                    |             |
                immediate      scheduled
                    |             |
                    |             v
                    |       Scheduling DB
                    |             |
                    |             v
                    |          Scheduler
                    |             |
                    +------+------+
                           |
                           v
                  Kafka: ready-to-send
                           |
                           v
                    Delivery Workers
                           |
                    Final Policy Check
                           |
                     Template Render
                           |
                       Device Lookup
                           |
                    Priority Queues
                           |
                       Rate Limiter
                      /            \
                     v              v
                   APNs            FCM
                     \              /
                      \            /
                       v          v
                     Delivery Status
                           |
                           v
                  Analytics / Metrics
```

---

# 33. Important Design Principles

## Keep product services decoupled from delivery

Product services express notification intent.

The notification platform owns delivery semantics.

---

## Re-check mutable policy at delivery time

Preferences may change after scheduling.

---

## Partition based on the scheduler's access pattern

The critical question is:

```text
"What needs to run now?"
```

Therefore use time-based partitions, not primarily user-based partitions.

---

## Use queues for buffering, not databases for execution

The database stores scheduled future work.

Kafka distributes work that is ready to execute.

---

## Expect duplicates

Duplicates can arise from:

```text
Kafka retries
producer retries
scheduler retries
delivery retries
provider ambiguity
```

Design operations to be safe under repetition.

---

## Distinguish logical notification from delivery attempts

```text
Logical notification
        |
        +--> attempt 1
        +--> attempt 2
        +--> attempt 3
```

A notification should remain one logical business object even when transport retries occur.

---

## Do not chase end-to-end exactly-once delivery

Across:

```text
Kafka
database
delivery worker
APNs / FCM
mobile device
```

exactly-once visible delivery is unrealistic.

Prefer:

```text
at-least-once infrastructure
+
idempotent processing
+
business deduplication
+
controlled retries
```

---

# 34. Interview Summary

A concise interview answer could be:

> I would build the notification platform as an asynchronous system. Product services publish structured notification requests into Kafka rather than calling APNs or FCM directly. Notification workers validate and deduplicate requests and perform an initial preference check. Immediate notifications flow into a ready-to-send topic, while scheduled notifications are persisted in time-partitioned scheduling storage. Scheduler workers move notifications into the delivery queue shortly before their send time.
>
> Delivery workers re-check current user preferences, quiet hours, and frequency limits, render localized templates, select the appropriate device, and send through provider-specific rate limiters to APNs or FCM. I would use priority queues and jitter to prevent large campaigns or local-time bursts from overwhelming the system.
>
> The design assumes at-least-once processing. `notification_id` handles transport-level duplication, while business-level deduplication keys prevent multiple logically equivalent notifications from reaching the user. Delivery attempts are modeled independently because an APNs or FCM timeout can leave the result ambiguous. We should not promise true exactly-once push delivery; instead we make retries safe and minimize duplicate user-visible notifications.