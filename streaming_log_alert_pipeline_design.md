# Streaming Alert Pipeline for Error Logs

## 1. Overview

The clean design is to evaluate alert rules **directly from the normalized log stream**, rather than repeatedly querying the log database.

```text
Application logs
      |
      v
Collectors / agents
      |
      v
Kafka: raw logs
      |
      v
Normalization / enrichment
      |
      v
Kafka: normalized logs
      |
      +----------------------+
      |                      |
      v                      v
Log search store       Flink alert engine
ClickHouse/OpenSearch        |
                              v
                     Alert transition topic
                              |
                              v
                       Alert service
                       /           \
                      v             v
               On-call provider   Slack
```

The searchable log database supports investigation. The Flink path performs low-latency detection.

---

## 2. Normalize Each Log Event

A normalized log should carry stable structured fields:

```json
{
  "event_id": "evt-123",
  "event_time": "2026-11-08T20:01:08.173Z",
  "ingest_time": "2026-11-08T20:01:08.692Z",

  "tenant_id": "acme",
  "service_name": "expense-api",
  "environment": "production",
  "severity": "ERROR",

  "region": "us-east-1",
  "host_id": "node-123",
  "source_instance_id": "expense-api-pod-abc",

  "message": "Database connection refused"
}
```

The `event_id` is important because collectors, producers, Kafka, or Flink recovery may cause duplicate delivery.

`event_time` is when the application generated the log. `ingest_time` is when the monitoring platform received it.

---

## 3. Partition the Normalized-Log Kafka Topic for Balanced Ingestion

Do not partition only by the minute:

```text
key = 2026-11-08T20:01
```

That would route every log in the same minute to one Kafka partition.

Also, using only:

```text
service_name + severity
```

can create a hot partition for a noisy service.

A good Kafka key is:

```text
tenant_id + service_name + source_instance_id
```

For example:

```text
acme|expense-api|expense-api-pod-abc
```

This distributes one busy service across many partitions while keeping logs from one source instance ordered.

The Kafka key is optimized for ingestion. It is **not** the same as the Flink alert-state key.

---

## 4. Distribute Alert Rules to Every Rule-Matching Subtask

Alert rules live in a database:

```text
AlertRule

rule_id
version
tenant_id
filter
threshold
window_seconds
group_by
action
cooldown
resolve_after
enabled
```

Example rule:

```json
{
  "rule_id": "expense-api-errors",
  "filter": {
    "service_name": ["expense-api"],
    "severity": ["ERROR"],
    "environment": ["production"]
  },
  "condition": {
    "operator": ">=",
    "count": 5,
    "window_seconds": 60
  },
  "group_by": [],
  "action": {
    "type": "PAGE_ONCALL",
    "destination": "expense-team-oncall"
  },
  "resolve_after_seconds": 300,
  "renotify_interval_seconds": 900
}
```

Rule changes flow through a second Kafka topic:

```text
Alert Rule API
      |
      v
Rule DB + outbox
      |
      v
Kafka: alert-rule-changes
      |
      v
Flink broadcast state
```

The rule definitions are usually **broadcast to every rule-matching subtask**, not partitioned by rule.

Each matcher subtask therefore has a local indexed copy of active rules.

---

## 5. Match Each Incoming Log Against Candidate Rules

The Flink matcher receives each normalized log and determines which rules it matches.

It should not compare every log against every rule.

Maintain an in-memory candidate index such as:

```text
tenant + service + severity
    -> candidate rules
```

Example:

```text
(acme, expense-api, ERROR)
    -> expense-api-errors
    -> all-production-errors
    -> database-errors
```

For an incoming `expense-api ERROR` log:

```text
1. Look up candidate rules.
2. Evaluate each candidate's full predicate.
3. Emit one RuleMatch per matching rule.
```

One log can match several rules.

Example `RuleMatch`:

```json
{
  "rule_id": "expense-api-errors",
  "rule_version": 7,
  "event_id": "evt-123",
  "event_time": "2026-11-08T20:01:08.173Z",
  "group_key": {
    "scope": "global"
  }
}
```

If the rule groups by region:

```json
{
  "group_by": ["region"]
}
```

then the group key might be:

```json
{
  "region": "us-east-1"
}
```

Logs that match no alert rules disappear from the alerting branch, but remain available in the log-search pipeline.

---

## 6. Repartition Matched Events by Alert-Evaluation Key

After rule matching, Flink performs:

```text
keyBy(rule_id, group_key)
```

For an ungrouped rule:

```text
expense-api-errors|global
```

For a rule grouped by region:

```text
expense-api-errors|us-east-1
expense-api-errors|eu-west-1
```

This `keyBy` causes a Flink network shuffle.

```text
Many Kafka partitions
        |
        v
Rule matching subtasks
        |
        v
keyBy(rule_id, group_key)
        |
        +-----------------------+
        |                       |
        v                       v
Alert evaluator 0       Alert evaluator 1
```

All matched logs for one alert key go to the same logical evaluator subtask.

The rules are replicated to matchers, but the **runtime alert state is partitioned** across evaluator subtasks.

---

## 7. Maintain Flink-Managed Keyed State

For each:

```text
(rule_id, group_key)
```

the alert evaluator maintains state such as:

```text
rolling event counts
alert status
first firing time
last matching event time
recovery start time
last notification time
renotification count
rule version
```

Use Flink-managed keyed state rather than an unmanaged Java map so the state participates in:

- checkpointing,
- recovery,
- state redistribution during rescaling,
- fault tolerance.

Possible Flink state types include:

```text
ValueState
MapState
ListState
```

---

## 8. Use a Rolling Window, Not a Fixed Minute Bucket

A requirement such as:

> Page on-call if there are 5 ERROR logs in any 60-second period.

is not equivalent to fixed minute buckets.

Suppose:

```text
12:00:58 -> 3 errors
12:01:02 -> 2 errors
```

A tumbling window sees:

```text
12:00–12:01 -> 3
12:01–12:02 -> 2
```

No alert fires.

But a rolling 60-second window sees five errors within four seconds, so the alert should fire.

The evaluator therefore maintains a rolling count over the previous 60 seconds.

### Exact Timestamp Approach

Store event timestamps:

```text
[12:00:40, 12:00:45, 12:00:58, 12:01:01]
```

When a new event arrives:

```text
1. Remove timestamps older than event_time - 60 seconds.
2. Add the new timestamp.
3. Count the remaining timestamps.
4. Evaluate the threshold.
```

This is exact but can consume significant state for high-volume keys.

### One-Second Bucket Approach

A more efficient design uses 60 one-second buckets:

```text
12:00:10 -> 2 errors
12:00:11 -> 0
12:00:12 -> 4
...
12:01:09 -> 1
```

Maintain:

```text
60-entry ring buffer
+
rolling sum
```

When time advances:

```text
rolling_sum =
rolling_sum
- expired_second_count
+ new_second_count
```

This gives one-second precision with bounded state.

---

## 9. Deduplicate Logs Before Counting

A duplicated log must not count twice toward an alert threshold.

Before rule matching or before state update:

```text
Have we already seen event_id?
      |
     yes -> ignore
      |
      no -> process
```

Deduplication state only needs to live longer than:

```text
largest alert window
+
maximum accepted lateness
+
retry margin
```

Example:

```text
largest window: 15 minutes
allowed lateness: 30 seconds
dedupe TTL: approximately 20 minutes
```

The deduplication and alert aggregation stages may be separate:

```text
normalized logs
      |
      v
keyBy(event_id)
      |
      v
deduplicate
      |
      v
rule matching
      |
      v
keyBy(rule_id, group_key)
      |
      v
alert evaluation
```

---

## 10. Use Event Time With Bounded Lateness

The alert engine should usually evaluate based on `event_time`, with a small watermark delay.

For example:

```text
bounded out-of-order allowance = 5–10 seconds
```

This lets slightly delayed logs enter the correct rolling window.

Also define a maximum alerting lateness:

```text
log arrives 10 seconds late:
include it

log arrives 20 minutes late:
store it for search
do not page retroactively
```

A reasonable policy might be:

```text
maximum alerting lateness = 30 seconds
```

Very late logs remain searchable but do not create old incidents.

---

## 11. Emit Only on State Transitions

Do not send a page for every event after the threshold is crossed.

Alert state:

```text
OK
 |
 | threshold crossed
 v
FIRING
 |
 | condition below threshold
 v
RECOVERING
 |
 | healthy for configured duration
 v
RESOLVED
```

Example:

```text
rolling count 4 -> 5
OK -> FIRING
emit FIRING event

rolling count 5 -> 6
FIRING -> FIRING
update state only

rolling count 6 -> 7
FIRING -> FIRING
update state only
```

When the condition remains below the threshold for five minutes:

```text
FIRING -> RECOVERING -> RESOLVED
```

emit a `RESOLVED` event.

You may also support:

```text
RENOTIFY
```

if a paging rule remains firing for 15 minutes.

---

## 12. Create an Alert Instance per Rule and Group Key

When a rule first transitions to `FIRING`, create a new logical alert episode.

Example transition event:

```json
{
  "alert_instance_id": "alert-123",
  "rule_id": "expense-api-errors",
  "rule_version": 7,
  "group_key": {
    "scope": "global"
  },
  "transition": "FIRING",
  "observed_count": 5,
  "threshold": 5,
  "window_start": "2026-11-08T20:00:08Z",
  "window_end": "2026-11-08T20:01:08Z",
  "sequence_number": 1
}
```

When it resolves:

```json
{
  "alert_instance_id": "alert-123",
  "rule_id": "expense-api-errors",
  "transition": "RESOLVED",
  "sequence_number": 2
}
```

A rule grouped by region may have several simultaneous alert instances:

```text
expense-api-errors | us-east-1
expense-api-errors | eu-west-1
```

Each evaluator subtask therefore owns a subset of **alert keys**, not necessarily entire rules.

---

## 13. Persist Alert Instances Outside Flink

Flink should detect transitions and emit them to Kafka:

```text
Flink Alert Engine
       |
       v
Kafka: alert-transitions
       |
       v
Alert Service
       |
       +--> AlertInstance DB
       |
       +--> Notification workflow
```

The Alert Service stores:

```text
AlertInstance

alert_instance_id
rule_id
rule_version
group_key
status
first_triggered_at
last_triggered_at
observed_value
sequence_number
resolved_at
```

Responsibility split:

```text
Flink:
real-time rolling state and transition detection

Alert Service:
durable history
acknowledgements
incident ownership
notification delivery
APIs
```

---

## 14. Make Alert-Transition Processing Idempotent

Kafka may redeliver a transition.

Use:

```text
(alert_instance_id, sequence_number)
```

as a unique transition identity.

Example database constraint:

```sql
UNIQUE(alert_instance_id, sequence_number)
```

This prevents:

- duplicate alert-instance updates,
- duplicate pages,
- duplicate Slack messages.

---

## 15. Notification Delivery

The Flink job should not call PagerDuty or Slack directly.

Instead:

```text
Alert transition
      |
      v
Notification Service
      |
      +--> on-call provider
      |
      +--> Slack
```

The Notification Service handles:

- provider credentials,
- retries,
- rate limits,
- message templates,
- destination lookup,
- notification idempotency,
- audit history.

A logical notification key might be:

```text
(
  alert_instance_id,
  transition,
  sequence_number,
  destination
)
```

Example:

```text
alert-123
FIRING
sequence=1
expense-team-oncall
```

---

## 16. Example Rules

### ERROR Paging Rule

```json
{
  "rule_id": "expense-api-errors",
  "filter": {
    "service_name": ["expense-api"],
    "severity": ["ERROR"]
  },
  "condition": {
    "count": 5,
    "window_seconds": 60
  },
  "action": {
    "type": "PAGE_ONCALL",
    "destination": "expense-team-oncall"
  },
  "resolve_after_seconds": 300,
  "renotify_interval_seconds": 900
}
```

Behavior:

```text
5 errors in rolling 60 seconds:
page once

still firing after 15 minutes:
page reminder

below threshold for 5 minutes:
resolve
```

### WARN Slack Rule

```json
{
  "rule_id": "expense-api-warnings",
  "filter": {
    "service_name": ["expense-api"],
    "severity": ["WARN"]
  },
  "condition": {
    "count": 10,
    "window_seconds": 60
  },
  "action": {
    "type": "SLACK",
    "destination": "expense-team-alerts"
  },
  "resolve_after_seconds": 300
}
```

Behavior:

```text
10 warnings in rolling 60 seconds:
send one Slack message

condition remains true:
do not send a message per warning

condition recovers:
optionally send resolved message
```

---

## 17. Full Flink Topology

```text
Kafka: normalized-logs
          |
          v
Deduplicate by event_id
          |
          v
Rule Matching Operator
rules in broadcast state
          |
          | zero or more RuleMatch events
          v
keyBy(rule_id, group_key)
          |
          v
Stateful Alert Evaluator
- rolling one-second buckets
- rolling count
- alert state
- recovery timers
- re-notification timers
          |
          v
Kafka: alert-transitions
          |
          v
Alert Service
          |
          +--> Alert DB
          |
          +--> Notification Service
                       |
                  +----+----+
                  |         |
                  v         v
              On-call     Slack
```

---

## 18. Precise Mental Model

The rules and runtime state are distributed differently.

```text
Rule definitions:
broadcast to all matching subtasks

Incoming logs:
matched against local candidate-rule index

RuleMatch records:
repartitioned by rule_id + group values

Alert evaluation state:
partitioned across Flink evaluator subtasks

On state transition:
emit AlertTransition

Downstream:
persist AlertInstance and deliver notifications
```

The essential design is:

> Normalize and deduplicate logs, match each log against a locally indexed broadcast rule set, repartition matching events by `(rule_id, group_by values)`, and maintain a rolling event-time count in Flink-managed keyed state. Emit only `FIRING`, `RESOLVED`, and optional `RENOTIFY` transitions. Persist alert instances and contact Slack or the on-call provider downstream through an idempotent notification service.
