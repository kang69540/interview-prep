# Flink Metrics Aggregation and Rollup Design

## 1. Overview

The overall design is:

```text
Raw service metrics
        |
        v
Kafka: raw-metrics
        |
        v
Flink Stage 1
1-minute aggregation
        |
        v
Kafka: minute-aggregates
        |
        +------------------+
        |        |         |
        v        v         v
      5-min    hourly     daily
      Flink     Flink      Flink
      job       job        job
```

The main clarification is that Flink normally keys the stream by the **metric-series identity**, while the window operator adds the time-window scope.

So the stream is not literally keyed by:

```text
service + hostname + minute
```

Instead:

```text
keyBy:
service + hostname + metric name + labels

window:
one-minute event-time window
```

Conceptually, Flink maintains state for:

```text
(metric-series key, minute window)
```

---

## 2. Raw Metric Event

A raw metric event might look like:

```json
{
  "service": "identity-api",
  "host": "host-1",
  "metric": "request_count",
  "status": "500",
  "event_time": "2026-11-03T12:00:17Z",
  "value": 1
}
```

The complete metric-series identity should include every dimension that distinguishes one time series from another:

```text
service
+ host or pod
+ metric name
+ relevant labels
```

Example:

```text
identity-api
+ host-1
+ request_count
+ status=500
```

If the metric name or labels are omitted, unrelated measurements such as request count, error count, and latency could be aggregated together incorrectly.

---

## 3. Raw Kafka Topic Partitioning

The raw Kafka topic may be partitioned by:

```text
service + source instance
```

where the source instance may be:

```text
hostname
pod ID
container ID
collector ID
```

Example keys:

```text
identity-api|host-1
identity-api|host-2
expense-api|host-7
```

This provides:

- balanced ingestion,
- per-source ordering,
- parallel Kafka consumption,
- distribution of a busy service across many partitions.

The Kafka key does not need to be identical to the Flink state key.

Kafka partitioning primarily supports transport, ingestion balance, and per-partition ordering.

Flink performs its own repartitioning with `keyBy(...)`.

---

## 4. Stage 1: One-Minute Aggregation

The first Flink job:

1. Reads raw metrics from Kafka.
2. Assigns event timestamps and watermarks.
3. Repartitions the stream by the complete metric-series identity.
4. Applies a one-minute tumbling event-time window.
5. Maintains an incremental aggregate for each key and minute.
6. Emits one-minute aggregate records.

Example:

```java
rawMetrics
    .assignTimestampsAndWatermarks(watermarkStrategy)
    .keyBy(event -> new MetricKey(
        event.service(),
        event.host(),
        event.metric(),
        event.status()
    ))
    .window(
        TumblingEventTimeWindows.of(
            Duration.ofMinutes(1)
        )
    )
    .aggregate(new SumAggregator());
```

The `keyBy` key is:

```text
service + host + metric + labels
```

The window operator provides:

```text
[12:00, 12:01)
```

Therefore, the effective state scope is:

```text
(
    identity-api,
    host-1,
    request_count,
    status=500,
    [12:00, 12:01)
)
```

A precise way to describe this is:

> The stream is keyed by the metric-series identity, and the window operator maintains separate state for every `(metric key, minute window)`.

---

## 5. Stage 1 Output Contract

A minute-level aggregate should include explicit bucket boundaries.

Example:

```json
{
  "metric_key": {
    "service": "identity-api",
    "host": "host-1",
    "metric": "request_count",
    "labels": {
      "status": "500"
    }
  },
  "bucket_start": "2026-11-03T12:00:00Z",
  "bucket_end": "2026-11-03T12:01:00Z",
  "count": 99,
  "revision": 1,
  "is_final": true
}
```

A shorthand such as:

```text
identity-api:host-1:minute-1:99
```

captures the basic idea, but a production record should also contain:

- metric name,
- relevant dimensions or labels,
- explicit bucket start and end,
- revision,
- finality,
- aggregation type if needed.

The one-minute aggregate Kafka topic can be partitioned by:

```text
service + host + metric + labels
```

This preserves ordering for updates to one metric series and distributes different metric series across Kafka partitions.

---

## 6. Downstream Rollup Jobs

The minute-aggregate topic can feed three downstream jobs:

```text
Kafka: minute-aggregates
          |
          +--> 5-minute Flink rollup
          |
          +--> hourly Flink rollup
          |
          +--> daily Flink rollup
```

Each job:

1. Reads minute-level aggregates.
2. Assigns event time using the minute bucket timestamp.
3. Keys the stream by the metric-series identity.
4. Applies the appropriate tumbling event-time window.
5. Merges the minute-level aggregate state.
6. Emits the larger rollup.

---

## 7. Five-Minute Rollup

Example:

```java
minuteAggregates
    .assignTimestampsAndWatermarks(
        minuteAggregateWatermarks
    )
    .keyBy(MinuteAggregate::metricKey)
    .window(
        TumblingEventTimeWindows.of(
            Duration.ofMinutes(5)
        )
    )
    .aggregate(new MergeAggregates());
```

Logical state:

```text
metric key:
identity-api + host-1 + request_count + status=500

window:
[12:00, 12:05)
```

Effective state scope:

```text
(metric-series key, five-minute window)
```

For example:

```text
12:00 minute -> 99
12:01 minute -> 103
12:02 minute -> 97
12:03 minute -> 110
12:04 minute -> 101

5-minute total -> 510
```

---

## 8. Hourly Rollup

Example:

```java
minuteAggregates
    .assignTimestampsAndWatermarks(
        minuteAggregateWatermarks
    )
    .keyBy(MinuteAggregate::metricKey)
    .window(
        TumblingEventTimeWindows.of(
            Duration.ofHours(1)
        )
    )
    .aggregate(new MergeAggregates());
```

Logical state:

```text
(metric-series key, [12:00, 13:00))
```

So it is conceptually reasonable to say:

```text
service + hostname + hour
```

but the more precise Flink terminology is:

```text
keyBy:
service + hostname + metric + labels

window:
hour boundary

effective state:
(metric key, hourly window)
```

---

## 9. Daily Rollup

Example:

```java
minuteAggregates
    .assignTimestampsAndWatermarks(
        minuteAggregateWatermarks
    )
    .keyBy(MinuteAggregate::metricKey)
    .window(
        TumblingEventTimeWindows.of(
            Duration.ofDays(1)
        )
    )
    .aggregate(new MergeAggregates());
```

Logical state:

```text
(metric-series key, UTC day)
```

For infrastructure monitoring, daily windows should normally use UTC to avoid daylight-saving-time ambiguity.

---

## 10. Visual Model

```text
Raw Kafka topic
Kafka key:
service + source instance
          |
          v
Flink Stage 1
keyBy:
metric-series identity
          |
one-minute event-time window
          |
          v
Kafka: minute-aggregates
Kafka key:
metric-series identity
          |
    +-----+------+------+
    |            |      |
    v            v      v
5-minute       hourly  daily
Flink job      job     job
    |            |      |
keyBy metric identity    |
window 5m     window 1h window 1d
    |            |      |
    v            v      v
5m aggregates   1h     1d
```

---

## 11. Kafka Partitioning Versus Flink State Partitioning

These are related but distinct concepts.

### Kafka partition key

Example:

```text
service + source instance
```

Purpose:

- balance ingestion,
- preserve source-level order,
- provide source parallelism,
- avoid one hot partition for a busy service.

### Flink `keyBy`

Example:

```text
service + host + metric + labels
```

Purpose:

- ensure all events for the same metric series reach the same logical keyed operator,
- maintain keyed state,
- support windowed aggregation.

### Window scope

Example:

```text
one minute
five minutes
one hour
one day
```

Purpose:

- separate aggregate state by time interval,
- define when results are emitted,
- define when state can be cleaned up.

The effective Flink window state is:

```text
(keyBy key, window)
```

---

## 12. Event Time and Watermarks

The rollup jobs should use event time.

For Stage 1:

```text
event timestamp = raw metric event_time
```

For downstream rollups:

```text
event timestamp = minute aggregate bucket_start
```

or another documented timestamp derived from the upstream window.

Watermarks tell Flink when event time has advanced enough to close and emit a window.

For example:

```text
hourly window:
[12:00, 13:00)

emit when:
watermark passes the end of the window
```

The job does not necessarily emit exactly at `13:00:00` wall-clock time. It emits when the event-time watermark passes that boundary.

---

## 13. Major Caveat: Revisions and Late Events

The pipeline is straightforward when Stage 1 emits exactly one final record per minute bucket.

Example:

```text
12:00 minute -> count 99
```

The hourly job can safely sum the record.

However, suppose Stage 1 emits an early result:

```text
revision 1 -> count 99
```

Then a late raw metric arrives and Stage 1 emits:

```text
revision 2 -> count 100
```

A naïve downstream sum would calculate:

```text
99 + 100 = 199
```

This is incorrect because revision 2 replaces revision 1.

The system needs an explicit correction contract.

---

## 14. Correction Option A: Final-Only Records

Stage 1 waits until the allowed-lateness period has passed and emits one final result.

```text
one minute bucket
    -> one final aggregate record
```

Example:

```json
{
  "count": 100,
  "revision": 1,
  "is_final": true
}
```

Downstream jobs can simply sum records.

### Advantages

- simplest downstream logic,
- no replacement state,
- no delta calculation.

### Disadvantages

- higher aggregation latency,
- results wait for the lateness allowance,
- larger rollups are delayed.

---

## 15. Correction Option B: Delta Records

Stage 1 emits changes instead of replacements.

Example:

```text
first output:
delta = +99

late correction:
delta = +1
```

The downstream rollup safely sums:

```text
99 + 1 = 100
```

This works well for additive metrics such as:

- count,
- sum,
- bytes,
- error count.

It is harder for:

- minimum,
- maximum,
- percentiles,
- distinct counts,
- histograms.

---

## 16. Correction Option C: Versioned Upserts

Stage 1 emits absolute replacement values:

```text
revision 1 -> 99
revision 2 -> 100
```

The downstream job stores the latest value for:

```text
(metric-series key, minute bucket)
```

When revision 2 arrives:

```text
previous value = 99
new value      = 100
delta          = +1
```

The downstream rollups apply:

```text
5-minute aggregate += 1
hourly aggregate   += 1
daily aggregate    += 1
```

Pseudo-logic:

```java
MinuteBucketKey key =
    new MinuteBucketKey(
        incoming.metricKey(),
        incoming.bucketStart()
    );

StoredMinute previous =
    minuteState.get(key);

if (previous == null
        || incoming.revision() > previous.revision()) {

    long previousValue =
        previous == null ? 0 : previous.value();

    long delta =
        incoming.value() - previousValue;

    updateCurrentRollup(delta);

    minuteState.put(
        key,
        new StoredMinute(
            incoming.value(),
            incoming.revision()
        )
    );
}
```

This gives low-latency early output while supporting later corrections, but requires additional keyed state.

---

## 17. One Job Versus Three Jobs

Three independent jobs are valid:

```text
minute topic -> 5-minute job
minute topic -> hourly job
minute topic -> daily job
```

### Advantages

- independent deployment,
- independent scaling,
- better fault isolation,
- separate ownership,
- independent replay,
- independent backfill.

### Disadvantages

- three source consumers,
- more deployments,
- more monitoring,
- more checkpointing infrastructure.

An alternative is one Flink job with three branches:

```text
minute aggregate source
       |
       +--> 5-minute window
       +--> hourly window
       +--> daily window
```

### One Branched Job

Advantages:

- simpler operations,
- shared source consumption,
- fewer deployments.

### Separate Jobs

Advantages:

- better isolation,
- independent scaling,
- independent release cycles.

A reasonable starting point is one branched job. Split it later if operational isolation or scaling requirements justify the added complexity.

---

## 18. Aggregation State Should Be Mergeable

If downstream jobs aggregate minute-level records, the output should preserve enough information to build larger windows correctly.

For counts and sums:

```text
larger count = sum(smaller counts)
larger sum   = sum(smaller sums)
```

For averages, preserve:

```text
sum
count
```

Do not calculate:

```text
average(hourly averages)
```

unless all hourly buckets contain equal observation counts.

Correct:

```text
daily_average =
SUM(hourly_sum) / SUM(hourly_count)
```

For min and max:

```text
daily_min = MIN(hourly_min)
daily_max = MAX(hourly_max)
```

For percentiles, retain a mergeable histogram or sketch such as:

```text
t-digest
DDSketch
HDR histogram
```

Do not attempt:

```text
daily_p99 = p99(hourly_p99 values)
```

For distinct counts, use a mergeable sketch such as HyperLogLog rather than summing distinct counts.

---

## 19. Recommended Aggregate Schema

A more complete minute-level output may look like:

```json
{
  "metric_key": {
    "service": "identity-api",
    "host": "host-1",
    "metric": "request_latency_ms",
    "labels": {
      "endpoint": "/login",
      "status": "200"
    }
  },
  "bucket_start": "2026-11-03T12:00:00Z",
  "bucket_end": "2026-11-03T12:01:00Z",
  "aggregation": {
    "count": 1000,
    "sum": 24500,
    "min": 3,
    "max": 850,
    "histogram": "..."
  },
  "revision": 2,
  "is_final": false
}
```

This lets downstream jobs merge the state into larger windows without returning to the raw event stream.

---

## 20. Corrected Summary

The design can be summarized as:

> Raw metrics are published to Kafka, typically partitioned by service and source instance. The first Flink job repartitions them with `keyBy` using the full metric-series identity, such as service, host, metric name, and labels, and then applies a one-minute event-time window. Flink therefore maintains one aggregate state for every `(metric-series key, minute window)`.
>
> The job emits minute aggregates containing explicit bucket boundaries. The minute-aggregate topic can then be consumed by 5-minute, hourly, and daily Flink jobs. Each downstream job again keys by the metric-series identity and applies its own time window, so its state is effectively scoped by `(metric-series key, rollup window)`.
>
> The major remaining design decision is whether minute outputs are final-only, delta records, or versioned upserts, because naïvely summing revised minute values would double-count.
