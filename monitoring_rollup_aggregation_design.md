# Monitoring Rollup Aggregation Design

## 1. Overview

For rollup aggregates such as every 5 minutes, 1 hour, or 1 day, there are two main approaches:

1. Persist one-minute aggregates and calculate larger rollups with scheduled database jobs.
2. Continue the streaming pipeline and aggregate one-minute outputs into larger windows using Flink.

For the streaming approach, the one-minute output should include explicit time metadata such as:

```text
bucket_start
bucket_end
```

This lets downstream processors determine which 5-minute, hourly, or daily bucket each one-minute result belongs to.

A third option is to use database-native continuous aggregates or materialized views.

---

## 2. Option 1: Database Rollup Jobs

Stage 1 writes one-minute aggregates into a database.

Example schema:

```text
MetricMinuteAggregate

metric_key
bucket_start
bucket_end
count
revision
```

Example:

```text
metric_key   = expense-api.requests.500
bucket_start = 12:00:00
bucket_end   = 12:01:00
count        = 100
revision     = 1
```

Use a uniqueness key such as:

```text
(metric_key, bucket_start)
```

Scheduled jobs then calculate larger intervals.

### Five-Minute Rollup

```sql
SELECT
    metric_key,
    five_minute_bucket(bucket_start) AS rollup_start,
    SUM(count) AS count
FROM metric_1m
WHERE bucket_start >= :start
  AND bucket_start < :end
GROUP BY metric_key, rollup_start;
```

Conceptually:

```text
12:00 = 100
12:01 = 120
12:02 = 110
12:03 = 130
12:04 = 140
----------------
12:00–12:05 = 600
```

Write the result into:

```text
MetricFiveMinuteAggregate

PRIMARY KEY(metric_key, bucket_start)
```

Likewise:

```text
60 one-minute buckets    -> one hourly bucket
1,440 one-minute buckets -> one daily bucket
```

---

## 3. Make Scheduled Rollups Idempotent

Scheduled jobs should write the absolute aggregate result.

Good:

```sql
INSERT INTO metric_5m (...)
VALUES (...)
ON CONFLICT (metric_key, bucket_start)
DO UPDATE SET
    count = EXCLUDED.count;
```

Avoid:

```sql
count = count + 600
```

Otherwise, rerunning the same job would double-count the metric.

The rollup job should always recompute the canonical value for the requested interval and replace the previous result.

---

## 4. Handling Late Data with Database Jobs

Suppose the one-minute bucket for `12:02` changes:

```text
old count = 110
new count = 113
```

The five-minute aggregate must change:

```text
600 -> 603
```

Scheduled jobs should therefore reprocess an overlap window.

Example:

```text
Every 5 minutes:
recalculate the previous 15–30 minutes

Every hour:
recalculate the previous 2–3 hours

Every day:
recalculate the previous 1–2 days
```

Because rollup writes replace absolute values, reprocessing is safe.

### Advantages

- Simple operational model.
- Easy to inspect and debug.
- Easy to backfill historical periods.
- Easy to recompute after a bug.
- Database remains the source of truth.
- Late-data correction is straightforward.

### Disadvantages

- Repeatedly scans one-minute rows.
- Rollup latency depends on job frequency.
- Can place significant load on the database.
- Requires scheduling, retries, monitoring, and backfill logic.
- Near-real-time larger windows are less natural.

This is often a good first version when metric volume is moderate.

---

## 5. Option 2: Streaming Rollups with Flink

Stage 1 emits one-minute aggregates into a stream.

Example message:

```json
{
  "metric_key": {
    "service": "expense-api",
    "metric": "request_count",
    "status": "500",
    "region": "us-east-1"
  },
  "bucket_start": "2026-11-03T12:00:00Z",
  "bucket_end": "2026-11-03T12:01:00Z",
  "count": 100,
  "revision": 1
}
```

The one-minute stream can be branched into multiple rollups:

```text
One-Minute Aggregate Stream
             |
       +-----+-------+--------+
       |             |        |
       v             v        v
  5-minute       1-hour     1-day
   rollup         rollup     rollup
```

Example Flink code:

```java
minuteAggregates
    .keyBy(MinuteAggregate::metricKey)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
    .aggregate(new SumMinuteCounts());
```

Likewise:

```java
Duration.ofHours(1)
Duration.ofDays(1)
```

Flink can maintain only the running aggregate rather than buffering every record.

---

## 6. One Flink Job or Separate Jobs

You do not necessarily need three separate Flink deployments.

One job can branch the stream:

```text
Source: minute-aggregates Kafka topic
              |
              v
         keyed stream
       /       |        \
      /        |         \
     v         v          v
  5-minute   hourly      daily
     |         |           |
     v         v           v
   sinks      sinks       sinks
```

### One Job

Advantages:

- simpler deployment,
- shared source consumption,
- fewer operational components,
- easier initial implementation.

### Separate Jobs

Advantages:

- independent scaling,
- independent deployment,
- better fault isolation,
- separate ownership,
- different service-level objectives.

A reasonable approach is to begin with one job and split later if isolation or scale requires it.

---

## 7. The Critical Issue: Late Updates

Suppose Stage 1 initially emits:

```text
12:00 minute:
count = 100
revision = 1
```

A late raw event arrives, so Stage 1 emits:

```text
12:00 minute:
count = 101
revision = 2
```

A naïve downstream sum would calculate:

```text
100 + 101 = 201
```

That is incorrect because the second message replaces the first one.

There are three main correction models.

---

## 8. Correction Model A: Emit Only Final Minute Aggregates

Stage 1 waits until:

```text
watermark >
minute end + allowed lateness
```

Then it emits one final result:

```text
12:00–12:01 final count = 101
```

Downstream processors can simply sum each minute record.

### Benefit

- Very simple downstream logic.
- No replacement or revision state needed.

### Cost

- One-minute results are delayed.
- Larger rollups are delayed as well.
- Long allowed-lateness periods increase end-to-end latency.

This works best when slightly delayed metrics are acceptable.

---

## 9. Correction Model B: Emit Deltas

Stage 1 emits changes instead of full replacement values.

Example:

```text
revision 1:
delta = +100

revision 2:
delta = +1
```

Downstream processors sum deltas:

```text
100 + 1 = 101
```

Example message:

```json
{
  "metric_key": "...",
  "bucket_start": "2026-11-03T12:00:00Z",
  "delta": 1,
  "revision": 2
}
```

This works naturally for additive metrics such as:

- count,
- sum,
- error count,
- bytes processed.

It is less straightforward for:

- minimum,
- maximum,
- distinct count,
- percentiles,
- histograms.

---

## 10. Correction Model C: Versioned Upserts

Stage 1 emits full replacement values:

```text
revision 1 -> 100
revision 2 -> 101
```

The rollup processor stores the latest known value for each minute:

```text
(metric_key, minute_bucket) -> previous count
```

When revision 2 arrives:

```text
previous value = 100
new value      = 101
difference     = +1
```

The rollup processor applies:

```text
5-minute aggregate += 1
hourly aggregate   += 1
daily aggregate    += 1
```

Pseudo-logic:

```java
MinuteKey key = new MinuteKey(metricKey, bucketStart);

long previous = state.getOrDefault(key, 0L);

if (incoming.revision() > storedRevision) {
    long delta = incoming.count() - previous;

    updateRollup(delta);
    state.put(key, incoming.count(), incoming.revision());
}
```

### Advantages

- Supports early outputs and later corrections.
- Gives clear replacement semantics.
- Works well with upsert-capable sinks.

### Disadvantages

- Requires more keyed state.
- Requires revision handling.
- More complex than final-only output.

---

## 11. Timestamp and Window Alignment

A one-minute aggregate should include:

```text
bucket_start
bucket_end
```

Suppose:

```text
bucket_start = 2026-11-03T12:37:00Z
```

It belongs to:

```text
5-minute:
12:35–12:40

hour:
12:00–13:00

day:
2026-11-03T00:00–2026-11-04T00:00
```

The downstream processor can calculate:

```text
five_minute_start = floor(bucket_start, 5 minutes)
hour_start        = floor(bucket_start, 1 hour)
day_start         = floor(bucket_start, 1 day)
```

Example output contract:

```json
{
  "bucket_start": "2026-11-03T12:37:00Z",
  "bucket_end": "2026-11-03T12:38:00Z"
}
```

Explicit boundaries make cross-system processing easier to audit and debug.

---

## 12. Prefer UTC for Infrastructure Metrics

For infrastructure monitoring, define rollups in UTC:

```text
hour = UTC hour
day  = UTC calendar day
```

This avoids daylight-saving-time problems such as:

- 23-hour days,
- 25-hour days,
- repeated local timestamps,
- nonexistent local timestamps.

The UI can convert UTC buckets into the viewer's timezone.

If business-local calendar periods are required, include the timezone in the rollup identity:

```text
(metric_key, timezone, bucket_start)
```

---

## 13. Direct Rollups Versus Hierarchical Rollups

There are two common streaming topologies.

### Direct Rollups from One-Minute Data

```text
1-minute stream
   +--> 5-minute
   +--> 1-hour
   +--> 1-day
```

Advantages:

- Every rollup depends only on the canonical one-minute stream.
- Failures are isolated.
- Easier independent replay.
- Fewer compounded errors.
- Simpler backfills.

Disadvantages:

- Hourly jobs consume 60 records per metric per hour.
- Daily jobs consume 1,440 records per metric per day.
- More total downstream processing.

For many systems, this is still inexpensive compared with processing raw metric events.

### Hierarchical Rollups

```text
1-minute
   |
   v
5-minute
   |
   v
1-hour
   |
   v
1-day
```

Or:

```text
1-minute -> 1-hour -> 1-day
```

Advantages:

- Less downstream traffic.
- Lower processing cost.
- Less state for larger windows.
- Daily rollups process 24 hourly values instead of 1,440 minute values.

Disadvantages:

- Errors or delays propagate.
- Backfills become dependent on intermediate stages.
- Not every metric can be safely re-aggregated.

---

## 14. Which Aggregations Can Be Re-Aggregated?

### Count and Sum

Safe:

```text
SUM(hourly counts) = daily count
```

### Average

Do not average averages unless every bucket has equal weight.

Incorrect:

```text
daily_average = average(hourly_averages)
```

Correct:

```text
daily_sum   = SUM(hourly_sum)
daily_count = SUM(hourly_count)

daily_average = daily_sum / daily_count
```

Therefore each lower-level bucket should preserve:

```text
sum
count
```

### Minimum and Maximum

Safe:

```text
daily_min = MIN(hourly_min)
daily_max = MAX(hourly_max)
```

### Percentiles

Incorrect:

```text
daily_p99 = p99(hourly_p99_values)
```

Percentiles cannot be reconstructed from percentile values alone.

Use mergeable summaries such as:

```text
t-digest
DDSketch
HDR histogram
```

### Distinct Count

A distinct count cannot normally be summed.

Use mergeable sketches such as:

```text
HyperLogLog
```

---

## 15. Richer Minute Aggregate Schema

A one-minute aggregate often should preserve mergeable state rather than only a displayed value.

Example:

```json
{
  "metric_key": "...",
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

This allows larger windows to calculate:

- count,
- sum,
- average,
- minimum,
- maximum,
- percentiles,
- distributions.

---

## 16. Option 3: Database-Native Continuous Rollups

A third approach is:

```text
Flink computes one-minute aggregates
        |
        v
Time-series or analytical database
        |
        v
continuous aggregates / materialized views
```

The database continuously maintains:

- five-minute aggregates,
- hourly aggregates,
- daily aggregates.

### Advantages

- No external cron orchestration.
- No separate rollup Flink jobs.
- Rollup logic stays close to storage.
- Can be simple to operate if the database supports it well.

### Disadvantages

- Stronger dependence on database-specific capabilities.
- Rollup load competes with queries and ingestion.
- Backfill and correction behavior depend on the database.
- Less portable architecture.

This can be a good middle ground when using a time-series database with mature continuous-aggregate support.

---

## 17. Recommended First Version

For moderate scale:

```text
Raw metrics
    |
    v
Flink one-minute aggregation
    |
    v
Minute aggregate database
    |
    v
Idempotent scheduled rollup jobs
```

This is:

- simple,
- easy to debug,
- easy to backfill,
- easy to correct,
- operationally understandable.

Use overlap windows and absolute-value upserts.

---

## 18. Recommended Large-Scale Version

For a large monitoring platform requiring low-latency dashboards and alerts:

```text
Raw metrics
    |
    v
Flink Stage 1: one-minute aggregates
    |
    v
Kafka: minute-aggregates
    |
    +--> 5-minute rollup
    +--> hourly rollup
    +--> daily rollup
    |
    v
Metric store
```

A good initial design is one Flink job with three branches, split later if independent scaling or isolation is needed.

The output contract should explicitly support:

```text
bucket identity
revision
finality
replacement or delta semantics
mergeable aggregate state
```

---

## 19. Key Design Decisions

Before implementation, decide:

### Window identity

```text
(metric_key, bucket_start)
```

### Time basis

```text
event time
```

rather than processing time.

### Correction semantics

Choose one:

```text
final-only records
delta records
versioned upsert records
```

### Timezone

Use:

```text
UTC
```

unless business-calendar semantics require another timezone.

### Re-Aggregation Model

Choose:

```text
direct rollups from one-minute data
```

or:

```text
hierarchical rollups
```

based on supported metric types and cost.

---

## 20. Interview Summary

> I would treat the one-minute aggregates as the canonical lower-resolution stream and include explicit `bucket_start` and `bucket_end` fields. Larger rollups can either be calculated by idempotent scheduled database jobs or by downstream Flink windows.
>
> For an initial version, database jobs are simpler and easier to backfill. Each job should recompute an absolute rollup value and upsert it, while periodically reprocessing an overlap window to absorb late minute-level corrections.
>
> At larger scale, I would branch the one-minute aggregate stream into 5-minute, hourly, and daily Flink windows. The critical design decision is how Stage 1 represents late corrections. If it emits replacement values, downstream processors must process versioned upserts or calculate deltas; otherwise they will double-count. An alternative is to emit only final minute aggregates after the allowed-lateness period.
>
> I would generally calculate all larger windows directly from the canonical one-minute stream for isolation and replay simplicity. Hierarchical rollups are more efficient but require preserving mergeable state such as `sum` and `count` for averages and sketches for percentiles or distinct counts.
