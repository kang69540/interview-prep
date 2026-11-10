# HTTP Metrics Monitoring System Design

## 1. Scope

This design covers collection, aggregation, storage, and querying of HTTP request metrics for many services.

For every completed HTTP request, the monitoring platform should support metrics such as:

- total request count,
- request count by HTTP method,
- request count by normalized API endpoint,
- request count by response code or status class,
- request count for selected composite dimensions,
- total response time,
- minimum and maximum response time,
- average response time,
- latency histograms and percentiles,
- host-level metrics,
- service-level metrics,
- one-minute, five-minute, hourly, and daily time windows,
- near-real-time queries over recent data,
- historical queries over longer periods.

The system will use:

- Kafka for durable event transport,
- Flink for deduplication and streaming aggregation,
- ClickHouse for both hot and historical storage,
- a query service to choose resolution and execute ClickHouse queries.

---

## 2. High-Level Architecture

```text
Application Services
        |
        | one event per completed HTTP request
        v
Metrics Collectors / Ingestion API
        |
        v
Kafka: raw-http-events
        |
        v
Flink Stage 0
validation, normalization, deduplication
        |
        v
Flink Stage 1
host-level one-minute aggregation
        |
        v
Kafka: http-host-minute-aggregates
        |
        +------------------------------+
        |                              |
        v                              v
ClickHouse Host 1m Table       Flink Stage 2
hot + historical storage       host-to-service rollup
                                       |
                                       v
                         Kafka: http-service-minute-aggregates
                                       |
                      +----------------+----------------+
                      |                                 |
                      v                                 v
             ClickHouse Service 1m Table       Generic Time Rollups
                                                5m / 1h / 1d
                                                      |
                                                      v
                                      ClickHouse Rollup Tables
```

The query layer uses ClickHouse for both recent and historical data:

```text
Dashboard / API
      |
      v
Metrics Query Service
      |
      v
ClickHouse
- host 1-minute table
- service 1-minute table
- 5-minute rollups
- hourly rollups
- daily rollups
```

---

## 3. Raw HTTP Request Event

Each completed HTTP request produces one normalized event.

```json
{
  "event_id": "evt-123",
  "event_time": "2026-11-10T15:01:14.382Z",
  "ingest_time": "2026-11-10T15:01:14.921Z",

  "service_name": "identity-api",
  "server_hostname": "host-7",
  "region": "us-east-1",
  "availability_zone": "us-east-1a",

  "http_method": "GET",
  "route": "/v1/users/{user_id}",
  "status_code": 200,
  "status_class": "2xx",

  "response_time_ms": 37
}
```

### Important rules

Use the normalized route template:

```text
/v1/users/{user_id}
```

Do not use the raw URL:

```text
/v1/users/123
/v1/users/456
```

Raw path parameters cause unbounded metric cardinality.

Avoid dimensions such as:

- request ID,
- user ID,
- session ID,
- raw URL,
- arbitrary error text,
- trace ID.

Those values belong in logs or traces, not metric labels.

---

## 4. Raw Kafka Topic Partitioning

A reasonable raw-topic key is:

```text
service_name + source_instance_id
```

where `source_instance_id` may be:

- hostname,
- pod ID,
- container ID,
- collector ID.

Example:

```text
identity-api|host-7
```

This gives:

- balanced Kafka ingestion,
- source-level event ordering,
- parallelism across hosts and pods,
- avoidance of one hot Kafka partition per service.

The Kafka partition key does not need to match the Flink aggregation key.

Kafka partitioning is optimized for durable transport and balanced source consumption.

Flink performs its own repartitioning through `keyBy(...)`.

---

## 5. Event Deduplication

HTTP request events may be delivered more than once because of:

- client retries,
- collector retries,
- Kafka producer retries,
- Flink recovery,
- network timeouts.

Deduplicate before any dimensional expansion or aggregation.

```text
Kafka raw events
      |
      v
keyBy(event_id)
      |
      v
Flink deduplication operator
      |
      v
deduplicated HTTP events
```

The deduplication state is conceptually:

```text
event_id -> seen
```

with a TTL longer than the maximum expected retry interval.

Deduplication and aggregation are separate keyed stages:

```text
Dedup state key:
event_id

Aggregation state key:
metric-series identity
```

A second `keyBy(...)` introduces a Flink shuffle.

---

## 6. Metric-Series Model

The central abstraction is a **metric series**.

A metric series is identified by:

```text
scope
service
optional host
selected dimensions
```

Example host-level series:

```text
scope=HOST
service=identity-api
host=host-7
method=GET
route=/v1/users/{user_id}
status_class=2xx
```

Example service-level series:

```text
scope=SERVICE
service=identity-api
method=GET
route=/v1/users/{user_id}
status_class=2xx
```

A generic key can be modeled as:

```java
record HttpMetricKey(
    Scope scope,
    String serviceName,
    String hostname,
    SortedMap<String, String> dimensions
) {}
```

For service-level metrics, `hostname` is omitted or null.

The dimensions map must be canonicalized so these represent the same key:

```text
method=GET,status_class=2xx

status_class=2xx,method=GET
```

Sort dimension names before hashing or serializing the key.

---

## 7. Desired Dimension Views

Every selected dimension set defines a distinct metric series.

Examples:

```text
TOTAL
    dimensions = {}

BY_METHOD
    dimensions = {method}

BY_STATUS_CLASS
    dimensions = {status_class}

BY_STATUS_CODE
    dimensions = {status_code}

BY_ENDPOINT
    dimensions = {route}

BY_METHOD_STATUS
    dimensions = {method, status_class}

BY_ROUTE_METHOD
    dimensions = {route, method}

BY_ROUTE_METHOD_STATUS
    dimensions = {route, method, status_class}

BY_REGION
    dimensions = {region}
```

One raw request may contribute to several materialized views.

For example:

```text
GET /v1/users/{user_id} -> 200, 37 ms
```

may produce:

```text
TOTAL

method=GET

status_class=2xx

route=/v1/users/{user_id}

method=GET,status_class=2xx

route=/v1/users/{user_id},method=GET

route=/v1/users/{user_id},method=GET,status_class=2xx
```

Each selected dimension or composite dimension emits its own metric contribution.

This is intentional write amplification.

---

## 8. Dimension Expansion

Use a `flatMap` after deduplication.

```text
Deduplicated HTTP event
        |
        v
Dimension contribution mapper
        |
        +--> TOTAL contribution
        +--> METHOD contribution
        +--> STATUS contribution
        +--> ROUTE contribution
        +--> METHOD+STATUS contribution
        +--> ROUTE+METHOD contribution
        +--> ROUTE+METHOD+STATUS contribution
```

Conceptual Java:

```java
public final class HttpMetricContributionMapper
        implements FlatMapFunction<
            HttpRequestEvent,
            HttpMetricContribution> {

    @Override
    public void flatMap(
            HttpRequestEvent event,
            Collector<HttpMetricContribution> out) {

        emit(out, event, Map.of());

        emit(out, event, Map.of(
            "method", event.httpMethod()
        ));

        emit(out, event, Map.of(
            "status_class", event.statusClass()
        ));

        emit(out, event, Map.of(
            "route", event.route()
        ));

        emit(out, event, Map.of(
            "method", event.httpMethod(),
            "status_class", event.statusClass()
        ));

        emit(out, event, Map.of(
            "route", event.route(),
            "method", event.httpMethod()
        ));

        emit(out, event, Map.of(
            "route", event.route(),
            "method", event.httpMethod(),
            "status_class", event.statusClass()
        ));
    }
}
```

Use an explicit registry of allowed metric views rather than generating arbitrary combinations.

---

## 9. Avoiding Cardinality Explosion

Dimensions multiply rapidly.

Suppose there are:

```text
10 methods
500 routes
50 status codes
20 regions
100 hosts
```

The theoretical combination count can become enormous.

Do not precompute every possible combination.

A practical bounded set might be:

```text
service total
service + host
service + method
service + status_class
service + route + method
service + route + method + status_class
service + region
```

Prefer status classes:

```text
2xx
3xx
4xx
5xx
```

for common dashboards and alerts.

Keep exact status codes only where there is a concrete use case.

Some coarser views can be derived at query time from finer-grained data.

For example, if ClickHouse stores:

```text
service + route + method + status_class
```

it can derive:

```text
service + route + method
```

by summing across `status_class`.

Therefore, do not automatically materialize both fine and coarse forms unless query demand justifies it.

---

## 10. Host-Level One-Minute Aggregation

After dimension expansion:

```text
keyBy(
  service,
  host,
  dimensions
)
```

Then apply a one-minute tumbling event-time window.

```java
DataStream<HttpHostMinuteAggregate> hostMinuteAggregates =
    contributions
        .keyBy(HttpMetricContribution::metricKey)
        .window(
            TumblingEventTimeWindows.of(
                Duration.ofMinutes(1)
            )
        )
        .aggregate(new HttpMetricAggregator());
```

The effective Flink state is:

```text
(metric-series key, one-minute window)
```

Example:

```text
(
  scope=HOST,
  service=identity-api,
  host=host-7,
  route=/v1/users/{user_id},
  method=GET,
  status_class=2xx,
  [15:01, 15:02)
)
```

---

## 11. Aggregate Fields

Each metric-series accumulator should retain mergeable values.

```text
request_count
response_time_sum_ms
response_time_min_ms
response_time_max_ms
latency histogram or mergeable sketch
```

Example:

```json
{
  "request_count": 421,
  "response_time_sum_ms": 18472,
  "response_time_min_ms": 4,
  "response_time_max_ms": 913,
  "latency_histogram": {
    "le_10": 82,
    "le_25": 201,
    "le_50": 348,
    "le_100": 397,
    "le_250": 414,
    "le_500": 419,
    "le_1000": 421
  }
}
```

Average response time is derived as:

```text
average =
response_time_sum_ms / request_count
```

Do not store only the average.

Percentiles cannot be correctly merged from minute-level percentile values.

Incorrect:

```text
hourly p99 = average(minute p99)
```

Use:

- histogram buckets,
- HDR histogram,
- DDSketch,
- t-digest.

Histogram bucket counts can be summed across hosts and time windows.

---

## 12. Host-Level Minute Output

Example:

```json
{
  "metric_key": {
    "scope": "HOST",
    "service": "identity-api",
    "host": "host-7",
    "dimensions": {
      "route": "/v1/users/{user_id}",
      "method": "GET",
      "status_class": "2xx"
    }
  },

  "bucket_start": "2026-11-10T15:01:00Z",
  "bucket_end": "2026-11-10T15:02:00Z",

  "request_count": 421,
  "response_time_sum_ms": 18472,
  "response_time_min_ms": 4,
  "response_time_max_ms": 913,
  "latency_histogram": {
    "le_10": 82,
    "le_25": 201,
    "le_50": 348,
    "le_100": 397,
    "le_250": 414,
    "le_500": 419,
    "le_1000": 421
  },

  "revision": 1,
  "is_final": true
}
```

Logical identity:

```text
(
  scope,
  service,
  host,
  dimensions,
  bucket_start
)
```

Kafka key:

```text
HOST
|identity-api
|host-7
|method=GET
|route=/v1/users/{user_id}
|status_class=2xx
```

All host-level dimension records can be emitted to one topic:

```text
http-host-minute-aggregates
```

provided the normalized key and schema identify the series unambiguously.

---

## 13. Service-Level Rollups from Host Aggregates

Service-level metrics are computed by a second aggregation stage.

```text
Host minute aggregates
        |
        v
remove host from the metric key
        |
        v
keyBy(service, dimensions)
        |
        v
same one-minute window
        |
        v
Service minute aggregate
```

Host key:

```text
service + host + dimensions
```

Service key:

```text
service + dimensions
```

Conceptual Java:

```java
hostAggregates
    .assignTimestampsAndWatermarks(hostAggregateWatermarks)
    .keyBy(aggregate -> new ServiceMetricKey(
        aggregate.metricKey().serviceName(),
        aggregate.metricKey().dimensions()
    ))
    .window(
        TumblingEventTimeWindows.of(
            Duration.ofMinutes(1)
        )
    )
    .aggregate(new MergeHostAggregates());
```

Use the host aggregate's `bucket_start` as its downstream event timestamp.

That ensures:

```text
host aggregate [15:01, 15:02)
```

belongs to service window:

```text
[15:01, 15:02)
```

---

## 14. Correct Host-to-Service Merging

For counts and sums:

```text
service request count =
SUM(host request count)

service response-time sum =
SUM(host response-time sum)
```

For minimum and maximum:

```text
service minimum =
MIN(host minimum)

service maximum =
MAX(host maximum)
```

For average:

```text
service average =
SUM(host response-time sum)
/
SUM(host request count)
```

Do not average host averages.

For histograms:

```text
service bucket X =
SUM(host bucket X)
```

Then derive service-level percentiles from the merged histogram.

---

## 15. Service-Level Minute Output

```json
{
  "metric_key": {
    "scope": "SERVICE",
    "service": "identity-api",
    "dimensions": {
      "route": "/v1/users/{user_id}",
      "method": "GET",
      "status_class": "2xx"
    }
  },

  "bucket_start": "2026-11-10T15:01:00Z",
  "bucket_end": "2026-11-10T15:02:00Z",

  "request_count": 1025,
  "response_time_sum_ms": 43800,
  "response_time_min_ms": 4,
  "response_time_max_ms": 720,
  "latency_histogram": {
    "le_10": 190,
    "le_25": 520,
    "le_50": 875,
    "le_100": 990,
    "le_250": 1018,
    "le_500": 1024,
    "le_1000": 1025
  },

  "revision": 1,
  "is_final": true
}
```

Logical identity:

```text
(
  scope=SERVICE,
  service,
  dimensions,
  bucket_start
)
```

Use a separate topic:

```text
http-service-minute-aggregates
```

This keeps host- and service-level records operationally distinct.

---

## 16. Hosts With No Requests

A host with no requests during a minute normally emits no aggregate.

For additive metrics, absence can be treated as zero.

```text
service request count =
sum of present host aggregates
```

Do not force every inactive host to emit zero-valued records.

However, zero traffic and missing telemetry are different:

```text
zero:
host reported and saw no requests

missing:
host or collector may have stopped reporting
```

Track telemetry health separately through:

- heartbeat events,
- an `up` metric,
- collector status,
- missing-series alerts.

---

## 17. Event Time and Watermarks

Use event time for raw requests.

```text
raw event timestamp =
event_time
```

Use the minute aggregate's `bucket_start` for downstream host-to-service and time rollups.

Watermarks tell Flink when enough event time has passed to emit and clean up a window.

Example:

```text
minute window:
[15:01, 15:02)

emit when:
watermark passes 15:02
```

The result may appear slightly after wall-clock `15:02`, depending on allowed out-of-order delay.

Configure idleness detection so an idle Kafka partition does not block global watermark progress.

---

## 18. Revisions and Late Events

The simplest pipeline uses **final-only minute records**.

```text
one metric series
+
one minute
=
one final aggregate
```

Then service rollups and later time rollups can safely sum records.

If host-level aggregates can be revised:

```text
revision 1 -> count 400
revision 2 -> count 402
```

a naïve service rollup would calculate:

```text
400 + 402 = 802
```

which is wrong.

Possible correction models:

### Final-only records

Wait until the lateness horizon expires and emit once.

Advantages:

- simplest downstream processing,
- safe materialized rollups,
- good for historical truth.

Disadvantage:

- delayed output.

### Delta records

Emit:

```text
initial delta = +400
correction delta = +2
```

Easy for additive metrics.

Harder for minimum and maximum corrections.

### Versioned absolute upserts

Emit full replacements with increasing revision numbers.

Add a replacement-aware stage keyed by:

```text
service
+ host
+ dimensions
+ minute bucket
```

It stores the latest host value and emits only changes downstream.

For non-additive metrics such as min and max, the service rollup may need to retain the latest aggregate from every host and recompute.

---

## 19. Recommended Provisional and Final Streams

A useful pattern is:

```text
Flink window
    |
    +--> provisional updates
    |
    +--> final aggregate after lateness horizon
```

Topics:

```text
http-host-minute-provisional
http-host-minute-final

http-service-minute-provisional
http-service-minute-final
```

Use provisional records for:

- current dashboards,
- low-latency operational views.

Use final records for:

- historical storage,
- precomputed time rollups,
- reporting,
- SLO calculations.

This avoids correction complexity in long-term rollup pipelines.

---

## 20. Time Rollups for Every Metric Series

Every materialized dimension set becomes its own minute-level metric series and therefore its own time rollups.

Examples:

```text
service + method

service + status_class

service + route + method

service + route + method + status_class
```

Each is rolled up independently.

However, you do not need a separate Flink job for each dimension.

Use one generic key:

```text
scope
+ service
+ optional host
+ canonical dimensions
```

Then:

```text
minuteAggregates
    .keyBy(MinuteAggregate::canonicalMetricKey)
    .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
    .aggregate(new MergeAggregates());
```

The same operator handles all metric-series identities.

---

## 21. Five-Minute, Hourly, and Daily Rollups

Recommended topology:

```text
Host minute final
    +--> Host 5-minute
    +--> Host hourly
    +--> Host daily

Service minute final
    +--> Service 5-minute
    +--> Service hourly
    +--> Service daily
```

Each time rollup preserves the metric dimensions.

Example:

```text
1-minute:
SERVICE|identity-api|method=GET

1-hour:
SERVICE|identity-api|method=GET
```

Only the time bucket changes.

Effective Flink state:

```text
(metric-series identity, rollup window)
```

Use UTC for daily infrastructure windows unless business requirements specify another calendar.

---

## 22. Direct Versus Hierarchical Time Rollups

### Direct from minute aggregates

```text
minute
  +--> 5-minute
  +--> hourly
  +--> daily
```

Advantages:

- independent pipelines,
- simple replay,
- less error propagation,
- daily data does not depend on hourly correctness.

Disadvantage:

- more processing.

This is the safer default.

### Hierarchical

```text
minute -> hourly -> daily
```

Advantages:

- less data processed downstream,
- daily consumes 24 hourly values instead of 1,440 minute values.

Disadvantages:

- corrections and failures propagate,
- more dependent backfills,
- requires mergeable aggregate state.

For counts, sums, min, max, and histograms, hierarchical rollups are possible.

For percentiles, retain mergeable sketches rather than displayed percentile values.

---

## 23. ClickHouse as Both Hot and Historical Store

The system will use ClickHouse for:

- recent seven-day low-latency queries,
- historical one-minute metrics,
- precomputed five-minute, hourly, and daily rollups,
- ad hoc dimensional aggregation,
- dashboard queries.

A single ClickHouse cluster can use:

- fast local/native storage for recent data,
- lower-cost or object-backed storage for older data,
- TTL policies for retention,
- separate tables by scope and granularity.

---

## 24. Suggested ClickHouse Tables

```text
http_host_metrics_1m
http_service_metrics_1m

http_host_metrics_5m
http_service_metrics_5m

http_host_metrics_1h
http_service_metrics_1h

http_host_metrics_1d
http_service_metrics_1d
```

Depending on historical needs, host-level long-term retention can be shorter than service-level retention.

Example policy:

| Table | Retention |
|---|---:|
| Host 1-minute | 7 days |
| Service 1-minute | 90 days |
| Service 5-minute | 1 year |
| Service hourly | Multiple years |
| Service daily | Long term |

Host-level historical data may be archived to cheaper ClickHouse storage or deleted after seven days.

---

## 25. Example ClickHouse Host Table

```sql
CREATE TABLE http_host_metrics_1m
(
    bucket_start             DateTime,
    bucket_end               DateTime,

    service_name             LowCardinality(String),
    hostname                 String,

    dimension_hash           UInt64,
    dimensions               Map(String, String),

    request_count            UInt64,
    response_time_sum_ms     UInt64,
    response_time_min_ms     UInt64,
    response_time_max_ms     UInt64,

    latency_histogram        Array(UInt64),

    revision                 UInt32,
    is_final                 Bool
)
ENGINE = MergeTree
PARTITION BY toDate(bucket_start)
ORDER BY
(
    service_name,
    hostname,
    dimension_hash,
    bucket_start
)
TTL bucket_start + INTERVAL 7 DAY DELETE;
```

A production design may use separate typed columns for frequently queried dimensions such as:

```text
method
route
status_class
region
```

rather than storing everything only in a map.

Typed columns improve filtering, compression, and query planning.

---

## 26. Example ClickHouse Service Table

```sql
CREATE TABLE http_service_metrics_1m
(
    bucket_start             DateTime,
    bucket_end               DateTime,

    service_name             LowCardinality(String),

    dimension_hash           UInt64,
    dimensions               Map(String, String),

    request_count            UInt64,
    response_time_sum_ms     UInt64,
    response_time_min_ms     UInt64,
    response_time_max_ms     UInt64,

    latency_histogram        Array(UInt64),

    revision                 UInt32,
    is_final                 Bool
)
ENGINE = MergeTree
PARTITION BY toDate(bucket_start)
ORDER BY
(
    service_name,
    dimension_hash,
    bucket_start
);
```

Retention can be longer than the host table.

---

## 27. Query-Time Aggregation in ClickHouse

ClickHouse can aggregate one-minute rows directly.

You do not need Flink rollups just to answer:

```text
host metrics over last 5 minutes
host metrics over last hour
service metrics over last day
```

### One total over the previous hour

```sql
SELECT
    service_name,
    hostname,

    sum(request_count) AS requests,
    sum(response_time_sum_ms) AS total_response_time_ms,

    total_response_time_ms / requests AS avg_response_time_ms,

    min(response_time_min_ms) AS min_response_time_ms,
    max(response_time_max_ms) AS max_response_time_ms

FROM http_host_metrics_1m

WHERE service_name = 'identity-api'
  AND hostname = 'host-7'
  AND bucket_start >= now() - INTERVAL 1 HOUR

GROUP BY
    service_name,
    hostname;
```

### Five-minute chart buckets over the previous hour

```sql
SELECT
    toStartOfInterval(
        bucket_start,
        INTERVAL 5 MINUTE
    ) AS five_minute_bucket,

    sum(request_count) AS requests,
    sum(response_time_sum_ms) AS response_time_sum_ms,
    min(response_time_min_ms) AS response_time_min_ms,
    max(response_time_max_ms) AS response_time_max_ms

FROM http_host_metrics_1m

WHERE service_name = 'identity-api'
  AND hostname = 'host-7'
  AND bucket_start >= now() - INTERVAL 1 HOUR

GROUP BY five_minute_bucket
ORDER BY five_minute_bucket;
```

For one metric series:

```text
5-minute query -> about 5 source rows
1-hour query   -> about 60 source rows
24-hour query  -> about 1,440 source rows
7-day query    -> about 10,080 source rows
```

These scans are usually inexpensive when the query is selective.

---

## 28. When Query-Time Aggregation Is Enough

Use ClickHouse query-time aggregation when:

- queries cover minutes or hours,
- filters are selective,
- dashboard QPS is manageable,
- cardinality is controlled,
- P95 query latency meets the SLO,
- the same expensive aggregation is not repeated continuously.

This should be the initial design.

Do not add Flink rollups solely because a dashboard needs:

```sql
GROUP BY five_minute_bucket
```

---

## 29. When to Precompute Rollups

Precompute 5-minute, hourly, or daily tables when:

- the same query shape runs constantly,
- dashboard concurrency becomes high,
- queries aggregate across many hosts,
- seven-day scans consume too much CPU,
- endpoint cardinality is high,
- historical ranges must have predictable low latency,
- other systems consume the rollup as a data product,
- query-time aggregation no longer meets the SLO.

There are two main implementation choices.

### Flink rollups

Use when:

- multiple storage systems need the same rollups,
- event-time and late-data behavior must be centralized,
- rollups feed alerts, reports, or exports,
- the rollup is an independently consumed stream.

### ClickHouse materialized views

Use when:

- the rollup exists primarily to accelerate ClickHouse queries,
- the input stream is append-only and final,
- operational simplicity matters.

---

## 30. ClickHouse Materialized Rollups

Example five-minute target table:

```sql
CREATE TABLE http_service_metrics_5m
(
    bucket_start           DateTime,
    service_name           LowCardinality(String),

    method                 LowCardinality(String),
    route                  LowCardinality(String),
    status_class           LowCardinality(String),

    request_count          UInt64,
    response_time_sum_ms   UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toDate(bucket_start)
ORDER BY
(
    service_name,
    route,
    method,
    status_class,
    bucket_start
);
```

Materialized view:

```sql
CREATE MATERIALIZED VIEW http_service_metrics_5m_mv
TO http_service_metrics_5m
AS
SELECT
    toStartOfInterval(
        bucket_start,
        INTERVAL 5 MINUTE
    ) AS bucket_start,

    service_name,
    method,
    route,
    status_class,

    sum(request_count) AS request_count,
    sum(response_time_sum_ms) AS response_time_sum_ms

FROM http_service_metrics_1m

GROUP BY
    bucket_start,
    service_name,
    method,
    route,
    status_class;
```

For richer mergeable aggregates, use `AggregatingMergeTree` and store aggregate states.

---

## 31. Materialized View Revision Caveat

ClickHouse incremental materialized views process inserted rows.

If the source receives:

```text
revision 1: count=100
revision 2: count=101
```

the rollup view may sum:

```text
100 + 101 = 201
```

unless correction semantics are explicitly handled.

Therefore:

```text
provisional updates
    -> current/revision-aware table

final-only one-minute records
    -> ClickHouse materialized rollups
```

This is a strong reason to separate provisional and final streams.

---

## 32. ClickHouse Hot and Cold Storage Strategy

Because ClickHouse is used for both hot and historical data, the architecture can remain within one query system.

```text
Recent data
    -> local/native high-performance storage

Older data
    -> lower-cost volume or object-backed storage

Long-range data
    -> lower-resolution rollup tables
```

The query service does not need to federate across different database technologies.

It only chooses:

- table,
- granularity,
- storage tier indirectly through ClickHouse.

---

## 33. Query Resolution Selection

The query service can automatically select the best table.

Example policy:

```text
range <= 6 hours
    -> 1-minute table

6 hours to 7 days
    -> 5-minute table, if available

7 to 90 days
    -> hourly table

more than 90 days
    -> daily table
```

Public API:

```http
GET /v1/metrics?
    scope=HOST&
    service=identity-api&
    host=host-7&
    start=...&
    end=...&
    step=5m
```

The caller requests a time range and desired step.

The query service decides whether to:

- dynamically group the one-minute table,
- read the five-minute table,
- read the hourly table,
- read the daily table.

The physical table choice should not be exposed to clients.

---

## 34. Kafka Output Topics

Recommended topics:

```text
http-host-minute-provisional
http-host-minute-final

http-service-minute-provisional
http-service-minute-final

http-host-5m-final
http-host-hourly-final
http-host-daily-final

http-service-5m-final
http-service-hourly-final
http-service-daily-final
```

Not all topics are required initially.

A simpler first version may use:

```text
http-host-minute-final
http-service-minute-final
```

and let ClickHouse aggregate at query time.

Add the larger rollup topics only when needed.

---

## 35. Storage Sink Pattern

Avoid direct synchronous dual writes from Flink to multiple storage targets.

Preferred:

```text
Flink
  |
  v
Kafka aggregate topic
  |
  +--> ClickHouse sink consumer
  |
  +--> another consumer if required
```

This gives:

- replay,
- independent sink recovery,
- no cross-database transaction,
- easier rebuilds,
- independent scaling.

Since ClickHouse is both the hot and historical store, the same sink can write:

- one-minute base tables,
- historical tables,
- or final aggregate tables.

ClickHouse TTLs and storage policies handle lifecycle management.

---

## 36. Recommended Initial Implementation

Start with:

```text
Raw HTTP events
      |
      v
Flink deduplication
      |
      v
Dimension expansion
      |
      v
Host-level 1-minute final aggregation
      |
      v
Kafka: http-host-minute-final
      |
      +--> ClickHouse http_host_metrics_1m
      |
      v
Flink host-to-service rollup
      |
      v
Kafka: http-service-minute-final
      |
      +--> ClickHouse http_service_metrics_1m
```

Use ClickHouse query-time aggregation for:

- last 5 minutes,
- last hour,
- last 24 hours,
- most seven-day queries.

Then add:

```text
5-minute tables
hourly tables
daily tables
```

only after measuring actual query latency and CPU cost.

---

## 37. Recommended Evolution

### Phase 1

```text
One-minute final host metrics
One-minute final service metrics
ClickHouse query-time aggregation
```

### Phase 2

Add ClickHouse materialized views for common:

```text
5-minute
hourly
daily
```

rollups.

### Phase 3

Add Flink rollup jobs only when:

- rollups are needed outside ClickHouse,
- event-time correction logic must be centralized,
- multiple downstream systems need the exact same rollup stream,
- ClickHouse materialized views no longer meet operational needs.

---

## 38. Key Design Principles

### Key by complete metric-series identity

```text
scope
service
optional host
selected dimensions
```

The window operator adds time scope.

### Use one generic schema

Every selected single or composite dimension becomes a distinct metric series.

A generic rollup operator can process all of them.

### Materialize only useful combinations

Do not generate every theoretically possible label combination.

### Preserve mergeable state

Store:

```text
count
sum
min
max
histogram or sketch
```

rather than only averages and percentiles.

### Separate host and service scopes

Host metrics support debugging.

Service metrics support dashboards and SLOs.

### Treat revisions explicitly

Prefer final-only records for historical storage and materialized rollups.

### Query-time aggregation first

ClickHouse can efficiently aggregate one-minute records for short and medium ranges.

### Precompute only when measured demand justifies it

Use ClickHouse materialized views for query acceleration and Flink rollups for reusable stream products.

---

## 39. Final Architecture

```text
Application HTTP Requests
          |
          v
Metrics Collector / Ingestion
          |
          v
Kafka: raw-http-events
          |
          v
Flink Deduplication
keyBy(event_id)
          |
          v
Dimension Expansion
flatMap selected views
          |
          v
Flink Host 1-Minute Aggregation
keyBy(service, host, dimensions)
          |
          v
Kafka: http-host-minute-final
          |
          +--> ClickHouse Host 1m Table
          |
          v
Flink Host-to-Service Rollup
keyBy(service, dimensions)
          |
          v
Kafka: http-service-minute-final
          |
          +--> ClickHouse Service 1m Table
          |
          +--> optional Flink time rollups
          |
          +--> optional ClickHouse materialized views
                      |
                      v
            ClickHouse 5m / 1h / 1d Tables
                      |
                      v
                Metrics Query Service
                      |
                      v
             Dashboards / Alerts / APIs
```

---

## 40. Interview Summary

> Each completed HTTP request is emitted as a normalized event containing the service, host, normalized route, method, status, response time, and event timestamp. The raw Kafka topic is partitioned for balanced ingestion, while Flink first deduplicates by event ID.
>
> After deduplication, the job expands each request into a bounded set of explicitly configured metric-series contributions. Each single or composite dimension—such as method, status class, route plus method, or route plus method plus status—defines a separate metric series. The host-level Flink aggregation keys by the full series identity and applies a one-minute event-time window.
>
> Host-level minute aggregates are published to Kafka and written to ClickHouse. A second Flink stage removes the host dimension, keys by service and the remaining dimensions, and merges all host aggregates for the same minute into service-level aggregates. Counts, sums, and histogram buckets are additive; minima and maxima are merged with `MIN` and `MAX`; averages are derived from total sum divided by total count.
>
> ClickHouse stores both the seven-day hot data and historical metrics. Recent five-minute and one-hour queries can be calculated directly from one-minute rows using SQL grouping. Precomputed five-minute, hourly, and daily rollups are added only when query volume or latency justifies them. ClickHouse materialized views are suitable when final append-only minute records are used, while Flink rollups are preferable when the rollups must also be consumed by other systems or require centralized event-time correction semantics.
>
> The system should avoid uncontrolled metric cardinality, use normalized routes and bounded status dimensions, retain mergeable latency histograms, and distinguish provisional near-real-time values from final historical records.
