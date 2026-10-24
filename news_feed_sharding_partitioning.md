# News Feed Sharding and Partitioning Design

For this news-feed system, I would **not use one universal sharding key**. Each dataset has a different dominant access pattern, so each should be partitioned according to how it is read and written.

The main rule is:

> Partition data by the entity used to answer the most common query, while explicitly handling hot publishers, hot topics, and very active users.

## 1. Partition the article store by `article_id`

The canonical article database needs to support:

```text
get article by article_id
get a batch of article IDs
deduplicate a publisher webhook
update article metadata or status
```

A reasonable sharding key is:

```text
shard = hash(article_id) % N
```

Example:

```text
Article shard 0:
article_10
article_55

Article shard 1:
article_11
article_93
```

The article record contains:

```text
Article

article_id
publisher_id
external_article_id
title
body
url
published_at
ingested_at
status
```

### Webhook deduplication

You also need to find an article by:

```text
(publisher_id, external_article_id)
```

because publishers retry webhook calls.

There are two common approaches.

### Option A: derive the internal article ID deterministically

For example:

```text
article_id = hash(publisher_id + external_article_id)
```

Then retries route to the same article shard naturally.

### Option B: separate webhook-deduplication index

```text
PublisherArticleKey

publisher_id
external_article_id
article_id

PRIMARY KEY(publisher_id, external_article_id)
```

This index can be sharded by:

```text
hash(publisher_id, external_article_id)
```

A stable internal article identity derived from the publisher’s external ID is often the simpler approach, assuming the publisher IDs are trustworthy and immutable.

---

## 2. Partition user-to-subscription data by `user_id`

This table answers:

> Which publishers and topics does this user follow?

That is needed when the user opens their feed or manages subscriptions.

Use:

```text
shard = hash(user_id) % N
```

```text
UserSubscription

user_id
subscription_type   PUBLISHER | TOPIC
subscription_id
created_at

PRIMARY KEY(user_id, subscription_type, subscription_id)
```

This makes these operations efficient:

```text
get all subscriptions for Alice
subscribe Alice to CNN
unsubscribe Alice from Technology
```

All of Alice’s subscriptions should generally live on the same logical shard.

---

## 3. Maintain a separately partitioned reverse subscription index

The fanout system needs the inverse query:

> Which users follow publisher CNN or topic Technology?

The user-sharded subscription table cannot efficiently answer that without scanning every shard.

Therefore maintain a second, denormalized representation:

```text
subscription target -> subscribers
```

For example:

```text
PublisherSubscribers

publisher_id
subscriber_shard
user_id
```

and:

```text
TopicSubscribers

topic_id
subscriber_shard
user_id
```

A logical partition key could be:

```text
(publisher_id, subscriber_shard)
```

where:

```text
subscriber_shard = hash(user_id) % S
```

For example:

```text
publisher:cnn:subscriber-shard:0
publisher:cnn:subscriber-shard:1
...
publisher:cnn:subscriber-shard:255
```

This is important because one publisher may have tens of millions of followers.

You do **not** want:

```text
publisher_id = CNN
```

to map to one physical database partition.

Instead:

```text
CNN subscribers
├── shard 0
├── shard 1
├── shard 2
...
└── shard 255
```

Each fanout task processes one subscriber shard:

```json
{
  "article_id": "article_123",
  "subscription_type": "PUBLISHER",
  "subscription_id": "cnn",
  "subscriber_shard": 17
}
```

That gives natural parallelism.

### How many subscriber shards?

Use a fixed virtual-shard count, such as:

```text
256
1024
4096
```

The number should be significantly larger than the number of physical database nodes.

Then map virtual shards to physical nodes using:

- consistent hashing,
- a shard-routing table,
- DynamoDB or Cassandra partitioning,
- another distributed key-value abstraction.

This makes future rebalancing easier.

---

## 4. Partition materialized user feeds by `user_id`

The dominant query is:

> Give me the next page of Alice’s feed.

So partition by:

```text
hash(user_id)
```

Example durable table:

```text
FeedEntry

user_id
feed_bucket
ranking_score
article_id
published_at
feed_generation
```

Possible primary key in a wide-column database:

```text
partition key:
(user_id, feed_bucket)

clustering key:
ranking_score DESC, article_id DESC
```

The `feed_bucket` prevents one very active user’s feed from growing without bound in a single partition.

For example:

```text
feed_bucket = publication_date
```

or:

```text
feed_bucket = YYYY-WW
```

Then:

```text
(user_123, 2026-W43)
(user_123, 2026-W42)
```

A feed read starts from the newest bucket and walks backward only as needed.

### Why not only `user_id`?

For most users, `user_id` alone may be fine. But for an extremely active account with years of history, one partition can become too large.

Since the product probably keeps only a bounded recent materialized feed, another option is:

```text
retain top 1,000 candidates per active user
```

Then a single user partition may remain small enough.

For Redis, a feed key can be:

```text
feed:{user_id}
```

Redis distributes keys by hash slot, but one user’s sorted set remains on one Redis shard. That is usually acceptable because reads and writes for a single user should be colocated.

---

## 5. Partition source timelines by source ID and time bucket

For high-fanout publishers and topics, maintain source timelines:

```text
publisher timeline
topic timeline
```

Typical queries are:

```text
get the newest articles from Reuters
get Technology articles newer than a watermark
```

A suitable partition key is:

```text
(source_type, source_id, time_bucket)
```

Examples:

```text
(PUBLISHER, reuters, 2026-10-23)
(TOPIC, technology, 2026-10-23)
```

Within each partition, order by:

```text
published_at DESC, article_id DESC
```

Schema:

```text
SourceTimelineEntry

source_type
source_id
time_bucket
published_at
article_id
base_score
```

Primary organization:

```text
partition key:
(source_type, source_id, time_bucket)

clustering key:
published_at DESC, article_id DESC
```

### Why add a time bucket?

A publisher like Reuters may have an enormous lifetime timeline. You do not want all of it in one unbounded partition.

Daily or weekly buckets keep partitions bounded:

```text
publisher:reuters:2026-10-23
publisher:reuters:2026-10-22
```

Feed generation reads the current bucket first and moves to older buckets only when necessary.

For low-volume sources, weekly or monthly buckets may be sufficient. For high-volume sources, daily or hourly buckets may be better.

A common adaptive strategy is:

```text
high-volume source -> hourly or daily bucket
medium source      -> daily bucket
low-volume source  -> weekly bucket
```

Adaptive bucket sizes complicate queries, so a simple daily bucket across all sources is often a good starting point.

---

## 6. Partition article-topic associations by `article_id`

The ingestion pipeline commonly asks:

> Which canonical topics were assigned to article 123?

So the primary copy should be partitioned by:

```text
article_id
```

```text
ArticleTopic

article_id
topic_id
confidence
classifier_version
```

Fanout may also need:

> Give me recent articles in topic Technology.

That access pattern is already served by the topic source timeline, so `ArticleTopic` does not need to perform both duties.

A useful distributed-data principle is:

> It is often better to maintain two purpose-built projections than to force one table to support incompatible access patterns.

---

## 7. Partition user activity data by `user_id`

For active versus inactive user segmentation:

```text
UserFeedActivity

user_id
last_feed_view_at
activity_segment
feed_watermark
feed_generation
```

Partition by:

```text
hash(user_id)
```

This is normally read:

- when the user opens the feed,
- when a fanout worker determines whether to push,
- when the activity-segmentation job updates status.

For fanout, do not perform one remote lookup per user. Use one of:

- batch reads by user IDs,
- a Redis cache keyed by user ID,
- activity segment embedded into subscriber-index records,
- periodically materialized active-subscriber lists.

---

## 8. Partition events in Kafka

Different event streams have different ordering requirements.

### `ArticlePublished`

Partition by:

```text
article_id
```

This keeps events for one article ordered:

```text
published
updated
retracted
```

Using `publisher_id` would preserve publisher-level ordering but risks a hot partition for a prolific publisher. `article_id` usually distributes load better.

### `FeedFanoutTask`

Partition by:

```text
hash(subscription_type, subscription_id, subscriber_shard)
```

or simply:

```text
task_id
```

The goal is parallelism. Global ordering across fanout shards is generally unnecessary.

### Per-user feed updates

If ordering of feed mutations for one user matters, use:

```text
user_id
```

as the Kafka key:

```text
UserFeedUpdate
key = user_id
```

Then all feed mutations for Alice land in one Kafka partition and are processed in order.

---

## 9. Avoid hot publisher and topic partitions

The largest sharding risk is a source such as:

```text
Reuters
CNN
Technology
World News
```

If every subscriber record or fanout operation is keyed only by `source_id`, one partition becomes overloaded.

Use composite sharding:

```text
partition key =
(source_id, hash(user_id) % subscriber_shard_count)
```

For example:

```text
topic:technology:0
topic:technology:1
...
topic:technology:1023
```

The fanout coordinator can create one task per subscriber shard:

```text
ArticlePublished
      |
      v
Technology has 1,024 subscriber shards
      |
      v
Create 1,024 fanout tasks
      |
      v
Workers process in parallel
```

For very small sources, empty shards can simply be skipped.

---

## 10. Avoid hot user-feed shards

Partitioning by user distributes users well, but a few especially active users or automated accounts might receive many feed updates.

Mitigations include:

- cap materialized candidates per user,
- compact repeated source updates,
- batch several feed insertions for one user,
- use source-timeline pull for extremely high-volume subscriptions,
- avoid writing every low-quality candidate,
- bucket a user’s feed by time.

A user following thousands of prolific sources may be better served with more read-time candidate generation and less fanout-on-write.

---

## 11. Keep related data together only when it helps transactions

There are two conflicting goals:

```text
co-locate data for transactions
```

and:

```text
partition data for query efficiency
```

For subscription creation, the system may need to update:

```text
user -> source subscription
source -> user subscriber index
```

Those are intentionally partitioned differently, so they cannot generally be updated in one local transaction.

Use an asynchronous projection:

```text
Subscription API
      |
      | commit authoritative user subscription
      v
UserSubscription DB
      |
      | SubscriptionCreated event
      v
Kafka / Outbox
      |
      v
Reverse Index Worker
      |
      v
Subscriber Index
```

The user-sharded subscription table is authoritative.

The source-sharded reverse index is eventually consistent.

This is preferable to performing a distributed transaction across two partitioning schemes.

---

## 12. Handling temporary subscription-index delay

Suppose Alice subscribes to CNN, but the reverse index has not yet been updated when CNN publishes an article.

Alice might miss fanout-on-write.

The source timeline provides the fallback:

```text
CNN article
   |
   v
CNN timeline always updated
```

When Alice next reads her feed, the Feed Service merges CNN’s source timeline and sees the article.

Therefore:

```text
reverse subscriber index
    = fanout optimization

source timelines
    = correctness and recovery path
```

Feed correctness does not rely entirely on every fanout write succeeding.

---

## 13. Sharding by geography or region

For a global system, add a regional layer:

```text
US users   -> US feed cluster
EU users   -> EU feed cluster
APAC users -> APAC feed cluster
```

A user can have a home region:

```text
user_id -> home_region
```

Then user-centric data is stored in that region:

- subscriptions,
- activity,
- materialized feed,
- read state.

Articles may be replicated globally or stored in a global article service.

Source timelines can be:

- regionally replicated,
- or built independently per region from global article events.

Example:

```text
Publisher webhook accepted in US
       |
       v
Global ArticlePublished stream
       |
       +--> US timeline/fanout
       +--> EU timeline/fanout
       +--> APAC timeline/fanout
```

Regionalization improves latency and isolates failures, but adds replication and data-residency complexity.

---

## 14. Suggested data layout

### Canonical article store

```text
Shard key:
hash(article_id)

Secondary uniqueness:
(publisher_id, external_article_id)
```

### User subscriptions

```text
Shard key:
hash(user_id)
```

### Reverse subscriber index

```text
Partition key:
(subscription_type, subscription_id, subscriber_shard)

subscriber_shard:
hash(user_id) % S
```

### Materialized feed

```text
Partition key:
(user_id, feed_time_bucket)

Sort:
ranking_score DESC, article_id DESC
```

### Publisher/topic timeline

```text
Partition key:
(source_type, source_id, time_bucket)

Sort:
published_at DESC, article_id DESC
```

### User activity/read state

```text
Shard key:
hash(user_id)
```

### Kafka article events

```text
Partition key:
article_id
```

### Kafka user-feed updates

```text
Partition key:
user_id
```

### Kafka fanout tasks

```text
Partition key:
hash(source_id, subscriber_shard)
```

---

## 15. Example end-to-end routing

Suppose Reuters publishes article `A123`.

### Article storage

```text
article shard =
hash(A123) % 128
```

### Source timeline

Write to:

```text
(PUBLISHER, Reuters, 2026-10-23)
```

and each classified topic timeline:

```text
(TOPIC, Economy, 2026-10-23)
(TOPIC, WorldNews, 2026-10-23)
```

### Fanout

Reuters has 1,024 subscriber shards:

```text
Reuters:0
Reuters:1
...
Reuters:1023
```

The Fanout Coordinator either:

- skips per-user fanout because Reuters is pull-based,
- or generates tasks only for active-user subscriber shards.

For a smaller topic such as:

```text
FederalReserve
```

it creates fanout tasks:

```text
article A123 + FederalReserve subscriber shard 0
article A123 + FederalReserve subscriber shard 1
...
```

### User feed

Alice maps to:

```text
user_feed_shard = hash(Alice) % 256
```

The fanout worker inserts:

```text
(Alice, current_feed_bucket, score, A123)
```

The uniqueness key prevents duplicate insertion from publisher/topic overlap.

---

## 16. Resharding strategy

Avoid tying logical shard IDs directly to physical database nodes.

Use virtual shards:

```text
virtual shard 0–4095
```

Then maintain a mapping:

```text
virtual shard -> physical node
```

When capacity is added:

```text
before:
4096 virtual shards across 16 nodes

after:
4096 virtual shards across 24 nodes
```

Move some virtual shards instead of rehashing every record.

This can be provided by:

- Cassandra token ranges,
- DynamoDB automatic partitioning,
- consistent-hash rings,
- Redis Cluster hash slots,
- an application-managed shard map.

For user-centric data, keep the routing function stable:

```text
virtual_shard = hash(user_id) % 4096
```

---

## 17. Rebalancing subscriber shards

A source may grow from 10,000 subscribers to 100 million.

Avoid changing:

```text
hash(user_id) % 16
```

to:

```text
hash(user_id) % 1024
```

in place, because almost every user would move.

Instead, use a large fixed logical shard space from the beginning:

```text
subscriber_virtual_shard = hash(user_id) % 4096
```

A small publisher may have data in only a small number of physical partitions, while a huge publisher’s 4,096 virtual shards can be distributed across many nodes.

---

## 18. Cross-shard feed reads

A normal materialized feed read should hit one user shard:

```text
user_id -> feed shard
```

The hybrid model also reads several source timelines.

Avoid querying every followed source independently over the network.

Possible optimizations:

- batch source-timeline requests,
- group sources by storage shard,
- fetch only top `N` from each,
- keep popular timelines in Redis,
- cache merged feed snapshots,
- cap the candidate count,
- prioritize sources by user affinity.

Example:

```text
Alice follows 200 sources

Do not issue 200 sequential reads.

Group them into:
source shard 1 -> 37 source timelines
source shard 2 -> 42 source timelines
...
```

The Feed Service can perform a bounded parallel scatter-gather.

---

## 19. Feed snapshot and cursor partitioning

The cursor should not expose physical shard details.

A cursor might encode:

```json
{
  "feed_generation": 8172,
  "feed_as_of": "2026-10-23T22:00:00Z",
  "last_score": 0.8421,
  "last_article_id": "A123"
}
```

The Feed Service uses this logical boundary across:

- the user’s materialized feed,
- publisher timelines,
- topic timelines.

Internally, it may maintain per-source positions, but those should normally remain server-side in a feed-session record rather than creating a very large client cursor.

For complex hybrid feeds:

```text
feed_session_id -> server-side snapshot state
```

may be simpler than embedding hundreds of source cursors into the client token.

---

## 20. Recommended Interview Answer

> I would partition each dataset based on its primary access pattern. Canonical articles are sharded by article ID. User subscriptions, activity, and materialized feeds are sharded by user ID because those are read per user. The reverse subscriber index is separately partitioned by publisher or topic plus a user-hash subshard so a source with millions of followers does not create one hot partition. Publisher and topic timelines are partitioned by source ID and time bucket, ordered by publication time.
>
> Feed fanout tasks operate on one subscriber subshard at a time, allowing a large fanout to be processed in parallel. The final feed table is partitioned by user and optionally a weekly or daily bucket, with entries sorted by ranking score. I would use fixed virtual shards rather than physical-node IDs so the system can rebalance without rehashing all data.
>
> Because the user-subscription table and the reverse subscriber index have incompatible partitioning keys, I would not use a distributed transaction. I would make the user-sharded table authoritative and update the reverse index asynchronously through an outbox event. Source timelines provide a correctness fallback when fanout or index propagation is delayed.

The most important split is:

```text
User-oriented access
    -> partition by user_id

Source-oriented access
    -> partition by publisher/topic + subscriber subshard

Time-ordered source history
    -> partition by source + time bucket

Article lookup
    -> partition by article_id
```
