# News Aggregator Feed Fanout System Design

## 1. Problem Statement

Design a news aggregator in which publishers submit newly published articles through webhook calls.

Users may subscribe to:

- specific publishers,
- specific topics,
- or both.

When a new article arrives, the system should make it appear in the feeds of relevant users.

The system should support:

- webhook retries,
- duplicate article delivery,
- topic classification,
- users matching an article through multiple subscriptions,
- high-fanout publishers and topics,
- scalable feed generation,
- low-latency feed reads,
- subscription and unsubscription,
- backfilling recent content after a new subscription,
- ranking and pagination.

---

## 2. Key Design Principle

Do not synchronously update every matching user's feed inside the webhook request.

Instead:

```text
Publisher
   |
   | webhook
   v
Webhook API
   |
   v
Article DB
   |
   | ArticlePublished
   v
Kafka
   |
   +-----------------------+
   |                       |
   v                       v
Topic Classifier      Feed Fanout Workers
   |                       |
   v                       v
Article Topics       User Feed Store
                           |
                           v
                      Feed API
```

The webhook path should do only relatively inexpensive work:

```text
authenticate publisher
validate request
deduplicate article
persist article
write outbox event
return success
```

The asynchronous pipeline performs topic classification, subscriber lookup, and feed generation.

---

## 3. Webhook Ingestion

Example webhook:

```http
POST /webhooks/articles
```

```json
{
  "publisher_id": "cnn",
  "article_id": "cnn-98765",
  "title": "Federal Reserve cuts rates",
  "url": "https://example.com/article",
  "published_at": "2026-10-23T20:05:00Z",
  "topics": ["economy", "federal-reserve"]
}
```

The webhook service should:

1. Authenticate the publisher.
2. Validate the request.
3. Deduplicate the article.
4. Persist the article.
5. Write an `ArticlePublished` event into an outbox.
6. Return success after the database transaction commits.

Use a uniqueness constraint such as:

```sql
UNIQUE(publisher_id, external_article_id)
```

Publishers may retry webhook calls after timeouts, so the same article must not be inserted twice.

---

## 4. Transactional Outbox

The article record and publication event should be written in one database transaction.

```text
BEGIN TRANSACTION

INSERT article(...)

INSERT outbox_event(
    event_id,
    event_type = 'ARTICLE_PUBLISHED',
    payload
)

COMMIT
```

An outbox publisher later sends the event to Kafka.

This avoids the failure case:

```text
Article persisted successfully
        |
        v
Service crashes before Kafka publish
        |
        v
Article never reaches feeds
```

---

## 5. Subscription Model

Users may subscribe to publishers and topics.

### Publisher subscriptions

```sql
CREATE TABLE publisher_subscription (
    user_id       BIGINT NOT NULL,
    publisher_id  BIGINT NOT NULL,
    created_at    TIMESTAMP NOT NULL,

    PRIMARY KEY (user_id, publisher_id)
);
```

### Topic subscriptions

```sql
CREATE TABLE topic_subscription (
    user_id     BIGINT NOT NULL,
    topic_id    BIGINT NOT NULL,
    created_at  TIMESTAMP NOT NULL,

    PRIMARY KEY (user_id, topic_id)
);
```

These tables are useful for questions such as:

```text
Which publishers does Alice follow?
Which topics does Bob follow?
```

Fanout requires the reverse lookup:

```text
Which users follow CNN?
Which users follow Technology?
```

At scale, maintain subscriber indexes keyed by publisher or topic:

```text
publisher:cnn -> subscriber IDs
topic:technology -> subscriber IDs
```

A distributed key-value or wide-column store can hold sharded subscriber lists.

---

## 6. Reverse Subscriber Indexes

Avoid repeatedly running large relational joins for every article:

```sql
SELECT DISTINCT user_id
FROM subscriptions
WHERE publisher_id = ?
   OR topic_id IN (...);
```

Instead, maintain reverse indexes:

```text
publisher:cnn
    shard 0 -> user IDs
    shard 1 -> user IDs
    ...
    shard 99 -> user IDs

topic:technology
    shard 0 -> user IDs
    shard 1 -> user IDs
    ...
```

Sharding lets workers process subscriber groups in parallel.

For example:

```text
publisher CNN, shard 17
```

can be processed independently from the other subscriber shards.

---

## 7. Topic Classification

Publishers may provide topics, but the platform should normally map articles to canonical internal topics.

```text
Article
   |
   v
Topic Classifier
   |
   +--> AI
   +--> Technology
   +--> Business
```

Store classifications as:

```sql
CREATE TABLE article_topic (
    article_id          BIGINT NOT NULL,
    topic_id            BIGINT NOT NULL,
    confidence          DECIMAL(5,4) NOT NULL,
    classifier_version  VARCHAR(64) NOT NULL,

    PRIMARY KEY (article_id, topic_id)
);
```

Example:

```text
AI confidence          = 0.94 -> include
Politics confidence    = 0.21 -> exclude
```

The fanout coordinator should use canonical internal topics rather than arbitrary publisher strings.

---

## 8. Simple Fanout-on-Write

For a new article:

```text
ArticlePublished
      |
      v
Find subscribers to publisher
      |
      +--> Alice
      +--> Bob
      +--> Carol

Find subscribers to topics
      |
      +--> Bob
      +--> Dan
      +--> Erin

Union users
      |
      v
Insert article into user feeds
```

Example article:

```text
publisher = CNN
topics = [economy, politics]
```

The fanout service reads:

```text
Subscribers(CNN)
Subscribers(economy)
Subscribers(politics)
```

and writes the article into each unique matching user's materialized feed.

---

## 9. Duplicate Matches

A user can match the same article through several paths.

Example:

```text
Alice follows CNN.
Alice follows Economy.
Alice follows Politics.

Article:
publisher = CNN
topics = Economy, Politics
```

Alice matches three times but should see the article once.

The feed store must enforce:

```text
one user
+
one article
=
one feed entry
```

Use:

```sql
PRIMARY KEY (user_id, article_id)
```

or:

```sql
UNIQUE(user_id, article_id)
```

This provides business-level idempotency and also protects against fanout worker retries.

---

## 10. Feed Entry Model

A durable feed entry might be:

```sql
CREATE TABLE feed_entry (
    user_id         BIGINT NOT NULL,
    article_id      BIGINT NOT NULL,
    score           DOUBLE PRECISION NOT NULL,
    published_at    TIMESTAMP NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    source_reasons  JSONB,

    PRIMARY KEY (user_id, article_id)
);
```

Example:

```text
Alice | article A | publisher:cnn
Bob   | article A | publisher:cnn,topic:economy
Dan   | article A | topic:politics
```

`source_reasons` is useful for debugging, unsubscribe semantics, and explaining why an item appeared.

---

## 11. Redis for Recent User Feeds

A user's hot feed can be stored as a Redis sorted set:

```text
feed:user:123
```

Use:

```text
member = article_id
score  = ranking score or publication timestamp
```

Example:

```redis
ZADD feed:user:123 1792795500 article:98765
```

Read the newest 50:

```redis
ZRANGE feed:user:123 0 49 REV
```

Then hydrate article metadata from the article cache or database.

Redis should usually hold only the recent hot portion:

```text
Redis:
last 500–1000 feed candidates per user

Durable store:
longer history if required
```

If feeds are reconstructible and the product does not require permanent history, the durable per-user feed table may be optional.

---

## 12. The High-Fanout Problem

Fanout-on-write becomes expensive for huge publishers or topics.

Example:

```text
Reuters subscribers = 50 million
```

One article could cause:

```text
50 million feed writes
```

A broad topic such as `US News` may have even more subscribers.

This can create:

- massive write amplification,
- long fanout delays,
- hot subscriber-list partitions,
- increased storage,
- large retry workloads,
- difficulty recovering from partial failure.

---

## 13. Hybrid Fanout Model

Use:

```text
Small or medium subscription group
        -> fanout-on-write

Huge subscription group
        -> fanout-on-read
```

### Small or medium publisher/topic

```text
Article arrives
   |
   v
Push article into users' materialized feeds
```

### Huge publisher/topic

Maintain one source timeline:

```text
publisher:reuters:timeline

article_100
article_99
article_98
```

or:

```text
topic:technology:timeline

article_900
article_850
article_700
```

The article is written once to the source timeline rather than copied to every subscriber.

---

## 14. Feed Read with Hybrid Fanout

Suppose Alice follows:

```text
Reuters
NPR
Technology
Skiing
```

Some sources may be fanout-on-write and others fanout-on-read.

The Feed Service combines:

```text
Materialized user feed
Reuters timeline
NPR timeline
Technology timeline
Skiing timeline
```

Then:

```text
Candidate sources
       |
       v
K-way merge
       |
       v
Deduplicate by article_id
       |
       v
Rank
       |
       v
Return top K
```

Architecture:

```text
                  Materialized User Feed
                           |
                  +--------+--------+
                  |                 |
                  v                 v
            Large Publisher   Large Topic
               Timelines       Timelines
                  |                 |
                  +--------+--------+
                           |
                           v
                       K-way merge
                           |
                           v
                       Deduplicate
                           |
                           v
                         Rank
                           |
                           v
                       Return top K
```

---

## 15. Choosing Push vs Pull

Classify publishers and topics by fanout size.

Example:

```text
subscriber count < threshold
    -> fanout-on-write

subscriber count >= threshold
    -> source timeline + fanout-on-read
```

The threshold should be based on:

- average article publication rate,
- subscriber count,
- acceptable fanout latency,
- write cost,
- cache capacity,
- feed-read latency,
- source popularity.

A publisher with 5 million followers but one article per week may be treated differently from a publisher with 500,000 followers and hundreds of posts per day.

---

## 16. Complete Article Processing Pipeline

```text
Publisher Webhook
       |
       v
Webhook Service
       |
       | transaction
       v
Article DB + Outbox
       |
       v
Kafka: ArticlePublished
       |
       +--------------------------+
       |                          |
       v                          v
Topic Classification       Source Timeline Writer
       |                          |
       v                          v
Article Topics            publisher/topic timelines
       |
       v
Fanout Coordinator
       |
       +--> small publisher subscriber batches
       |
       +--> small topic subscriber batches
       |
       v
Kafka: FeedFanoutTask
       |
       v
Fanout Workers
       |
       v
User Feed Store
```

---

## 17. Fanout Task Sharding

Do not assign millions of subscribers to one worker.

For example:

```text
Article A -> 3 million eligible users
```

Break the work into bounded tasks:

```text
Task 1 -> subscriber shard 0
Task 2 -> subscriber shard 1
Task 3 -> subscriber shard 2
...
```

or:

```text
Task 1 -> users 1–10,000
Task 2 -> users 10,001–20,000
Task 3 -> users 20,001–30,000
```

This enables:

- parallel processing,
- bounded retries,
- easier monitoring,
- work stealing,
- horizontal scaling.

A task may include:

```json
{
  "task_id": "task_123",
  "article_id": "article_98765",
  "subscription_type": "PUBLISHER",
  "subscription_id": "cnn",
  "subscriber_shard": 17
}
```

---

## 18. Idempotency

There are several duplicate paths.

### 18.1 Publisher webhook retry

Solution:

```sql
UNIQUE(publisher_id, external_article_id)
```

### 18.2 Outbox or Kafka redelivery

Use an immutable `event_id`.

Consumers may track processed event IDs, or make all downstream writes naturally idempotent.

### 18.3 Fanout task retry

Each task should have a stable `task_id`.

Workers may record task completion, but the feed-entry uniqueness constraint is still the final protection.

### 18.4 User matches multiple subscriptions

Solution:

```sql
UNIQUE(user_id, article_id)
```

### 18.5 Multiple topics match the same user

Same business invariant:

```text
one user + one article = one feed entry
```

The most important idempotency boundary is the feed entry itself.

---

## 19. New Subscription Backfill

Suppose Alice subscribes to Reuters today.

The product must define whether she should see:

```text
only future Reuters articles
```

or:

```text
the most recent Reuters articles too
```

A common design is to backfill the latest 20–50 articles.

Because source timelines already exist:

```text
publisher:reuters:timeline
```

the subscription workflow can:

1. Add Alice to the Reuters subscriber index.
2. Read recent Reuters articles.
3. Insert or merge those articles into Alice's feed.
4. Deduplicate against existing feed entries.

Source timelines therefore support both fanout-on-read and efficient subscription backfill.

---

## 20. Unsubscribe Semantics

Suppose Alice unsubscribes from CNN.

Possible behaviors:

### Future-only semantics

The unsubscribe stops future fanout.

Existing articles remain until they age out.

This is simple and common.

### Strict removal

Remove or hide all CNN articles immediately.

This is more expensive.

### Source-aware filtering

Track why each article entered the feed:

```text
article A matched:
publisher:cnn
topic:technology
```

If Alice unsubscribes from CNN but still follows Technology, the article may still be valid.

The system should avoid synchronously rewriting large historical feeds during unsubscribe unless the product strictly requires it.

---

## 21. Ranking

Initially, use freshness:

```text
score = published_at
```

Later, ranking can use:

```text
score =
    freshness
  + topic affinity
  + publisher affinity
  + popularity
  + quality
  + diversity adjustments
```

Keep candidate generation and ranking conceptually separate:

```text
Subscriptions:
determine which articles are candidates

Ranking:
determines their order
```

Pipeline:

```text
Candidate articles
       |
       v
Ranking Service
       |
       v
Top K articles
```

---

## 22. Feed Read API

```http
GET /v1/users/{user_id}/feed?limit=50&cursor=...
```

Flow:

```text
Feed API
   |
   +--> read materialized user-feed candidates
   |
   +--> read timelines for high-fanout subscribed sources
   |
   v
Merge
   |
   v
Deduplicate by article_id
   |
   v
Rank
   |
   v
Hydrate article metadata
   |
   v
Return response
```

Use cursor-based pagination rather than large offsets.

A cursor may include:

```text
last_score
last_published_at
last_article_id
feed_generation
```

---

## 23. Feed Read Response

Example:

```json
{
  "articles": [
    {
      "article_id": "article_98765",
      "publisher_id": "cnn",
      "title": "Federal Reserve cuts rates",
      "published_at": "2026-10-23T20:05:00Z",
      "topics": ["economy", "federal-reserve"],
      "score": 0.982
    }
  ],
  "next_cursor": "..."
}
```

---

## 24. Failure Handling

### Webhook service fails before commit

The publisher retries.

No article record exists yet.

### Webhook service commits but response is lost

The publisher retries.

The uniqueness constraint returns the existing article.

### Outbox publisher fails

It resumes and republishes pending outbox rows.

Consumers must tolerate duplicate events.

### Topic classifier fails

Retry classification independently.

The article may remain in a pending classification state.

Publisher-only fanout can proceed if desired.

### Fanout worker crashes

Kafka redelivers the task.

Feed-entry uniqueness prevents duplicates.

### Feed store unavailable

Kafka retains fanout tasks while workers recover.

### One subscriber shard is slow

Other shards continue processing.

Monitor per-shard age and backlog.

---

## 25. Consistency Model

The feed is eventually consistent.

A practical service-level objective might be:

```text
P95 article-to-feed latency: under 5 seconds
P99 article-to-feed latency: under 30 seconds
```

For extremely large sources using fanout-on-read, the article may become visible almost immediately because only the source timeline needs to be updated.

Feed ordering need not be globally strongly consistent.

---

## 26. Storage Choices

### Article store

Use a relational or document database for canonical article metadata.

### Subscriber indexes

Use a distributed key-value or wide-column database when subscriber lists are very large.

### Materialized feed store

Possible choices:

- Redis sorted sets for hot recent feeds,
- Cassandra or DynamoDB for durable per-user feeds,
- a combination of Redis and durable storage.

### Source timelines

Use sorted collections keyed by publisher or topic:

```text
publisher:{publisher_id}:timeline
topic:{topic_id}:timeline
```

### Event stream

Kafka or another durable distributed log.

---

## 27. Example End-to-End Flow

Suppose CNN publishes an Economy article.

### Step 1: Webhook

CNN sends the article webhook.

### Step 2: Persist and deduplicate

The Webhook Service inserts the article using:

```text
UNIQUE(cnn, external_article_id)
```

and writes an `ArticlePublished` outbox event.

### Step 3: Publish event

The outbox publisher sends `ArticlePublished` to Kafka.

### Step 4: Classify topics

The Topic Classifier maps the article to:

```text
Economy
Federal Reserve
US News
```

### Step 5: Update source timelines

The article is inserted into:

```text
publisher:cnn:timeline
topic:economy:timeline
topic:federal-reserve:timeline
topic:us-news:timeline
```

### Step 6: Choose fanout strategy

Suppose:

```text
CNN             -> high fanout, pull on read
Economy         -> high fanout, pull on read
Federal Reserve -> small enough, push on write
US News         -> high fanout, pull on read
```

Only Federal Reserve subscribers receive materialized feed writes.

### Step 7: Fanout

Subscriber shards are converted into `FeedFanoutTask` messages.

Workers insert:

```text
(user_id, article_id)
```

into the feed store.

### Step 8: Feed read

When Alice opens her feed, the service combines:

- Alice's materialized feed,
- CNN's timeline,
- Economy's timeline,
- US News timeline,
- other followed-source timelines.

The service merges, deduplicates, ranks, hydrates, and returns the top articles.

---

## 28. Observability

Important metrics include:

### Webhook ingestion

```text
webhook requests/sec
webhook validation failures
duplicate webhook rate
article persistence latency
publisher authentication failures
```

### Event pipeline

```text
outbox backlog
Kafka producer errors
consumer lag
oldest unprocessed article age
```

### Classification

```text
classification latency
classification failure rate
low-confidence rate
topic distribution
```

### Fanout

```text
fanout tasks/sec
users processed/sec
feed insert failures
duplicate feed-entry rate
fanout completion latency
hot subscriber shards
```

### Feed reads

```text
feed read latency
candidate count
source timelines merged
dedupe ratio
ranking latency
cache hit rate
```

---

## 29. Abuse and Quality Controls

Publishers may accidentally or maliciously flood the platform.

Use:

- publisher-specific rate limits,
- article duplicate detection,
- content validation,
- maximum payload sizes,
- per-publisher quotas,
- reputation controls,
- moderation workflows,
- spam classification.

A publisher should not be able to cause unbounded feed writes without controls.

---

## 30. Recommended Final Architecture

```text
                         PUBLISHERS
                              |
                              v
                    Webhook Ingestion API
                 auth, validation, deduplication
                              |
                              v
                     Article DB + Outbox
                              |
                              v
                   Kafka: ArticlePublished
                    /                    \
                   /                      \
                  v                        v
         Topic Classification      Source Timeline Writer
                  |                        |
                  v                        v
           Canonical Topics     Publisher/Topic Timelines
                  |
                  v
           Fanout Coordinator
                  |
        +---------+----------+
        |                    |
        v                    v
Small/Medium Sources    High-Fanout Sources
Fanout-on-Write         Fanout-on-Read
        |                    |
        v                    |
Kafka: FeedFanoutTask        |
        |                    |
        v                    |
   Fanout Workers            |
        |                    |
        v                    |
Materialized User Feeds      |
        \                    /
         \                  /
          v                v
               Feed Service
                    |
              Merge + Dedup
                    |
                  Rank
                    |
                 Hydrate
                    |
                    v
                 Client
```

---

## 31. Key Design Principles

### Keep the webhook path small

Do not perform user fanout in the publisher-facing request.

### Persist before publishing

Use a transactional outbox so article persistence and event publication cannot diverge.

### Maintain reverse subscription indexes

Fanout requires:

```text
publisher/topic -> users
```

not only:

```text
user -> subscriptions
```

### Enforce feed-entry uniqueness

```text
UNIQUE(user_id, article_id)
```

handles:

- webhook retry,
- Kafka retry,
- task retry,
- publisher + topic overlap,
- multiple-topic overlap.

### Use hybrid fanout

```text
small fanout -> push on write
large fanout -> pull on read
```

### Maintain source timelines

They support:

- high-fanout sources,
- feed-read merging,
- new-subscription backfill,
- recovery and replay.

### Separate candidate generation from ranking

Subscriptions determine candidates; the ranking system determines order.

---

## 32. Interview Summary

A concise system-design answer could be:

> When a publisher submits an article through a webhook, I would authenticate and validate the request, deduplicate it using `(publisher_id, external_article_id)`, persist the article, and write an `ArticlePublished` event using a transactional outbox. The webhook would return after the transaction commits rather than waiting for feed generation.
>
> An asynchronous pipeline classifies the article into canonical topics, updates publisher and topic timelines, and determines the fanout strategy. For publishers and topics with manageable subscriber counts, fanout workers read sharded subscriber indexes and insert the article into each user's materialized feed. The feed store enforces uniqueness on `(user_id, article_id)`, so a user who follows both the publisher and one or more matching topics sees the article only once.
>
> For publishers or topics with tens of millions of subscribers, I would avoid write amplification by using fanout-on-read. The article is written once to the source timeline. When a user opens their feed, the Feed Service merges the materialized user feed with the timelines of high-fanout subscribed publishers and topics, deduplicates by article ID, ranks the candidates, and returns the top K.
>
> This hybrid design provides fast reads for normal subscriptions while avoiding millions of writes for celebrity publishers and very broad topics.
