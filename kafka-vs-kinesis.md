# Apache Kafka vs. Amazon Kinesis Data Streams

## Executive summary

Apache Kafka and Amazon Kinesis Data Streams both provide durable, real-time event streaming, but they optimize for different priorities:

- Choose **Apache Kafka** for portability, ecosystem depth, flexible retention, complex stream processing, and a reusable event backbone shared by many teams.
- Choose **Amazon Kinesis Data Streams** for minimal infrastructure management and close integration with AWS services.
- If the goal is Kafka without managing brokers, compare **Amazon Managed Streaming for Apache Kafka (Amazon MSK)** with Kinesis rather than comparing self-managed Kafka with Kinesis.

## Side-by-side comparison

| Area | Apache Kafka | Amazon Kinesis Data Streams |
| --- | --- | --- |
| Product type | Open-source distributed event-streaming platform | Proprietary, fully managed AWS streaming service |
| Deployment | Self-hosted, Kubernetes, cloud-managed Kafka, or Amazon MSK | AWS only |
| Scaling unit | Topic partition | Stream shard |
| Ordering | Within a partition | Within a shard; partition keys control shard placement |
| Consumption | Consumer groups and offsets | Consumer applications, commonly using the Kinesis Client Library and checkpoints |
| Retention | Configurable by time or storage; may be effectively indefinite when capacity permits | 24 hours by default; configurable up to 365 days |
| Replay | Reset consumer offsets and reread retained records | Restart from a sequence number or timestamp while records remain retained |
| Processing ecosystem | Kafka Streams, Kafka Connect, Flink, Spark, and a large connector ecosystem | Lambda, Firehose, AWS managed Flink, S3, Redshift, and other AWS services |
| Delivery semantics | At-least-once by default; transactions support exactly-once processing in supported workflows | Applications should normally expect at-least-once processing and be idempotent |
| Operations | Brokers, storage, balancing, monitoring, security, and upgrades unless using a managed provider | AWS manages the service infrastructure |
| Portability | High | Low; applications use AWS-specific APIs |
| Cost model | Compute, storage, network, support, and operational labor | Usage or provisioned-capacity charges |

## Concept mapping

The two systems use different names for similar concepts:

| Kafka | Kinesis |
| --- | --- |
| Cluster | AWS-managed regional service |
| Topic | Data stream |
| Partition | Shard |
| Record key | Partition key |
| Offset | Sequence number |
| Consumer group | Coordinated consumer application |

Both distribute keyed records across parallel ordered logs. Once a topic or stream has multiple partitions or shards, neither provides one global ordering across every record. Ordering is local to a Kafka partition or Kinesis shard.

For provisioned Kinesis streams, a shard supports up to 1 MB per second or 1,000 records per second for writes and up to 2 MB per second for reads. On-demand mode allows AWS to manage capacity automatically. Kafka does not package throughput into an equivalent fixed unit: results depend on broker resources, storage, replication, networking, message sizes, batching, acknowledgements, and partition design.

## Where Kafka is stronger

Kafka is generally a better fit when:

- Events form a long-lived organizational data backbone.
- Many independent applications or teams consume the same streams.
- Kafka Connect, Kafka Streams, or Kafka-compatible tooling is important.
- Log compaction is needed to retain the latest value for each key.
- Long or effectively indefinite retention is required.
- The platform must run outside AWS or remain portable across clouds.
- Existing applications already use Kafka clients and protocols.
- Kafka transactions or its exactly-once processing facilities are required.

Kafka's primary disadvantage is operational complexity. A production deployment requires capacity planning, partition management, monitoring, upgrades, security configuration, replication management, and failure testing. A managed Kafka service reduces this burden but does not eliminate Kafka-level architecture and tuning decisions.

## Where Kinesis is stronger

Kinesis is generally a better fit when:

- Applications and data platforms already run primarily in AWS.
- Events need to flow directly to Lambda, Firehose, S3, Redshift, or AWS-managed Apache Flink.
- The team wants a managed or serverless service without brokers.
- Traffic is intermittent or unpredictable, making on-demand capacity useful.
- A retention period between 24 hours and 365 days is sufficient.
- Native IAM, KMS, and CloudWatch integration is valuable.

Its main tradeoffs are AWS lock-in, a smaller general-purpose ecosystem than Kafka, service-specific throughput constraints, and a maximum native retention period of 365 days.

## Are Kafka and Kinesis message queues or pub-sub systems?

They are best described as **durable event streams**. They can provide pub-sub behavior and queue-like work distribution, but they are not traditional message queues.

| Model | Who receives each message? | What happens after processing? | Typical purpose |
| --- | --- | --- | --- |
| Message queue | Normally one competing worker | Message is usually acknowledged and removed | Background jobs and command distribution |
| Pub-sub | Every subscribed application | Depends on the broker | Broadcasting notifications or events |
| Event stream | Every consumer group/application; workers within a group divide the records | Record stays until its retention period expires | Event history, analytics, state changes, and replay |

### Example: an order is placed

With a traditional queue, three workers compete for one job:

```text
OrderPlaced -> one of three workers
```

With pub-sub, each subscriber receives the event:

```text
OrderPlaced -> Billing
            -> Inventory
            -> Email
```

With Kafka or Kinesis, independent applications consume the retained event stream and track their own positions:

```text
Order stream -> Billing consumer
             -> Inventory consumer
             -> Analytics consumer
```

Kafka combines both behaviors through consumer groups:

- Different consumer groups each receive the event, which resembles pub-sub.
- Consumers within one group divide the records, which resembles a work queue.
- Records remain available for replay until retention removes them, which is the distinguishing event-stream behavior.

Kinesis provides a comparable pattern through multiple consumer applications and workers assigned across shards.

## Decision guide

Choose **Kinesis Data Streams** when the workload is AWS-native, operational simplicity is the priority, and AWS integrations cover the required processing and delivery paths.

Choose **Kafka or Amazon MSK** when the stream is a central platform used by many teams, connector breadth and sophisticated processing matter, long replay windows are required, or portability justifies the added complexity.

For asynchronous commands or background jobs where exactly one worker should handle each item, a traditional queue such as **Amazon SQS** is often simpler. For facts or state changes that multiple independent applications need to consume, retain, and replay, Kafka or Kinesis is usually more suitable.

> A useful shorthand: Kinesis is an excellent AWS-native ingestion service; Kafka is a broader event-streaming platform.

## Official references

- [Apache Kafka documentation](https://kafka.apache.org/documentation/)
- [Amazon Kinesis Data Streams concepts](https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html)
- [Amazon Kinesis Data Streams retention](https://docs.aws.amazon.com/streams/latest/dev/kinesis-extended-retention.html)
- [Amazon Managed Streaming for Apache Kafka](https://aws.amazon.com/msk/)
