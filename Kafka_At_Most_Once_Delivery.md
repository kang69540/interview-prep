# Kafka At-Most-Once Delivery

Kafka achieves **at-most-once consumption** by committing a message's offset **before** processing the message.

The core rule is:

> Record progress first, then perform the work.

If the consumer crashes after committing the offset but before finishing the work, Kafka considers the message consumed and will not deliver it again. The application can lose the message, but it does not process the message twice.

## Example

A consumer receives a record from partition 3 at offset 42:

```text
1. Consumer receives offset 42.
2. Consumer commits offset 43.
3. Consumer processes offset 42.
```

Kafka commits offset `43` because a committed offset identifies the **next record to consume**.

If the consumer crashes between steps 2 and 3:

```text
Committed offset: 43
Processed offset 42: No
```

When another consumer takes ownership of the partition, it starts at offset 43. Kafka does not redeliver offset 42.

## Consumer pseudocode

```python
while True:
    records = consumer.poll()

    # Commit the next offsets before performing the work.
    consumer.commit_sync(offsets_after(records))

    for record in records:
        process(record)
```

If processing fails after the commit, the application does not retry the records through Kafka.

## Batch-loss risk

Suppose a poll returns offsets 100 through 199:

```text
1. Commit offset 200.
2. Process records 100 through 199.
```

If the consumer crashes after processing offset 120, offsets 121 through 199 are skipped after restart.

Committing each record immediately before processing reduces the potential loss window:

```text
commit 101
process 100

commit 102
process 101
```

However, this produces substantially more offset-commit traffic. Most implementations either accept batch-level loss or use smaller batches.

## Comparison with other delivery semantics

| Semantic | Operation order | Crash consequence |
| --- | --- | --- |
| At-most-once | Commit, then process | Message can be lost |
| At-least-once | Process, then commit | Message can be processed more than once |
| Exactly-once | Commit processing and output atomically | Neither duplicated nor lost within the transaction boundary |

For at-least-once delivery:

```text
1. Process offset 42.
2. Commit offset 43.
```

If the consumer crashes between those operations, Kafka delivers offset 42 again.

## Producer-side behavior

Consumer offset ordering controls only consumption. End-to-end at-most-once semantics must also account for producer retries.

A basic at-most-once producer can disable retries:

```properties
retries=0
```

If the broker receives a message but the acknowledgement is lost, the producer does not retry. This avoids a possible duplicate but can lose the message.

A better option is usually Kafka's idempotent producer:

```properties
enable.idempotence=true
acks=all
```

Kafka uses producer IDs and sequence numbers to recognize retry duplicates. This allows the producer to retry safely without appending the same record more than once during the supported producer session.

An end-to-end design can therefore combine:

- Idempotent production to prevent duplicate Kafka records.
- Offset commits before processing to prevent consumer reprocessing.

## Auto-commit caution

Using the following configuration does not automatically provide well-controlled at-most-once behavior:

```properties
enable.auto.commit=true
```

Offsets are committed periodically, making their relationship to application processing more difficult to reason about.

For explicit at-most-once behavior, use:

```properties
enable.auto.commit=false
```

Then manually commit the appropriate offsets before processing.

## When to use at-most-once delivery

At-most-once is reasonable when occasional message loss is acceptable but duplicate processing is undesirable:

- Sampled telemetry.
- Noncritical analytics.
- High-frequency metrics.
- Disposable cache-refresh signals.
- Best-effort presence updates.

Avoid at-most-once for:

- Payments.
- Financial ledger entries.
- Orders.
- Security audit logs.
- Inventory changes.
- Legally required notifications.

For most business systems, **at-least-once delivery with idempotent processing** is safer. Duplicate messages can usually be detected and suppressed, whereas a lost message may be impossible to recover.
