# How Elasticsearch Indexes Log Messages and Stack Traces

## 1. Overview

Whether Elasticsearch indexes every word in a log entry depends entirely on the field mapping.

For each field, the mapping determines whether Elasticsearch:

- analyzes the content into searchable word-like tokens,
- indexes the whole value as one exact term,
- builds a pattern-oriented index,
- or stores the field without indexing it.

Elasticsearch does not indiscriminately index every character or line in every log entry.

---

## 2. When `message` Is Mapped as `text`

A `text` field is passed through an analyzer.

The default standard analyzer generally:

- splits text at Unicode word boundaries,
- removes most punctuation,
- lowercases the resulting terms,
- retains stop words by default.

Those analyzed terms are stored in Lucene's inverted index.

Example log message:

```text
Failed to load user_id=ABC123 after 3 attempts
```

The analyzer may produce tokens similar to:

```text
failed
to
load
user_id
abc123
after
3
attempts
```

The exact tokens depend on the configured analyzer.

Conceptually, the inverted index looks like:

```text
term        documents containing term
----------------------------------------
failed      log-17, log-92, log-105
abc123      log-17, log-88
attempts    log-17, log-240
```

Elasticsearch stores token-derived terms rather than necessarily preserving each original word exactly as written.

A normal `text` field may also store:

- document numbers,
- term frequency,
- token positions.

That supports phrase and proximity queries.

---

## 3. Exception Stack Traces

If an exception stack trace is mapped as `text`, Elasticsearch analyzes the whole stack-trace string into searchable terms.

Example:

```text
java.lang.NullPointerException: User profile was null
    at com.acme.identity.UserService.load(UserService.java:42)
```

Searchable terms may include:

- exception names,
- package names,
- class names,
- method names,
- file names,
- error-message words,
- line numbers, depending on tokenization.

Elasticsearch does not create a special search document for every stack frame. It analyzes the stack trace as the value of one field.

---

## 4. Stack Traces Often Need More Than Word Search

Operational users may want grep-like searches such as:

```text
*NullPointerException*
com.acme.identity.*
UserService.java:42
```

A useful stack-trace mapping can support both:

- word-based text search,
- wildcard or regular-expression search.

A practical field model is:

```text
error.type              keyword
error.message           match_only_text
error.stack_trace       wildcard
error.stack_trace.text  match_only_text
```

This supports:

- exact exception-type filtering,
- word-based stack-trace search,
- wildcard and pattern queries.

---

## 5. `match_only_text` for Logs

For logs, relevance scoring is often less important than simply finding matching records.

`match_only_text` is a space-efficient alternative to normal `text`.

It:

- analyzes the content into searchable terms,
- stores document membership,
- omits some scoring and positional information,
- is well suited to log messages.

Phrase searches may still work, but can require additional source verification and may be slower.

Example mapping:

```json
{
  "mappings": {
    "properties": {
      "message": {
        "type": "match_only_text"
      },
      "error.type": {
        "type": "keyword"
      },
      "error.message": {
        "type": "match_only_text"
      },
      "error.stack_trace": {
        "type": "wildcard",
        "fields": {
          "text": {
            "type": "match_only_text"
          }
        }
      }
    }
  }
}
```

---

## 6. `keyword` Fields Do Not Index Individual Words

A `keyword` field treats the entire value as one exact term.

For example:

```text
service.name = identity-api
```

is indexed as:

```text
identity-api
```

It is not split into:

```text
identity
api
```

Use `keyword` for:

- service names,
- hostnames,
- user IDs,
- trace IDs,
- request IDs,
- log levels,
- exception types,
- HTTP status codes.

Use `text` or `match_only_text` for content whose individual words must be searchable.

---

## 7. Structured Fields Are Better Than Searching the Raw Message

Suppose a log is:

```json
{
  "service": {
    "name": "identity-api"
  },
  "user": {
    "id": "abc123"
  },
  "message": "Loaded profile for user_id=abc123"
}
```

Recommended mapping:

```text
service.name -> keyword
user.id      -> keyword
message      -> match_only_text
```

Then search using:

```text
service.name = identity-api
AND user.id = abc123
AND @timestamp >= now-15m
```

This is faster and more reliable than searching the message for:

```text
user_id = abc123
```

Raw message formats may vary:

```text
user_id=abc123
userId: abc123
user abc123
{"user_id":"abc123"}
```

Extracting structured identifiers at ingestion avoids repeatedly parsing text during queries.

---

## 8. Avoid Relying on Default Dynamic Mapping

With dynamic mapping, an unknown string field may be mapped as both:

```text
field         -> text
field.keyword -> keyword
```

That means the same value can be indexed twice.

This is convenient during development but expensive for high-volume logs.

For production, use:

- explicit mappings,
- dynamic templates,
- a controlled schema.

Example dynamic template:

```json
{
  "dynamic_templates": [
    {
      "ids_as_keywords": {
        "match": "*_id",
        "match_mapping_type": "string",
        "mapping": {
          "type": "keyword"
        }
      }
    }
  ]
}
```

Critical fields should still be mapped explicitly.

---

## 9. Fields Can Be Stored Without Being Indexed

A field may be retained in the document but excluded from the search index.

Example:

```json
{
  "event.original": {
    "type": "keyword",
    "index": false,
    "doc_values": false
  }
}
```

This is useful for the complete raw log line when the system needs it for:

- display,
- audit,
- archival,
- debugging,

but does not need to search it directly.

Disabling indexing reduces:

- index size,
- ingestion work,
- merge cost.

---

## 10. Recommended Mapping for a Log Monitoring System

```json
{
  "mappings": {
    "dynamic": false,
    "properties": {
      "@timestamp": {
        "type": "date"
      },
      "event_id": {
        "type": "keyword"
      },
      "service_name": {
        "type": "keyword"
      },
      "host_name": {
        "type": "keyword"
      },
      "environment": {
        "type": "keyword"
      },
      "severity": {
        "type": "keyword"
      },
      "user_id": {
        "type": "keyword"
      },
      "trace_id": {
        "type": "keyword"
      },
      "request_id": {
        "type": "keyword"
      },
      "message": {
        "type": "match_only_text"
      },
      "error": {
        "properties": {
          "type": {
            "type": "keyword"
          },
          "message": {
            "type": "match_only_text"
          },
          "stack_trace": {
            "type": "wildcard",
            "fields": {
              "text": {
                "type": "match_only_text"
              }
            }
          }
        }
      },
      "event_original": {
        "type": "keyword",
        "index": false,
        "doc_values": false
      }
    }
  }
}
```

---

## 11. Field-Type Summary

```text
message / error message:
analyzed into searchable terms

stack trace:
word search plus optional wildcard/pattern search

IDs, service, host, severity, exception class:
exact keyword terms

raw original event:
stored but optionally not indexed
```

---

## 12. Practical Design Guidance

### Use `keyword` for exact identifiers

Examples:

```text
service_name
host_name
user_id
trace_id
request_id
severity
error.type
status_code
```

### Use `match_only_text` for ordinary log text

Examples:

```text
message
error.message
stack-trace text search
```

### Use `wildcard` selectively

Examples:

```text
stack traces
machine-generated paths
package/class patterns
grep-like queries
```

Wildcard indexing has additional storage and ingestion cost, so apply it only where the query behavior is needed.

### Store but do not index raw content

Examples:

```text
event.original
raw provider payload
complete original log line
```

---

## 13. Bottom Line

Elasticsearch can index word-like tokens from the full log message and exception stack trace when those fields are mapped using text-family field types.

It does not have to index them this way.

A deliberate production design should choose:

```text
structured exact fields
    -> keyword

free-form messages
    -> match_only_text or text

grep-like machine-generated content
    -> wildcard plus optional text subfield

raw original event
    -> stored, but potentially not indexed
```

The most important optimization is to extract commonly searched identifiers such as `user_id`, `trace_id`, and `request_id` during ingestion and index them as exact keyword fields, rather than relying on full-text search of the raw log message.
