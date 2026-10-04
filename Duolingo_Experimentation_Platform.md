# Duolingo A/B Experimentation Platform

## 1. Problem Statement

Design an experimentation platform that allows Duolingo product teams to define, run, and measure A/B experiments across backend services, mobile clients, and web applications.

The system should support:

- experiment creation and configuration
- control and multiple treatment variants
- user eligibility rules
- deterministic user assignment
- user-level overrides
- gradual traffic ramp-up
- consistent assignment across requests and devices
- exposure logging
- experiment measurement
- safe rollout and rollback
- large-scale concurrent experiments

A key requirement is that a user should generally see the same variant consistently during the life of an experiment.

---

# 2. High-Level Product Model

A product service evaluates an experiment by sending:

```text
experiment_key
user_id
user attributes
```

Example:

```json
{
  "experiment_key": "lesson_redesign_v1",
  "user_id": "user_123",
  "attributes": {
    "country": "US",
    "platform": "IOS",
    "app_version": "10.4"
  }
}
```

The experimentation platform determines:

```text
1. Is this experiment active?
2. Is this user eligible?
3. Is there an override for this user?
4. Which bucket does this user belong to?
5. Which variant owns that bucket?
6. What configuration should be returned?
```

Example response:

```json
{
  "experiment": "lesson_redesign_v1",
  "variant": "TREATMENT_A",
  "config": {
    "lesson_layout": "new",
    "animation_enabled": true
  }
}
```

---

# 3. Core Concept: Eligibility vs Bucketing

This distinction is fundamental.

## 3.1 Eligibility

Eligibility determines:

> Should this user participate in the experiment at all?

Eligibility may depend on:

```text
country
age
platform
app version
subscription status
language
product surface
account type
```

Example:

```text
country = US
AND
platform = IOS
AND
app_version >= 10.0
```

Users who do not match the eligibility rules do not participate.

---

## 3.2 Bucketing

Bucketing determines:

> Which variant should an eligible user receive?

Bucketing should generally be randomized and deterministic.

For example:

```text
Eligible users
      |
      v
Deterministic hash
      |
  +---+---+
  |       |
Control Treatment
```

Do not normally use attributes such as age or geography directly to decide control versus treatment.

For example, this would be bad experimental design:

```text
age < 30  -> treatment
age >= 30 -> control
```

because age becomes correlated with the treatment itself.

Instead:

```text
Eligibility:
age >= 18
country = US

Assignment:
50% control
50% treatment
```

---

# 4. Experiment Definition

A basic experiment model might be:

```text
Experiment

experiment_id
experiment_key
status
version
start_time
end_time

eligibility_rules

variants

traffic_allocation

owner_team

created_at
updated_at
```

Example:

```text
experiment_key:
lesson_redesign_v1

status:
ACTIVE

eligibility:
country = US
platform = IOS
app_version >= 10.0

variants:
CONTROL
TREATMENT_A

allocation:
CONTROL      50%
TREATMENT_A  50%
```

Each variant may carry configuration.

Example:

```text
CONTROL
{
    lesson_layout: "classic"
}

TREATMENT_A
{
    lesson_layout: "new",
    animation_enabled: true
}
```

This makes the platform more flexible than returning only Boolean feature flags.

---

# 5. Deterministic Bucketing

A simple assignment strategy is:

```text
bucket =
hash(experiment_id + user_id) % 10000
```

This gives 10,000 stable buckets.

Example:

```text
0–4999      CONTROL
5000–9999   TREATMENT_A
```

If Alice hashes to:

```text
7234
```

she always receives:

```text
TREATMENT_A
```

for that experiment.

This gives deterministic assignment without storing one row per user.

---

# 6. Why Deterministic Assignment Matters

Do not use a random number on every request.

Bad:

```text
request 1 -> CONTROL
request 2 -> TREATMENT
request 3 -> CONTROL
```

That creates inconsistent experiences and contaminates experiment results.

Instead:

```text
same user
+
same experiment
=
same bucket
=
same variant
```

This is often referred to as sticky assignment.

---

# 7. User Overrides

The platform should support explicit overrides:

```text
experiment_id
user_id
forced_variant
```

Example:

```text
user_123 -> TREATMENT_A
user_456 -> CONTROL
```

Overrides are useful for:

- QA
- internal employees
- demos
- debugging
- incident reproduction

Evaluation order might be:

```text
Experiment active?
      |
      v
Eligible?
      |
      v
Explicit override?
   /       \
 yes       no
  |         |
  v         v
override   deterministic bucketing
```

Overrides should generally not become the primary mechanism for product behavior because they distort randomized populations.

---

# 8. High-Level Architecture

```text
                  Product Teams
                        |
                        |
                 Experiment Config
                        |
                        v
              Experiment Control Plane
                        |
                        v
               Configuration Store
                        |
                        v
             Config Distribution Layer
                        |
          +-------------+-------------+
          |                           |
          v                           v
 Backend SDK / Cache            Mobile/Web SDK
          |                           |
          v                           v
     Local Evaluation             Evaluation
          |
          v
      Variant
```

Separately:

```text
Application
    |
    | feature actually shown
    v
Exposure Event
    |
    v
Event Pipeline
    |
    v
Analytics / Experiment Measurement
```

The platform has three major areas:

```text
Configuration
Assignment
Measurement
```

---

# 9. Control Plane vs Data Plane

This is an important architectural distinction.

## 9.1 Control Plane

Responsible for:

- creating experiments
- changing eligibility rules
- defining variants
- configuring allocation percentages
- activating/stopping experiments
- user overrides
- ownership and permissions

These operations are relatively low volume.

Example:

```text
Product Manager / Engineer
            |
            v
    Experiment Admin API
            |
            v
     Experiment Database
```

---

## 9.2 Data Plane

Responsible for runtime evaluation.

This path may execute millions or billions of times per day.

Example:

```text
Product Service
      |
      v
Evaluate experiment
      |
      v
Return variant
```

This must be extremely fast and highly available.

---

# 10. Avoiding a Remote Call for Every Evaluation

A naive design is:

```text
Product Service
      |
      v
Experimentation Service
      |
      v
Product Service
```

If a page evaluates 20 experiments, this can cause:

```text
20 network calls
```

This adds latency and creates a critical dependency.

A better design is:

```text
             Control Plane
                  |
                  |
          Config Distribution
                  |
                  v
             Local Cache
                  |
                  v
Product Service -> Experiment SDK
                  |
                  v
            Local Evaluation
```

The central platform distributes experiment definitions.

Each service or client evaluates locally.

Advantages:

- low latency
- less network traffic
- higher availability
- experimentation service not on every request path

---

# 11. Configuration Distribution

Experiment configuration should be pushed or periodically refreshed.

Possible approaches:

```text
polling
streaming
pub/sub
configuration cache
CDN for clients
```

Backend services may hold experiment configuration in memory.

Example:

```text
Experiment SDK
    |
    +--> in-memory configuration
    |
    +--> refresh periodically
```

Mobile clients may receive a compact configuration bundle.

The system should version configuration so services know which version they evaluated.

---

# 12. Evaluation Flow

A typical evaluation flow:

```text
evaluate(
    experiment_key,
    user_id,
    attributes
)
```

Steps:

```text
1. Load experiment config
2. Check experiment status
3. Check eligibility
4. Check explicit override
5. Compute deterministic bucket
6. Map bucket to variant
7. Return variant + config
```

Example:

```text
experiment:
lesson_redesign_v1

user:
user_123

hash:
7234

allocation:
0–4999      CONTROL
5000–9999   TREATMENT_A

result:
TREATMENT_A
```

---

# 13. Exposure Logging

Assignment alone does not mean the user experienced the treatment.

Suppose Alice is assigned:

```text
TREATMENT_A
```

but never opens the lesson page.

She should not necessarily count as exposed.

The system therefore records an exposure when the experimental behavior is actually shown.

Example:

```json
{
  "experiment_id": "lesson_redesign_v1",
  "user_id": "user_123",
  "variant": "TREATMENT_A",
  "config_version": 17,
  "timestamp": "..."
}
```

Conceptually:

```text
Assignment
    |
    v
Feature actually shown
    |
    v
Exposure Event
```

---

# 14. Measurement

Exposure events are combined with product outcome events.

Examples:

```text
LessonCompleted
SessionStarted
SubscriptionPurchased
CourseCompleted
AppOpened
```

Analytics can compare:

```text
CONTROL

exposed users:
1,000,000

lesson completion:
71%
```

versus:

```text
TREATMENT_A

exposed users:
1,000,000

lesson completion:
76%
```

The experimentation platform therefore has a measurement pipeline:

```text
Exposure Events
       |
       +-------------------+
                           |
Outcome Events             |
       |                   |
       +---------+---------+
                 |
                 v
          Analytics Pipeline
                 |
                 v
        Experiment Results
```

---

# 15. Experiment Ramp-Up

A common rollout pattern is:

```text
1%
5%
10%
25%
50%
100%
```

The system should avoid moving users unnecessarily between variants as allocations change.

Suppose:

```text
0–8999      CONTROL
9000–9999   TREATMENT
```

This gives 10% treatment.

To increase treatment to 50%:

```text
0–4999      CONTROL
5000–9999   TREATMENT
```

Existing treatment users:

```text
9000–9999
```

remain in treatment.

New treatment users:

```text
5000–8999
```

move from control into treatment.

No treatment user moves backward.

This is a monotonic expansion.

---

# 16. Sticky Allocation

There are two broad strategies.

## Option A: Stateless Deterministic Assignment

Use:

```text
hash(experiment_id + user_id)
```

Advantages:

- no per-user assignment storage
- fast
- scalable
- deterministic
- easy to evaluate locally

Disadvantages:

- configuration changes must be carefully managed
- changing variant structure can move users

---

## Option B: Persist User Assignments

Store:

```text
ExperimentAssignment

experiment_id
user_id
variant
assigned_at

PRIMARY KEY(experiment_id, user_id)
```

Once Alice is assigned:

```text
(exp_123, alice) -> TREATMENT_A
```

she remains there.

Advantages:

- strongest stickiness
- safe across large configuration changes
- simple semantics

Disadvantages:

- potentially enormous storage
- read/write traffic
- harder to evaluate fully locally
- increased operational complexity

At Duolingo scale, this may mean billions of assignment rows.

---

# 17. Cache vs Persistent Assignment

A cache is not enough if stable assignment depends on stored state.

Suppose:

```text
Alice -> TREATMENT_A
```

is only stored in Redis.

If that cache entry expires and experiment rules have changed, recomputation may move Alice.

Therefore:

```text
cache != authoritative persistence
```

If assignments must survive configuration changes, they need durable storage.

---

# 18. Adding a New Treatment

This is one of the hardest lifecycle problems.

Suppose the experiment starts as:

```text
0–4999      CONTROL
5000–9999   TREATMENT_A
```

Now product wants:

```text
CONTROL
TREATMENT_A
TREATMENT_C
```

A naive rebalance:

```text
0–3332      CONTROL
3333–6665   TREATMENT_A
6666–9999   TREATMENT_C
```

moves many users.

Users in:

```text
6666–9999
```

previously saw `TREATMENT_A` but now see `TREATMENT_C`.

This contaminates the experiment.

---

# 19. Reserved Bucket Space

One solution is to leave some buckets unused.

Initial experiment:

```text
0–3999      CONTROL
4000–7999   TREATMENT_A
8000–9999   UNALLOCATED
```

Later:

```text
0–3999      CONTROL
4000–7999   TREATMENT_A
8000–9999   TREATMENT_C
```

Existing users do not move.

This is simple but requires planning.

---

# 20. Persisted Assignments for Variant Changes

Another solution:

```text
existing users
    |
    v
keep persisted variant

new users
    |
    v
evaluate using latest allocation
```

This gives maximum stability but adds state.

---

# 21. Experiment Versioning

For materially different experiment structures, the cleanest approach may be to create a new experiment:

```text
lesson_redesign_v1
lesson_redesign_v2
```

Example:

```text
v1:
CONTROL
TREATMENT_A
```

then:

```text
v2:
CONTROL
TREATMENT_A
TREATMENT_C
```

This gives a clean statistical boundary.

A useful rule is:

> Traffic percentage changes can often remain within the same experiment, but adding fundamentally new treatments may justify a new experiment version.

---

# 22. Experiment Lifecycle

Possible states:

```text
DRAFT
READY
RUNNING
PAUSED
STOPPED
COMPLETED
```

Example:

```text
DRAFT
  |
  v
READY
  |
  v
RUNNING
  |
  +--> PAUSED
  |
  v
COMPLETED
```

Experiment configuration changes should be audited.

---

# 23. Experiment Configuration Versioning

Each change should create a new immutable config version.

Example:

```text
experiment_id
config_version
allocation
eligibility
variants
created_at
```

Example:

```text
version 1:
10% treatment

version 2:
25% treatment

version 3:
50% treatment
```

Exposure events should include:

```text
config_version
```

so analytics can reconstruct exactly what configuration was active when the user was exposed.

---

# 24. Multiple Concurrent Experiments

Duolingo may run hundreds of experiments simultaneously.

Problems can arise if two experiments modify the same feature.

Example:

```text
Experiment A:
new lesson UI

Experiment B:
new lesson navigation
```

If Alice receives both treatments, the combined experience may invalidate both experiments.

The system may therefore support experiment namespaces or exclusion groups.

Example:

```text
mutual_exclusion_group:
lesson_ui
```

Then:

```text
Experiment A
Experiment B
Experiment C

        |
        v
only one active experiment per user
within lesson_ui group
```

---

# 25. Mutually Exclusive Experiments

A possible assignment scheme:

```text
hash(namespace + user_id) % 100
```

Example:

```text
0–49   experiment A
50–79  experiment B
80–99  no experiment
```

Within each experiment's population, another deterministic hash assigns variants.

This reduces experiment contamination.

---

# 26. Feature Flags vs Experiments

The platform may support both.

Feature flag:

```text
NEW_UI = true / false
```

Main purpose:

```text
release control
```

Experiment:

```text
CONTROL
TREATMENT_A
```

Main purpose:

```text
measure causal effect
```

They are related but not identical.

A mature platform may expose a unified configuration layer while maintaining different semantics.

---

# 27. Failure Handling

## Experiment service unavailable

Backend services should generally use cached configuration.

Possible fallback:

```text
cached config
```

or:

```text
CONTROL
```

depending on product risk.

---

## Stale configuration

Attach:

```text
config_version
```

and use TTL/refresh logic.

---

## Invalid experiment configuration

Validate before activation.

Examples:

```text
allocation > 100%
overlapping ranges
invalid eligibility rules
missing control
duplicate variant names
```

---

## Exposure pipeline unavailable

Feature delivery should generally continue.

Buffer exposure events asynchronously.

The user experience should not fail because analytics is unavailable.

---

# 28. Consistency Requirements

Strong global consistency is usually unnecessary.

Runtime evaluation needs:

```text
low latency
high availability
stable user assignment
```

Configuration propagation can be eventually consistent over seconds.

Example:

```text
Experiment updated
      |
      v
Control Plane
      |
      v
Config Distribution
      |
      v
Services update within seconds
```

This is acceptable for most experiments.

---

# 29. Latency Targets

Evaluation should ideally happen locally.

Example:

```text
local evaluation:
< 1 ms
```

Remote evaluation should be avoided on hot product paths.

Admin/configuration APIs have much less stringent latency requirements.

---

# 30. Data Model

## Experiment

```text
experiment_id
experiment_key
status
owner_team
start_time
end_time
current_version
```

---

## ExperimentConfig

```text
experiment_id
version
eligibility_rules
variants
bucket_ranges
mutual_exclusion_group
created_at
```

---

## Override

```text
experiment_id
user_id
variant
expires_at
```

---

## Exposure

```text
event_id
experiment_id
config_version
user_id
variant
timestamp
```

Exposure events should generally flow through an event pipeline rather than a transactional OLTP table.

---

## Optional Assignment

```text
experiment_id
user_id
variant
assigned_at
```

Use only if persistent assignment is required.

---

# 31. Exposure Event Pipeline

```text
Application
     |
     v
Exposure Event
     |
     v
Kafka
     |
     v
Analytics Pipeline
     |
     v
Data Warehouse
     |
     v
Experiment Results
```

Exposure logging should be idempotent where possible.

An exposure event may include:

```text
exposure_id
user_id
experiment_id
variant
timestamp
```

Duplicate exposure events should not artificially inflate user counts.

---

# 32. Result Computation

Metrics may include:

```text
lesson completion rate
daily active usage
retention
subscription conversion
session duration
XP earned
course completion
```

Experiment results should compare:

```text
CONTROL
vs
TREATMENT
```

among appropriately exposed users.

Statistical testing is downstream from assignment and exposure infrastructure.

---

# 33. Guardrail Metrics

Experiments should not optimize one metric while harming the product elsewhere.

Example:

```text
Primary metric:
lesson completion +5%

Guardrails:
app crashes
latency
unsubscribe rate
retention
revenue
```

The platform may automatically stop or alert on severe regressions.

---

# 34. Access and Governance

At large scale, many teams will create experiments.

The platform should support:

```text
ownership
RBAC
audit logs
approval workflows
naming conventions
experiment namespaces
automatic expiration
```

Example:

```text
owner_team = Growth
```

Only authorized users should modify that experiment.

---

# 35. Safe Rollout

A common lifecycle:

```text
internal users
    |
    v
1%
    |
    v
5%
    |
    v
10%
    |
    v
25%
    |
    v
50%
    |
    v
100%
```

At each stage, monitor:

```text
errors
latency
business metrics
guardrail metrics
```

Rollback should be fast.

For high-risk changes:

```text
treatment traffic -> 0%
```

without requiring a code deployment.

---

# 36. End-to-End Example

Product team creates:

```text
lesson_redesign_v1
```

Eligibility:

```text
country = US
platform = IOS
app_version >= 10.0
```

Variants:

```text
CONTROL
TREATMENT_A
```

Initial allocation:

```text
90% CONTROL
10% TREATMENT_A
```

Alice requests the lesson page.

```text
user_id = Alice
```

Evaluation:

```text
1. Experiment active
2. Alice eligible
3. No override
4. hash(experiment + Alice) = bucket 9342
5. bucket 9342 belongs to TREATMENT_A
```

Response:

```text
TREATMENT_A
```

Alice sees the new lesson UI.

An exposure event is generated:

```text
Alice
lesson_redesign_v1
TREATMENT_A
config_version = 1
```

Later Alice completes a lesson.

Analytics eventually joins:

```text
Exposure
+
LessonCompleted
```

and computes treatment metrics.

Later the experiment ramps to 50%.

Bucket ranges expand monotonically:

```text
before:
0–8999      CONTROL
9000–9999   TREATMENT_A

after:
0–4999      CONTROL
5000–9999   TREATMENT_A
```

Alice remains in `TREATMENT_A`.

---

# 37. Final Architecture

```text
                     PRODUCT TEAMS
                           |
                           v
                Experiment Admin APIs
                           |
                           v
                 Experiment Control Plane
                           |
                           v
                  Configuration Database
                           |
                           v
                Config Distribution Layer
                      /            \
                     /              \
                    v                v
          Backend SDK / Cache    Mobile/Web SDK
                    |                |
                    v                v
             Local Evaluation   Local Evaluation
                    |                |
                    +-------+--------+
                            |
                            v
                     Variant / Config
                            |
                            v
                    Feature Rendered
                            |
                            v
                    Exposure Event
                            |
                            v
                          Kafka
                            |
                            v
                   Analytics Pipeline
                            |
                            v
                      Data Warehouse
                            |
                            v
                    Experiment Results
```

---

# 38. Key Design Principles

## Separate eligibility from assignment

Eligibility answers:

```text
"Should this user participate?"
```

Bucketing answers:

```text
"Which variant should this eligible user receive?"
```

---

## Use deterministic bucketing

```text
hash(experiment_id + user_id)
```

gives stable assignments without storing every user.

---

## Avoid remote evaluation on every request

Distribute configuration and evaluate locally.

---

## Assignment is not exposure

A user may be assigned but never see the feature.

Record an exposure event only when the experimental experience is actually presented.

---

## Prefer stable bucket ranges

Ramp treatment by expanding into new buckets rather than remapping existing users.

---

## Treat major variant changes carefully

Adding a new treatment may require:

```text
reserved bucket space
persistent assignments
or
a new experiment version
```

---

## Version configuration

Every exposure should be traceable to the exact experiment configuration that produced it.

---

## Separate experimentation from analytics availability

Experiment evaluation should continue even if the measurement pipeline is temporarily unavailable.

---

# 39. Interview Summary

A concise interview answer could be:

> I would split the experimentation platform into a control plane and a runtime data plane. Product teams use the control plane to define experiments, eligibility rules, variants, traffic allocations, overrides, and lifecycle state. Experiment configuration is then distributed to backend and client SDKs so runtime evaluation can happen locally rather than requiring a remote service call on every request.
>
> At evaluation time, the SDK first checks whether the user is eligible, then applies any explicit override, and otherwise deterministically hashes the experiment ID and user ID into a fixed bucket space. Bucket ranges map users to control or treatment variants, giving stable assignment across requests and devices.
>
> Assignment and exposure are separate concepts. When the experimental feature is actually shown, the application emits an exposure event containing the experiment, variant, user, and config version. Exposure events and downstream product events flow into the analytics pipeline so control and treatment outcomes can be compared.
>
> For traffic ramping, I would preserve existing bucket ranges and expand treatment monotonically so users do not move backward between variants. If we materially change the experiment—for example by adding a new treatment to a fully allocated experiment—I would either use reserved bucket space, persistent assignments, or create a new experiment version rather than silently remapping existing users.