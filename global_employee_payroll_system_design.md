# Global Employee Payroll System Design

## 1. Core Design Principle

Payroll is not primarily a high-QPS system. It is a **batch-oriented financial system where correctness, auditability, and repeatability matter more than raw latency**.

The design should guarantee that:

1. Every payroll run evaluates an immutable snapshot of employee and compensation data.
2. Regional tax rules are versioned and effective-dated.
3. A finalized payroll result cannot be edited in place.
4. Each finalized net-pay result creates exactly one logical set of payment obligations.
5. Retries never create a second logical payment.
6. Ambiguous bank responses are reconciled before another transfer is attempted.

A useful top-level flow is:

```text
HR / Time / Benefits / Compensation Systems
                    |
                    v
             Payroll Input Store
                    |
                    v
              Payroll Scheduler
                    |
                    v
              Payroll Run Engine
           snapshot, gross-to-net
                    |
              +-----+------+
              |            |
              v            v
         Tax Engines    Deductions
              |            |
              +-----+------+
                    |
                    v
             Payroll Validation
                    |
                    v
                Approval
                    |
                    v
             Finalized Results
                    |
                    v
          Payment Obligation Ledger
                    |
                    v
            Payment Orchestrator
                    |
             +------+------+
             |             |
             v             v
          Banks        Payroll Rails
             |
             v
          Reconciliation
```

---

## 2. Requirements

### Functional Requirements

The system should support:

- Monthly, semi-monthly, bi-weekly, weekly, and custom pay frequencies.
- Employees working for different legal entities and in different jurisdictions.
- Salaried and hourly employees.
- Regular pay, overtime, bonuses, commissions, reimbursements, and benefits.
- Pre-tax and post-tax deductions.
- National, regional, state, provincial, and municipal taxes.
- Year-to-date and cumulative tax calculations.
- Multiple currencies.
- Split direct deposits.
- Off-cycle payroll.
- Retroactive adjustments.
- Payroll approval.
- Payslip generation.
- General-ledger exports.
- Tax remittances and reporting.
- Payment reconciliation.
- Full audit history.

### Non-Functional Requirements

The important guarantees are:

```text
Strong correctness for payroll calculations
Strong consistency for payroll state transitions
No duplicate logical payment obligations
Durable and auditable records
Regional data residency
Deterministic recalculation
High availability around payroll deadlines
```

Dashboards and reports may be eventually consistent. Payment state, balances, and finalized payroll data must not be.

---

## 3. Regional Architecture

For a global system, use a **global control plane with regional payroll cells**.

```text
                         Global Control Plane
             legal entities, calendars, rule catalog,
               routing, aggregate reporting, access
                              |
       +----------------------+----------------------+
       |                      |                      |
       v                      v                      v
  US Payroll Cell        EU Payroll Cell       APAC Payroll Cell
       |                      |                      |
 employee/payroll PII    employee/payroll PII  employee/payroll PII
 local tax engine        local tax engine      local tax engine
 local payment rails     local payment rails   local payment rails
```

Each employee or employment relationship has a payroll home region determined by:

- employer legal entity,
- work location,
- tax residency,
- local data-residency requirements.

The global layer should avoid storing unnecessary raw payroll or identity data. It can retain identifiers, configuration metadata, and aggregated status.

Regional cells provide:

- fault isolation,
- local data residency,
- lower latency to local payment providers,
- local tax-rule deployment,
- independent payroll deadlines.

---

## 4. Employees, Employments, and Legal Entities

Do not model payroll solely by `employee_id`.

One person may have multiple employments:

```text
Person Alice
   |
   +--> US employment with Company US
   |
   +--> part-time Canadian employment with Company Canada
```

Payroll should primarily operate on an `employment_id`.

```sql
CREATE TABLE employee (
    employee_id       UUID PRIMARY KEY,
    global_person_id  UUID NOT NULL,
    preferred_name    TEXT,
    created_at        TIMESTAMP NOT NULL
);
```

```sql
CREATE TABLE legal_entity (
    legal_entity_id       UUID PRIMARY KEY,
    country_code          CHAR(2) NOT NULL,
    default_currency      CHAR(3) NOT NULL,
    payroll_region        VARCHAR(64) NOT NULL,
    timezone              VARCHAR(64) NOT NULL
);
```

```sql
CREATE TABLE employment (
    employment_id          UUID PRIMARY KEY,
    employee_id            UUID NOT NULL REFERENCES employee(employee_id),
    legal_entity_id        UUID NOT NULL REFERENCES legal_entity(legal_entity_id),

    employment_status      VARCHAR(32) NOT NULL,
    hire_date              DATE NOT NULL,
    termination_date       DATE,

    work_country           CHAR(2) NOT NULL,
    work_region            VARCHAR(64),
    tax_residence_country  CHAR(2),
    payment_currency       CHAR(3) NOT NULL,

    pay_group_id           UUID NOT NULL,
    created_at             TIMESTAMP NOT NULL,
    updated_at             TIMESTAMP NOT NULL
);
```

Fields that change historically should be effective-dated rather than overwritten.

```sql
CREATE TABLE employment_tax_profile (
    tax_profile_id        UUID PRIMARY KEY,
    employment_id         UUID NOT NULL,
    effective_from        DATE NOT NULL,
    effective_to          DATE,

    work_country          CHAR(2) NOT NULL,
    work_region           VARCHAR(64),
    residence_country     CHAR(2),
    filing_status         VARCHAR(64),
    exemptions            JSONB,
    tax_attributes        JSONB,

    UNIQUE (employment_id, effective_from)
);
```

---

## 5. Pay Groups and Pay Frequencies

Employees with the same payroll calendar and general processing rules belong to a **pay group**.

Examples:

```text
US salaried bi-weekly
US hourly weekly
UK monthly
Germany monthly
Canada semi-monthly
```

```sql
CREATE TABLE pay_group (
    pay_group_id           UUID PRIMARY KEY,
    legal_entity_id        UUID NOT NULL,
    name                   VARCHAR(255) NOT NULL,

    frequency              VARCHAR(32) NOT NULL,
    timezone               VARCHAR(64) NOT NULL,
    payment_currency       CHAR(3) NOT NULL,

    processing_lead_days   INTEGER NOT NULL,
    calendar_rule          JSONB NOT NULL,

    CHECK (
        frequency IN (
            'WEEKLY',
            'BIWEEKLY',
            'SEMIMONTHLY',
            'MONTHLY',
            'CUSTOM'
        )
    )
);
```

The system pre-generates concrete pay periods.

```sql
CREATE TABLE pay_period (
    pay_period_id        UUID PRIMARY KEY,
    pay_group_id         UUID NOT NULL,

    period_start         DATE NOT NULL,
    period_end           DATE NOT NULL,
    input_cutoff_at      TIMESTAMP NOT NULL,
    approval_due_at      TIMESTAMP NOT NULL,
    pay_date             DATE NOT NULL,

    sequence_number      BIGINT NOT NULL,

    UNIQUE (pay_group_id, sequence_number),
    UNIQUE (pay_group_id, period_start, period_end)
);
```

The calendar generator must account for:

- weekends,
- banking holidays,
- local payroll cutoffs,
- leap years,
- varying month lengths,
- time zones.

The scheduler should never calculate “bi-weekly” dynamically at payment time. It should work from an approved, concrete `pay_period`.

---

## 6. Payroll Run State Machine

A payroll run applies to one pay group and one pay period.

```text
CREATED
   |
   v
COLLECTING_INPUTS
   |
   v
CALCULATING
   |
   v
VALIDATION_FAILED <----+
   |                   |
   | corrections       |
   +-------------------+
   |
   v
READY_FOR_APPROVAL
   |
   v
APPROVED
   |
   v
FINALIZED
   |
   v
PAYMENTS_CREATED
   |
   v
PAYMENTS_SUBMITTED
   |
   v
RECONCILED
```

```sql
CREATE TABLE payroll_run (
    payroll_run_id       UUID PRIMARY KEY,
    pay_group_id         UUID NOT NULL,
    pay_period_id        UUID NOT NULL,

    run_type             VARCHAR(32) NOT NULL,
    run_sequence         INTEGER NOT NULL,
    status               VARCHAR(32) NOT NULL,

    calculation_version  INTEGER NOT NULL DEFAULT 0,
    created_at           TIMESTAMP NOT NULL,
    finalized_at         TIMESTAMP,
    approved_by          UUID,

    UNIQUE (
        pay_group_id,
        pay_period_id,
        run_type,
        run_sequence
    )
);
```

`run_type` may be:

```text
REGULAR
OFF_CYCLE
CORRECTION
TERMINATION
BONUS
```

There should normally be one regular run per pay group and pay period. Off-cycle and correction runs use explicit sequence numbers.

---

## 7. Immutable Payroll Input Snapshots

The payroll engine should not calculate directly from live HR tables.

Otherwise:

```text
1. Payroll starts calculating Alice.
2. HR changes Alice's salary.
3. Another worker calculates Bob.
4. The run contains inconsistent input versions.
```

When the run begins, create an immutable snapshot.

```sql
CREATE TABLE payroll_employee_snapshot (
    snapshot_id           UUID PRIMARY KEY,
    payroll_run_id        UUID NOT NULL,
    employment_id         UUID NOT NULL,

    employee_data         JSONB NOT NULL,
    compensation_data     JSONB NOT NULL,
    tax_profile_data      JSONB NOT NULL,
    payment_data          JSONB NOT NULL,

    source_version        BIGINT NOT NULL,
    content_hash          VARCHAR(128) NOT NULL,

    UNIQUE (payroll_run_id, employment_id)
);
```

Inputs may include:

- base salary,
- hourly rate,
- hours worked,
- overtime,
- bonuses,
- commissions,
- benefits,
- deductions,
- tax elections,
- garnishments,
- leave,
- prior payroll balances,
- bank allocations.

Once snapshotted, the calculation operates only on that version.

If data changes, increment the calculation version and create a new snapshot or corrected input set. Do not silently change the data underneath a calculation.

---

## 8. Compensation and Payroll Inputs

Compensation definitions should be effective-dated.

```sql
CREATE TABLE compensation_component (
    component_id         UUID PRIMARY KEY,
    employment_id        UUID NOT NULL,

    component_type       VARCHAR(64) NOT NULL,
    amount_minor         BIGINT,
    rate_decimal         DECIMAL(20, 8),
    currency             CHAR(3),

    effective_from       DATE NOT NULL,
    effective_to         DATE,
    metadata             JSONB
);
```

Variable inputs might be:

```sql
CREATE TABLE payroll_input (
    payroll_input_id       UUID PRIMARY KEY,
    employment_id          UUID NOT NULL,
    pay_period_id          UUID NOT NULL,

    input_type             VARCHAR(64) NOT NULL,
    amount_minor           BIGINT,
    quantity               DECIMAL(20, 8),
    rate                   DECIMAL(20, 8),
    currency               CHAR(3),

    source_system          VARCHAR(64) NOT NULL,
    source_record_id       VARCHAR(255) NOT NULL,
    input_version          INTEGER NOT NULL,
    metadata               JSONB,

    UNIQUE (
        source_system,
        source_record_id,
        input_version
    )
);
```

Use integer minor units for finalized monetary values:

```text
USD 123.45 -> 12345
JPY 500    -> 500
```

Rates and intermediate calculations may require high-precision decimal arithmetic. Never use binary floating point for payroll money.

---

## 9. Regional Tax Engine

Tax logic should be isolated behind a stable interface:

```text
calculateTax(
    jurisdiction,
    rule_version,
    employee_snapshot,
    pay_period,
    current_earnings,
    year_to_date_balances
) -> tax lines
```

Conceptually:

```text
Payroll Calculation Engine
           |
           v
  Jurisdiction Resolver
           |
      +----+----+----------------+
      |         |                |
      v         v                v
   US Tax    UK Tax        Germany Tax
   Engine    Engine            Engine
```

### Eligibility Versus Calculation

First determine applicable jurisdictions:

```text
employer legal entity
work location
residence
employment type
pay date
tax treaty or cross-border configuration
```

Then invoke the relevant rule modules.

### Versioned Rule Definitions

```sql
CREATE TABLE tax_rule_version (
    tax_rule_version_id   UUID PRIMARY KEY,
    jurisdiction_code     VARCHAR(64) NOT NULL,

    effective_from        DATE NOT NULL,
    effective_to          DATE,

    engine_version        VARCHAR(64) NOT NULL,
    ruleset_hash          VARCHAR(128) NOT NULL,
    rule_configuration    JSONB NOT NULL,

    UNIQUE (
        jurisdiction_code,
        effective_from,
        engine_version
    )
);
```

Every payroll result should record the exact rules used.

```text
US-FEDERAL: version 2026.04
NJ-STATE: version 2026.02
NYC-LOCAL: version 2026.01
```

This allows the system to answer:

> Why was this tax amount calculated?

months or years later.

### Declarative Rules Versus Code Modules

Simple rules may be represented through a declarative DSL:

```text
taxable_base
brackets
thresholds
allowances
caps
rounding
```

Complex jurisdictions may require certified plug-ins or dedicated calculation modules.

The platform should expose the same logical interface regardless of implementation.

---

## 10. Year-to-Date and Cumulative Balances

Many payroll calculations depend on prior payments:

```text
year-to-date gross
year-to-date tax
social insurance cap usage
retirement contribution limits
cumulative taxable earnings
```

Maintain an append-only balance ledger.

```sql
CREATE TABLE payroll_balance_entry (
    balance_entry_id      UUID PRIMARY KEY,
    employment_id         UUID NOT NULL,
    jurisdiction_code     VARCHAR(64) NOT NULL,
    tax_year              INTEGER NOT NULL,
    balance_type          VARCHAR(64) NOT NULL,

    amount_minor          BIGINT NOT NULL,
    payroll_result_id     UUID NOT NULL,
    entry_type            VARCHAR(32) NOT NULL,

    created_at            TIMESTAMP NOT NULL,

    UNIQUE (
        payroll_result_id,
        jurisdiction_code,
        balance_type,
        entry_type
    )
);
```

Current balances are the sum of ledger entries.

Do not mutate a prior finalized balance in place. A correction creates an adjusting entry.

```text
Original gross:      +5,000
Correction:            -500
Corrected cumulative: 4,500
```

This preserves auditability and makes retries idempotent.

---

## 11. Gross-to-Net Calculation

The calculation pipeline for one employment may be:

```text
Base earnings
     |
     +--> variable earnings
     +--> overtime
     +--> bonuses
     +--> retro pay
     |
     v
Gross pay
     |
     +--> pre-tax deductions
     |
     v
Taxable bases by jurisdiction
     |
     +--> employee taxes
     +--> social insurance
     +--> local taxes
     |
     v
Post-tax deductions
     |
     v
Net pay
```

The output should be line-item based.

```sql
CREATE TABLE payroll_result (
    payroll_result_id       UUID PRIMARY KEY,
    payroll_run_id          UUID NOT NULL,
    employment_id           UUID NOT NULL,

    calculation_version     INTEGER NOT NULL,
    input_hash              VARCHAR(128) NOT NULL,

    gross_pay_minor         BIGINT NOT NULL,
    employee_tax_minor      BIGINT NOT NULL,
    deduction_minor         BIGINT NOT NULL,
    net_pay_minor           BIGINT NOT NULL,
    currency                CHAR(3) NOT NULL,

    status                  VARCHAR(32) NOT NULL,
    created_at              TIMESTAMP NOT NULL,
    finalized_at            TIMESTAMP,

    UNIQUE (
        payroll_run_id,
        employment_id,
        calculation_version
    )
);
```

```sql
CREATE TABLE payroll_result_line (
    payroll_result_line_id  UUID PRIMARY KEY,
    payroll_result_id       UUID NOT NULL,

    line_type               VARCHAR(32) NOT NULL,
    code                    VARCHAR(128) NOT NULL,
    jurisdiction_code       VARCHAR(64),
    amount_minor            BIGINT NOT NULL,
    currency                CHAR(3) NOT NULL,

    rule_version_id         UUID,
    source_input_id         UUID,
    metadata                JSONB
);
```

Line types include:

```text
EARNING
TAX
DEDUCTION
EMPLOYER_TAX
BENEFIT
REIMBURSEMENT
GARNISHMENT
```

---

## 12. Deterministic Calculation

Given the same:

```text
employee snapshot
payroll inputs
prior balance snapshot
tax rule versions
FX rates
calculation engine version
```

the system should produce the same result.

Store:

```text
input_hash
engine_version
rule versions
FX-rate version
rounding mode
```

This allows independent recalculation and audit.

Tax engines and payroll calculators should not depend on mutable wall-clock state. The relevant date should be passed explicitly:

```text
pay date
period start
period end
tax year
```

---

## 13. Parallel Calculation

Payroll calculation can be parallelized by employment.

```text
Payroll Run
    |
    v
Create Calculation Tasks
    |
    +--> employee shard 0
    +--> employee shard 1
    +--> employee shard 2
    ...
```

Kafka or a workflow engine may distribute tasks:

```text
PayrollCalculationTask

payroll_run_id
employment_id
calculation_version
input_snapshot_id
```

Partitioning by `employment_id` distributes load.

Workers must be idempotent:

```sql
UNIQUE (
    payroll_run_id,
    employment_id,
    calculation_version
)
```

If the task is delivered twice, both workers converge on the same result record rather than creating two results.

---

## 14. Validation and Approval

Before finalization, run validation rules such as:

- net pay is not negative unless explicitly allowed,
- gross pay deviates by more than a threshold,
- employee has no valid bank account,
- tax profile is incomplete,
- unusually high bonus,
- unexpected zero pay,
- total payroll differs significantly from prior periods,
- payroll input arrived after cutoff,
- balancing equations fail.

Validation results should be persisted:

```sql
CREATE TABLE payroll_validation (
    validation_id       UUID PRIMARY KEY,
    payroll_run_id      UUID NOT NULL,
    employment_id       UUID,

    validation_code     VARCHAR(128) NOT NULL,
    severity            VARCHAR(16) NOT NULL,
    message             TEXT NOT NULL,
    details             JSONB,

    UNIQUE (
        payroll_run_id,
        employment_id,
        validation_code
    )
);
```

Approval should require segregation of duties:

```text
person preparing payroll
!=
person approving payroll
```

All actions should be auditable.

---

## 15. Finalization

Finalization is the boundary after which results become immutable.

```text
Before FINALIZED:
calculations may be replaced by a new version

After FINALIZED:
results are immutable
corrections require adjustment runs
```

Finalization transaction:

```text
1. Verify every employment has a successful result.
2. Verify no blocking validations remain.
3. Verify approval.
4. Lock the selected calculation version.
5. Append YTD balance entries.
6. Create net-pay obligations.
7. Mark payroll run FINALIZED.
8. Write outbox events.
9. Commit.
```

These actions should occur in one regional database transaction where possible.

---

## 16. Preventing Duplicate Employee Payments

This is the most critical part of the design.

The system should not try to solve it with a Redis lock or a best-effort flag. Use multiple durable protections.

### Layer 1: One Finalized Result per Employment and Run

```sql
UNIQUE (
    payroll_run_id,
    employment_id,
    finalized_result_type
)
```

Only one payroll result version can be selected as final.

### Layer 2: One Logical Payment Set per Finalized Payroll Result

A payroll result may produce multiple transfers because of split deposit.

Example:

```text
Alice net pay: $5,000

$4,000 -> checking
$1,000 -> savings
```

Therefore the invariant is not literally “one transfer per employee.” It is:

> One immutable set of payment obligations whose total equals the finalized net pay.

```sql
CREATE TABLE payment_batch (
    payment_batch_id      UUID PRIMARY KEY,
    payroll_run_id        UUID NOT NULL,
    legal_entity_id       UUID NOT NULL,
    currency              CHAR(3) NOT NULL,
    status                VARCHAR(32) NOT NULL,

    UNIQUE (
        payroll_run_id,
        legal_entity_id,
        currency
    )
);
```

```sql
CREATE TABLE payment_obligation (
    payment_obligation_id  UUID PRIMARY KEY,
    payment_batch_id       UUID NOT NULL,
    payroll_result_id      UUID NOT NULL,

    recipient_type         VARCHAR(32) NOT NULL,
    recipient_id           UUID NOT NULL,
    destination_id         UUID NOT NULL,
    allocation_sequence    INTEGER NOT NULL,

    amount_minor           BIGINT NOT NULL,
    currency               CHAR(3) NOT NULL,

    logical_payment_key    VARCHAR(255) NOT NULL,
    status                 VARCHAR(32) NOT NULL,

    created_at             TIMESTAMP NOT NULL,
    settled_at             TIMESTAMP,

    UNIQUE (logical_payment_key),
    UNIQUE (
        payroll_result_id,
        recipient_type,
        destination_id,
        allocation_sequence
    )
);
```

A logical key might be:

```text
payroll_result_id
+
recipient
+
destination
+
allocation_sequence
```

Example:

```text
pr_123:EMPLOYEE:checking:1
pr_123:EMPLOYEE:savings:2
```

Retrying obligation creation cannot create a second set.

### Layer 3: Payment Provider Idempotency

Use the obligation ID as the external idempotency reference:

```text
provider_idempotency_key = payment_obligation_id
```

Every retry for the same logical payment uses the same key.

```http
POST /payments
Idempotency-Key: po_123
```

The provider should return the original transfer rather than initiate another one.

### Layer 4: Conditional State Transitions

Payment workers claim obligations with a compare-and-set:

```sql
UPDATE payment_obligation
SET status = 'DISPATCHING'
WHERE payment_obligation_id = :id
  AND status = 'READY';
```

If zero rows are affected, another worker already claimed or processed it.

The state machine might be:

```text
READY
  |
  v
DISPATCHING
  |
  +--> SUBMITTED
  |
  +--> UNKNOWN
  |
  +--> FAILED
  |
  v
SETTLED
```

Every transition should check the expected previous state.

### Layer 5: Payment Attempts

```sql
CREATE TABLE payment_attempt (
    payment_attempt_id       UUID PRIMARY KEY,
    payment_obligation_id    UUID NOT NULL,

    attempt_number           INTEGER NOT NULL,
    provider                 VARCHAR(64) NOT NULL,
    provider_idempotency_key VARCHAR(255) NOT NULL,
    provider_payment_id      VARCHAR(255),

    status                   VARCHAR(32) NOT NULL,
    started_at               TIMESTAMP NOT NULL,
    completed_at             TIMESTAMP,
    error_code               VARCHAR(128),

    UNIQUE (payment_obligation_id, attempt_number),
    UNIQUE (provider, provider_idempotency_key)
);
```

Retries after network timeouts should normally reuse the same provider idempotency key.

---

## 17. The Ambiguous Timeout Problem

Consider:

```text
Payment Worker                     Bank

send $5,000 ----------------------> accepted

response <------------------------- lost

worker times out
```

The worker cannot conclude that payment failed.

It must not blindly create a second transfer.

Mark the attempt:

```text
UNKNOWN
```

Then:

```text
1. Query bank/provider by idempotency key or client reference.
2. If found, record the provider payment ID.
3. If settled, mark SETTLED.
4. If confirmed rejected or nonexistent, retry according to policy.
5. If still ambiguous, escalate rather than double-pay.
```

The retry logic should be:

```text
explicit transient failure before acceptance
    -> safe retry

timeout after possible acceptance
    -> query/reconcile first

confirmed permanent failure
    -> repair payment details and create controlled new attempt
```

End-to-end “exactly once” cannot be obtained merely by wrapping the network call in a transaction. It requires durable logical identities plus provider-side idempotency and reconciliation.

---

## 18. Payment Dispatch Flow

```text
Finalized Payroll Results
          |
          v
Payment Obligation Generator
          |
          v
Payment Obligation DB
          |
          | PaymentReady outbox
          v
Payment Queue
          |
          v
Payment Workers
          |
          +--> claim with conditional update
          |
          +--> create payment attempt
          |
          +--> submit with obligation ID
          |
          v
Bank / Payment Provider
          |
          v
Webhook / Polling / Statement
          |
          v
Reconciliation Service
```

Kafka may distribute ready obligations, but Kafka is not the source of truth for whether an employee has been paid. The obligation and payment-attempt database records are.

Use the transactional outbox:

```text
DB state change
+
outbox event
=
one transaction
```

This prevents an obligation from being committed without eventually generating dispatch work.

---

## 19. Reconciliation

Never assume an accepted API request means money settled.

Reconciliation compares:

```text
expected payment obligations
```

with:

```text
provider transfers
bank settlement files
bank account statements
returned-payment files
```

```sql
CREATE TABLE payment_reconciliation (
    reconciliation_id      UUID PRIMARY KEY,
    payment_obligation_id  UUID NOT NULL,

    provider_payment_id    VARCHAR(255),
    bank_reference         VARCHAR(255),
    expected_amount_minor  BIGINT NOT NULL,
    settled_amount_minor   BIGINT,
    settlement_date        DATE,

    result                 VARCHAR(32) NOT NULL,
    details                JSONB,

    UNIQUE (
        payment_obligation_id,
        bank_reference
    )
);
```

Possible results:

```text
MATCHED
MISSING
AMOUNT_MISMATCH
DUPLICATE
RETURNED
UNKNOWN
```

A duplicate bank settlement should trigger a critical incident and recovery workflow.

---

## 20. Double-Entry Payroll Ledger

Financial movement should be backed by an immutable double-entry ledger.

Example:

```text
On payroll finalization:

Debit: Salary Expense
Credit: Payroll Payable

On payment settlement:

Debit: Payroll Payable
Credit: Cash
```

```sql
CREATE TABLE ledger_transaction (
    ledger_transaction_id  UUID PRIMARY KEY,
    reference_type         VARCHAR(64) NOT NULL,
    reference_id           UUID NOT NULL,
    created_at             TIMESTAMP NOT NULL,

    UNIQUE (reference_type, reference_id)
);
```

```sql
CREATE TABLE ledger_entry (
    ledger_entry_id        UUID PRIMARY KEY,
    ledger_transaction_id  UUID NOT NULL,
    account_code           VARCHAR(128) NOT NULL,
    currency               CHAR(3) NOT NULL,
    amount_minor           BIGINT NOT NULL,
    direction              VARCHAR(8) NOT NULL
);
```

For each transaction:

```text
sum(debits) = sum(credits)
```

The uniqueness of the reference prevents duplicate ledger posting.

---

## 21. Corrections and Off-Cycle Payroll

Once a payroll result has settled, do not edit or rerun the original payment.

Instead:

```text
Original payroll result
       |
       v
Correction calculation
       |
       v
Adjustment result
       |
       v
New payment obligation or recovery obligation
```

Example:

```text
Alice was underpaid $500.

Do not resend the original $5,000 payment.

Create:
correction result = +$500
payment obligation = $500
```

For overpayment:

```text
correction result = -$500
```

Recovery may be handled through:

- future payroll deduction,
- employee repayment,
- legal/local overpayment rules.

Each correction is separately auditable.

---

## 22. Multi-Currency Handling

Every calculation and obligation must identify its currency.

```text
amount = 500000
currency = EUR
```

Never add or compare monetary values from different currencies without explicit conversion.

If a payroll calculation requires FX:

```sql
CREATE TABLE fx_rate_snapshot (
    fx_rate_snapshot_id  UUID PRIMARY KEY,
    base_currency        CHAR(3) NOT NULL,
    quote_currency       CHAR(3) NOT NULL,
    rate                 DECIMAL(30, 12) NOT NULL,
    effective_at         TIMESTAMP NOT NULL,
    source               VARCHAR(64) NOT NULL
);
```

Store the exact rate used in the payroll result.

Employee payments should generally be batched by:

```text
legal entity
funding account
currency
payment rail
pay date
```

---

## 23. APIs

Most payroll APIs are internal and permission-controlled.

### Create a Payroll Run

```http
POST /v1/payroll-runs
Idempotency-Key: ...
```

```json
{
  "pay_group_id": "pg_123",
  "pay_period_id": "period_456",
  "run_type": "REGULAR"
}
```

The database additionally enforces business uniqueness.

### Start Calculation

```http
POST /v1/payroll-runs/{run_id}/calculate
Idempotency-Key: ...
```

Response:

```http
202 Accepted
```

```json
{
  "payroll_run_id": "run_123",
  "calculation_version": 4,
  "status": "CALCULATING"
}
```

### Get Run Status

```http
GET /v1/payroll-runs/{run_id}
```

### Approve

```http
POST /v1/payroll-runs/{run_id}/approvals
Idempotency-Key: ...
```

```json
{
  "decision": "APPROVE",
  "expected_calculation_version": 4
}
```

### Finalize

```http
POST /v1/payroll-runs/{run_id}/finalize
Idempotency-Key: ...
If-Match: "4"
```

### Get Employee Result

```http
GET /v1/payroll-runs/{run_id}/employees/{employment_id}/result
```

### Dispatch Payments

Normally initiated by a workflow after funding approval:

```http
POST /v1/payment-batches/{batch_id}/dispatch
Idempotency-Key: ...
```

---

## 24. API Idempotency Versus Business Idempotency

These are separate protections.

### API-Request Idempotency

Handles:

```text
request committed
response lost
caller retries
```

Use:

```text
Idempotency-Key
+
request hash
+
stored response
```

### Business Idempotency

Handles:

```text
caller sends two different idempotency keys
for the same logical payroll action
```

Use database constraints such as:

```sql
UNIQUE (
    pay_group_id,
    pay_period_id,
    run_type,
    run_sequence
)
```

and:

```sql
UNIQUE (logical_payment_key)
```

### Worker Idempotency

Handles:

```text
Kafka redelivery
worker crash
workflow retry
```

Use:

- unique result keys,
- conditional status transitions,
- immutable event IDs,
- outbox records,
- unique ledger references.

All three layers are necessary.

---

## 25. Workflow Orchestration

Payroll consists of long-running steps:

```text
collect inputs
calculate
validate
approve
fund
submit payments
wait for settlement
reconcile
```

This is better modeled as a durable workflow than as one synchronous request.

```text
Payroll Workflow

OPEN_INPUTS
    |
SNAPSHOT_INPUTS
    |
CALCULATE
    |
VALIDATE
    |
WAIT_FOR_APPROVAL
    |
FINALIZE
    |
CREATE_PAYMENTS
    |
WAIT_FOR_FUNDING
    |
DISPATCH
    |
WAIT_FOR_SETTLEMENT
    |
RECONCILE
    |
COMPLETE
```

Every workflow step should be:

- idempotent,
- retryable,
- time-bounded,
- auditable,
- protected by expected-state checks.

Do not hold distributed locks for the lifetime of a payroll workflow.

---

## 26. Data Partitioning

Different data should use different partition keys.

### Employee and Employment Data

```text
partition by employment_id
or by regional employee shard
```

### Payroll Runs

```text
partition by legal_entity_id or pay_group_id
```

This keeps a pay group’s runs and calendars colocated.

### Payroll Results

```text
partition by payroll_run_id
subpartition or cluster by employment_id
```

Calculation workers can process employment ranges in parallel.

### Balance Ledger

```text
partition by employment_id + tax year
```

A tax calculation typically needs one employee’s cumulative balances.

### Payment Obligations

```text
partition by payment_batch_id
```

or by legal entity and payment date, depending on dispatch needs.

### Events

```text
calculation tasks -> key by employment_id
payroll run events -> key by payroll_run_id
payment events -> key by payment_obligation_id
```

This preserves entity-level ordering without creating a global ordered stream.

---

## 27. Data Consistency

Use strong consistency for:

- payroll run state,
- finalized results,
- YTD balance entries,
- payment obligations,
- payment attempts,
- ledger entries,
- reconciliation.

Use eventual consistency for:

- dashboards,
- global reporting,
- aggregate analytics,
- employee notifications,
- search,
- monitoring views.

Kafka events should be treated as notifications of durable state, not the only record of that state.

---

## 28. Security and Compliance

Payroll data is among the most sensitive data a company holds.

Required controls include:

- encryption in transit and at rest,
- regional PII storage,
- field-level encryption for bank and tax identifiers,
- tokenization of bank accounts,
- strict RBAC,
- segregation of duties,
- approval thresholds,
- immutable audit logs,
- access logging,
- key rotation,
- data-retention controls,
- masked values in operational UIs,
- no sensitive fields in general application logs.

Examples of roles:

```text
Payroll Administrator
Payroll Approver
Tax Administrator
Auditor
Employee Self-Service
Payment Operator
```

No single role should be able to change payroll inputs, approve the run, and release payments without controls.

---

## 29. Failure Scenarios

### Calculation Worker Processes an Employee Twice

Protected by:

```sql
UNIQUE (
    payroll_run_id,
    employment_id,
    calculation_version
)
```

### Tax Service Temporarily Unavailable

Calculation task remains retryable. The payroll run cannot advance to approval until all required calculations complete.

### Payroll Run Is Finalized Twice

Use a conditional update:

```sql
UPDATE payroll_run
SET status = 'FINALIZED'
WHERE payroll_run_id = ?
  AND status = 'APPROVED';
```

Only one transaction succeeds.

### Payment Worker Crashes After Bank Submission

The obligation becomes `UNKNOWN`. Query the provider using the same idempotency key before doing anything else.

### Bank Rejects a Payment

Mark the obligation `FAILED` only after explicit rejection. Correct the bank details and create an authorized retry attempt tied to the same obligation.

### Duplicate Provider Webhook

Use:

```text
provider_event_id UNIQUE
```

and idempotent status transitions.

### Employee Changes Bank Details After Payroll Finalization

The payment obligation should retain the snapshotted destination used at finalization. A controlled administrative workflow decides whether it is still safe to update before dispatch.

### Regional Cell Outage

Other regional cells continue. The affected region fails over within its permitted geography or waits for recovery according to local requirements.

---

## 30. Observability

Important operational metrics include:

### Payroll Calculation

```text
employees calculated / total
calculation failures
calculation duration
tax-engine latency
validation failures
stale input count
```

### Payment Safety

```text
payment obligations created
duplicate-key conflicts
payments in UNKNOWN
payments awaiting reconciliation
failed transfers
duplicate provider callbacks
amount mismatches
```

### Deadlines

```text
time until input cutoff
time until approval deadline
time until bank submission deadline
unapproved payroll runs
unfunded payment batches
```

Any payment in `UNKNOWN` near the bank cutoff should trigger a high-severity alert.

---

## 31. End-to-End Example

Alice belongs to the US bi-weekly pay group.

### Step 1: Schedule

The calendar contains:

```text
period:
November 2–November 15

input cutoff:
November 16, 12:00 ET

pay date:
November 20
```

### Step 2: Snapshot

The system snapshots:

```text
employment
salary
hours
bonus
benefits
tax profile
bank allocations
YTD balances
tax rule versions
```

### Step 3: Calculate

```text
salary                $5,000
bonus                   $500
gross                  $5,500

pre-tax deductions      -$300
taxable base           $5,200

federal tax             -$900
state tax               -$250
other deductions        -$150

net pay                $3,900
```

### Step 4: Approve and Finalize

The selected result becomes immutable.

YTD ledger entries are appended.

### Step 5: Create Obligations

Alice has:

```text
80% checking
20% savings
```

The system creates:

```text
obligation 1:
$3,120 -> checking
logical key = result123:checking:1

obligation 2:
$780 -> savings
logical key = result123:savings:2
```

Unique constraints prevent another obligation set from being created.

### Step 6: Dispatch

The payment worker submits obligation 1 using:

```text
Idempotency-Key: obligation-1
```

It submits obligation 2 using:

```text
Idempotency-Key: obligation-2
```

### Step 7: Reconcile

Provider webhooks and the settlement file confirm both transfers.

The system verifies:

```text
$3,120 + $780 = $3,900
```

and marks Alice’s payroll payment fully settled.

---

## 32. Recommended Architecture

```text
 HRIS     Time       Benefits      Compensation
   \        |            |               /
    \       |            |              /
     +------+------------+-------------+
                     |
                     v
              Payroll Input Store
                     |
                     v
              Payroll Scheduler
                     |
                     v
            Regional Payroll Workflow
                     |
        +------------+-------------+
        |                          |
        v                          v
 Snapshot Service          Payroll Calculation
                                   |
                        +----------+----------+
                        |                     |
                        v                     v
                   Tax Engines          Deduction Engine
                        |                     |
                        +----------+----------+
                                   |
                                   v
                              Validation
                                   |
                                   v
                                Approval
                                   |
                                   v
                              Finalization
                        immutable results + ledger
                                   |
                                   v
                       Payment Obligation Generator
                                   |
                                   v
                         Payment Orchestration DB
                                   |
                            transactional outbox
                                   |
                                   v
                             Payment Queue
                                   |
                                   v
                            Payment Workers
                                   |
                         +---------+---------+
                         |                   |
                         v                   v
                    Local Banks       Payroll Providers
                         |                   |
                         +---------+---------+
                                   |
                                   v
                            Reconciliation
```

## Interview Summary

> I would group employees into pay groups that define a concrete payroll calendar, currency, legal entity, and pay frequency. A scheduler creates payroll runs for each pay period. At the start of a run, the system snapshots all employee, compensation, time, benefit, tax-profile, and prior-balance inputs so calculation is deterministic and auditable.
>
> The gross-to-net engine calculates each employment independently and invokes effective-dated regional tax modules through a common interface. Every result records the exact employee snapshot, tax-rule versions, FX rates, and engine version. Year-to-date balances are maintained as an append-only ledger. After validation and dual approval, the run is finalized; finalized results are immutable, and later corrections use separate adjustment runs.
>
> To prevent duplicate pay, each finalized result creates one immutable set of payment obligations with database-enforced logical uniqueness. Payment workers claim obligations through conditional state transitions and submit them to banks using the obligation ID as the provider idempotency key. If a request times out after possible acceptance, the obligation enters an `UNKNOWN` state and is reconciled with the provider before any retry. Settlement files and bank statements are matched against expected obligations, and all financial movements are recorded in a double-entry ledger.
>
> I would deploy this as regional payroll cells for data residency and failure isolation, with a global control plane for legal entities, calendars, rule metadata, routing, and aggregate reporting.
