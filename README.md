# Zoolanding Commerce

Generic server-only commercial state for draft-configured catalogs, immutable offers, inventory, orders, subscriptions, fulfillment, migration requests, and isolated manual fiscal requests.

Phase 3 is local-only. `TASK-025` establishes the repository boundary and immutable published-policy resolver, `TASK-026` adds only the storage resources, `TASK-027` adds the first pure domain rules, `TASK-028` adds the immutable catalog/offer/discount contract, and `TASK-029` adds the conditional inventory transaction contract described below. There is no AWS `dev` environment, and the test/production workflows fail closed until the remaining Phase 3 handlers, authorization, tests, and deployment gates are complete and explicitly approved.

## Boundaries

- Commerce owns commercial decisions and projections; it does not own provider credentials or canonical Stripe state.
- Data Spaces remains optional. A future sellable may reference one immutable, allowlisted snapshot during activation; Checkout never performs a live generic-data join.
- Authoritative money will use integer minor units and ISO currency codes. Browser-supplied prices are never trusted.
- Prices, inventory, orders, subscriptions, payment state, fiscal data, credentials, and customer PII do not belong in Data Spaces or draft configuration.
- Existing Content Hub blogs remain unchanged.

## Current Contract

`src/common/published_policy.py` implements PAT-007 for `server/commerce.json`:

- read the current Config Registry pointer on every resolution;
- derive the exact immutable version prefix;
- validate the closed environment/tenant/draft/domain scope;
- load only the exact Commerce and optional Auth Admin descriptors;
- cache immutable descriptors by environment, scope, domain, version, and access mode;
- fail closed on missing, oversized, malformed, duplicate-key, prefix-confusable, or contract-invalid input.

The module does not accept prices, call Stripe, or expose an API. Those responsibilities begin in later approved tasks.

## Storage Foundation

The undeployed SAM template owns three draft-scoped storage boundaries:

- `CommerceCatalogTable` for catalog, immutable offers, stock, and reservations;
- `CommerceOperationsTable` for orders, non-PII commercial projections, inbox/outbox, and audit;
- `FiscalTable` as the separate boundary for optional manual fiscal requests.

All three use on-demand billing, server-side encryption, point-in-time recovery, retained replacement/deletion policy, and server-owned `pk`/`sk` keys. Catalog and Operations enable `expiresAt` TTL for eligible cleanup records only; Fiscal has no TTL until its approved retention policy exists. Only Operations enables a `NEW_IMAGE` DynamoDB Stream.

Stream filtering, partial-batch responses, the failure destination, Lambda IAM, and catalog/fiscal handler isolation require an event-source mapping and real consumers. They remain in `TASK-030` and `TASK-032`; this task does not add placeholder Lambdas, queues, APIs, roles, or indexes.

## Domain Foundation

The pure `src/domain/` modules keep the first code-owned invariants independent from transport and persistence:

- exact sellable, shipping, fiscal-disclosure, and tax-behavior registries;
- immutable non-negative integer minor-unit money whose uppercase three-letter ASCII currency must belong to an immutable server-resolved allowlist;
- non-negative integer quantities and integer-only order line totals;
- immutable catalog items with safe variants/SKUs and an optional pointer to one exact Data Spaces record revision and field allowlist;
- immutable offer versions for one-time or fixed-semantics monthly/yearly recurring sales, including rejection of physical recurring offers and one-time subscriptions;
- immutable percentage or positive fixed-amount discount versions with closed duration, eligibility, limit, deadline, and an optional exact customer-facing code;
- canonical schema-versioned provider fingerprints that include every provider economic/restriction input but exclude version identity, lifecycle, presentation, and the policy currency allowlist;
- the exact `draft -> provisioning -> active -> existing_only -> retired` transition path plus independently monotonic lifecycle and presentation revisions.

Every money value requires the owning provider/policy currency allowlist and fails closed when the selected code is absent. This layer deliberately does not add a stale ISO registry or silently normalize browser input. Customer-facing discount code casing remains part of the immutable restriction fingerprint; TASK-041 will derive a separate case-folded lookup key to enforce active-code uniqueness. Data Spaces references contain identifiers only: the future authorized activation handler must derive environment/tenant/draft/domain from trusted policy, obtain and validate the allowlisted internal snapshot, and persist that snapshot before an offer becomes buyable. Constructors support trusted rehydration; future mutation handlers must load current server state, enforce bounded request/cardinality/provider limits, and apply revision/transition helpers with conditional persistence rather than accepting browser lifecycle values.

Subscription workflows, fiscal fields, handlers, IAM, Data Spaces calls, Stripe mappings/calls, and deployment remain in their later tasks. The catalog/offer layer adds no network operation, credential, PII, provider payload, or AWS resource.

## Inventory Transactions

`src/domain/inventory.py`, `src/domain/orders.py`, and `src/storage.py` implement the local TASK-029 contract:

- tracked stock keeps exact integer `onHand = available + reserved` state and a conditional revision; untracked lines create no stock mutation;
- a positive adjustment from expected revision zero initializes tracked stock, while later positive or negative adjustments cannot consume reserved stock;
- reservation aggregates lines by stock target, then creates stock updates, immutable movements, the Catalog reservation/due marker, Operations order, scoped PaymentAttempt binding, and 90-day idempotency receipt in one cross-table transaction;
- a Checkout accepts at most 20 distinct immutable OfferVersions; 20 distinct tracked targets produce 45 unique actions, validated with the complete serialized plan before DynamoDB is called;
- `reservationCreatedAt`, `checkoutExpiresAt = created + 2,100 seconds`, and the initial reconciliation time `expires + 300 seconds` use one injected server timestamp;
- commit and release update stock, reservation, due marker, and order atomically, are mutually exclusive, and require a closed canonical completion reason; refund is not a stock transition;
- timeout, network, throttling, `5xx`, and unknown provider results classify as hold/retry/reconcile, never release; a missing response is reconciled through the durable receipt before an outcome is returned;
- uncertain due reservations move their draft-scoped marker forward by exactly five minutes, so one unresolved item cannot indefinitely hide later items; no scan or TTL acts as a commercial timer;
- environment, tenant, and draft prefix every key, and the short DynamoDB client token is derived from both that scope and the exact transaction plan.

TASK-030 still owns handlers, fresh authorization/CSRF, least-privilege IAM, the five-minute scheduler, published-scope enumeration, alarms, and API routes. TASK-040 still owns the exact internal provider-status call and adapter evidence mapping. No Lambda, route, role, schedule, provider client, dependency, AWS deployment, credential, customer PII, or provider payload was added in TASK-029.

## Local Verification

```powershell
python -m unittest discover -s tests -p "test_*.py"
python -m pip_audit -r requirements.txt
sam validate --lint
sam build --no-cached
actionlint
```

No command above deploys AWS. There is no AWS `dev` profile or workflow.
