# Zoolanding Commerce

Generic server-only commercial state for draft-configured catalogs, immutable offers, inventory, orders, subscriptions, fulfillment, migration requests, and isolated manual fiscal requests.

Phase 3 is local-only. `TASK-025` establishes the repository boundary and immutable published-policy resolver, `TASK-026` adds only the storage resources, `TASK-027` adds the first pure domain rules, and `TASK-028` adds the immutable catalog/offer/discount contract described below. There is no AWS `dev` environment, and the test/production workflows fail closed until the remaining Phase 3 handlers, authorization, tests, and deployment gates are complete and explicitly approved.

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

Stock movements, orders, subscription workflows, fiscal fields, handlers, IAM, Data Spaces calls, Stripe mappings/calls, and persistence remain in their later tasks. TASK-028 adds no network operation, dependency, credential, PII, provider payload, or AWS resource.

## Local Verification

```powershell
python -m unittest discover -s tests -p "test_*.py"
python -m pip_audit -r requirements.txt
sam validate --lint
sam build --no-cached
actionlint
```

No command above deploys AWS. There is no AWS `dev` profile or workflow.
