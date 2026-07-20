# Zoolanding Commerce

Generic server-only commercial state for draft-configured catalogs, immutable offers, inventory, orders, subscriptions, fulfillment, migration requests, and isolated manual fiscal requests.

Phase 3 is local-only. `TASK-025` establishes the repository boundary and immutable published-policy resolver; `TASK-026` adds only the storage resources described below. There is no AWS `dev` environment, and the test/production workflows fail closed until the remaining Phase 3 handlers, authorization, tests, and deployment gates are complete and explicitly approved.

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

## Local Verification

```powershell
python -m unittest discover -s tests -p "test_*.py"
python -m pip_audit -r requirements.txt
sam validate --lint
sam build --no-cached
actionlint
```

No command above deploys AWS. There is no AWS `dev` profile or workflow.
