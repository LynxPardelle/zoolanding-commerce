# Zoolanding Commerce

Generic server-only commercial state for draft-configured catalogs, immutable offers, inventory, orders, subscriptions, fulfillment, migration requests, and isolated manual fiscal requests.

Phase 3 is local-only. `TASK-025` establishes only the repository boundary and immutable published-policy resolver. There is no AWS `dev` environment, and the test/production workflows fail closed until the remaining Phase 3 resources, handlers, authorization, and tests are complete and deployment is explicitly approved.

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

The module does not create tables, accept prices, call Stripe, or expose an API. Those responsibilities begin in later approved tasks.

## Local Verification

```powershell
python -m unittest discover -s tests -p "test_*.py"
python -m pip_audit -r requirements.txt
sam validate --lint
sam build --no-cached
actionlint
```

No command above deploys AWS. There is no AWS `dev` profile or workflow.
