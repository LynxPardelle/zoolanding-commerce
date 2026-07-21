# Zoolanding Commerce

Generic server-only commercial state for draft-configured catalogs, immutable offers, inventory, orders, subscriptions, fulfillment, migration requests, and isolated manual fiscal requests.

The Phase 5 Commerce boundary and the Phase 6 Commerce notification contract are implemented and verified locally; they remain undeployed. The local service includes the immutable policy resolver, retained storage, provider-neutral domain rules, conditional inventory transactions, eight literal browser routes, exact AWS_IAM Integrations commands/status lookup, normalized integration-event consumption, an idempotent notification outbox, subscription projections, resumable bulk-migration requests and approval, reservation reconciliation, and isolated manual fiscal intake. There is no AWS `dev` environment. Test and production deployment remain closed until their external identities, alarms, quotas, cross-service dependencies, and live gates are reviewed and explicitly approved.

## Boundaries

- Commerce owns commercial decisions and projections; it does not own provider credentials or canonical Stripe state.
- Data Spaces remains optional. A future sellable may reference one immutable, allowlisted snapshot during activation; Checkout never performs a live generic-data join.
- Authoritative money will use integer minor units and ISO currency codes. Browser-supplied prices are never trusted.
- Prices, inventory, orders, subscriptions, payment state, fiscal data, credentials, and customer PII do not belong in Data Spaces or draft configuration.
- Existing Content Hub blogs remain unchanged.

## Current Contract

`src/common/published_policy.py` implements PAT-007 for the Commerce boundary:

- read the current Config Registry pointer on every resolution;
- derive the exact immutable version prefix;
- validate the closed environment/tenant/draft/domain scope;
- load the exact Commerce descriptor, optional Auth Admin descriptor for protected routes, and same-version Notification Policies only for Checkout when explicitly referenced;
- cache immutable descriptors by environment, scope, domain, version, and access mode;
- fail closed on missing, oversized, malformed, duplicate-key, prefix-confusable, scope-mismatched, or contract-invalid input;
- require a server-owned transport approval on an active production notification policy.

The resolver never loads credentials or provider payloads. Public catalog reads resolve only Commerce policy. Prices are resolved from stored active OfferVersions, not accepted from the browser, and no Commerce handler calls Stripe directly; provider commands cross only the exact signed Integrations routes.

## Storage Foundation

The undeployed SAM template owns three draft-scoped storage boundaries:

- `CommerceCatalogTable` for catalog, immutable offers, stock, and reservations;
- `CommerceOperationsTable` for orders, non-PII commercial projections, inbox/outbox, and audit;
- `FiscalTable` as the separate boundary for optional manual fiscal requests.

All three use on-demand billing, server-side encryption, point-in-time recovery, retained replacement/deletion policy, and server-owned `pk`/`sk` keys. Catalog and Operations enable `expiresAt` TTL for eligible cleanup records only; Fiscal has no TTL until its approved retention policy exists. Only Operations enables a `NEW_IMAGE` DynamoDB Stream. Catalog has one sparse `KEYS_ONLY` `ReservationDueIndex`; the reconciler queries it and strongly rereads each base marker.

The undeployed template also defines the Commerce notification-request topic, the Integration Events queue and DLQ, and a distinct outbox-stream failure queue. The SQS consumer and DynamoDB Stream relay report partial batch failures. The stream mapping accepts only pending Commerce Outbox records. The canonical provider-status gateway is wired locally. The five-minute reconciliation schedule remains disabled until an approved deployment supplies the reviewed environment-specific `IntegrationsApiId`, verifies the cross-service route/IAM contract, and explicitly enables the schedule.

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

Every money value requires the owning provider/policy currency allowlist and fails closed when the selected code is absent. This layer deliberately does not add a stale ISO registry or silently normalize browser input. Customer-facing discount code casing remains part of the immutable restriction fingerprint; the wired Integrations command layer derives the separate case-folded active-code claim and enforces uniqueness. Data Spaces references contain identifiers only: an authorized activation path must derive environment/tenant/draft/domain from trusted policy, obtain and validate the allowlisted internal snapshot, and persist that snapshot before an offer becomes buyable. Mutation handlers load current server state, enforce bounded request/cardinality/provider limits, and apply revision/transition helpers with conditional persistence rather than accepting browser lifecycle values.

Subscription commands remain provider-neutral and use the exact implemented AWS_IAM command gateway for bounded subscription changes, discounts, pause policy, and fresh Customer Portal handoffs. The gateway is local-only until an approved deployment supplies `IntegrationsApiId`; it never accepts provider credentials or account identifiers from the browser. Fiscal PII is accepted only by the isolated fiscal routes/table after opt-in and live-gate checks; it is not written to drafts, Data Spaces, events, logs, or general Commerce projections. Data Spaces activation snapshots and every AWS/provider-backed rollout remain later authorized work.

## Bulk Subscription Migration

Commerce owns the durable commercial request and operator approval, while Integrations owns provider jobs and item execution:

- preview resolves immutable source and target OfferVersions server-side, binds their exact schema-versioned snapshots and hashes, and sends only the draft scope, configured connection, closed policy, aggregate limits, commercial request ID, and technical idempotency metadata;
- the `MigrationRequest` is retained business state and deliberately has no DynamoDB TTL. Raw idempotency values and operator identities are never stored: Commerce retains scoped SHA-256 digests and a scoped actor hash;
- every bulk-migration operation (preview, execute, pause, resume, cancel, and protected status) requires the distinct `subscription:migration:execute` capability; every mutation also requires fresh Auth Admin state and CSRF;
- execution requires explicit browser confirmation plus an exact, unexpired dry-run revision/hash approved by the current actor. Browser input cannot choose a provider account, connection, snapshots, candidate scope, canary, or concurrency;
- preview, execute, and control use literal AWS_IAM Integrations routes. A persisted-but-not-dispatched `pending` result returns a retryable error without advancing Commerce or creating a local command receipt, so an exact retry can safely resume dispatch;
- cancel can reopen a `completed` or `completed_with_errors` request as `cancel_requested` so Integrations can remove owned future next-renewal schedules. Integrations remains the authoritative guard: it accepts that terminal rollback only for `next_renewal` jobs that retain applied items with owned Schedule IDs, restores or releases validated phases, and never cancels the active subscription;
- command and normalized-event receipts expire after 90 days, are scope/input-hash/result bound, and reject idempotency-key reuse with changed dry-run proof, confirmation, control action, or expected revision. Every accepted command receipt retains only the scoped pseudonymous actor hash; exact replays preserve that original attribution and remain available only after the caller passes current authorization. Command results must advance the exact expected Integrations revision, while closed `needs_review` results may only reference an existing earlier/current revision. State and state-revision checks accept valid same-revision item progress while preventing stale events, conflicting progress snapshots for one revision, or multiple distinct transitions from rolling a request backward;
- public projections contain only the commercial request/job IDs, state, revisions, the closed latest-command signal `null|accepted|needs_review`, dry-run proof metadata, expiry, and aggregate counts. `needs_review` preserves the safe command outcome without exposing a provider reason or payload. No provider payload, customer PII, credential, bank data, or signed URL is stored or returned.

## Inventory Transactions

`src/domain/inventory.py`, `src/domain/orders.py`, and `src/storage.py` implement the local TASK-029 contract:

- tracked stock keeps exact integer `onHand = available + reserved` state and a conditional revision; untracked lines create no stock mutation;
- a positive adjustment from expected revision zero initializes tracked stock, while later positive or negative adjustments cannot consume reserved stock;
- reservation aggregates lines by stock target, then creates stock updates, immutable movements, the Catalog reservation/due marker, Operations order, scoped PaymentAttempt binding, and 90-day idempotency receipt in one cross-table transaction;
- a Checkout accepts at most 20 distinct immutable OfferVersions and at most 1,000,000 units per line as a code-owned abuse/storage ceiling; drafts and provider adapters may impose lower business limits. Twenty distinct tracked targets produce 45 unique actions, validated with the complete serialized plan before DynamoDB is called;
- `reservationCreatedAt`, `checkoutExpiresAt = created + 2,100 seconds`, and the initial reconciliation time `expires + 300 seconds` use one injected server timestamp;
- commit and release update stock, reservation, due marker, and order atomically, are mutually exclusive, and require a closed canonical completion reason; refund is not a stock transition;
- timeout, network, throttling, `5xx`, and unknown provider results classify as hold/retry/reconcile, never release; a missing response is reconciled through the durable receipt before an outcome is returned;
- uncertain due reservations move their draft-scoped marker forward by exactly five minutes, so one unresolved item cannot indefinitely hide later items; no scan or TTL acts as a commercial timer;
- environment, tenant, and draft prefix every key, and the short DynamoDB client token is derived from both that scope and the exact transaction plan.

The reconciler never treats unavailable or ambiguous provider evidence as permission to release stock. The exact internal provider-status call and evidence adapter are implemented locally, while the schedule stays disabled until the reviewed `IntegrationsApiId` and explicit deployment enablement are present. No Commerce code contains a provider credential, customer identity document, raw provider payload, or persisted public provider URL.

## Server Boundaries

The API uses separate literal POST routes instead of a body-selected IAM router:

- `/features/commerce/public-read` returns only sanitized active offers and has Catalog-only data access;
- `/features/commerce/read` is protected by `commerce:catalog:read`;
- `/features/commerce/catalog/action` is protected by `commerce:catalog:write`, CSRF, conditional revisions, and a durable 90-day Operations idempotency receipt;
- `/features/commerce/inventory/action` is protected by `commerce:inventory:write` and uses the policy-owned stock location;
- `/features/commerce/subscription/action` calls only exact signed Integrations routes. Regular subscription actions use `commerce:subscription:manage`; all six bulk-migration operations use `subscription:migration:execute`;
- `/features/commerce/public-action` admits bounded Checkout requests using server-resolved scope, time, IDs, location, prices, the draft's closed currency allowlist, and a same-version notification target. Its `Idempotency-Key` is a recovery capability: the draft runtime must generate exactly 32 cryptographically random bytes, encode them as canonical unpadded base64url, keep the raw value out of logs/storage, and reuse it only for the exact lost-response retry;
- `/features/commerce/fiscal/request` accepts a same-origin, single-use order-access proof only after verified payment state makes it eligible;
- `/features/commerce/fiscal/admin` uses the distinct `commerce:fiscal:manage` capability and Fiscal-only PII access.

Protected handlers re-read Auth Admin session and current user state and require CSRF for mutations. Each of the eleven Lambdas has a dedicated explicit role with inline least-privilege access and writes only to the retained Commerce log group; no AWS-managed execution policy or wildcard resource is attached. Catalog, inventory, Checkout, fiscal, event-consumer, relay, and reconciler Lambdas receive only their required table/topic/config permissions. Normalized Integration Events contain no email address, fiscal field, secret, or raw provider object. Confirmed payment state can create a pinned `notification.requested.v1` outbox record in the same transaction; only the relay publishes it and marks delivery idempotently.

Checkout derives `notificationPolicyId`, immutable `publishedVersionId`, the one MVP recipient-set ID/version/member, and the exact configured `notificationType -> templateId` mapping from the same-version policy referenced by Commerce configuration. Browser input cannot choose any notification policy, recipient, type, or template. The complete target is stored on the Order and included in the Checkout request hash; a payment transition not enabled by that pinned mapping creates no notification outbox. Every emitted event ID and dedupe key bind the scope, source event, and complete canonical payload excluding the self-referential dedupe field. `notification.requested.v1` permits only the exact payment-success/payment-failure template pairs, a `commerce-order` source, and typed `orderId`, `amountMinor`, and `currency` variables. Unknown fields and any address, message body, credential reference, fiscal field, provider payload, or payment-provider data fail closed before write or publish.

Catalog cursors are signed with the required deployment parameter `CommerceCursorSigningKey`. CloudFormation marks it `NoEcho`, and only the public and authenticated catalog reader Lambdas receive it. The value must be supplied through the approved deployment secret path; it has no default and must never be stored in drafts, configuration payloads, `samconfig.toml`, source, tests, examples, logs, or evidence.

In test, a fiscal-enabled Checkout returns a random opaque proof while Commerce stores only its SHA-256 hash in Operations. The proof is dormant until a verified paid (or refund-confirmed) event atomically opens the 24-hour request window; terminal unpaid makes it ineligible. An exact Checkout replay while payment is still pending conditionally rotates the stored hash and returns a replacement proof, so a lost response is recoverable without persisting the raw proof. That recovery is available only through the exact 256-bit Checkout capability described above; its internal idempotency namespace is separate from protected catalog, inventory, subscription, and fiscal mutations. Production fiscal capture remains blocked in code until the retention/deletion and accountant-access controls are implemented and approved; draft fields or deployment parameters cannot open that gate.

## Deployment boundary

The local SAM contract requires an environment-specific Integrations API identifier, Config Registry table and payload bucket names, Auth Admin session/state table names, the normalized Integration Events topic ARN, and a NoEcho Commerce cursor-signing key. Production fiscal capture additionally remains closed behind its explicit approval identifiers and code gate. This README records no parameter value, secret, account identifier, or deployed resource.

The reservation reconciler's five-minute schedule is declared but disabled. Phase 8 must verify the exact cross-service IAM routes, deployment parameters, queues/topics, alarms, quotas, rollback, and provider-status evidence before explicitly enabling it. No local Phase 5 result authorizes an AWS deployment or schedule activation.

## Local Verification

The Phase 6 Commerce working tree passes 263 unit and contract tests. One local test is intentionally skipped when `botocore`, supplied by the Lambda/SAM runtime, is not importable from the workstation interpreter.

```powershell
python -m unittest discover -s tests -p "test_*.py"
python -m pip_audit -r src/requirements.txt
sam validate --lint
sam build --no-cached
python tests/verify_sam_build.py
actionlint
```

No command above deploys AWS. There is no AWS `dev` profile or workflow.
