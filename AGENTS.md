# Zoolanding Commerce Agent Workflow

## Read Before Editing

1. Start with [README.md](README.md) for the current local contract and verification commands.
2. Use the Zoolandingpage hub documents `docs/api-driven-config/22-server-only-integration-microservices.md` and `plan/infrastructure-server-only-integrations-1.md` as the canonical cross-repository design.
3. Verify the current branch, worktree, and repository status before editing or releasing.

## Safety And Scope

- Keep the service generic per draft. Do not add pilot-, provider-, or customer-specific behavior to its core.
- Never store secrets, credentials, tokens, banking/identity documents, payment-provider payloads, customer PII, raw session cookies, signed URLs, or real fiscal data in source, tests, fixtures, docs, or logs.
- Money is integer minor units plus an ISO currency code. Browser values never decide an authoritative price, tenant, draft, provider account, table, key, expression, or authorization result.
- `dev` is local/CI only. There is no AWS `dev` stack, deployment workflow, SAM profile, GitHub deployment Environment, or cloud resource.
- This phase is local-only. Do not deploy or mutate AWS until the plan's test gates and explicit authorization are complete.
- Keep provider truth and credentials in Integrations; keep generic records in Data Spaces.
- Add no dependency without a documented need and dependency audit.

## Delivery

- Work test-first and preserve fail-closed behavior.
- Before declaring work correct, audit, fix, and rerun the audit at least three times.
- Promote only through `dev -> test -> main`; never push implementation directly to a protected release branch.
- Keep timestamps in Central Time and sensitive runtime values out of evidence.
