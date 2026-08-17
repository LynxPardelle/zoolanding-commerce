import copy
import hashlib
import json
import os
import unittest
from unittest.mock import Mock, patch

from src.common.auth_admin import AuthorizedContext
from src.common.published_policy import ResolvedPolicies
from src.domain.offers import Money, OfferRecurrence, OfferVersion
from src.handlers import subscription_action
from src.integrations_gateway import canonical_hash
from src.integrations_gateway import (
    IntegrationsUnavailable,
    InternalIntegrationsGateway,
)
from src.storage import CommerceScope, StorageConflict, StorageOutcomeUnknown
from src.storage import CommerceStore
from src.events import (
    IntegrationEventProcessor,
    IntegrationEventValidationError,
    parse_integration_event,
)
from tests.test_storage import CATALOG_TABLE, OPERATIONS_TABLE, FakeBackend


NOW = 1_800_000_000
DOMAIN = "example.com"
MIGRATION_ITEM_1 = "migration-item-" + "1" * 40
MIGRATION_ITEM_2 = "migration-item-" + "2" * 40


def _policies() -> ResolvedPolicies:
    commerce = {
        "status": "active",
        "adminAccess": {
            "mode": "auth-profile",
            "authProfileId": "staff",
            "capabilities": [
                "commerce:subscription:manage",
                "subscription:migration:execute",
            ],
        },
        "sellableTypes": ["subscription"],
        "payments": {
            "bindingId": "payments-primary",
            "supportedCurrencies": ["MXN"],
            "oneTime": False,
            "subscriptions": True,
            "editablePrices": True,
            "coupons": True,
            "planChangePolicy": {"mode": "next-renewal"},
            "migrationPolicy": {"canarySize": 7, "accountConcurrency": 3},
            "pausePolicy": {"enabled": False},
        },
        "inventory": {
            "enabled": False,
            "tracked": False,
            "backorders": False,
            "locationId": "primary",
        },
        "shipping": {"enabled": False, "methods": ["free"]},
        "fiscal": {"enabled": False},
        "checkout": {
            "successPath": "/success",
            "cancelPath": "/cancel",
            "termsPath": "/terms",
            "privacyPath": "/privacy",
            "refundPolicyPath": "/refunds",
            "supportPath": "/support",
        },
    }
    return ResolvedPolicies(
        environment="test",
        tenant_id="tenant-a",
        draft_id="draft-a",
        domain=DOMAIN,
        version_id="version-1",
        prefix="sites/example.com/versions/version-1/",
        commerce={
            "version": 1,
            "scope": {
                "environment": "test",
                "tenantId": "tenant-a",
                "draftId": "draft-a",
                "domain": DOMAIN,
            },
            "commerce": commerce,
        },
        auth_registry={"version": 1, "profiles": []},
    )


def _context(policies: ResolvedPolicies) -> AuthorizedContext:
    return AuthorizedContext(
        policies.environment,
        policies.tenant_id,
        policies.draft_id,
        policies.domain,
        "operator-1",
        ("billing",),
        {"authProfileId": "staff"},
        policies.commerce["commerce"],
        {"subject": "operator-1"},
    )


def _event(operation: str, input_value: dict, *, idempotency_key: str = "migration-key-1"):
    return {
        "httpMethod": "POST",
        "path": "/features/commerce/subscription/action",
        "headers": {
            "X-ZLP-Domain": DOMAIN,
            "X-ZLP-Auth-Profile-Id": "staff",
            "Cookie": "__Host-zlp_session=session-value",
            "Idempotency-Key": idempotency_key,
        },
        "body": json.dumps({"operation": operation, "input": input_value}),
        "isBase64Encoded": False,
        "requestContext": {"requestId": "request-migration-1"},
    }


def _offer(version_id: str, amount_minor: int) -> OfferVersion:
    return OfferVersion(
        version_id,
        "catalog-item-1",
        None,
        3,
        "subscription",
        Money(amount_minor, "MXN", frozenset({"MXN"})),
        "exclusive",
        OfferRecurrence("month"),
        lifecycle_state="active",
        lifecycle_revision=3,
    )


class MigrationBoundaryRedTests(unittest.TestCase):
    def setUp(self):
        self.policies = _policies()
        self.context = _context(self.policies)
        self.catalog = Mock()
        self.catalog.get_offer_version.side_effect = [
            _offer("offer-source", 90_000),
            _offer("offer-target", 120_000),
        ]
        self.store = Mock()
        self.store.replay_command.return_value = None
        self.store.prepare_preview.return_value = {
            "commercialRequestId": "migration-request-1",
            "jobId": None,
            "revision": 0,
        }
        self.store.record_command_result.return_value = {
            "commercialRequestId": "migration-request-1",
            "jobId": "migration-job-1",
            "state": "previewing",
            "revision": 1,
            "commandStatus": "accepted",
            "dryRunRevision": None,
            "dryRunHash": None,
            "expiresAt": None,
            "counts": {
                "total": 0,
                "pending": 0,
                "applied": 0,
                "needsReview": 0,
                "failed": 0,
            },
        }
        self.gateway = Mock()
        self.gateway.execute.return_value = {
            "commandId": "command-migration-1",
            "status": "accepted",
            "jobId": "migration-job-1",
            "revision": 1,
        }

    def _invoke(self, operation: str, input_value: dict):
        with (
            patch.object(subscription_action, "resolve_policies", return_value=self.policies),
            patch.object(
                subscription_action,
                "authorize_request",
                return_value=self.context,
            ) as authorize,
            patch.object(subscription_action, "_gateway", return_value=self.gateway),
            patch.object(subscription_action, "_catalog_store", return_value=self.catalog, create=True),
            patch.object(subscription_action, "_migration_store", return_value=self.store, create=True),
            patch.object(subscription_action.time, "time", return_value=NOW),
        ):
            response = subscription_action.lambda_handler(_event(operation, input_value), None)
        return response, authorize

    def test_preview_derives_offer_snapshots_scope_policy_and_bulk_authorization(self):
        response, authorize = self._invoke(
            "migrationPreview",
            {
                "sourceOfferVersionId": "offer-source",
                "targetOfferVersionId": "offer-target",
            },
        )

        self.assertEqual(response["statusCode"], 200, response["body"])
        authorize.assert_called_once()
        self.assertEqual(
            authorize.call_args.kwargs["capability"],
            "subscription:migration:execute",
        )
        self.assertTrue(authorize.call_args.kwargs["mutation"])
        operation, _scope, command, metadata = self.gateway.execute.call_args.args[:3] + (
            self.gateway.execute.call_args.kwargs,
        )
        self.assertEqual(operation, "migrationPreview")
        self.assertEqual(command["commercialRequestId"], "migration-request-1")
        self.assertEqual(command["requestedPolicy"], {"mode": "next_renewal"})
        self.assertEqual(command["candidateScope"], {"kind": "all_matching_source_price"})
        self.assertEqual((command["canarySize"], command["accountConcurrency"]), (7, 3))
        self.assertEqual(command["sourceOffer"]["offerVersionId"], "offer-source")
        self.assertEqual(command["targetOffer"]["offerVersionId"], "offer-target")
        self.assertRegex(command["sourceOffer"]["contentHash"], r"^[a-f0-9]{64}$")
        self.assertNotIn("actor", repr(command).lower())
        self.assertEqual(metadata["connection_id"], "payments-primary")
        expected_actor_hash = hashlib.sha256(
            b"test\0tenant-a\0draft-a\0example.com\0operator-1"
        ).hexdigest()
        self.assertEqual(
            self.store.prepare_preview.call_args.kwargs["actor_hash"],
            expected_actor_hash,
        )

    def test_migration_operation_authorization_matrix_is_action_scoped(self):
        cases = (
            (
                "migrationPreview",
                {"sourceOfferVersionId": "offer-source", "targetOfferVersionId": "offer-target"},
                "subscription:migration:execute",
                True,
            ),
            (
                "migrationExecute",
                {
                    "commercialRequestId": "migration-request-1",
                    "dryRunRevision": 2,
                    "dryRunHash": "a" * 64,
                    "confirmation": True,
                },
                "subscription:migration:execute",
                True,
            ),
            (
                "migrationPause",
                {"commercialRequestId": "migration-request-1", "expectedRevision": 3},
                "subscription:migration:execute",
                True,
            ),
            (
                "migrationResume",
                {"commercialRequestId": "migration-request-1", "expectedRevision": 4},
                "subscription:migration:execute",
                True,
            ),
            (
                "migrationCancel",
                {"commercialRequestId": "migration-request-1", "expectedRevision": 5},
                "subscription:migration:execute",
                True,
            ),
            (
                "migrationStatus",
                {"commercialRequestId": "migration-request-1"},
                "subscription:migration:execute",
                False,
            ),
        )

        for operation, input_value, capability, mutation in cases:
            with (
                self.subTest(operation=operation),
                patch.object(
                    subscription_action,
                    "resolve_policies",
                    return_value=self.policies,
                ),
                patch.object(
                    subscription_action,
                    "authorize_request",
                    return_value=self.context,
                ) as authorize,
                patch.object(
                    subscription_action,
                    "_handle_migration",
                    return_value={"operation": operation},
                ),
            ):
                response = subscription_action.lambda_handler(
                    _event(operation, input_value), None
                )

            self.assertEqual(response["statusCode"], 200, response["body"])
            self.assertEqual(authorize.call_args.kwargs["capability"], capability)
            self.assertIs(authorize.call_args.kwargs["mutation"], mutation)

    def test_status_with_a_corrupt_missing_job_binding_fails_as_upstream_not_browser_input(self):
        self.store.get_request.return_value = {
            "commercialRequestId": "migration-request-1",
            "connectionId": "payments-primary",
            "jobId": None,
        }

        response, authorize = self._invoke(
            "migrationStatus",
            {"commercialRequestId": "migration-request-1"},
        )

        self.assertEqual(response["statusCode"], 503)
        self.assertEqual(
            authorize.call_args.kwargs["capability"],
            "subscription:migration:execute",
        )
        self.assertFalse(authorize.call_args.kwargs["mutation"])
        self.gateway.execute.assert_not_called()

    def test_status_adds_only_the_local_closed_command_status(self):
        self.store.get_request.return_value = {
            "commercialRequestId": "migration-request-1",
            "connectionId": "payments-primary",
            "jobId": "migration-job-1",
            "lastCommand": {
                "operation": "migrationExecute",
                "idempotencyDigest": "a" * 64,
                "requestHash": "f" * 64,
                "actorHash": "c" * 64,
                "result": {
                    "commandId": "command-migration-review",
                    "status": "needs_review",
                    "jobId": "migration-job-1",
                    "revision": 2,
                },
            },
        }
        self.gateway.execute.return_value = {
            "commercialRequestId": "migration-request-1",
            "jobId": "migration-job-1",
            "connectionId": "payments-primary",
            "revision": 2,
            "state": "awaiting_approval",
            "dryRunRevision": 1,
            "dryRunHash": "b" * 64,
            "expiresAt": NOW + 900,
            "counts": {
                "total": 0,
                "pending": 0,
                "applied": 0,
                "needsReview": 0,
                "failed": 0,
            },
            "items": [],
            "nextCursor": None,
        }

        response, authorize = self._invoke(
            "migrationStatus",
            {"commercialRequestId": "migration-request-1"},
        )

        body = json.loads(response["body"])["data"]
        self.assertEqual(response["statusCode"], 200, response["body"])
        self.assertEqual(body["commandStatus"], "needs_review")
        self.assertNotIn("lastcommand", repr(body).lower())
        self.assertEqual(
            authorize.call_args.kwargs["capability"],
            "subscription:migration:execute",
        )
        self.assertFalse(authorize.call_args.kwargs["mutation"])

    def test_command_response_whitelists_the_public_projection(self):
        self.store.record_command_result.return_value = {
            "commercialRequestId": "migration-request-1",
            "jobId": "migration-job-1",
            "state": "previewing",
            "revision": 1,
            "commandStatus": "accepted",
            "dryRunRevision": None,
            "dryRunHash": None,
            "previewExpiresAt": None,
            "counts": {
                "total": 0,
                "pending": 0,
                "applied": 0,
                "needsReview": 0,
                "failed": 0,
            },
            "actorHash": "a" * 64,
            "requestHash": "b" * 64,
            "sourceOffer": _binding("offer-source", 90_000),
            "connectionId": "payments-primary",
        }

        response, _authorize = self._invoke(
            "migrationPreview",
            {
                "sourceOfferVersionId": "offer-source",
                "targetOfferVersionId": "offer-target",
            },
        )

        body = json.loads(response["body"])["data"]
        self.assertEqual(set(body), {
            "commercialRequestId", "jobId", "state", "revision",
            "commandStatus", "dryRunRevision", "dryRunHash", "expiresAt", "counts",
        })
        self.assertNotIn("actor", repr(body).lower())
        self.assertNotIn("sourceoffer", repr(body).lower())

    def test_execute_replay_short_circuits_approval_and_integrations(self):
        replay = {
            "commercialRequestId": "migration-request-1",
            "jobId": "migration-job-1",
            "state": "scheduled",
            "revision": 3,
            "commandStatus": "accepted",
            "dryRunRevision": 2,
            "dryRunHash": "a" * 64,
            "expiresAt": NOW + 900,
            "counts": {
                "total": 0,
                "pending": 0,
                "applied": 0,
                "needsReview": 0,
                "failed": 0,
            },
        }
        self.store.replay_command.return_value = replay

        response, authorize = self._invoke(
            "migrationExecute",
            {
                "commercialRequestId": "migration-request-1",
                "dryRunRevision": 2,
                "dryRunHash": "a" * 64,
                "confirmation": True,
            },
        )

        self.assertEqual(json.loads(response["body"])["data"], replay)
        self.assertEqual(authorize.call_args.kwargs["capability"], "subscription:migration:execute")
        self.store.approve_execution.assert_not_called()
        self.gateway.execute.assert_not_called()

    def test_execute_requires_dedicated_capability_and_exact_confirmation(self):
        self.store.approve_execution.return_value = {
            "commercialRequestId": "migration-request-1",
            "jobId": "migration-job-1",
            "connectionId": "payments-primary",
            "revision": 2,
            "dryRunRevision": 2,
            "dryRunHash": "a" * 64,
        }
        self.store.record_command_result.return_value = {
            "commercialRequestId": "migration-request-1",
            "jobId": "migration-job-1",
            "state": "scheduled",
            "revision": 3,
            "commandStatus": "accepted",
            "dryRunRevision": 2,
            "dryRunHash": "a" * 64,
            "expiresAt": NOW + 900,
            "counts": {
                "total": 0,
                "pending": 0,
                "applied": 0,
                "needsReview": 0,
                "failed": 0,
            },
        }
        self.gateway.execute.return_value = {
            "commandId": "command-migration-2",
            "status": "accepted",
            "jobId": "migration-job-1",
            "revision": 3,
        }

        response, authorize = self._invoke(
            "migrationExecute",
            {
                "commercialRequestId": "migration-request-1",
                "dryRunRevision": 2,
                "dryRunHash": "a" * 64,
                "confirmation": True,
            },
        )

        self.assertEqual(response["statusCode"], 200, response["body"])
        self.assertEqual(authorize.call_args.kwargs["capability"], "subscription:migration:execute")
        self.assertTrue(authorize.call_args.kwargs["mutation"])
        self.store.approve_execution.assert_called_once()
        operation, _scope, command = self.gateway.execute.call_args.args[:3]
        self.assertEqual(operation, "migrationExecute")
        self.assertEqual(command, {
            "commercialRequestId": "migration-request-1",
            "jobId": "migration-job-1",
            "dryRunRevision": 2,
            "dryRunHash": "a" * 64,
            "confirmation": True,
        })

    def test_execute_returns_only_the_closed_needs_review_signal(self):
        self.store.approve_execution.return_value = {
            "commercialRequestId": "migration-request-1",
            "jobId": "migration-job-1",
            "connectionId": "payments-primary",
            "revision": 2,
            "dryRunRevision": 2,
            "dryRunHash": "a" * 64,
        }
        self.gateway.execute.return_value = {
            "commandId": "command-migration-review",
            "status": "needs_review",
            "jobId": "migration-job-1",
            "revision": 2,
        }
        self.store.record_command_result.return_value = {
            "commercialRequestId": "migration-request-1",
            "jobId": "migration-job-1",
            "state": "awaiting_approval",
            "revision": 2,
            "commandStatus": "needs_review",
            "dryRunRevision": 2,
            "dryRunHash": "a" * 64,
            "expiresAt": NOW + 900,
            "counts": {
                "total": 0,
                "pending": 0,
                "applied": 0,
                "needsReview": 0,
                "failed": 0,
            },
        }

        response, _authorize = self._invoke(
            "migrationExecute",
            {
                "commercialRequestId": "migration-request-1",
                "dryRunRevision": 2,
                "dryRunHash": "a" * 64,
                "confirmation": True,
            },
        )

        body = json.loads(response["body"])["data"]
        self.assertEqual(body["state"], "awaiting_approval")
        self.assertEqual(body["commandStatus"], "needs_review")
        self.assertNotIn("reason", repr(body).lower())
        self.assertNotIn("provider", repr(body).lower())

    def test_pending_preview_execute_and_control_are_retried_without_local_receipts(self):
        cases = (
            (
                "migrationPreview",
                {"sourceOfferVersionId": "offer-source", "targetOfferVersionId": "offer-target"},
                1,
            ),
            (
                "migrationExecute",
                {
                    "commercialRequestId": "migration-request-1",
                    "dryRunRevision": 2,
                    "dryRunHash": "a" * 64,
                    "confirmation": True,
                },
                3,
            ),
            (
                "migrationPause",
                {"commercialRequestId": "migration-request-1", "expectedRevision": 4},
                5,
            ),
        )
        for operation, input_value, revision in cases:
            with self.subTest(operation=operation):
                self.setUp()
                self.catalog.get_offer_version.side_effect = [
                    _offer("offer-source", 90_000),
                    _offer("offer-target", 120_000),
                    _offer("offer-source", 90_000),
                    _offer("offer-target", 120_000),
                ]
                bound = {
                    "commercialRequestId": "migration-request-1",
                    "jobId": "migration-job-1",
                    "connectionId": "payments-primary",
                    "revision": max(1, revision - 1),
                    "state": "running",
                }
                self.store.approve_execution.return_value = bound
                self.store.prepare_control.return_value = bound
                self.gateway.execute.side_effect = [
                    {
                        "commandId": "command-migration-1",
                        "status": "pending",
                        "jobId": "migration-job-1",
                        "revision": revision,
                    },
                    {
                        "commandId": "command-migration-1",
                        "status": "accepted",
                        "jobId": "migration-job-1",
                        "revision": revision,
                    },
                ]

                pending, _authorize = self._invoke(operation, input_value)
                self.assertEqual(pending["statusCode"], 503, pending["body"])
                self.store.record_command_result.assert_not_called()

                accepted, _authorize = self._invoke(operation, input_value)
                self.assertEqual(accepted["statusCode"], 200, accepted["body"])
                self.assertEqual(self.gateway.execute.call_count, 2)
                self.store.record_command_result.assert_called_once()
                self.assertEqual(
                    self.store.record_command_result.call_args.kwargs["actor_hash"],
                    hashlib.sha256(
                        b"test\0tenant-a\0draft-a\0example.com\0operator-1"
                    ).hexdigest(),
                )


def _binding(version_id: str, amount_minor: int) -> dict:
    selected = _offer(version_id, amount_minor)
    snapshot = selected.provider_snapshot()
    return {
        "offerVersionId": version_id,
        "revision": selected.revision,
        "schemaVersion": 1,
        "snapshot": snapshot,
        "contentHash": canonical_hash({"schemaVersion": 1, "snapshot": snapshot}),
    }


class MigrationStorageTests(unittest.TestCase):
    def setUp(self):
        from src.migration_storage import MigrationRequestStore

        self.backend = FakeBackend()
        self.scope = CommerceScope("test", "tenant-a", "draft-a", DOMAIN)
        self.store = MigrationRequestStore(self.backend, "operations-table")
        self.source = _binding("offer-source", 90_000)
        self.target = _binding("offer-target", 120_000)

    def record_result(
        self, *args, actor_hash="c" * 64, request_hash="f" * 64, **kwargs
    ):
        return self.store.record_command_result(
            *args, actor_hash=actor_hash, request_hash=request_hash, **kwargs
        )

    def replay_result(self, *args, request_hash="f" * 64, **kwargs):
        return self.store.replay_command(
            *args, request_hash=request_hash, **kwargs
        )

    def prepare(self, *, idempotency_key="migration-key-1"):
        return self.store.prepare_preview(
            self.scope,
            connection_id="payments-primary",
            source_offer=self.source,
            target_offer=self.target,
            requested_policy={"mode": "next_renewal"},
            candidate_scope={"kind": "all_matching_source_price"},
            canary_size=5,
            account_concurrency=2,
            actor_hash="a" * 64,
            idempotency_key=idempotency_key,
            request_id="request-migration-1",
            now_epoch=NOW,
        )

    def bind_preview(self):
        request = self.prepare()
        return self.record_result(
            self.scope,
            request["commercialRequestId"],
            operation="migrationPreview",
            idempotency_key="migration-key-1",
            result={
                "commandId": "command-migration-1",
                "status": "accepted",
                "jobId": "migration-job-1",
                "revision": 1,
            },
            now_epoch=NOW,
        )

    def preview_ready(self):
        bound = self.bind_preview()
        data = {
            "commercialRequestId": bound["commercialRequestId"],
            "jobId": "migration-job-1",
            "connectionId": "payments-primary",
            "revision": 2,
            "dedupeKey": "migration-event-dedupe-1",
            "dryRunRevision": 1,
            "dryRunHash": "b" * 64,
            "expiresAt": NOW + 900,
            "counts": {
                "total": 2,
                "pending": 2,
                "applied": 0,
                "needsReview": 0,
                "failed": 0,
            },
        }
        return self.store.apply_verified_event(
            self.scope,
            event_id="migration-event-1",
            event_type="migration.preview_ready.v1",
            occurred_at=NOW + 10,
            data=data,
            now_epoch=NOW + 10,
        )

    def test_storage_rejects_noncanonical_review_item_contract(self):
        ready = self.preview_ready()
        valid = {
            "commercialRequestId": ready["commercialRequestId"],
            "jobId": "migration-job-1",
            "connectionId": "payments-primary",
            "revision": 2,
            "dedupeKey": "migration-review-invalid",
            "itemId": MIGRATION_ITEM_1,
            "reasonCode": "source-drift",
        }
        for index, (field, value) in enumerate((
            ("itemId", "migration-item-1"),
            ("itemId", "migration-item-" + "g" * 40),
            ("reasonCode", "snapshot-drift"),
            ("reasonCode", "conflict"),
            ("reasonCode", "unknown-reason"),
        ), start=1):
            with self.subTest(field=field, value=value):
                with self.assertRaises(ValueError):
                    self.store.apply_verified_event(
                        self.scope,
                        event_id=f"migration-review-invalid-{index}",
                        event_type="migration.item_needs_review.v1",
                        occurred_at=NOW + 11,
                        data={
                            **valid,
                            "dedupeKey": f"migration-review-invalid-{index}",
                            field: value,
                        },
                        now_epoch=NOW + 11,
                    )

    def scheduled(self):
        ready = self.preview_ready()
        approved = self.store.approve_execution(
            self.scope,
            ready["commercialRequestId"],
            dry_run_revision=1,
            dry_run_hash="b" * 64,
            actor_hash="d" * 64,
            idempotency_key="execute-key-1",
            now_epoch=NOW + 20,
        )
        return self.record_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationExecute",
            idempotency_key="execute-key-1",
            result={
                "commandId": "command-migration-2",
                "status": "accepted",
                "jobId": "migration-job-1",
                "revision": 3,
            },
            now_epoch=NOW + 21,
        )

    def completed(self, state="completed"):
        scheduled = self.scheduled()
        running = self.store.apply_verified_event(
            self.scope,
            event_id="migration-running-for-rollback",
            event_type="migration.progressed.v1",
            occurred_at=NOW + 22,
            data={
                "commercialRequestId": scheduled["commercialRequestId"],
                "jobId": "migration-job-1",
                "connectionId": "payments-primary",
                "revision": 4,
                "dedupeKey": "migration-running-for-rollback-dedupe",
                "state": "running",
                "counts": {
                    "total": 2,
                    "pending": 2,
                    "applied": 0,
                    "needsReview": 0,
                    "failed": 0,
                },
            },
            now_epoch=NOW + 22,
        )
        return self.store.apply_verified_event(
            self.scope,
            event_id=f"migration-{state}-for-rollback",
            event_type="migration.completed.v1",
            occurred_at=NOW + 23,
            data={
                "commercialRequestId": running["commercialRequestId"],
                "jobId": "migration-job-1",
                "connectionId": "payments-primary",
                "revision": 5,
                "dedupeKey": f"migration-{state}-for-rollback-dedupe",
                "state": state,
                "counts": {
                    "total": 2,
                    "pending": 0,
                    "applied": 2 if state == "completed" else 1,
                    "needsReview": 0 if state == "completed" else 1,
                    "failed": 0,
                },
            },
            now_epoch=NOW + 23,
        )

    def test_preview_is_non_ttl_immutable_and_replays_only_the_exact_request(self):
        created = self.prepare()
        replay = self.prepare()

        self.assertEqual(replay, created)
        self.assertEqual(created["itemType"], "MigrationRequest")
        self.assertEqual((
            created["state"], created["revision"], created["stateRevision"], created["jobId"],
        ), (
            "draft",
            0,
            0,
            None,
        ))
        self.assertNotIn("expiresAt", created)
        self.assertNotIn("migration-key-1", repr(created))
        self.assertEqual(created["sourceOffer"], self.source)
        self.assertEqual(created["targetOffer"], self.target)
        self.assertEqual(created["actorHash"], "a" * 64)

        changed = _binding("offer-target", 130_000)
        with self.assertRaises(StorageConflict):
            self.store.prepare_preview(
                self.scope,
                connection_id="payments-primary",
                source_offer=self.source,
                target_offer=changed,
                requested_policy={"mode": "next_renewal"},
                candidate_scope={"kind": "all_matching_source_price"},
                canary_size=5,
                account_concurrency=2,
                actor_hash="a" * 64,
                idempotency_key="migration-key-1",
                request_id="request-migration-1",
                now_epoch=NOW,
            )

    def test_preview_offer_amount_uses_the_canonical_catalog_bounds(self):
        zero = _binding("offer-free", 0)
        maximum = _binding("offer-maximum", 99_999_999)
        accepted = self.store.prepare_preview(
            self.scope,
            connection_id="payments-primary",
            source_offer=zero,
            target_offer=maximum,
            requested_policy={"mode": "next_renewal"},
            candidate_scope={"kind": "all_matching_source_price"},
            canary_size=5,
            account_concurrency=2,
            actor_hash="a" * 64,
            idempotency_key="migration-bounds-key",
            request_id="request-migration-bounds",
            now_epoch=NOW,
        )
        self.assertEqual(accepted["sourceOffer"]["snapshot"]["amountMinor"], 0)
        self.assertEqual(
            accepted["targetOffer"]["snapshot"]["amountMinor"], 99_999_999
        )

        oversized = {**maximum, "snapshot": {**maximum["snapshot"], "amountMinor": 100_000_000}}
        oversized["contentHash"] = canonical_hash({
            "schemaVersion": 1,
            "snapshot": oversized["snapshot"],
        })
        with self.assertRaises(ValueError):
            self.store.prepare_preview(
                self.scope,
                connection_id="payments-primary",
                source_offer=zero,
                target_offer=oversized,
                requested_policy={"mode": "next_renewal"},
                candidate_scope={"kind": "all_matching_source_price"},
                canary_size=5,
                account_concurrency=2,
                actor_hash="a" * 64,
                idempotency_key="migration-oversized-key",
                request_id="request-migration-oversized",
                now_epoch=NOW,
            )

    def test_preview_rejects_cross_currency_or_cross_cadence_offer_pairs(self):
        mismatches = []
        for field, replacement in (
            ("currency", "USD"),
            (
                "recurrence",
                {"interval": "year", "intervalCount": 1, "usageType": "licensed"},
            ),
        ):
            target = copy.deepcopy(self.target)
            target["snapshot"][field] = replacement
            target["contentHash"] = canonical_hash({
                "schemaVersion": 1,
                "snapshot": target["snapshot"],
            })
            mismatches.append((field, target))

        for field, target in mismatches:
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.store.prepare_preview(
                    self.scope,
                    connection_id="payments-primary",
                    source_offer=self.source,
                    target_offer=target,
                    requested_policy={"mode": "next_renewal"},
                    candidate_scope={"kind": "all_matching_source_price"},
                    canary_size=5,
                    account_concurrency=2,
                    actor_hash="a" * 64,
                    idempotency_key=f"migration-{field}-key",
                    request_id=f"request-migration-{field}",
                    now_epoch=NOW,
                )

    def test_pending_preview_command_does_not_advance_or_create_a_receipt(self):
        draft = self.prepare()
        with self.assertRaises(StorageOutcomeUnknown):
            self.record_result(
                self.scope,
                draft["commercialRequestId"],
                operation="migrationPreview",
                idempotency_key="migration-key-1",
                result={
                    "commandId": "command-migration-pending",
                    "status": "pending",
                    "jobId": "migration-job-1",
                    "revision": 1,
                },
                now_epoch=NOW + 1,
            )

        current = self.store.get_request(self.scope, draft["commercialRequestId"])
        self.assertEqual((current["state"], current["revision"], current["jobId"]), (
            "draft",
            0,
            None,
        ))
        self.assertFalse(any(
            item.get("itemType") == "MigrationCommandReceipt"
            for item in self.backend.items.values()
        ))

    def test_needs_review_preview_is_rejected_by_storage_without_corrupting_draft(self):
        draft = self.prepare()
        with self.assertRaises(StorageConflict):
            self.record_result(
                self.scope,
                draft["commercialRequestId"],
                operation="migrationPreview",
                idempotency_key="migration-key-1",
                result={
                    "commandId": "command-migration-review",
                    "status": "needs_review",
                    "jobId": "migration-job-1",
                    "revision": 1,
                },
                now_epoch=NOW + 1,
            )

        current = self.store.get_request(self.scope, draft["commercialRequestId"])
        self.assertEqual((current["state"], current["revision"], current["jobId"]), (
            "draft", 0, None,
        ))
        self.assertFalse(any(
            item.get("itemType") == "MigrationCommandReceipt"
            for item in self.backend.items.values()
        ))

    def test_exact_unexpired_preview_can_be_approved_once_with_actor_hash_only(self):
        ready = self.preview_ready()
        approved = self.store.approve_execution(
            self.scope,
            ready["commercialRequestId"],
            dry_run_revision=1,
            dry_run_hash="b" * 64,
            actor_hash="d" * 64,
            idempotency_key="execute-key-1",
            now_epoch=NOW + 20,
        )

        self.assertEqual(approved["approval"], {
            "dryRunRevision": 1,
            "dryRunHash": "b" * 64,
            "actorHash": "d" * 64,
            "approvedAt": NOW + 20,
        })
        self.assertNotIn("execute-key-1", repr(approved))
        self.assertEqual(
            self.store.approve_execution(
                self.scope,
                ready["commercialRequestId"],
                dry_run_revision=1,
                dry_run_hash="b" * 64,
                actor_hash="d" * 64,
                idempotency_key="execute-key-1",
                now_epoch=NOW + 21,
            ),
            approved,
        )
        with self.assertRaises(StorageConflict):
            self.store.approve_execution(
                self.scope,
                ready["commercialRequestId"],
                dry_run_revision=1,
                dry_run_hash="c" * 64,
                actor_hash="d" * 64,
                idempotency_key="execute-key-2",
                now_epoch=NOW + 30,
            )
        with self.assertRaises(StorageConflict):
            self.store.approve_execution(
                self.scope,
                ready["commercialRequestId"],
                dry_run_revision=1,
                dry_run_hash="b" * 64,
                actor_hash="d" * 64,
                idempotency_key="execute-key-expired",
                now_epoch=NOW + 900,
            )

    def test_events_are_idempotent_scope_bound_and_revision_monotonic(self):
        ready = self.preview_ready()
        replay = self.store.apply_verified_event(
            self.scope,
            event_id="migration-event-1",
            event_type="migration.preview_ready.v1",
            occurred_at=NOW + 10,
            data={
                "commercialRequestId": ready["commercialRequestId"],
                "jobId": "migration-job-1",
                "connectionId": "payments-primary",
                "revision": 2,
                "dedupeKey": "migration-event-dedupe-1",
                "dryRunRevision": 1,
                "dryRunHash": "b" * 64,
                "expiresAt": NOW + 900,
                "counts": {
                    "total": 2,
                    "pending": 2,
                    "applied": 0,
                    "needsReview": 0,
                    "failed": 0,
                },
            },
            now_epoch=NOW + 10,
        )
        self.assertEqual(replay, ready)

        with self.assertRaises(StorageConflict):
            self.store.apply_verified_event(
                self.scope,
                event_id="migration-event-2",
                event_type="migration.progressed.v1",
                occurred_at=NOW + 20,
                data={
                    "commercialRequestId": ready["commercialRequestId"],
                    "jobId": "migration-job-other",
                    "connectionId": "payments-primary",
                    "revision": 3,
                    "dedupeKey": "migration-event-dedupe-2",
                    "state": "running",
                    "counts": {
                        "total": 2,
                        "pending": 2,
                        "applied": 0,
                        "needsReview": 0,
                        "failed": 0,
                    },
                },
                now_epoch=NOW + 20,
            )

        approved = self.store.approve_execution(
            self.scope,
            ready["commercialRequestId"],
            dry_run_revision=1,
            dry_run_hash="b" * 64,
            actor_hash="d" * 64,
            idempotency_key="execute-key-1",
                now_epoch=NOW + 21,
            )

    def test_execute_needs_review_is_a_durable_closed_command_status(self):
        ready = self.preview_ready()
        approved = self.store.approve_execution(
            self.scope,
            ready["commercialRequestId"],
            dry_run_revision=1,
            dry_run_hash="b" * 64,
            actor_hash="d" * 64,
            idempotency_key="execute-needs-review-key",
            now_epoch=NOW + 20,
        )

        result = self.record_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationExecute",
            idempotency_key="execute-needs-review-key",
            result={
                "commandId": "command-needs-review",
                "status": "needs_review",
                "jobId": "migration-job-1",
                "revision": 2,
            },
            now_epoch=NOW + 21,
        )
        replay = self.replay_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationExecute",
            idempotency_key="execute-needs-review-key",
        )
        stored = self.store.get_request(self.scope, approved["commercialRequestId"])

        self.assertEqual(result["state"], "awaiting_approval")
        self.assertEqual(result["commandStatus"], "needs_review")
        self.assertEqual(replay, result)
        self.assertEqual(stored["lastCommand"]["operation"], "migrationExecute")
        self.assertEqual(stored["lastCommand"]["result"]["status"], "needs_review")
        self.assertNotIn("reason", repr(result).lower())
        self.record_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationExecute",
            idempotency_key="execute-key-1",
            result={
                "commandId": "command-migration-2",
                "status": "accepted",
                "jobId": "migration-job-1",
                "revision": 3,
            },
            now_epoch=NOW + 22,
        )

        progressed = self.store.apply_verified_event(
            self.scope,
            event_id="migration-event-3",
            event_type="migration.progressed.v1",
            occurred_at=NOW + 30,
            data={
                "commercialRequestId": ready["commercialRequestId"],
                "jobId": "migration-job-1",
                "connectionId": "payments-primary",
                "revision": 4,
                "dedupeKey": "migration-event-dedupe-3",
                "state": "running",
                "counts": {
                    "total": 2,
                    "pending": 1,
                    "applied": 1,
                    "needsReview": 0,
                    "failed": 0,
                },
            },
            now_epoch=NOW + 30,
        )
        self.assertEqual((progressed["state"], progressed["revision"]), ("running", 4))

        stale = self.store.apply_verified_event(
            self.scope,
            event_id="migration-event-4",
            event_type="migration.item_needs_review.v1",
            occurred_at=NOW + 25,
            data={
                "commercialRequestId": ready["commercialRequestId"],
                "jobId": "migration-job-1",
                "connectionId": "payments-primary",
                "revision": 3,
                "dedupeKey": "migration-event-dedupe-4",
                "itemId": MIGRATION_ITEM_1,
                "reasonCode": "source-drift",
            },
            now_epoch=NOW + 40,
        )
        self.assertTrue(stale["stale"])
        self.assertEqual(self.store.get_request(self.scope, ready["commercialRequestId"])["state"], "running")

    def test_event_receipt_projection_is_hash_and_scope_bound(self):
        ready = self.preview_ready()
        receipt_key, receipt = next(
            (key, copy.deepcopy(item))
            for key, item in self.backend.items.items()
            if item.get("itemType") == "MigrationEventInbox"
        )
        receipt["result"]["commercialRequestId"] = "migration-request-other"
        self.backend.items[receipt_key] = receipt
        event = {
            "commercialRequestId": ready["commercialRequestId"],
            "jobId": "migration-job-1",
            "connectionId": "payments-primary",
            "revision": 2,
            "dedupeKey": "migration-event-dedupe-1",
            "dryRunRevision": 1,
            "dryRunHash": "b" * 64,
            "expiresAt": NOW + 900,
            "counts": {
                "total": 2,
                "pending": 2,
                "applied": 0,
                "needsReview": 0,
                "failed": 0,
            },
        }

        with self.assertRaises(StorageConflict):
            self.store.apply_verified_event(
                self.scope,
                event_id="migration-event-1",
                event_type="migration.preview_ready.v1",
                occurred_at=NOW + 10,
                data=event,
                now_epoch=NOW + 10,
            )

    def test_multiple_review_items_at_the_current_revision_are_recorded_idempotently(self):
        ready = self.preview_ready()
        approved = self.store.approve_execution(
            self.scope,
            ready["commercialRequestId"],
            dry_run_revision=1,
            dry_run_hash="b" * 64,
            actor_hash="d" * 64,
            idempotency_key="execute-key-1",
            now_epoch=NOW + 20,
        )
        self.record_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationExecute",
            idempotency_key="execute-key-1",
            result={
                "commandId": "command-migration-2",
                "status": "accepted",
                "jobId": "migration-job-1",
                "revision": 3,
            },
            now_epoch=NOW + 21,
        )
        self.store.apply_verified_event(
            self.scope,
            event_id="migration-progress-1",
            event_type="migration.progressed.v1",
            occurred_at=NOW + 22,
            data={
                "commercialRequestId": approved["commercialRequestId"],
                "jobId": "migration-job-1",
                "connectionId": "payments-primary",
                "revision": 4,
                "dedupeKey": "migration-progress-dedupe-1",
                "state": "running",
                "counts": {
                    "total": 2,
                    "pending": 2,
                    "applied": 0,
                    "needsReview": 0,
                    "failed": 0,
                },
            },
            now_epoch=NOW + 22,
        )

        results = []
        for index, item_id in ((1, MIGRATION_ITEM_1), (2, MIGRATION_ITEM_2)):
            results.append(self.store.apply_verified_event(
                self.scope,
                event_id=f"migration-review-{index}",
                event_type="migration.item_needs_review.v1",
                occurred_at=NOW + 22 + index,
                data={
                    "commercialRequestId": approved["commercialRequestId"],
                    "jobId": "migration-job-1",
                    "connectionId": "payments-primary",
                    "revision": 4,
                    "dedupeKey": f"migration-review-dedupe-{index}",
                    "itemId": item_id,
                    "reasonCode": "source-drift",
                },
                now_epoch=NOW + 22 + index,
            ))

        current = self.store.get_request(self.scope, approved["commercialRequestId"])
        self.assertEqual([(value["stale"], value["revision"]) for value in results], [
            (False, 4),
            (False, 4),
        ])
        self.assertEqual(current["state"], "running")
        self.assertEqual(current["revision"], 4)
        self.assertEqual(current["lastNeedsReview"], {
            "itemId": MIGRATION_ITEM_2,
            "reasonCode": "source-drift",
        })
        self.assertIsNotNone(self.backend.get(
            OPERATIONS_TABLE,
            self.scope.partition_key,
            "MIGRATION_EVENT#migration-review-dedupe-1",
        ))
        self.assertIsNotNone(self.backend.get(
            OPERATIONS_TABLE,
            self.scope.partition_key,
            "MIGRATION_EVENT#migration-review-dedupe-2",
        ))

    def test_same_revision_legal_progress_can_follow_an_earlier_review_event(self):
        scheduled = self.scheduled()
        review = {
            "commercialRequestId": scheduled["commercialRequestId"],
            "jobId": "migration-job-1",
            "connectionId": "payments-primary",
            "revision": 4,
            "dedupeKey": "migration-review-before-progress-dedupe",
            "itemId": MIGRATION_ITEM_1,
            "reasonCode": "source-drift",
        }
        self.store.apply_verified_event(
            self.scope,
            event_id="migration-review-before-progress",
            event_type="migration.item_needs_review.v1",
            occurred_at=NOW + 22,
            data=review,
            now_epoch=NOW + 22,
        )
        progress = {
            "commercialRequestId": scheduled["commercialRequestId"],
            "jobId": "migration-job-1",
            "connectionId": "payments-primary",
            "revision": 4,
            "dedupeKey": "migration-progress-after-review-dedupe",
            "state": "running",
            "counts": {
                "total": 2,
                "pending": 1,
                "applied": 0,
                "needsReview": 1,
                "failed": 0,
            },
        }

        progressed = self.store.apply_verified_event(
            self.scope,
            event_id="migration-progress-after-review",
            event_type="migration.progressed.v1",
            occurred_at=NOW + 23,
            data=progress,
            now_epoch=NOW + 23,
        )
        replay = self.store.apply_verified_event(
            self.scope,
            event_id="migration-progress-after-review",
            event_type="migration.progressed.v1",
            occurred_at=NOW + 23,
            data=progress,
            now_epoch=NOW + 23,
        )

        self.assertEqual((progressed["state"], progressed["revision"]), ("running", 4))
        self.assertEqual(replay, progressed)
        current = self.store.get_request(self.scope, scheduled["commercialRequestId"])
        self.assertEqual(current["lastNeedsReview"], {
            "itemId": MIGRATION_ITEM_1,
            "reasonCode": "source-drift",
        })
        self.assertEqual(current["stateRevision"], 4)

    def test_stale_execute_needs_review_never_replays_as_a_previous_accepted_command(self):
        ready = self.preview_ready()
        approved = self.store.approve_execution(
            self.scope,
            ready["commercialRequestId"],
            dry_run_revision=1,
            dry_run_hash="b" * 64,
            actor_hash="d" * 64,
            idempotency_key="execute-missing-job-key",
            now_epoch=NOW + 20,
        )

        result = self.record_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationExecute",
            idempotency_key="execute-missing-job-key",
            result={
                "commandId": "command-missing-job-review",
                "status": "needs_review",
                "jobId": "migration-job-1",
                "revision": 1,
            },
            now_epoch=NOW + 21,
        )
        replay = self.replay_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationExecute",
            idempotency_key="execute-missing-job-key",
        )
        stored = self.store.get_request(self.scope, approved["commercialRequestId"])

        self.assertEqual(result["commandStatus"], "needs_review")
        self.assertEqual(replay, result)
        self.assertEqual(stored["lastCommand"]["operation"], "migrationExecute")
        self.assertEqual(stored["lastCommand"]["result"]["status"], "needs_review")

    def test_same_revision_progress_after_review_rejects_an_illegal_transition(self):
        scheduled = self.scheduled()
        self.store.apply_verified_event(
            self.scope,
            event_id="migration-review-before-illegal-progress",
            event_type="migration.item_needs_review.v1",
            occurred_at=NOW + 22,
            data={
                "commercialRequestId": scheduled["commercialRequestId"],
                "jobId": "migration-job-1",
                "connectionId": "payments-primary",
                "revision": 4,
                "dedupeKey": "migration-review-before-illegal-progress-dedupe",
                "itemId": MIGRATION_ITEM_1,
                "reasonCode": "source-drift",
            },
            now_epoch=NOW + 22,
        )

        with self.assertRaises(StorageConflict):
            self.store.apply_verified_event(
                self.scope,
                event_id="migration-illegal-progress-after-review",
                event_type="migration.progressed.v1",
                occurred_at=NOW + 23,
                data={
                    "commercialRequestId": scheduled["commercialRequestId"],
                    "jobId": "migration-job-1",
                    "connectionId": "payments-primary",
                    "revision": 4,
                    "dedupeKey": "migration-illegal-progress-after-review-dedupe",
                    "state": "previewing",
                    "counts": scheduled["counts"],
                },
                now_epoch=NOW + 23,
            )
        current = self.store.get_request(self.scope, scheduled["commercialRequestId"])
        self.assertEqual((current["state"], current["revision"]), ("scheduled", 4))

    def test_same_revision_cannot_apply_two_distinct_state_transitions(self):
        scheduled = self.scheduled()
        self.store.apply_verified_event(
            self.scope,
            event_id="migration-review-before-transition-chain",
            event_type="migration.item_needs_review.v1",
            occurred_at=NOW + 22,
            data={
                "commercialRequestId": scheduled["commercialRequestId"],
                "jobId": "migration-job-1",
                "connectionId": "payments-primary",
                "revision": 4,
                "dedupeKey": "migration-review-before-transition-chain-dedupe",
                "itemId": MIGRATION_ITEM_1,
                "reasonCode": "source-drift",
            },
            now_epoch=NOW + 22,
        )
        self.store.apply_verified_event(
            self.scope,
            event_id="migration-running-transition-chain",
            event_type="migration.progressed.v1",
            occurred_at=NOW + 23,
            data={
                "commercialRequestId": scheduled["commercialRequestId"],
                "jobId": "migration-job-1",
                "connectionId": "payments-primary",
                "revision": 4,
                "dedupeKey": "migration-running-transition-chain-dedupe",
                "state": "running",
                "counts": {
                    "total": 2,
                    "pending": 1,
                    "applied": 0,
                    "needsReview": 1,
                    "failed": 0,
                },
            },
            now_epoch=NOW + 23,
        )

        with self.assertRaises(StorageConflict):
            self.store.apply_verified_event(
                self.scope,
                event_id="migration-paused-transition-chain",
                event_type="migration.progressed.v1",
                occurred_at=NOW + 24,
                data={
                    "commercialRequestId": scheduled["commercialRequestId"],
                    "jobId": "migration-job-1",
                    "connectionId": "payments-primary",
                    "revision": 4,
                    "dedupeKey": "migration-paused-transition-chain-dedupe",
                    "state": "paused",
                    "counts": {
                        "total": 2,
                        "pending": 1,
                        "applied": 0,
                        "needsReview": 1,
                        "failed": 0,
                    },
                },
                now_epoch=NOW + 24,
            )
        current = self.store.get_request(self.scope, scheduled["commercialRequestId"])
        self.assertEqual((current["state"], current["revision"]), ("running", 4))

    def test_command_receipt_replays_after_later_commands_without_provider_reexecution(self):
        ready = self.preview_ready()
        approved = self.store.approve_execution(
            self.scope,
            ready["commercialRequestId"],
            dry_run_revision=1,
            dry_run_hash="b" * 64,
            actor_hash="d" * 64,
            idempotency_key="execute-key-1",
            now_epoch=NOW + 20,
        )
        execute_result = {
            "commandId": "command-migration-2",
            "status": "accepted",
            "jobId": "migration-job-1",
            "revision": 3,
        }
        first = self.record_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationExecute",
            idempotency_key="execute-key-1",
            result=execute_result,
            now_epoch=NOW + 21,
        )
        self.store.apply_verified_event(
            self.scope,
            event_id="migration-progress-1",
            event_type="migration.progressed.v1",
            occurred_at=NOW + 22,
            data={
                "commercialRequestId": approved["commercialRequestId"],
                "jobId": "migration-job-1",
                "connectionId": "payments-primary",
                "revision": 4,
                "dedupeKey": "migration-progress-dedupe-1",
                "state": "running",
                "counts": {
                    "total": 2,
                    "pending": 2,
                    "applied": 0,
                    "needsReview": 0,
                    "failed": 0,
                },
            },
            now_epoch=NOW + 22,
        )
        paused = self.record_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationPause",
            idempotency_key="pause-key-1",
            result={
                "commandId": "command-migration-3",
                "status": "accepted",
                "jobId": "migration-job-1",
                "revision": 5,
            },
            now_epoch=NOW + 23,
        )
        replay = self.record_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationExecute",
            idempotency_key="execute-key-1",
            result=execute_result,
            now_epoch=NOW + 24,
        )

        self.assertEqual(replay, first)
        self.assertEqual(
            self.replay_result(
                self.scope,
                approved["commercialRequestId"],
                operation="migrationExecute",
                idempotency_key="execute-key-1",
            ),
            first,
        )
        self.assertEqual(paused["state"], "paused")
        self.assertEqual(
            self.store.get_request(self.scope, approved["commercialRequestId"])["state"],
            "paused",
        )

    def test_command_receipt_rejects_idempotency_key_reuse_with_changed_input(self):
        ready = self.preview_ready()
        approved = self.store.approve_execution(
            self.scope,
            ready["commercialRequestId"],
            dry_run_revision=1,
            dry_run_hash="b" * 64,
            actor_hash="d" * 64,
            idempotency_key="execute-input-bound-key",
            now_epoch=NOW + 20,
        )
        self.record_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationExecute",
            idempotency_key="execute-input-bound-key",
            request_hash="1" * 64,
            result={
                "commandId": "command-input-bound-execute",
                "status": "needs_review",
                "jobId": "migration-job-1",
                "revision": 2,
            },
            now_epoch=NOW + 21,
        )

        self.assertEqual(
            self.replay_result(
                self.scope,
                approved["commercialRequestId"],
                operation="migrationExecute",
                idempotency_key="execute-input-bound-key",
                request_hash="1" * 64,
            )["commandStatus"],
            "needs_review",
        )
        with self.assertRaises(StorageConflict):
            self.replay_result(
                self.scope,
                approved["commercialRequestId"],
                operation="migrationExecute",
                idempotency_key="execute-input-bound-key",
                request_hash="2" * 64,
            )

        self.setUp()
        scheduled = self.scheduled()
        running = self.store.apply_verified_event(
            self.scope,
            event_id="migration-running-before-input-bound-pause",
            event_type="migration.progressed.v1",
            occurred_at=NOW + 22,
            data={
                "commercialRequestId": scheduled["commercialRequestId"],
                "jobId": "migration-job-1",
                "connectionId": "payments-primary",
                "revision": 4,
                "dedupeKey": "migration-running-before-input-bound-pause-dedupe",
                "state": "running",
                "counts": scheduled["counts"],
            },
            now_epoch=NOW + 22,
        )
        self.record_result(
            self.scope,
            running["commercialRequestId"],
            operation="migrationPause",
            idempotency_key="reused-control-key",
            request_hash="3" * 64,
            result={
                "commandId": "command-input-bound-pause",
                "status": "accepted",
                "jobId": "migration-job-1",
                "revision": 5,
            },
            now_epoch=NOW + 23,
        )
        self.record_result(
            self.scope,
            running["commercialRequestId"],
            operation="migrationResume",
            idempotency_key="resume-after-input-bound-pause",
            request_hash="4" * 64,
            result={
                "commandId": "command-resume-after-input-bound-pause",
                "status": "accepted",
                "jobId": "migration-job-1",
                "revision": 6,
            },
            now_epoch=NOW + 24,
        )

        with self.assertRaises(StorageConflict):
            self.replay_result(
                self.scope,
                running["commercialRequestId"],
                operation="migrationPause",
                idempotency_key="reused-control-key",
                request_hash="5" * 64,
            )
        current = self.store.get_request(self.scope, running["commercialRequestId"])
        self.assertEqual((current["state"], current["revision"]), ("running", 6))

    def test_storage_rejects_provider_revision_jumps_without_writing_a_receipt(self):
        ready = self.preview_ready()
        approved = self.store.approve_execution(
            self.scope,
            ready["commercialRequestId"],
            dry_run_revision=1,
            dry_run_hash="b" * 64,
            actor_hash="d" * 64,
            idempotency_key="execute-revision-jump-key",
            now_epoch=NOW + 20,
        )

        for status in ("accepted", "needs_review"):
            with self.subTest(status=status), self.assertRaises(StorageConflict):
                self.record_result(
                    self.scope,
                    approved["commercialRequestId"],
                    operation="migrationExecute",
                    idempotency_key=f"execute-revision-jump-{status}",
                    request_hash=("6" if status == "accepted" else "7") * 64,
                    result={
                        "commandId": f"command-revision-jump-{status}",
                        "status": status,
                        "jobId": "migration-job-1",
                        "revision": 999,
                    },
                    now_epoch=NOW + 21,
                )

        current = self.store.get_request(self.scope, approved["commercialRequestId"])
        self.assertEqual((current["state"], current["revision"]), (
            "awaiting_approval", 2,
        ))
        self.assertFalse(any(
            item.get("itemType") == "MigrationCommandReceipt"
            and item.get("operation") == "migrationExecute"
            for item in self.backend.items.values()
        ))

    def test_command_receipt_survives_a_progress_event_that_wins_the_storage_race(self):
        ready = self.preview_ready()
        approved = self.store.approve_execution(
            self.scope,
            ready["commercialRequestId"],
            dry_run_revision=1,
            dry_run_hash="b" * 64,
            actor_hash="d" * 64,
            idempotency_key="execute-key-race",
            now_epoch=NOW + 20,
        )
        for revision, state in ((3, "scheduled"), (4, "running")):
            self.store.apply_verified_event(
                self.scope,
                event_id=f"migration-progress-race-{revision}",
                event_type="migration.progressed.v1",
                occurred_at=NOW + 18 + revision,
                data={
                    "commercialRequestId": approved["commercialRequestId"],
                    "jobId": "migration-job-1",
                    "connectionId": "payments-primary",
                    "revision": revision,
                    "dedupeKey": f"migration-progress-race-dedupe-{revision}",
                    "state": state,
                    "counts": {
                        "total": 2,
                        "pending": 2,
                        "applied": 0,
                        "needsReview": 0,
                        "failed": 0,
                    },
                },
                now_epoch=NOW + 18 + revision,
            )
        provider_result = {
            "commandId": "command-migration-race",
            "status": "accepted",
            "jobId": "migration-job-1",
            "revision": 3,
        }

        recorded = self.record_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationExecute",
            idempotency_key="execute-key-race",
            result=provider_result,
            now_epoch=NOW + 22,
        )
        replay = self.replay_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationExecute",
            idempotency_key="execute-key-race",
        )

        self.assertEqual(recorded, replay)
        self.assertEqual((recorded["state"], recorded["revision"]), ("running", 4))
        current = self.store.get_request(self.scope, approved["commercialRequestId"])
        self.assertEqual((current["state"], current["revision"]), ("running", 4))

    def test_command_receipt_survives_an_event_between_read_and_transaction(self):
        scheduled = self.scheduled()
        running = self.store.apply_verified_event(
            self.scope,
            event_id="migration-running-before-pause-race",
            event_type="migration.progressed.v1",
            occurred_at=NOW + 22,
            data={
                "commercialRequestId": scheduled["commercialRequestId"],
                "jobId": "migration-job-1",
                "connectionId": "payments-primary",
                "revision": 4,
                "dedupeKey": "migration-running-before-pause-race-dedupe",
                "state": "running",
                "counts": {
                    "total": 2,
                    "pending": 2,
                    "applied": 0,
                    "needsReview": 0,
                    "failed": 0,
                },
            },
            now_epoch=NOW + 22,
        )

        def progress_before_command_receipt():
            self.store.apply_verified_event(
                self.scope,
                event_id="migration-paused-wins-command-race",
                event_type="migration.progressed.v1",
                occurred_at=NOW + 23,
                data={
                    "commercialRequestId": running["commercialRequestId"],
                    "jobId": "migration-job-1",
                    "connectionId": "payments-primary",
                    "revision": 5,
                    "dedupeKey": "migration-paused-wins-command-race-dedupe",
                    "state": "paused",
                    "counts": {
                        "total": 2,
                        "pending": 2,
                        "applied": 0,
                        "needsReview": 0,
                        "failed": 0,
                    },
                },
                now_epoch=NOW + 23,
            )

        self.backend.before_transact = progress_before_command_receipt
        provider_result = {
            "commandId": "command-pause-race",
            "status": "accepted",
            "jobId": "migration-job-1",
            "revision": 5,
        }

        recorded = self.record_result(
            self.scope,
            running["commercialRequestId"],
            operation="migrationPause",
            idempotency_key="pause-race-key",
            result=provider_result,
            now_epoch=NOW + 23,
        )
        replay = self.replay_result(
            self.scope,
            running["commercialRequestId"],
            operation="migrationPause",
            idempotency_key="pause-race-key",
        )

        self.assertEqual(recorded, replay)
        self.assertEqual((recorded["state"], recorded["revision"]), ("paused", 5))
        self.assertEqual(recorded["commandStatus"], "accepted")

    def test_command_receipt_projection_is_hash_and_request_bound(self):
        ready = self.preview_ready()
        approved = self.store.approve_execution(
            self.scope,
            ready["commercialRequestId"],
            dry_run_revision=1,
            dry_run_hash="b" * 64,
            actor_hash="d" * 64,
            idempotency_key="execute-key-bound",
            now_epoch=NOW + 20,
        )
        self.record_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationExecute",
            idempotency_key="execute-key-bound",
            result={
                "commandId": "command-migration-bound",
                "status": "accepted",
                "jobId": "migration-job-1",
                "revision": 3,
            },
            now_epoch=NOW + 21,
        )
        receipt_key, original_receipt = next(
            (key, copy.deepcopy(item))
            for key, item in self.backend.items.items()
            if item.get("itemType") == "MigrationCommandReceipt"
            and item.get("operation") == "migrationExecute"
        )
        receipt = copy.deepcopy(original_receipt)
        receipt["result"]["commercialRequestId"] = "migration-request-other"
        self.backend.items[receipt_key] = receipt

        with self.assertRaises(StorageConflict):
            self.replay_result(
                self.scope,
                approved["commercialRequestId"],
                operation="migrationExecute",
                idempotency_key="execute-key-bound",
            )

        receipt = copy.deepcopy(original_receipt)
        receipt["result"]["commandStatus"] = "pending"
        receipt["receiptHash"] = canonical_hash({
            "commercialRequestId": receipt["commercialRequestId"],
            "operation": receipt["operation"],
            "idempotencyDigest": receipt["idempotencyDigest"],
            "requestHash": receipt["requestHash"],
            "actorHash": receipt["actorHash"],
            "commandResult": receipt["commandResult"],
            "result": receipt["result"],
        })
        self.backend.items[receipt_key] = receipt
        with self.assertRaises(StorageConflict):
            self.replay_result(
                self.scope,
                approved["commercialRequestId"],
                operation="migrationExecute",
                idempotency_key="execute-key-bound",
            )

    def test_cancel_from_previewing_is_a_valid_nonterminal_control_transition(self):
        preview = self.bind_preview()
        self.store.prepare_control(
            self.scope,
            preview["commercialRequestId"],
            action="cancel",
            expected_revision=1,
        )
        canceled = self.record_result(
            self.scope,
            preview["commercialRequestId"],
            operation="migrationCancel",
            idempotency_key="cancel-key-1",
            result={
                "commandId": "command-migration-cancel",
                "status": "accepted",
                "jobId": "migration-job-1",
                "revision": 2,
            },
            now_epoch=NOW + 1,
        )
        self.assertEqual(canceled["state"], "cancel_requested")

    def test_cancel_reopens_a_completed_next_renewal_job_for_schedule_rollback(self):
        for terminal_state in ("completed", "completed_with_errors"):
            with self.subTest(terminal_state=terminal_state):
                self.setUp()
                completed = self.completed(terminal_state)

                self.store.prepare_control(
                    self.scope,
                    completed["commercialRequestId"],
                    action="cancel",
                    expected_revision=5,
                )
                requested = self.record_result(
                    self.scope,
                    completed["commercialRequestId"],
                    operation="migrationCancel",
                    idempotency_key=f"cancel-{terminal_state}-rollback",
                    result={
                        "commandId": f"command-{terminal_state}-rollback",
                        "status": "accepted",
                        "jobId": "migration-job-1",
                        "revision": 6,
                    },
                    now_epoch=NOW + 24,
                )

                self.assertEqual(
                    (requested["state"], requested["revision"]),
                    ("cancel_requested", 6),
                )

    def test_same_revision_cancel_completion_is_authoritative_and_idempotent(self):
        preview = self.bind_preview()
        command = {
            "commandId": "command-migration-cancel",
            "status": "accepted",
            "jobId": "migration-job-1",
            "revision": 2,
        }
        requested = self.record_result(
            self.scope,
            preview["commercialRequestId"],
            operation="migrationCancel",
            idempotency_key="cancel-key-same-revision",
            result=command,
            now_epoch=NOW + 1,
        )
        completion = {
            "commercialRequestId": preview["commercialRequestId"],
            "jobId": "migration-job-1",
            "connectionId": "payments-primary",
            "revision": 2,
            "dedupeKey": "migration-cancel-completed-dedupe",
            "state": "canceled",
            "counts": {
                "total": 0,
                "pending": 0,
                "applied": 0,
                "needsReview": 0,
                "failed": 0,
            },
        }

        completed = self.store.apply_verified_event(
            self.scope,
            event_id="migration-cancel-completed",
            event_type="migration.completed.v1",
            occurred_at=NOW + 2,
            data=completion,
            now_epoch=NOW + 2,
        )
        replay = self.store.apply_verified_event(
            self.scope,
            event_id="migration-cancel-completed",
            event_type="migration.completed.v1",
            occurred_at=NOW + 2,
            data=completion,
            now_epoch=NOW + 2,
        )

        self.assertEqual((requested["state"], requested["revision"]), (
            "cancel_requested",
            2,
        ))
        self.assertEqual((completed["state"], completed["revision"]), ("canceled", 2))
        self.assertEqual(replay, completed)

    def test_resume_command_returns_the_job_directly_to_running(self):
        ready = self.preview_ready()
        approved = self.store.approve_execution(
            self.scope,
            ready["commercialRequestId"],
            dry_run_revision=1,
            dry_run_hash="b" * 64,
            actor_hash="d" * 64,
            idempotency_key="execute-key-resume",
            now_epoch=NOW + 20,
        )
        scheduled = self.record_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationExecute",
            idempotency_key="execute-key-resume",
            result={
                "commandId": "command-execute-resume",
                "status": "accepted",
                "jobId": "migration-job-1",
                "revision": 3,
            },
            now_epoch=NOW + 21,
        )
        running = self.store.apply_verified_event(
            self.scope,
            event_id="migration-running-resume",
            event_type="migration.progressed.v1",
            occurred_at=NOW + 22,
            data={
                "commercialRequestId": approved["commercialRequestId"],
                "jobId": "migration-job-1",
                "connectionId": "payments-primary",
                "revision": 4,
                "dedupeKey": "migration-running-resume-dedupe",
                "state": "running",
                "counts": scheduled["counts"],
            },
            now_epoch=NOW + 22,
        )
        paused = self.record_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationPause",
            idempotency_key="pause-key-resume",
            result={
                "commandId": "command-pause-resume",
                "status": "accepted",
                "jobId": "migration-job-1",
                "revision": 5,
            },
            now_epoch=NOW + 23,
        )
        resumed = self.record_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationResume",
            idempotency_key="resume-key-resume",
            result={
                "commandId": "command-resume-resume",
                "status": "accepted",
                "jobId": "migration-job-1",
                "revision": 6,
            },
            now_epoch=NOW + 24,
        )

        self.assertEqual(running["state"], "running")
        self.assertEqual(paused["state"], "paused")
        self.assertEqual((resumed["state"], resumed["revision"]), ("running", 6))
        stored = self.store.get_request(self.scope, approved["commercialRequestId"])
        self.assertEqual(stored["lastCommand"]["actorHash"], "c" * 64)
        receipt = next(
            item
            for item in self.backend.items.values()
            if item.get("itemType") == "MigrationCommandReceipt"
            and item.get("operation") == "migrationResume"
        )
        self.assertEqual(receipt["actorHash"], "c" * 64)
        self.assertNotIn("actor", repr(resumed).lower())

    def test_stored_request_recomputes_hashes_and_validates_derived_state(self):
        ready = self.preview_ready()
        key = (
            "operations-table",
            self.scope.partition_key,
            f"MIGRATION_REQUEST#{ready['commercialRequestId']}",
        )
        original = copy.deepcopy(self.backend.items[key])
        corruptions = []
        changed_policy = copy.deepcopy(original)
        changed_policy["canarySize"] = 6
        corruptions.append(changed_policy)
        bad_command = copy.deepcopy(original)
        bad_command["lastCommand"] = {"rawProviderPayload": {}}
        corruptions.append(bad_command)
        pending_command = copy.deepcopy(original)
        pending_command["lastCommand"]["result"]["status"] = "pending"
        corruptions.append(pending_command)
        bad_approval = copy.deepcopy(original)
        bad_approval["approval"] = {
            "dryRunRevision": 9,
            "dryRunHash": "b" * 64,
            "actorHash": "d" * 64,
            "approvedAt": NOW + 20,
        }
        bad_approval["approvalIdempotencyDigest"] = "e" * 64
        corruptions.append(bad_approval)
        bad_state_revision = copy.deepcopy(original)
        bad_state_revision["stateRevision"] = original["revision"] + 1
        corruptions.append(bad_state_revision)
        for corrupted in corruptions:
            with self.subTest(keys=set(corrupted)):
                self.backend.items[key] = corrupted
                with self.assertRaises(StorageConflict):
                    self.store.get_request(self.scope, ready["commercialRequestId"])
        self.backend.items[key] = original

    def test_stored_request_rejects_impossible_state_dependent_approval_shapes(self):
        ready = self.preview_ready()
        key = (
            OPERATIONS_TABLE,
            self.scope.partition_key,
            f"MIGRATION_REQUEST#{ready['commercialRequestId']}",
        )
        original = copy.deepcopy(self.backend.items[key])
        corruptions = []
        for state in (
            "scheduled",
            "running",
            "paused",
            "canceling",
            "completed",
            "completed_with_errors",
        ):
            missing_approval = copy.deepcopy(original)
            missing_approval.update({"state": state, "revision": 3})
            corruptions.append(missing_approval)
        partial_cancel_preview = copy.deepcopy(original)
        partial_cancel_preview.update({
            "state": "cancel_requested",
            "dryRunHash": None,
        })
        corruptions.append(partial_cancel_preview)

        for corrupted in corruptions:
            with self.subTest(state=corrupted["state"]):
                self.backend.items[key] = corrupted
                with self.assertRaises(StorageConflict):
                    self.store.get_request(self.scope, ready["commercialRequestId"])
        self.backend.items[key] = original

    def test_same_revision_progress_event_reconciles_command_owned_state_idempotently(self):
        ready = self.preview_ready()
        approved = self.store.approve_execution(
            self.scope,
            ready["commercialRequestId"],
            dry_run_revision=1,
            dry_run_hash="b" * 64,
            actor_hash="d" * 64,
            idempotency_key="execute-key-1",
            now_epoch=NOW + 20,
        )
        scheduled = self.record_result(
            self.scope,
            approved["commercialRequestId"],
            operation="migrationExecute",
            idempotency_key="execute-key-1",
            result={
                "commandId": "command-migration-2",
                "status": "accepted",
                "jobId": "migration-job-1",
                "revision": 3,
            },
            now_epoch=NOW + 21,
        )
        event = {
            "commercialRequestId": approved["commercialRequestId"],
            "jobId": "migration-job-1",
            "connectionId": "payments-primary",
            "revision": 3,
            "dedupeKey": "migration-scheduled-dedupe",
            "state": "scheduled",
            "counts": scheduled["counts"],
        }
        reconciled = self.store.apply_verified_event(
            self.scope,
            event_id="migration-scheduled-event",
            event_type="migration.progressed.v1",
            occurred_at=NOW + 22,
            data=event,
            now_epoch=NOW + 22,
        )
        replay = self.store.apply_verified_event(
            self.scope,
            event_id="migration-scheduled-event",
            event_type="migration.progressed.v1",
            occurred_at=NOW + 22,
            data=event,
            now_epoch=NOW + 22,
        )
        self.assertFalse(reconciled["stale"])
        self.assertEqual(replay, reconciled)

    def test_distinct_progress_snapshots_cannot_rewrite_counts_at_the_same_revision(self):
        scheduled = self.scheduled()
        first_data = {
            "commercialRequestId": scheduled["commercialRequestId"],
            "jobId": "migration-job-1",
            "connectionId": "payments-primary",
            "revision": 3,
            "dedupeKey": "migration-same-revision-first-dedupe",
            "state": "scheduled",
            "counts": {
                "total": 2,
                "pending": 1,
                "applied": 1,
                "needsReview": 0,
                "failed": 0,
            },
        }
        first = self.store.apply_verified_event(
            self.scope,
            event_id="migration-same-revision-first",
            event_type="migration.progressed.v1",
            occurred_at=NOW + 22,
            data=first_data,
            now_epoch=NOW + 22,
        )
        replay = self.store.apply_verified_event(
            self.scope,
            event_id="migration-same-revision-first",
            event_type="migration.progressed.v1",
            occurred_at=NOW + 22,
            data=first_data,
            now_epoch=NOW + 22,
        )

        with self.assertRaises(StorageConflict):
            self.store.apply_verified_event(
                self.scope,
                event_id="migration-same-revision-second",
                event_type="migration.progressed.v1",
                occurred_at=NOW + 23,
                data={
                    **first_data,
                    "dedupeKey": "migration-same-revision-second-dedupe",
                    "counts": {
                        "total": 2,
                        "pending": 2,
                        "applied": 0,
                        "needsReview": 0,
                        "failed": 0,
                    },
                },
                now_epoch=NOW + 23,
            )

        self.assertEqual(replay, first)
        current = self.store.get_request(self.scope, scheduled["commercialRequestId"])
        self.assertEqual(current["counts"], first_data["counts"])


class RecordingTransport:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def request(self, method, path, payload):
        self.calls.append((method, path, copy.deepcopy(payload)))
        return self.result(payload) if callable(self.result) else copy.deepcopy(self.result)


class MigrationGatewayTests(unittest.TestCase):
    def setUp(self):
        self.scope = CommerceScope("test", "tenant-a", "draft-a", DOMAIN)
        self.preview_input = {
            "commercialRequestId": "migration-request-1",
            "sourceOffer": _binding("offer-source", 90_000),
            "targetOffer": _binding("offer-target", 120_000),
            "requestedPolicy": {"mode": "next_renewal"},
            "candidateScope": {"kind": "all_matching_source_price"},
            "canarySize": 7,
            "accountConcurrency": 3,
        }

    def test_preview_uses_exact_signed_route_and_validates_bound_result(self):
        transport = RecordingTransport(lambda payload: {
            "commandId": payload["commandId"],
            "status": "accepted",
            "jobId": "migration-job-1",
            "revision": 1,
        })
        gateway = InternalIntegrationsGateway(transport)

        result = gateway.execute(
            "migrationPreview",
            self.scope,
            self.preview_input,
            connection_id="payments-primary",
            idempotency_key="browser-key-must-not-cross",
        )

        method, path, payload = transport.calls[0]
        self.assertEqual((method, path), (
            "POST", "/internal/v1/stripe/migrations/preview"
        ))
        self.assertEqual(payload["input"], self.preview_input)
        self.assertEqual(result, {
            "commandId": payload["commandId"],
            "status": "accepted",
            "jobId": "migration-job-1",
            "revision": 1,
        })
        self.assertNotIn("browser-key-must-not-cross", repr(payload))

    def test_preview_rejects_a_needs_review_result_that_cannot_form_a_valid_request(self):
        transport = RecordingTransport(lambda payload: {
            "commandId": payload["commandId"],
            "status": "needs_review",
            "jobId": "migration-job-1",
            "revision": 1,
        })

        with self.assertRaises(IntegrationsUnavailable):
            InternalIntegrationsGateway(transport).execute(
                "migrationPreview",
                self.scope,
                self.preview_input,
                connection_id="payments-primary",
                idempotency_key="preview-needs-review-key",
            )

    def test_execute_and_control_use_only_their_literal_routes(self):
        transport = RecordingTransport(lambda payload: {
            "commandId": payload["commandId"],
            "status": "accepted",
            "jobId": "migration-job-1",
            "revision": payload["input"].get("expectedRevision", 2) + 1,
        })
        gateway = InternalIntegrationsGateway(transport)
        execute_input = {
            "commercialRequestId": "migration-request-1",
            "jobId": "migration-job-1",
            "dryRunRevision": 1,
            "dryRunHash": "a" * 64,
            "confirmation": True,
        }
        gateway.execute(
            "migrationExecute",
            self.scope,
            execute_input,
            connection_id="payments-primary",
            idempotency_key="execute-key-1",
            expected_result_revision=3,
        )
        gateway.execute(
            "migrationPause",
            self.scope,
            {
                "commercialRequestId": "migration-request-1",
                "jobId": "migration-job-1",
                "expectedRevision": 3,
                "action": "pause",
            },
            connection_id="payments-primary",
            idempotency_key="pause-key-1",
        )

        self.assertEqual(
            [(method, path) for method, path, _payload in transport.calls],
            [
                ("POST", "/internal/v1/stripe/migrations/execute"),
                ("POST", "/internal/v1/stripe/migrations/control"),
            ],
        )

    def test_migration_command_results_require_exact_revision_progression(self):
        cases = (
            (
                "migrationPreview",
                self.preview_input,
                {},
                1,
                (2, 999),
            ),
            (
                "migrationExecute",
                {
                    "commercialRequestId": "migration-request-1",
                    "jobId": "migration-job-1",
                    "dryRunRevision": 1,
                    "dryRunHash": "a" * 64,
                    "confirmation": True,
                },
                {"expected_result_revision": 3},
                3,
                (1, 2, 4, 999),
            ),
            (
                "migrationPause",
                {
                    "commercialRequestId": "migration-request-1",
                    "jobId": "migration-job-1",
                    "expectedRevision": 4,
                    "action": "pause",
                },
                {},
                5,
                (1, 4, 6, 999),
            ),
        )
        for operation, command_input, metadata, expected, invalid in cases:
            with self.subTest(operation=operation, revision=expected):
                valid = RecordingTransport(lambda payload, revision=expected: {
                    "commandId": payload["commandId"],
                    "status": "accepted",
                    "jobId": "migration-job-1",
                    "revision": revision,
                })
                result = InternalIntegrationsGateway(valid).execute(
                    operation,
                    self.scope,
                    command_input,
                    connection_id="payments-primary",
                    idempotency_key=f"{operation}-revision-key",
                    **metadata,
                )
                self.assertEqual(result["revision"], expected)
            for revision in invalid:
                with self.subTest(operation=operation, revision=revision):
                    transport = RecordingTransport(
                        lambda payload, revision=revision: {
                            "commandId": payload["commandId"],
                            "status": "accepted",
                            "jobId": "migration-job-1",
                            "revision": revision,
                        }
                    )
                    with self.assertRaises(IntegrationsUnavailable):
                        InternalIntegrationsGateway(transport).execute(
                            operation,
                            self.scope,
                            command_input,
                            connection_id="payments-primary",
                            idempotency_key=f"{operation}-revision-key",
                            **metadata,
                        )

        for revision in (1, 2):
            with self.subTest(status="needs_review", revision=revision):
                review = RecordingTransport(lambda payload, revision=revision: {
                    "commandId": payload["commandId"],
                    "status": "needs_review",
                    "jobId": "migration-job-1",
                    "revision": revision,
                })
                result = InternalIntegrationsGateway(review).execute(
                    "migrationExecute",
                    self.scope,
                    cases[1][1],
                    connection_id="payments-primary",
                    idempotency_key="execute-review-revision-key",
                    expected_result_revision=3,
                )
                self.assertEqual(result["revision"], revision)
        for revision in (3, 999):
            with self.subTest(status="needs_review", revision=revision):
                review = RecordingTransport(lambda payload, revision=revision: {
                    "commandId": payload["commandId"],
                    "status": "needs_review",
                    "jobId": "migration-job-1",
                    "revision": revision,
                })
                with self.assertRaises(IntegrationsUnavailable):
                    InternalIntegrationsGateway(review).execute(
                        "migrationExecute",
                        self.scope,
                        cases[1][1],
                        connection_id="payments-primary",
                        idempotency_key="execute-review-revision-key",
                        expected_result_revision=3,
                    )

    def test_status_is_closed_provider_neutral_and_rejects_extra_fields(self):
        valid = {
            "commercialRequestId": "migration-request-1",
            "jobId": "migration-job-1",
            "connectionId": "payments-primary",
            "revision": 4,
            "state": "running",
            "dryRunRevision": 1,
            "dryRunHash": "a" * 64,
            "expiresAt": NOW + 900,
            "counts": {
                "total": 2,
                "pending": 1,
                "applied": 1,
                "needsReview": 0,
                "failed": 0,
            },
            "items": [{
                "itemId": MIGRATION_ITEM_1,
                "state": "applied",
                "reasonCode": None,
                "attempts": 1,
            }],
            "nextCursor": None,
        }
        transport = RecordingTransport(valid)
        gateway = InternalIntegrationsGateway(transport)
        result = gateway.execute(
            "migrationStatus",
            self.scope,
            {
                "commercialRequestId": "migration-request-1",
                "jobId": "migration-job-1",
                "limit": 25,
            },
            connection_id="payments-primary",
            idempotency_key="ignored-status-key",
        )
        self.assertEqual(result, valid)
        self.assertEqual(transport.calls[0][:2], (
            "GET", "/internal/v1/stripe/migrations/status"
        ))

        invalid_transport = RecordingTransport({**valid, "providerAccountId": "forbidden"})
        with self.assertRaises(IntegrationsUnavailable):
            InternalIntegrationsGateway(invalid_transport).execute(
                "migrationStatus",
                self.scope,
                {"commercialRequestId": "migration-request-1", "jobId": "migration-job-1"},
                connection_id="payments-primary",
                idempotency_key="ignored-status-key",
            )

    def test_status_accepts_canceled_preview_without_a_dry_run(self):
        canceled = {
            "commercialRequestId": "migration-request-1",
            "jobId": "migration-job-1",
            "connectionId": "payments-primary",
            "revision": 2,
            "state": "canceled",
            "dryRunRevision": None,
            "dryRunHash": None,
            "expiresAt": None,
            "counts": {
                "total": 0,
                "pending": 0,
                "applied": 0,
                "needsReview": 0,
                "failed": 0,
            },
            "items": [],
            "nextCursor": None,
        }

        result = InternalIntegrationsGateway(RecordingTransport(canceled)).execute(
            "migrationStatus",
            self.scope,
            {
                "commercialRequestId": "migration-request-1",
                "jobId": "migration-job-1",
            },
            connection_id="payments-primary",
            idempotency_key="ignored-status-key",
        )

        self.assertEqual(result, canceled)

    @patch.dict(os.environ, {
        "ENVIRONMENT_NAME": "production",
        "INTEGRATIONS_API_ID": "abcdefghij",
        "AWS_REGION": "us-east-1",
    }, clear=True)
    def test_production_commerce_maps_to_integrations_production_stage(self):
        from src.integrations_gateway import SigV4ExecuteApiTransport

        transport = SigV4ExecuteApiTransport.from_environment()

        self.assertEqual(
            transport._origin,
            "https://abcdefghij.execute-api.us-east-1.amazonaws.com/production",
        )

    def test_sam_iam_maps_all_integrations_arns_to_the_external_stage(self):
        with open(
            os.path.join(os.path.dirname(__file__), "..", "template.yaml"),
            encoding="utf-8",
        ) as stream:
            template = stream.read()
        self.assertIn("IntegrationsStageByEnvironment:", template)
        self.assertIn("Stage: production", template)
        self.assertNotIn("${IntegrationsApiId}/${EnvironmentName}/", template)
        for path in (
            "POST/internal/v1/stripe/migrations/preview",
            "POST/internal/v1/stripe/migrations/execute",
            "POST/internal/v1/stripe/migrations/control",
            "GET/internal/v1/stripe/migrations/status",
        ):
            self.assertIn(path, template)


class MigrationEventContractTests(unittest.TestCase):
    def envelope(self, event_type, data, *, event_id="migration-event-1"):
        return {
            "schemaVersion": 1,
            "eventId": event_id,
            "eventType": event_type,
            "occurredAt": NOW,
            "environment": "test",
            "tenantId": "tenant-a",
            "draftId": "draft-a",
            "domain": DOMAIN,
            "data": data,
        }

    def common(self):
        return {
            "commercialRequestId": "migration-request-1",
            "jobId": "migration-job-1",
            "connectionId": "payments-primary",
            "revision": 2,
            "dedupeKey": "migration-dedupe-1",
        }

    def counts(self):
        return {
            "total": 2,
            "pending": 1,
            "applied": 1,
            "needsReview": 0,
            "failed": 0,
        }

    def test_parser_accepts_only_the_four_closed_provider_neutral_event_shapes(self):
        cases = {
            "migration.preview_ready.v1": {
                **self.common(),
                "dryRunRevision": 1,
                "dryRunHash": "a" * 64,
                "expiresAt": NOW + 900,
                "counts": self.counts(),
            },
            "migration.progressed.v1": {
                **self.common(), "state": "running", "counts": self.counts()
            },
            "migration.item_needs_review.v1": {
                **self.common(),
                "itemId": MIGRATION_ITEM_1,
                "reasonCode": "source-drift",
            },
            "migration.completed.v1": {
                **self.common(), "state": "completed", "counts": self.counts()
            },
        }
        for event_type, data in cases.items():
            with self.subTest(event_type=event_type):
                parsed = parse_integration_event(self.envelope(event_type, data))
                self.assertEqual(parsed.event_type, event_type)
                self.assertEqual(dict(parsed.data), data)

                with self.assertRaises(IntegrationEventValidationError):
                    parse_integration_event(self.envelope(
                        event_type,
                        {**data, "providerAccountId": "forbidden"},
                    ))

    def test_parser_rejects_noncanonical_review_item_contract(self):
        valid = {
            **self.common(),
            "itemId": MIGRATION_ITEM_1,
            "reasonCode": "source-drift",
        }
        for field, value in (
            ("itemId", "migration-item-1"),
            ("itemId", "migration-item-" + "g" * 40),
            ("reasonCode", "snapshot-drift"),
            ("reasonCode", "conflict"),
            ("reasonCode", "unknown-reason"),
        ):
            with self.subTest(field=field, value=value):
                with self.assertRaises(IntegrationEventValidationError):
                    parse_integration_event(self.envelope(
                        "migration.item_needs_review.v1",
                        {**valid, field: value},
                    ))

    def test_processor_dispatches_migration_events_only_to_scope_bound_store(self):
        backend = FakeBackend()
        commerce = CommerceStore(backend, CATALOG_TABLE, OPERATIONS_TABLE)
        migration_store = Mock()
        migration_store.apply_verified_event.return_value = {
            "commercialRequestId": "migration-request-1",
            "state": "running",
            "revision": 2,
            "stale": False,
        }
        processor = IntegrationEventProcessor(
            commerce,
            subscription_projector=Mock(),
            migration_store=migration_store,
        )
        data = {**self.common(), "state": "running", "counts": self.counts()}
        parsed = parse_integration_event(
            self.envelope("migration.progressed.v1", data)
        )

        result = processor.process(parsed, now_epoch=NOW + 10)

        self.assertEqual(result["state"], "running")
        migration_store.apply_verified_event.assert_called_once_with(
            parsed.scope,
            event_id="migration-event-1",
            event_type="migration.progressed.v1",
            occurred_at=NOW,
            data=data,
            now_epoch=NOW + 10,
        )


if __name__ == "__main__":
    unittest.main()
