import hashlib
import importlib
import importlib.util
from unittest.mock import Mock
import unittest


DOMAIN = "example.com"


class FakeAuthStore:
    def __init__(self, *, session=None, user=None):
        self.session = session
        self.user = user
        self.session_hashes = []
        self.user_keys = []

    def get_session(self, session_hash):
        self.session_hashes.append(session_hash)
        return self.session

    def get_user(self, tenant_profile_key, user_key):
        self.user_keys.append((tenant_profile_key, user_key))
        return self.user


def policies(*, capabilities=None):
    from src.common.published_policy import ResolvedPolicies

    capabilities = capabilities or [
        "commerce:catalog:read",
        "commerce:catalog:write",
        "commerce:inventory:write",
        "commerce:subscription:manage",
        "commerce:fiscal:manage",
    ]
    return ResolvedPolicies(
        environment="test",
        tenant_id="tenant-example",
        draft_id="draft-example",
        domain=DOMAIN,
        version_id="version-1",
        prefix="sites/example.com/versions/version-1/",
        commerce={
            "version": 1,
            "scope": {
                "environment": "test",
                "tenantId": "tenant-example",
                "draftId": "draft-example",
                "domain": DOMAIN,
            },
            "commerce": {
                "status": "active",
                "adminAccess": {
                    "mode": "auth-profile",
                    "authProfileId": "staff",
                    "capabilities": capabilities,
                },
            },
        },
        auth_registry={
            "version": 1,
            "profiles": [{
                "authProfileId": "staff",
                "status": "active",
                "tenantId": "tenant-example",
                "domain": DOMAIN,
                "environment": "test",
                "adminGroups": ["commerce-admin"],
                "allowedGroups": ["commerce-admin", "viewer"],
                "session": {
                    "csrfCookieName": "zlp_csrf",
                    "csrfHeaderName": "x-zlp-csrf",
                },
            }],
        },
    )


def event(*, domain=DOMAIN, csrf=True):
    headers = {
        "x-zlp-domain": domain,
        "x-zlp-auth-profile-id": "staff",
        "cookie": "__Host-zlp_session=session-value; zlp_csrf=csrf-value",
    }
    if csrf:
        headers["x-zlp-csrf"] = "csrf-value"
    return {"headers": headers}


def auth_store():
    return FakeAuthStore(
        session={
            "tenantProfileKey": f"{DOMAIN}#staff#test",
            "subject": "operator-1",
            "domain": DOMAIN,
            "authProfileId": "staff",
            "environment": "test",
            "tenantId": "tenant-example",
            "sessionVersion": 2,
            "expiresAt": 2_000,
            "csrfHash": hashlib.sha256(b"csrf-value").hexdigest(),
        },
        user={
            "sessionVersion": 2,
            "enabled": True,
            "approvalStatus": "approved",
            "roles": ["commerce-admin"],
        },
    )


class AuthorizationContractTests(unittest.TestCase):
    def test_commerce_auth_module_exists(self):
        self.assertIsNotNone(importlib.util.find_spec("src.common.auth_admin"))

    def test_validates_fresh_state_capability_and_csrf(self):
        auth = importlib.import_module("src.common.auth_admin")
        self.assertTrue(hasattr(auth, "authorize_request"))
        store = auth_store()

        context = auth.authorize_request(
            event=event(),
            policies=policies(),
            capability="commerce:catalog:write",
            mutation=True,
            store=store,
            now_epoch=1_000,
        )

        self.assertEqual(context.subject, "operator-1")
        self.assertEqual(context.roles, ("commerce-admin",))
        self.assertEqual(context.domain, DOMAIN)
        self.assertEqual(store.session_hashes, [hashlib.sha256(b"session-value").hexdigest()])
        self.assertEqual(store.user_keys, [(f"{DOMAIN}#staff#test", "USER#operator-1")])

    def test_dynamo_session_and_current_user_reads_are_strongly_consistent(self):
        auth = importlib.import_module("src.common.auth_admin")
        session_table = Mock()
        session_table.get_item.return_value = {"Item": {"sessionVersion": 2}}
        user_table = Mock()
        user_table.get_item.return_value = {"Item": {"sessionVersion": 2}}
        dynamodb = Mock()
        dynamodb.Table.side_effect = {
            "sessions": session_table,
            "users": user_table,
        }.__getitem__
        store = auth.DynamoAuthStore("sessions", "users", dynamodb=dynamodb)

        store.get_session("a" * 64)
        store.get_user("example.com#staff#test", "USER#operator-1")

        session_table.get_item.assert_called_once_with(
            Key={"sessionIdHash": "a" * 64},
            ConsistentRead=True,
        )
        user_table.get_item.assert_called_once_with(
            Key={
                "tenantProfileKey": "example.com#staff#test",
                "userKey": "USER#operator-1",
            },
            ConsistentRead=True,
        )

    def test_session_versions_must_both_be_positive_exact_integers(self):
        auth = importlib.import_module("src.common.auth_admin")
        cases = {
            "missing": object(),
            "boolean": True,
            "zero": 0,
            "string": "2",
            "float": 2.0,
        }
        for label, value in cases.items():
            store = auth_store()
            if label == "missing":
                store.session.pop("sessionVersion")
                store.user.pop("sessionVersion")
            else:
                store.session["sessionVersion"] = value
                store.user["sessionVersion"] = value
            with self.subTest(label=label), self.assertRaises(auth.AuthenticationError):
                auth.authorize_request(
                    event=event(),
                    policies=policies(),
                    capability="commerce:catalog:read",
                    store=store,
                    now_epoch=1_000,
                )

    def test_rejects_unknown_or_unassigned_capability(self):
        auth = importlib.import_module("src.common.auth_admin")
        for capability in ("commerce:any", "commerce:fiscal:manage"):
            with self.subTest(capability=capability), self.assertRaises(auth.AuthorizationError):
                auth.authorize_request(
                    event=event(),
                    policies=policies(capabilities=["commerce:catalog:read"]),
                    capability=capability,
                    store=auth_store(),
                    now_epoch=1_000,
                )

    def test_rejects_wrong_domain_stale_user_or_missing_csrf(self):
        auth = importlib.import_module("src.common.auth_admin")
        stale = auth_store()
        stale.user["sessionVersion"] = 3
        cases = (
            (event(domain="other.example.com"), auth_store(), False, auth.AuthenticationError),
            (event(), stale, False, auth.AuthenticationError),
            (event(csrf=False), auth_store(), True, auth.AuthorizationError),
        )
        for request, store, mutation, failure in cases:
            with self.subTest(failure=failure.__name__), self.assertRaises(failure):
                auth.authorize_request(
                    event=request,
                    policies=policies(),
                    capability="commerce:catalog:write",
                    mutation=mutation,
                    store=store,
                    now_epoch=1_000,
                )


if __name__ == "__main__":
    unittest.main()
