import io
import json
import unittest


DOMAIN = "example.com"
TENANT_ID = "tenant-example"
DRAFT_ID = "draft-example"


def commerce_policy(environment="test"):
    return {
        "version": 1,
        "scope": {
            "environment": environment,
            "tenantId": TENANT_ID,
            "draftId": DRAFT_ID,
            "domain": DOMAIN,
        },
        "commerce": {
            "status": "active",
            "adminAccess": {
                "mode": "auth-profile",
                "authProfileId": "staff",
                "capabilities": [
                    "commerce:catalog:read",
                    "commerce:catalog:write",
                    "commerce:inventory:write",
                    "commerce:subscription:manage",
                ],
            },
            "sellableTypes": ["physical", "service", "subscription", "add_on"],
            "payments": {
                "bindingId": "stripe-main",
                "oneTime": True,
                "subscriptions": True,
                "editablePrices": True,
                "coupons": True,
                "operatorPauses": True,
                "proration": "operator-selectable",
            },
            "inventory": {
                "enabled": True,
                "tracked": True,
                "backorders": False,
                "locationId": "primary",
            },
            "shipping": {"enabled": True, "methods": ["fixed", "free", "pickup"]},
            "fiscal": {"enabled": False},
            "checkout": {
                "successPath": "/checkout/success",
                "cancelPath": "/checkout/cancel",
                "termsPath": "/terminos",
                "privacyPath": "/privacidad",
                "refundPolicyPath": "/reembolsos",
                "supportPath": "/contacto",
            },
            "notificationPolicyIds": ["payment-status"],
        },
    }


def auth_registry():
    return {"version": 1, "profiles": [{"authProfileId": "staff"}]}


class FakeRegistryTable:
    def __init__(self, metadata):
        self.metadata = metadata
        self.calls = []

    def get_item(self, **request):
        self.calls.append(request)
        return {"Item": self.metadata}


class FakeS3:
    def __init__(self, objects):
        self.objects = dict(objects)
        self.calls = []

    def get_object(self, **request):
        self.calls.append(request)
        value = self.objects[request["Key"]]
        raw = value if isinstance(value, bytes) else json.dumps(value).encode("utf-8")
        return {"ContentLength": len(raw), "Body": io.BytesIO(raw)}


class FailingRegistryTable:
    def get_item(self, **_request):
        raise RuntimeError("synthetic provider detail")


class PublishedPolicyResolverTests(unittest.TestCase):
    def setUp(self):
        from src.common.published_policy import PublishedPolicyResolver

        self.prefix = f"sites/{DOMAIN}/versions/v1/"
        self.metadata = {
            "domain": DOMAIN,
            "serverScope": {"tenantId": TENANT_ID, "draftId": DRAFT_ID},
            "publishedEnvironments": {
                "test": {"versionId": "v1", "prefix": self.prefix},
            },
        }
        self.commerce_key = f"{self.prefix}{DOMAIN}/server/commerce.json"
        self.auth_key = f"{self.prefix}{DOMAIN}/server/auth-profile-registry.json"
        self.table = FakeRegistryTable(self.metadata)
        self.s3 = FakeS3({
            self.commerce_key: commerce_policy(),
            self.auth_key: auth_registry(),
        })
        self.resolver = PublishedPolicyResolver(self.table, self.s3, "config-bucket")

    def resolve(self, **overrides):
        request = {
            "environment": "test",
            "domain": DOMAIN,
            "tenant_id": TENANT_ID,
            "draft_id": DRAFT_ID,
            **overrides,
        }
        return self.resolver.resolve(**request)

    def test_resolves_exact_commerce_and_auth_descriptors(self):
        resolved = self.resolve()

        self.assertEqual(resolved.version_id, "v1")
        self.assertEqual(resolved.scope, commerce_policy()["scope"])
        self.assertEqual(resolved.commerce["commerce"]["payments"]["bindingId"], "stripe-main")
        self.assertEqual(resolved.auth_registry["profiles"][0]["authProfileId"], "staff")
        self.assertEqual([call["Key"] for call in self.s3.calls], [self.commerce_key, self.auth_key])

    def test_reads_pointer_fresh_and_caches_only_immutable_descriptors(self):
        first = self.resolve()
        self.s3.objects[self.commerce_key] = {"invalid": True}
        second = self.resolve()

        self.assertIs(first.commerce, second.commerce)
        self.assertEqual(len(self.table.calls), 2)
        self.assertEqual(len(self.s3.calls), 2)

    def test_public_resolution_reads_only_commerce_descriptor(self):
        resolved = self.resolver.resolve_commerce(
            environment="test",
            domain=DOMAIN,
            tenant_id=TENANT_ID,
            draft_id=DRAFT_ID,
        )

        self.assertEqual(resolved.auth_registry, {})
        self.assertEqual([call["Key"] for call in self.s3.calls], [self.commerce_key])

    def test_public_and_protected_caches_are_separate(self):
        public = self.resolver.resolve_commerce(
            environment="test",
            domain=DOMAIN,
            tenant_id=TENANT_ID,
            draft_id=DRAFT_ID,
        )
        protected = self.resolve()

        self.assertEqual(public.auth_registry, {})
        self.assertEqual(protected.auth_registry["version"], 1)
        self.assertEqual(
            [call["Key"] for call in self.s3.calls],
            [self.commerce_key, self.commerce_key, self.auth_key],
        )

    def test_production_uses_only_the_production_pointer(self):
        prefix = f"sites/{DOMAIN}/versions/prod-v1/"
        self.metadata["published"] = {"versionId": "prod-v1", "prefix": prefix}
        self.s3.objects[f"{prefix}{DOMAIN}/server/commerce.json"] = commerce_policy("production")
        self.s3.objects[f"{prefix}{DOMAIN}/server/auth-profile-registry.json"] = auth_registry()

        resolved = self.resolve(environment="prod")

        self.assertEqual(resolved.environment, "production")
        self.assertEqual(resolved.version_id, "prod-v1")

    def test_rejects_dev_prefix_confusion_and_extra_contract_fields(self):
        from src.common.published_policy import PolicyResolutionError

        with self.assertRaises(PolicyResolutionError):
            self.resolve(environment="dev")

        self.setUp()
        self.metadata["publishedEnvironments"]["test"]["prefix"] = f"sites/{DOMAIN}/versions/v10/"
        with self.assertRaises(PolicyResolutionError):
            self.resolve()

        self.setUp()
        self.s3.objects[self.commerce_key]["commerce"]["tableName"] = "unsafe"
        with self.assertRaises(PolicyResolutionError):
            self.resolve()

    def test_rejects_duplicate_json_keys(self):
        from src.common.published_policy import PolicyResolutionError

        self.s3.objects[self.commerce_key] = b'{"version":1,"version":1}'
        with self.assertRaises(PolicyResolutionError):
            self.resolve()

    def test_rejects_domain_longer_than_the_published_schema_limit_before_aws_reads(self):
        from src.common.published_policy import PolicyResolutionError

        oversized_domain = ".".join(["a" * 63] * 4)
        self.assertGreater(len(oversized_domain), 253)

        with self.assertRaises(PolicyResolutionError):
            self.resolver.resolve(environment="test", domain=oversized_domain)

        self.assertEqual(self.table.calls, [])
        self.assertEqual(self.s3.calls, [])

    def test_rejects_registry_ids_that_only_become_valid_after_trimming(self):
        from src.common.published_policy import PolicyResolutionError

        self.metadata["serverScope"]["tenantId"] = f" {TENANT_ID} "

        with self.assertRaises(PolicyResolutionError):
            self.resolve()

        self.assertEqual(self.s3.calls, [])

    def test_rejects_scope_mismatch_and_sanitizes_provider_failures(self):
        from src.common.published_policy import PolicyResolutionError, PublishedPolicyResolver

        with self.assertRaises(PolicyResolutionError):
            self.resolve(tenant_id="other-tenant")
        self.assertEqual(self.s3.calls, [])

        failing = PublishedPolicyResolver(FailingRegistryTable(), self.s3, "config-bucket")
        with self.assertRaises(PolicyResolutionError) as caught:
            failing.resolve(environment="test", domain=DOMAIN)
        self.assertIsNone(caught.exception.__cause__)
        self.assertNotIn("synthetic", str(caught.exception))

    def test_rejects_oversized_excessively_nested_and_non_object_descriptors(self):
        from src.common.published_policy import PolicyResolutionError

        nested = {"value": "leaf"}
        for _ in range(33):
            nested = {"next": nested}
        cases = (
            b" " * (256 * 1024 + 1),
            json.dumps(nested).encode("utf-8"),
            b"[]",
        )
        for raw in cases:
            with self.subTest(size=len(raw)):
                self.setUp()
                self.s3.objects[self.commerce_key] = raw
                with self.assertRaises(PolicyResolutionError):
                    self.resolve()

    def test_rejects_invalid_nested_commerce_shapes(self):
        from src.common.published_policy import PolicyResolutionError

        def mutate(path, value):
            policy = commerce_policy()
            target = policy
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
            return policy

        cases = (
            (("commerce", "sellableTypes"), ["subscription", "subscription"]),
            (("commerce", "payments", "subscriptions"), False),
            (("commerce", "inventory", "backorders"), True),
            (("commerce", "shipping", "methods"), ["carrier-api"]),
            (("commerce", "checkout", "successPath"), "https://example.com/success"),
            (("commerce", "adminAccess", "capabilities"), ["commerce:any"]),
        )
        for path, value in cases:
            with self.subTest(path=path):
                self.setUp()
                self.s3.objects[self.commerce_key] = mutate(path, value)
                with self.assertRaises(PolicyResolutionError):
                    self.resolve()


if __name__ == "__main__":
    unittest.main()
