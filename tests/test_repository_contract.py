from pathlib import Path
import re
import tomllib
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]


def load_template():
    return yaml.load((ROOT / "template.yaml").read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


class RepositoryContractTests(unittest.TestCase):
    def test_scaffold_contains_only_the_approved_workflows(self):
        workflows = ROOT / ".github" / "workflows"

        self.assertEqual(
            {path.name for path in workflows.glob("*.yml")},
            {"ci.yml", "deploy-test.yml", "deploy-production.yml"},
        )
        self.assertFalse((workflows / "deploy-dev.yml").exists())

    def test_runtime_dependency_and_sam_profiles_are_closed(self):
        runtime_requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8").strip()
        self.assertEqual(
            runtime_requirements,
            "boto3==1.39.13",
        )
        self.assertEqual(
            (ROOT / "src" / "requirements.txt").read_text(encoding="utf-8").strip(),
            runtime_requirements,
        )

        config = tomllib.loads((ROOT / "samconfig.toml").read_text(encoding="utf-8"))
        self.assertEqual(set(config), {"version", "test", "production"})
        self.assertEqual(config["test"]["deploy"]["parameters"]["parameter_overrides"], ["EnvironmentName=test"])
        self.assertEqual(config["production"]["deploy"]["parameters"]["parameter_overrides"], ["EnvironmentName=production"])

    def test_template_and_ci_pin_the_approved_python_and_sam_versions(self):
        template = (ROOT / "template.yaml").read_text(encoding="utf-8")
        ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

        self.assertIn("Transform: AWS::Serverless-2016-10-31", template)
        self.assertIn("Runtime: python3.13", template)
        self.assertRegex(template, re.compile(r"AllowedValues:\s*\n\s*- test\s*\n\s*- production"))
        self.assertNotIn("dev", (ROOT / "samconfig.toml").read_text(encoding="utf-8").lower())
        self.assertIn("python-version: '3.13'", ci)
        self.assertIn("PyYAML==6.0.2", ci)
        self.assertIn("aws-sam-translator==1.111.0", ci)
        self.assertIn("version: 1.163.0", ci)
        self.assertIn("python tests/verify_sam_build.py", ci)

    def test_external_resource_parameters_are_bounded_and_injection_safe(self):
        parameters = load_template()["Parameters"]
        for name in (
            "IntegrationsApiId",
            "ConfigRegistryTableName",
            "ConfigPayloadsBucketName",
            "AuthSessionTableName",
            "AuthUserStateTableName",
            "IntegrationEventsTopicArn",
        ):
            parameter = parameters[name]
            self.assertEqual(parameter["Type"], "AWS::SSM::Parameter::Value<String>")
            self.assertNotIn("Default", parameter)
            self.assertNotIn("NoEcho", parameter)

        for name in ("FiscalRetentionApprovalId", "FiscalAccessApprovalId"):
            parameter = parameters[name]
            self.assertEqual(parameter.get("MaxLength"), "64")
            self.assertEqual(
                parameter.get("AllowedPattern"),
                "^$|^[a-z0-9][a-z0-9._-]{0,63}$",
            )

        cursor_key = parameters.get("CommerceCursorSigningKey", {})
        self.assertEqual(cursor_key.get("NoEcho"), "true")
        self.assertEqual(cursor_key.get("MinLength"), "43")
        self.assertEqual(cursor_key.get("MaxLength"), "128")
        cursor_key_pattern = (
            r"^(?:[A-Za-z0-9_-]{4})*"
            r"(?:[A-Za-z0-9_-][AQgw]|[A-Za-z0-9_-]{2}[AEIMQUYcgkosw048])?$"
        )
        self.assertEqual(cursor_key.get("AllowedPattern"), cursor_key_pattern)
        compiled_cursor_key_pattern = re.compile(cursor_key_pattern, re.ASCII)
        for accepted in ("A" * 43, "A" * 44, "A" * 46, "_" * 128):
            with self.subTest(accepted=accepted):
                self.assertIsNotNone(compiled_cursor_key_pattern.fullmatch(accepted))
        for rejected in ("A" * 33, "A" * 33 + "B", "A" * 34 + "B"):
            with self.subTest(rejected=rejected):
                self.assertIsNone(compiled_cursor_key_pattern.fullmatch(rejected))
        self.assertNotIn("Default", cursor_key)

    def test_repository_entrypoints_state_local_only_and_no_dev_aws(self):
        for name in ("AGENTS.md", "Codex.md", "README.md"):
            self.assertTrue((ROOT / name).is_file(), name)

        combined = "\n".join(
            (ROOT / name).read_text(encoding="utf-8")
            for name in ("AGENTS.md", "README.md")
        ).lower()
        self.assertIn("local-only", combined)
        self.assertIn("no aws `dev`", combined)

    def test_template_owns_three_isolated_on_demand_encrypted_recoverable_tables(self):
        resources = load_template()["Resources"]
        tables = {
            logical_id: resource
            for logical_id, resource in resources.items()
            if resource["Type"] == "AWS::DynamoDB::Table"
        }

        self.assertEqual(
            set(tables),
            {"CommerceCatalogTable", "CommerceOperationsTable", "FiscalTable"},
        )
        for resource in tables.values():
            properties = resource["Properties"]
            self.assertEqual(resource["DeletionPolicy"], "Retain")
            self.assertEqual(resource["UpdateReplacePolicy"], "Retain")
            self.assertEqual(properties["BillingMode"], "PAY_PER_REQUEST")
            self.assertEqual(properties["SSESpecification"], {"SSEEnabled": "true"})
            self.assertEqual(
                properties["PointInTimeRecoverySpecification"],
                {"PointInTimeRecoveryEnabled": "true"},
            )
            expected_attributes = [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ]
            if resource is tables["CommerceCatalogTable"]:
                expected_attributes.extend([
                    {"AttributeName": "duePartition", "AttributeType": "S"},
                    {"AttributeName": "dueKey", "AttributeType": "S"},
                ])
            self.assertEqual(properties["AttributeDefinitions"], expected_attributes)
            self.assertEqual(properties["KeySchema"], [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ])
            self.assertNotIn("LocalSecondaryIndexes", properties)

        self.assertEqual(
            tables["CommerceCatalogTable"]["Properties"]["GlobalSecondaryIndexes"],
            [{
                "IndexName": "ReservationDueIndex",
                "KeySchema": [
                    {"AttributeName": "duePartition", "KeyType": "HASH"},
                    {"AttributeName": "dueKey", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            }],
        )
        self.assertNotIn("GlobalSecondaryIndexes", tables["CommerceOperationsTable"]["Properties"])
        self.assertNotIn("GlobalSecondaryIndexes", tables["FiscalTable"]["Properties"])

        for logical_id in ("CommerceCatalogTable", "CommerceOperationsTable"):
            self.assertEqual(
                tables[logical_id]["Properties"]["TimeToLiveSpecification"],
                {"AttributeName": "expiresAt", "Enabled": "true"},
            )
        self.assertNotIn("TimeToLiveSpecification", tables["FiscalTable"]["Properties"])
        self.assertNotIn("StreamSpecification", tables["CommerceCatalogTable"]["Properties"])
        self.assertNotIn("StreamSpecification", tables["FiscalTable"]["Properties"])
        self.assertEqual(
            tables["CommerceOperationsTable"]["Properties"]["StreamSpecification"],
            {"StreamViewType": "NEW_IMAGE"},
        )

    def test_template_has_literal_route_to_handler_boundaries_without_body_router(self):
        resources = load_template()["Resources"]
        functions = {
            logical_id: resource
            for logical_id, resource in resources.items()
            if resource["Type"] == "AWS::Serverless::Function"
        }
        expected = {
            "CatalogPublicReadFunction": "/features/commerce/public-read",
            "CatalogReadFunction": "/features/commerce/read",
            "CatalogActionFunction": "/features/commerce/catalog/action",
            "InventoryActionFunction": "/features/commerce/inventory/action",
            "SubscriptionActionFunction": "/features/commerce/subscription/action",
            "CheckoutFunction": "/features/commerce/public-action",
            "FiscalRequestFunction": "/features/commerce/fiscal/request",
            "FiscalAdminFunction": "/features/commerce/fiscal/admin",
        }
        routes = {}
        for logical_id, function in functions.items():
            for event in function["Properties"].get("Events", {}).values():
                if event["Type"] == "Api":
                    routes[logical_id] = event["Properties"]["Path"]
        self.assertEqual(routes, expected)
        self.assertNotIn("/features/commerce/action", routes.values())
        self.assertTrue(all("*" not in path for path in routes.values()))

        checkout_environment = resources["CheckoutFunction"]["Properties"]["Environment"]["Variables"]
        self.assertEqual(
            checkout_environment["TEST_PREVIEW_ORIGIN"],
            "https://test.zoolandingpage.com.mx",
        )

    def test_template_keeps_public_catalog_catalog_only_and_fiscal_iam_isolated(self):
        resources = load_template()["Resources"]

        def serialized(logical_id):
            return str(resources[logical_id]["Properties"])

        public = serialized("CatalogPublicReadFunction")
        self.assertIn("CommerceCatalogTable", public)
        self.assertNotIn("CommerceOperationsTable", public)
        self.assertNotIn("FiscalTable", public)
        self.assertNotIn("AUTH_SESSION_TABLE_NAME", public)
        self.assertNotIn("AUTH_USER_STATE_TABLE_NAME", public)

        catalog_action = resources["CatalogActionFunction"]["Properties"]
        self.assertEqual(
            catalog_action["Environment"]["Variables"]["COMMERCE_OPERATIONS_TABLE_NAME"],
            {"Ref": "CommerceOperationsTable"},
        )
        catalog_action_policies = str(resources.get("CatalogActionRole", {}))
        self.assertIn("dynamodb:TransactWriteItems", catalog_action_policies)
        self.assertIn("CommerceCatalogTable", catalog_action_policies)
        self.assertIn("CommerceOperationsTable", catalog_action_policies)
        self.assertNotIn("dynamodb:Scan", catalog_action_policies)
        self.assertNotIn("dynamodb:Query", catalog_action_policies)

        for logical_id, resource in resources.items():
            if resource["Type"] != "AWS::Serverless::Function":
                continue
            body = serialized(logical_id)
            if logical_id in {"FiscalRequestFunction", "FiscalAdminFunction"}:
                self.assertIn("FiscalTable", body)
            else:
                self.assertNotIn("FiscalTable", body, logical_id)

        fiscal_request = resources["FiscalRequestFunction"]["Properties"]
        self.assertEqual(
            fiscal_request["Environment"]["Variables"]["COMMERCE_OPERATIONS_TABLE_NAME"],
            {"Ref": "CommerceOperationsTable"},
        )
        fiscal_request_body = serialized("FiscalRequestFunction")
        self.assertIn("CommerceOperationsTable", fiscal_request_body)
        self.assertNotIn("CommerceCatalogTable", fiscal_request_body)
        self.assertEqual(
            resources["FiscalAdminFunction"]["Properties"]["Environment"]["Variables"]["COMMERCE_OPERATIONS_TABLE_NAME"],
            {"Ref": "CommerceOperationsTable"},
        )

    def test_template_wires_due_index_queues_stream_partial_failures_and_scheduler(self):
        resources = load_template()["Resources"]
        worker_events = resources["IntegrationEventWorkerFunction"]["Properties"]["Events"]
        for logical_id in ("IntegrationEventWorkerFunction", "OutboxRelayFunction"):
            self.assertEqual(
                resources[logical_id]["Properties"]["Environment"]["Variables"]["ENVIRONMENT_NAME"],
                {"Ref": "EnvironmentName"},
            )
        queue_event = next(event for event in worker_events.values() if event["Type"] == "SQS")
        self.assertEqual(queue_event["Properties"]["FunctionResponseTypes"], ["ReportBatchItemFailures"])

        relay_events = resources["OutboxRelayFunction"]["Properties"]["Events"]
        relay_properties = resources["OutboxRelayFunction"]["Properties"]
        self.assertEqual(
            relay_properties["Environment"]["Variables"]["COMMERCE_CATALOG_TABLE_NAME"],
            {"Ref": "CommerceCatalogTable"},
        )
        self.assertNotIn("CommerceCatalogTable", str(resources["OutboxRelayRole"]))
        stream = next(event for event in relay_events.values() if event["Type"] == "DynamoDB")["Properties"]
        self.assertEqual(stream["FunctionResponseTypes"], ["ReportBatchItemFailures"])
        self.assertIn("FilterCriteria", stream)
        self.assertIn("OnFailure", stream["DestinationConfig"])

        schedule_events = resources["ReservationReconcilerFunction"]["Properties"]["Events"]
        schedule = next(event for event in schedule_events.values() if event["Type"] == "Schedule")
        self.assertEqual(schedule["Properties"]["Schedule"], "rate(5 minutes)")
        self.assertEqual(schedule["Properties"]["Enabled"], "true")
        reconciler = str(resources.get("ReservationReconcilerRole", {}))
        self.assertIn("ReservationDueIndex", reconciler)
        self.assertIn("dynamodb:Query", reconciler)

    def test_all_functions_use_explicit_roles_without_managed_policies_or_wildcard_resources(self):
        resources = load_template()["Resources"]
        expected_roles = {
            "CatalogPublicReadFunction": "CatalogPublicReadRole",
            "CatalogReadFunction": "CatalogReadRole",
            "CatalogActionFunction": "CatalogActionRole",
            "InventoryActionFunction": "InventoryActionRole",
            "CheckoutFunction": "CheckoutRole",
            "SubscriptionActionFunction": "SubscriptionActionRole",
            "FiscalRequestFunction": "FiscalRequestRole",
            "FiscalAdminFunction": "FiscalAdminRole",
            "IntegrationEventWorkerFunction": "IntegrationEventWorkerRole",
            "OutboxRelayFunction": "OutboxRelayRole",
            "ReservationReconcilerFunction": "ReservationReconcilerRole",
        }
        functions = {
            logical_id: resource
            for logical_id, resource in resources.items()
            if resource["Type"] == "AWS::Serverless::Function"
        }
        self.assertEqual(set(functions), set(expected_roles))
        for function_id, role_id in expected_roles.items():
            function = resources[function_id]["Properties"]
            self.assertEqual(function.get("Role"), {"Fn::GetAtt": [role_id, "Arn"]})
            self.assertNotIn("Policies", function)
            role = resources.get(role_id, {})
            self.assertEqual(role.get("Type"), "AWS::IAM::Role")
            self.assertNotIn("ManagedPolicyArns", role.get("Properties", {}))
            serialized = str(role)
            self.assertNotIn("AWSLambdaSQSQueueExecutionRole", serialized)
            self.assertNotIn("AWSLambdaDynamoDBExecutionRole", serialized)
            self.assertNotIn("AWSXRayDaemonWriteAccess", serialized)
            statements = role.get("Properties", {}).get("Policies", [{}])[0].get(
                "PolicyDocument", {}
            ).get("Statement", [])
            self.assertEqual(
                statements[0],
                {
                    "Effect": "Allow",
                    "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                    "Resource": {"Fn::GetAtt": ["CommerceLogGroup", "Arn"]},
                },
            )
            for statement in statements:
                self.assertNotEqual(statement.get("Resource"), "*", role_id)
                self.assertNotIn("*", statement.get("Resource", []), role_id)

    def test_subscription_action_has_only_the_operations_write_surface_it_needs(self):
        resources = load_template()["Resources"]
        role = resources["SubscriptionActionRole"]["Properties"]
        statements = role["Policies"][0]["PolicyDocument"]["Statement"]
        operations_arn = {"Fn::GetAtt": ["CommerceOperationsTable", "Arn"]}
        operation_statements = [
            statement
            for statement in statements
            if statement.get("Resource") == operations_arn
        ]
        self.assertEqual(
            operation_statements,
            [{
                "Effect": "Allow",
                "Action": ["dynamodb:GetItem", "dynamodb:TransactWriteItems"],
                "Resource": operations_arn,
            }],
        )
        catalog_arn = {"Fn::GetAtt": ["CommerceCatalogTable", "Arn"]}
        self.assertEqual(
            [statement for statement in statements if statement.get("Resource") == catalog_arn],
            [{
                "Effect": "Allow",
                "Action": "dynamodb:GetItem",
                "Resource": catalog_arn,
            }],
        )
        variables = resources["SubscriptionActionFunction"]["Properties"]["Environment"]["Variables"]
        self.assertEqual(variables["COMMERCE_OPERATIONS_TABLE_NAME"], {"Ref": "CommerceOperationsTable"})
        self.assertEqual(variables["COMMERCE_CATALOG_TABLE_NAME"], {"Ref": "CommerceCatalogTable"})

    def test_internal_integrations_permissions_are_literal_method_and_route_scoped(self):
        resources = load_template()["Resources"]
        expected = {
            "CatalogActionRole": {
                "POST/internal/v1/stripe/offer",
                "POST/internal/v1/stripe/product-presentation",
                "POST/internal/v1/stripe/discount",
                "POST/internal/v1/stripe/discount-lifecycle",
            },
            "CheckoutRole": {"POST/internal/v1/stripe/checkout"},
            "SubscriptionActionRole": {
                "POST/internal/v1/stripe/subscription/change",
                "POST/internal/v1/stripe/subscription/discount",
                "POST/internal/v1/stripe/subscription/pause",
                "POST/internal/v1/stripe/customer-portal",
                "POST/internal/v1/stripe/migrations/preview",
                "POST/internal/v1/stripe/migrations/execute",
                "POST/internal/v1/stripe/migrations/control",
                "GET/internal/v1/stripe/migrations/status",
            },
            "ReservationReconcilerRole": {
                "GET/internal/v1/stripe/checkout-status"
            },
        }
        prefix = (
            "arn:${AWS::Partition}:execute-api:${AWS::Region}:"
            "${AWS::AccountId}:${IntegrationsApiId}/${IntegrationsStage}/"
        )
        expected_stage = {
            "Fn::FindInMap": [
                "IntegrationsStageByEnvironment",
                {"Ref": "EnvironmentName"},
                "Stage",
            ]
        }
        for role_id, suffixes in expected.items():
            statements = resources[role_id]["Properties"]["Policies"][0][
                "PolicyDocument"
            ]["Statement"]
            invoke = [
                statement
                for statement in statements
                if statement.get("Action") == "execute-api:Invoke"
            ]
            self.assertEqual(len(invoke), 1, role_id)
            actual = invoke[0]["Resource"]
            actual = actual if isinstance(actual, list) else [actual]
            substitutions = [resource["Fn::Sub"] for resource in actual]
            self.assertTrue(all(isinstance(value, list) for value in substitutions))
            self.assertEqual(
                {value[0] for value in substitutions},
                {prefix + suffix for suffix in suffixes},
            )
            self.assertTrue(all(
                value[1] == {"IntegrationsStage": expected_stage}
                for value in substitutions
            ))
            self.assertNotIn("*", str(actual), role_id)
            variables = resources[role_id.removesuffix("Role") + "Function"][
                "Properties"
            ]["Environment"]["Variables"]
            self.assertEqual(
                variables["INTEGRATIONS_API_ID"], {"Ref": "IntegrationsApiId"}
            )

    def test_cursor_signing_key_is_injected_only_into_catalog_readers(self):
        resources = load_template()["Resources"]
        functions = {
            logical_id: resource["Properties"]
            for logical_id, resource in resources.items()
            if resource["Type"] == "AWS::Serverless::Function"
        }
        with_cursor_key = {
            logical_id
            for logical_id, properties in functions.items()
            if "COMMERCE_CURSOR_SIGNING_KEY"
            in properties.get("Environment", {}).get("Variables", {})
        }
        self.assertEqual(
            with_cursor_key,
            {"CatalogPublicReadFunction", "CatalogReadFunction"},
        )
        for logical_id in with_cursor_key:
            self.assertEqual(
                functions[logical_id]["Environment"]["Variables"]["COMMERCE_CURSOR_SIGNING_KEY"],
                {"Ref": "CommerceCursorSigningKey"},
            )

    def test_async_ingress_filter_is_exact(self):
        resources = load_template()["Resources"]

        subscription = resources["CommerceIntegrationEventsSubscription"]["Properties"]
        self.assertEqual(subscription["FilterPolicyScope"], "MessageBody")
        self.assertEqual(
            subscription["FilterPolicy"],
            {
                "eventType": [
                    "commerce.payment.succeeded.v1",
                    "commerce.payment.terminal_unpaid.v1",
                    "commerce.refund.confirmed.v1",
                    "commerce.subscription.updated.v1",
                    "migration.preview_ready.v1",
                    "migration.progressed.v1",
                    "migration.item_needs_review.v1",
                    "migration.completed.v1",
                ]
            },
        )

    def test_logs_and_queues_have_bounded_retention_and_cost_tags(self):
        template = load_template()
        globals_function = template["Globals"]["Function"]
        self.assertNotIn("Tracing", globals_function)
        self.assertEqual(
            globals_function["LoggingConfig"],
            {"LogFormat": "JSON", "LogGroup": {"Ref": "CommerceLogGroup"}},
        )

        resources = template["Resources"]
        log_group = resources["CommerceLogGroup"]
        self.assertEqual(log_group["Type"], "AWS::Logs::LogGroup")
        self.assertEqual(log_group["Properties"]["RetentionInDays"], "30")

        for logical_id in (
            "CommerceIntegrationEventsDlq",
            "CommerceIntegrationEventsQueue",
            "CommerceOutboxFailureQueue",
        ):
            tags = {
                tag["Key"]: tag["Value"]
                for tag in resources[logical_id]["Properties"]["Tags"]
            }
            self.assertEqual(tags["CostCenter"], "Commerce")
            self.assertEqual(tags["Environment"], {"Ref": "EnvironmentName"})


if __name__ == "__main__":
    unittest.main()
