from pathlib import Path
import re
import tomllib
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"


class Phase8InfrastructureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.template_text = (ROOT / "template.yaml").read_text(encoding="utf-8")
        cls.template = yaml.safe_load(cls.template_text)
        cls.resources = cls.template["Resources"]

    def test_environment_and_cross_service_inputs_are_exact_and_fail_closed(self):
        parameters = self.template["Parameters"]
        self.assertEqual(
            parameters["EnvironmentName"],
            {"Type": "String", "AllowedValues": ["test", "production"]},
        )
        exact_ssm_names = {
            "IntegrationsApiId": "services/integrations/api-id",
            "ConfigRegistryTableName": "config/registry-table-name",
            "ConfigPayloadsBucketName": "config/payload-bucket-name",
            "AuthSessionTableName": "auth/session-table-name",
            "AuthUserStateTableName": "auth/user-state-table-name",
            "IntegrationEventsTopicArn": "topics/integration-events-arn",
        }
        for name, suffix in exact_ssm_names.items():
            self.assertEqual(
                parameters[name]["Type"], "AWS::SSM::Parameter::Value<String>"
            )
            self.assertNotIn("Default", parameters[name])
            self.assertNotIn("NoEcho", parameters[name])
            self.assertEqual(
                parameters[name]["AllowedValues"],
                [
                    f"/zoolanding/test/{suffix}",
                    f"/zoolanding/production/{suffix}",
                ],
            )
        self.assertEqual(parameters["CommerceCursorSigningKey"]["MinLength"], 43)
        self.assertEqual(
            parameters["AlarmTopicArn"]["AllowedPattern"],
            r"^arn:(aws|aws-us-gov|aws-cn):sns:[a-z0-9-]+:[0-9]{12}:[A-Za-z0-9_.-]+$",
        )

    def test_stack_publishes_only_safe_service_identifiers_to_ssm(self):
        api_parameter = self.resources["CommerceApiIdParameter"]["Properties"]
        self.assertEqual(
            api_parameter["Name"],
            {"Fn::Sub": "/zoolanding/${EnvironmentName}/services/commerce/api-id"},
        )
        self.assertEqual(api_parameter["Type"], "String")
        self.assertEqual(api_parameter["Value"], {"Ref": "CommerceApi"})

        topic_parameter = self.resources[
            "CommerceNotificationRequestsTopicArnParameter"
        ]["Properties"]
        self.assertEqual(
            topic_parameter["Name"],
            {
                "Fn::Sub": (
                    "/zoolanding/${EnvironmentName}/topics/"
                    "commerce-notification-requests-arn"
                )
            },
        )
        self.assertEqual(topic_parameter["Type"], "String")
        self.assertEqual(
            topic_parameter["Value"], {"Ref": "CommerceNotificationRequestsTopic"}
        )

        caller_roles_parameter = self.resources[
            "CommerceIntegrationsCallerRoleArnsParameter"
        ]["Properties"]
        self.assertEqual(
            caller_roles_parameter["Name"],
            {
                "Fn::Sub": (
                    "/zoolanding/${EnvironmentName}/services/commerce/"
                    "integrations-caller-role-arns"
                )
            },
        )
        self.assertEqual(caller_roles_parameter["Type"], "StringList")
        self.assertEqual(
            caller_roles_parameter["Value"],
            {
                "Fn::Join": [
                    ",",
                    [
                        {"Fn::GetAtt": ["CatalogActionRole", "Arn"]},
                        {"Fn::GetAtt": ["CheckoutRole", "Arn"]},
                        {"Fn::GetAtt": ["SubscriptionActionRole", "Arn"]},
                        {"Fn::GetAtt": ["ReservationReconcilerRole", "Arn"]},
                    ],
                ]
            },
        )

        rendered_outputs = yaml.safe_dump(
            self.template["Outputs"], sort_keys=True
        ).lower()
        for forbidden in ("secret", "credential", "token", "signingkey"):
            self.assertNotIn(forbidden, rendered_outputs)

    def test_outbox_stream_iam_uses_the_exact_stream_arn_including_list_streams(self):
        statements = self.resources["OutboxRelayRole"]["Properties"]["Policies"][0][
            "PolicyDocument"
        ]["Statement"]
        stream_statement = next(
            statement
            for statement in statements
            if "dynamodb:ListStreams"
            in (
                statement["Action"]
                if isinstance(statement["Action"], list)
                else [statement["Action"]]
            )
        )
        self.assertEqual(
            set(stream_statement["Action"]),
            {
                "dynamodb:DescribeStream",
                "dynamodb:GetRecords",
                "dynamodb:GetShardIterator",
                "dynamodb:ListStreams",
            },
        )
        self.assertEqual(
            stream_statement["Resource"],
            {"Fn::GetAtt": ["CommerceOperationsTable", "StreamArn"]},
        )
        self.assertNotIn("/stream/*", self.template_text)

    def test_integration_events_queue_policy_binds_topic_and_source_account(self):
        statement = self.resources["CommerceIntegrationEventsQueuePolicy"][
            "Properties"
        ]["PolicyDocument"]["Statement"]
        self.assertEqual(len(statement), 1)
        self.assertEqual(
            statement[0]["Condition"],
            {
                "ArnEquals": {
                    "aws:SourceArn": {"Ref": "IntegrationEventsTopicArn"}
                },
                "StringEquals": {
                    "aws:SourceAccount": {"Ref": "AWS::AccountId"}
                },
            },
        )

    def test_required_operational_alarms_target_only_the_operator_topic(self):
        required = {
            "Api5xxAlarm",
            "PublicRead4xxAlarm",
            "PublicAction4xxAlarm",
            "FiscalRequest4xxAlarm",
            "IntegrationEventWorkerErrorsAlarm",
            "IntegrationEventWorkerThrottlesAlarm",
            "OutboxRelayErrorsAlarm",
            "OutboxRelayThrottlesAlarm",
            "ReservationReconcilerErrorsAlarm",
            "ReservationReconcilerThrottlesAlarm",
            "IntegrationEventsQueueAgeAlarm",
            "IntegrationEventsDlqAgeAlarm",
            "IntegrationEventsDlqDepthAlarm",
            "OutboxFailureQueueAgeAlarm",
            "OutboxFailureQueueDepthAlarm",
            "StaleReservationsAlarm",
            "MigrationBacklogAlarm",
            "MigrationFailuresAlarm",
            "ProviderFailuresAlarm",
            "TestLiveMismatchAlarm",
        }
        self.assertTrue(required.issubset(self.resources))
        for logical_id in required:
            alarm = self.resources[logical_id]
            self.assertEqual(alarm["Type"], "AWS::CloudWatch::Alarm")
            self.assertEqual(
                alarm["Properties"]["AlarmActions"], [{"Ref": "AlarmTopicArn"}]
            )
            self.assertEqual(alarm["Properties"]["TreatMissingData"], "notBreaching")

        public_routes = {
            "PublicRead4xxAlarm": "/features/commerce/public-read",
            "PublicAction4xxAlarm": "/features/commerce/public-action",
            "FiscalRequest4xxAlarm": "/features/commerce/fiscal/request",
        }
        method_settings = self.resources["CommerceApi"]["Properties"][
            "MethodSettings"
        ]
        self.assertEqual(
            method_settings,
            [
                {
                    "HttpMethod": "POST",
                    "ResourcePath": "/~1features~1commerce~1public-read",
                    "MetricsEnabled": True,
                    "ThrottlingRateLimit": 25,
                    "ThrottlingBurstLimit": 50,
                },
                {
                    "HttpMethod": "POST",
                    "ResourcePath": "/~1features~1commerce~1read",
                    "MetricsEnabled": True,
                    "ThrottlingRateLimit": 10,
                    "ThrottlingBurstLimit": 20,
                },
                {
                    "HttpMethod": "POST",
                    "ResourcePath": "/~1features~1commerce~1catalog~1action",
                    "MetricsEnabled": True,
                    "ThrottlingRateLimit": 5,
                    "ThrottlingBurstLimit": 10,
                },
                {
                    "HttpMethod": "POST",
                    "ResourcePath": "/~1features~1commerce~1inventory~1action",
                    "MetricsEnabled": True,
                    "ThrottlingRateLimit": 5,
                    "ThrottlingBurstLimit": 10,
                },
                {
                    "HttpMethod": "POST",
                    "ResourcePath": "/~1features~1commerce~1public-action",
                    "MetricsEnabled": True,
                    "ThrottlingRateLimit": 2,
                    "ThrottlingBurstLimit": 4,
                },
                {
                    "HttpMethod": "POST",
                    "ResourcePath": "/~1features~1commerce~1subscription~1action",
                    "MetricsEnabled": True,
                    "ThrottlingRateLimit": 5,
                    "ThrottlingBurstLimit": 10,
                },
                {
                    "HttpMethod": "POST",
                    "ResourcePath": "/~1features~1commerce~1fiscal~1request",
                    "MetricsEnabled": True,
                    "ThrottlingRateLimit": 5,
                    "ThrottlingBurstLimit": 10,
                },
                {
                    "HttpMethod": "POST",
                    "ResourcePath": "/~1features~1commerce~1fiscal~1admin",
                    "MetricsEnabled": True,
                    "ThrottlingRateLimit": 5,
                    "ThrottlingBurstLimit": 10,
                },
            ],
        )
        for logical_id, route in public_routes.items():
            properties = self.resources[logical_id]["Properties"]
            self.assertEqual(properties["Namespace"], "AWS/ApiGateway")
            self.assertEqual(properties["MetricName"], "4XXError")
            self.assertEqual(properties["Statistic"], "Sum")
            self.assertEqual(properties["Threshold"], 1)
            self.assertEqual(
                properties["Dimensions"],
                [
                    {"Name": "ApiName", "Value": {"Fn::Sub": "${AWS::StackName}-api"}},
                    {"Name": "Stage", "Value": {"Ref": "EnvironmentName"}},
                    {"Name": "Method", "Value": "POST"},
                    {"Name": "Resource", "Value": route},
                ],
            )
        self.assertNotIn("AccessLogSetting", self.template_text)
        self.assertFalse(
            any(
                resource["Type"].startswith("AWS::WAF")
                for resource in self.resources.values()
            )
        )

        custom_names = {
            "StaleReservationsAlarm": "StaleReservations",
            "MigrationBacklogAlarm": "MigrationBacklog",
            "MigrationFailuresAlarm": "MigrationFailures",
            "ProviderFailuresAlarm": "ProviderFailures",
            "TestLiveMismatchAlarm": "TestLiveMismatch",
        }
        for logical_id, metric_name in custom_names.items():
            properties = self.resources[logical_id]["Properties"]
            self.assertEqual(properties["Namespace"], "Zoolanding/Commerce")
            self.assertEqual(properties["MetricName"], metric_name)
            self.assertEqual(
                properties["Dimensions"],
                [{"Name": "Environment", "Value": {"Ref": "EnvironmentName"}}],
            )

    def test_ci_runs_on_every_branch_and_deploys_use_protected_artifacts(self):
        ci = (WORKFLOWS / "ci.yml").read_text(encoding="utf-8")
        self.assertRegex(ci, r"(?m)^on:\n  push:\s*$\n  pull_request:\s*$")
        self.assertNotIn("id-token: write", ci)
        self.assertIn("group: commerce-ci-${{ github.workflow }}-${{ github.ref }}", ci)
        self.assertIn("cancel-in-progress: true", ci)
        self.assertIn("timeout-minutes: 5", ci)
        self.assertIn("timeout-minutes: 30", ci)
        self.assertIn("fetch-depth: 0", ci)
        self.assertIn(
            "gitleaks/gitleaks-action@ff98106e4c7b2bc287b24eaf42907196329070c7",
            ci,
        )
        self.assertIn("Enforce protected promotion graph", ci)
        self.assertIn("python -m unittest discover", ci)
        self.assertIn("sam build --no-cached", ci)
        self.assertIn("python tests/verify_sam_build.py", ci)
        self.assertLess(
            ci.index("Verify exact clean commit"),
            ci.index("Scan current tree and history for secrets"),
        )
        self.assertLess(
            ci.index("Validate exact SAM build"),
            ci.index("Scan current tree and history for secrets"),
        )

        for filename, branch, source, environment in (
            ("deploy-test.yml", "test", "dev", "test"),
            ("deploy-production.yml", "main", "test", "production"),
        ):
            text = (WORKFLOWS / filename).read_text(encoding="utf-8")
            self.assertIn(f"branches: [{branch}]", text)
            self.assertIn(f"SOURCE_BRANCH: {source}", text)
            self.assertIn(f"TARGET_BRANCH: {branch}", text)
            self.assertIn(f"environment: {environment}", text)
            deploy_start = text.index("\n  deploy:")
            deploy_steps = text.index("\n    steps:", deploy_start)
            self.assertNotIn("${{ vars.", text)
            self.assertNotIn("${{ secrets.", text[deploy_start:deploy_steps])
            for secret_name in (
                "AWS_ROLE_ARN",
                "AWS_CLOUDFORMATION_ROLE_ARN",
                "ALARM_TOPIC_ARN",
                "COMMERCE_CURSOR_SIGNING_KEY",
            ):
                self.assertIn(f"${{{{ secrets.{secret_name} }}}}", text)
            self.assertIn("mask-aws-account-id: true", text)
            self.assertEqual(text.count("timeout-minutes: 30"), 2)
            self.assertEqual(text.count("id-token: write"), 1)
            self.assertIn("actions/upload-artifact@", text)
            self.assertIn("actions/download-artifact@", text)
            self.assertIn("build-manifest.sha256", text)
            self.assertIn("sha256sum --check --strict", text)
            self.assertGreaterEqual(
                text.count("find .aws-sam/build -type l -print -quit"), 2
            )
            self.assertIn("Reverify exact", text)
            self.assertLess(
                text.index("Reverify exact"),
                text.index("configure-aws-credentials@"),
            )
            self.assertIn("Validate exact cross-service SSM values", text)
            self.assertIn("aws ssm get-parameter", text)
            self.assertLess(
                text.index("configure-aws-credentials@"),
                text.index("Validate exact cross-service SSM values"),
            )
            self.assertLess(
                text.index("Validate exact cross-service SSM values"),
                text.index("sam deploy"),
            )
            self.assertIn(f'"EnvironmentName={environment}"', text)
            self.assertIn(
                '[[ "$cloudformation_account" = "$deployment_account" ]]', text
            )
            self.assertIn('[[ "$alarm_account" = "$deployment_account" ]]', text)
            self.assertIn('[[ "$alarm_region" = "$AWS_REGION" ]]', text)
            self.assertIn('[[ "$events_account" = "$deployment_account" ]]', text)
            self.assertIn('[[ "$events_region" = "$AWS_REGION" ]]', text)
            for parameter, suffix in (
                ("IntegrationsApiId", "services/integrations/api-id"),
                ("IntegrationEventsTopicArn", "topics/integration-events-arn"),
                ("ConfigRegistryTableName", "config/registry-table-name"),
                ("ConfigPayloadsBucketName", "config/payload-bucket-name"),
                ("AuthSessionTableName", "auth/session-table-name"),
                ("AuthUserStateTableName", "auth/user-state-table-name"),
            ):
                self.assertIn(
                    f'"{parameter}=/zoolanding/{environment}/{suffix}"', text
                )
            self._assert_actions_are_commit_pinned(text)

    def test_samconfig_has_only_test_and_production_deploy_profiles(self):
        with (ROOT / "samconfig.toml").open("rb") as handle:
            config = tomllib.load(handle)
        self.assertEqual(set(config), {"version", "test", "production"})
        self.assertEqual(
            config["test"]["deploy"]["parameters"]["parameter_overrides"],
            ["EnvironmentName=test"],
        )
        self.assertEqual(
            config["production"]["deploy"]["parameters"]["parameter_overrides"],
            ["EnvironmentName=production"],
        )
        self.assertNotIn(
            "dev", (ROOT / "samconfig.toml").read_text(encoding="utf-8").lower()
        )

    def test_readiness_smoke_is_present_and_has_no_cli_secret_surface(self):
        path = ROOT / "tools" / "commerce_readiness_smoke.py"
        self.assertTrue(path.exists())
        text = path.read_text(encoding="utf-8")
        for forbidden in (
            "--token",
            "--secret",
            "--password",
            "print(request",
            "print(response",
        ):
            self.assertNotIn(forbidden, text)

    def test_readme_documents_bootstrap_and_no_deployment_claim(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        for required in (
            "/zoolanding/{environment}/services/integrations/api-id",
            "/zoolanding/{environment}/topics/integration-events-arn",
            "/zoolanding/{environment}/config/registry-table-name",
            "/zoolanding/{environment}/config/payload-bucket-name",
            "/zoolanding/{environment}/auth/session-table-name",
            "/zoolanding/{environment}/auth/user-state-table-name",
            "/zoolanding/{environment}/services/commerce/api-id",
            "/zoolanding/{environment}/services/commerce/integrations-caller-role-arns",
            "/zoolanding/{environment}/topics/commerce-notification-requests-arn",
            "COMMERCE_CURSOR_SIGNING_KEY",
            "ALARM_TOPIC_ARN",
            "ZLP_COMMERCE_SMOKE_API_URL",
            "observedAtEpoch",
            "validated API URL stage",
            "No AWS deployment was performed",
        ):
            self.assertIn(required, readme)
        self.assertIn("dev -> test -> main", readme)
        self.assertIn("fail-closed two-pass bootstrap", readme)
        self.assertIn(
            "4XXError alarms intentionally combine HTTP 400, 403, and 429", readme
        )
        self.assertIn("No API Gateway access logs or AWS WAF", readme)

    def _assert_actions_are_commit_pinned(self, text):
        uses = re.findall(r"(?m)^\s*-?\s*uses:\s*([^\s#]+)", text)
        self.assertTrue(uses)
        for action in uses:
            if action.startswith("./"):
                continue
            self.assertRegex(action, r"^[^@]+@[a-f0-9]{40}$")


if __name__ == "__main__":
    unittest.main()
