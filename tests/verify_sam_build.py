from pathlib import Path
import os
import re
import subprocess
import sys

import yaml
from samtranslator.translator.transform import transform


ROOT = Path(__file__).resolve().parents[1]


class _NoManagedPolicies:
    def load(self):
        return {}


def main():
    source = yaml.safe_load((ROOT / "template.yaml").read_text(encoding="utf-8"))
    function_handlers = {
        logical_id: resource["Properties"]["Handler"]
        for logical_id, resource in source["Resources"].items()
        if resource["Type"] == "AWS::Serverless::Function"
    }
    for logical_id, resource in source["Resources"].items():
        if resource["Type"] == "AWS::Serverless::Function":
            resource["Properties"]["CodeUri"] = f"s3://sam-transform-check/{logical_id}.zip"
    translated = transform(
        source,
        {
            "EnvironmentName": "test",
            "ConfigRegistryTableName": "zoolanding-config-registry-test",
            "ConfigPayloadsBucketName": "zoolanding-config-payloads-test",
            "AuthSessionTableName": "zoolanding-auth-sessions-test",
            "AuthUserStateTableName": "zoolanding-auth-users-test",
            "IntegrationEventsTopicArn": "arn:aws:sns:us-east-1:111122223333:zoolanding-events-test",
            "IntegrationsApiId": "abcdefghij",
            "CommerceCursorSigningKey": "A" * 43,
            "AlarmTopicArn": "arn:aws:sns:us-east-1:111122223333:operator-alarms-test",
            "FiscalProductionApproved": "false",
            "FiscalRetentionApprovalId": "",
            "FiscalAccessApprovalId": "",
        },
        _NoManagedPolicies(),
    )
    resources = translated["Resources"]
    function_roles = {
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
    for function_id, role_id in function_roles.items():
        assert resources[function_id]["Properties"]["Role"] == {
            "Fn::GetAtt": [role_id, "Arn"]
        }
        assert f"{function_id}Role" not in resources
        assert "Policies" not in resources[function_id]["Properties"]

    expected_actions = {
        "CatalogPublicReadRole": {
            "dynamodb:GetItem", "dynamodb:Query", "logs:CreateLogStream",
            "logs:PutLogEvents", "s3:GetObject",
        },
        "CatalogReadRole": {
            "dynamodb:GetItem", "dynamodb:Query", "logs:CreateLogStream",
            "logs:PutLogEvents", "s3:GetObject",
        },
        "CatalogActionRole": {
            "dynamodb:GetItem", "dynamodb:TransactWriteItems",
            "execute-api:Invoke", "logs:CreateLogStream", "logs:PutLogEvents",
            "s3:GetObject",
        },
        "InventoryActionRole": {
            "dynamodb:GetItem", "dynamodb:TransactWriteItems",
            "logs:CreateLogStream", "logs:PutLogEvents", "s3:GetObject",
        },
        "CheckoutRole": {
            "dynamodb:GetItem", "dynamodb:TransactWriteItems",
            "execute-api:Invoke", "logs:CreateLogStream", "logs:PutLogEvents",
            "s3:GetObject",
        },
        "SubscriptionActionRole": {
            "dynamodb:GetItem", "dynamodb:TransactWriteItems",
            "execute-api:Invoke", "logs:CreateLogStream", "logs:PutLogEvents",
            "s3:GetObject",
        },
        "FiscalRequestRole": {
            "dynamodb:GetItem", "dynamodb:TransactWriteItems",
            "logs:CreateLogStream", "logs:PutLogEvents", "s3:GetObject",
        },
        "FiscalAdminRole": {
            "dynamodb:GetItem", "dynamodb:TransactWriteItems",
            "logs:CreateLogStream", "logs:PutLogEvents", "s3:GetObject",
        },
        "IntegrationEventWorkerRole": {
            "dynamodb:GetItem",
            "dynamodb:TransactWriteItems",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
            "sqs:DeleteMessage",
            "sqs:GetQueueAttributes",
            "sqs:ReceiveMessage",
        },
        "OutboxRelayRole": {
            "dynamodb:DescribeStream",
            "dynamodb:GetItem",
            "dynamodb:GetRecords",
            "dynamodb:GetShardIterator",
            "dynamodb:ListStreams",
            "dynamodb:TransactWriteItems",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
            "sns:Publish",
            "sqs:SendMessage",
        },
        "ReservationReconcilerRole": {
            "dynamodb:GetItem", "dynamodb:Query", "dynamodb:TransactWriteItems",
            "execute-api:Invoke", "logs:CreateLogStream", "logs:PutLogEvents",
            "s3:GetObject",
        },
    }
    iam_roles = {
        logical_id
        for logical_id, resource in resources.items()
        if resource["Type"] == "AWS::IAM::Role"
    }
    assert iam_roles == set(expected_actions)
    for role_id, allowed_actions in expected_actions.items():
        role_properties = resources[role_id]["Properties"]
        assert "ManagedPolicyArns" not in role_properties
        statements = role_properties["Policies"][0]["PolicyDocument"]["Statement"]
        actual_actions = set()
        for statement in statements:
            actions = statement["Action"]
            actual_actions.update(actions if isinstance(actions, list) else [actions])
            assert statement["Resource"] != "*"
            if isinstance(statement["Resource"], list):
                assert "*" not in statement["Resource"]
        assert actual_actions == allowed_actions, role_id

    with_cursor_key = {
        logical_id
        for logical_id, resource in resources.items()
        if resource["Type"] == "AWS::Lambda::Function"
        and "COMMERCE_CURSOR_SIGNING_KEY"
        in resource["Properties"].get("Environment", {}).get("Variables", {})
    }
    assert with_cursor_key == {"CatalogPublicReadFunction", "CatalogReadFunction"}
    for function_id in with_cursor_key:
        assert resources[function_id]["Properties"]["Environment"]["Variables"][
            "COMMERCE_CURSOR_SIGNING_KEY"
        ] == {"Ref": "CommerceCursorSigningKey"}

    rendered = str(translated)
    for forbidden in (
        "AWSLambdaSQSQueueExecutionRole",
        "AWSLambdaDynamoDBExecutionRole",
        "AWSXRayDaemonWriteAccess",
        "xray:PutTraceSegments",
        "xray:PutTelemetryRecords",
    ):
        assert forbidden not in rendered, forbidden

    for resource in resources.values():
        if resource["Type"] == "AWS::Lambda::Function":
            assert "TracingConfig" not in resource["Properties"]

    functions = list(function_handlers)
    build_root = ROOT / ".aws-sam" / "build"
    safe_environment = os.environ.copy()
    for key in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    ):
        safe_environment.pop(key, None)
    safe_environment["AWS_EC2_METADATA_DISABLED"] = "true"

    for logical_id in functions:
        packaged_root = build_root / logical_id
        packaged_boto3 = packaged_root / "boto3" / "__init__.py"
        assert packaged_boto3.is_file(), f"boto3 was not packaged for {logical_id}"
        match = re.search(
            r"^__version__\s*=\s*['\"]([^'\"]+)",
            packaged_boto3.read_text(encoding="utf-8"),
            re.MULTILINE,
        )
        assert match is not None and match.group(1) == "1.39.13", logical_id

        handler = function_handlers[logical_id]
        module_name, attribute_name = handler.rsplit(".", 1)
        import_environment = safe_environment.copy()
        import_environment["PYTHONPATH"] = str(packaged_root)
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import importlib,sys;"
                    "sys.path.insert(0,sys.argv[3]);"
                    "module=importlib.import_module(sys.argv[1]);"
                    "assert callable(getattr(module,sys.argv[2]))"
                ),
                module_name,
                attribute_name,
                str(packaged_root),
            ],
            cwd=packaged_root,
            env=import_environment,
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        assert completed.returncode == 0, (
            f"built handler import failed for {logical_id}: {completed.stderr.strip()}"
        )

    print(f"verified {len(functions)} packaged and importable functions")
    for role_id, actions in expected_actions.items():
        print(f"{role_id}: {', '.join(sorted(actions))}")


if __name__ == "__main__":
    main()
