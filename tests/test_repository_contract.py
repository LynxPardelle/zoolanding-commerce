from pathlib import Path
import re
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[1]


class RepositoryContractTests(unittest.TestCase):
    def test_scaffold_contains_only_the_approved_workflows(self):
        workflows = ROOT / ".github" / "workflows"

        self.assertEqual(
            {path.name for path in workflows.glob("*.yml")},
            {"ci.yml", "deploy-test.yml", "deploy-production.yml"},
        )
        self.assertFalse((workflows / "deploy-dev.yml").exists())

    def test_runtime_dependency_and_sam_profiles_are_closed(self):
        self.assertEqual(
            (ROOT / "requirements.txt").read_text(encoding="utf-8").strip(),
            "boto3==1.39.13",
        )

        config = tomllib.loads((ROOT / "samconfig.toml").read_text(encoding="utf-8"))
        self.assertEqual(set(config), {"version", "test", "prod"})
        self.assertEqual(config["test"]["deploy"]["parameters"]["parameter_overrides"], ["EnvironmentName=test"])
        self.assertEqual(config["prod"]["deploy"]["parameters"]["parameter_overrides"], ["EnvironmentName=prod"])

    def test_template_and_ci_pin_the_approved_python_and_sam_versions(self):
        template = (ROOT / "template.yaml").read_text(encoding="utf-8")
        ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

        self.assertIn("Transform: AWS::Serverless-2016-10-31", template)
        self.assertIn("Runtime: python3.13", template)
        self.assertRegex(template, re.compile(r"AllowedValues:\s*\n\s*- test\s*\n\s*- prod"))
        self.assertNotIn("dev", (ROOT / "samconfig.toml").read_text(encoding="utf-8").lower())
        self.assertIn("python-version: '3.13'", ci)
        self.assertIn("version: 1.163.0", ci)

    def test_repository_entrypoints_state_local_only_and_no_dev_aws(self):
        for name in ("AGENTS.md", "Codex.md", "README.md"):
            self.assertTrue((ROOT / name).is_file(), name)

        combined = "\n".join(
            (ROOT / name).read_text(encoding="utf-8")
            for name in ("AGENTS.md", "README.md")
        ).lower()
        self.assertIn("local-only", combined)
        self.assertIn("no aws `dev`", combined)


if __name__ == "__main__":
    unittest.main()
