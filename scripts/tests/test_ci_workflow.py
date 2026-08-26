from __future__ import annotations

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]


class CiWorkflowTests(unittest.TestCase):
    def test_manpage_job_installs_the_authoritative_runtime_component(self) -> None:
        workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        self.assertIn(
            'cmake --install build/man-ci --prefix "${dest}" --component Runtime',
            workflow,
        )
        self.assertNotIn("--component ManPages", workflow)

    def test_supply_chain_retains_primary_archives_for_cross_builder_diagnostics(self) -> None:
        workflow = (ROOT / ".github/workflows/supply-chain.yml").read_text(encoding="utf-8")
        upload = workflow[workflow.index("Retain SDK artifacts, checksums") :]
        for pattern in ("*.whl", "*.gem", "*.tar.gz", "*package-info.txt"):
            self.assertIn(f"dist/sdk-artifacts/{pattern}", upload)


if __name__ == "__main__":
    unittest.main()
