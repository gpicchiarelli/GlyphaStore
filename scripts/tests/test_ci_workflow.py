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


if __name__ == "__main__":
    unittest.main()
