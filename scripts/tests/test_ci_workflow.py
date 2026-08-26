from __future__ import annotations

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]


class CiWorkflowTests(unittest.TestCase):
    def test_manpage_job_installs_the_authoritative_runtime_component(self) -> None:
        workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        self.assertIn("-DBUILD_TESTING=OFF", workflow)
        self.assertIn("cmake --build build/man-ci\n", workflow)
        self.assertNotIn(
            "cmake --build build/man-ci --target glyphastore_manpages",
            workflow,
        )
        self.assertIn(
            'cmake --install build/man-ci --prefix "${dest}" --component Runtime',
            workflow,
        )
        self.assertNotIn("--component ManPages", workflow)

    def test_installed_cpp_smoke_cannot_resolve_a_build_tree_package(self) -> None:
        builder = (ROOT / "scripts/build-installed-cpp-interop.sh").read_text(
            encoding="utf-8"
        )
        installed = (
            ROOT / "scripts/test-secure-profile-installed-artifacts.sh"
        ).read_text(encoding="utf-8")

        self.assertIn('PATHS "\\${GLYPHASTORE_PACKAGE_DIR}"', builder)
        self.assertIn("NO_DEFAULT_PATH", builder)
        self.assertIn('-DGLYPHASTORE_PACKAGE_DIR="$package_dir"', builder)
        self.assertNotIn('-DGlyphaStore_DIR="$package_dir"', builder)
        self.assertIn('--component AbiRuntime', builder)
        self.assertIn('--component Development', builder)
        self.assertIn(
            'cpp_artifact="$(cd "$(dirname "$daemon")" && pwd -P)"',
            installed,
        )
        self.assertNotIn('$(dirname "$daemon")/..', installed)

    def test_supply_chain_retains_primary_archives_for_cross_builder_diagnostics(self) -> None:
        workflow = (ROOT / ".github/workflows/supply-chain.yml").read_text(encoding="utf-8")
        upload = workflow[workflow.index("Retain SDK artifacts, checksums") :]
        for pattern in ("*.whl", "*.gem", "*.tar.gz", "*package-info.txt"):
            self.assertIn(f"dist/sdk-artifacts/{pattern}", upload)

    def test_sanitizers_cancel_only_obsolete_runs_for_the_same_branch(self) -> None:
        workflow = (ROOT / ".github/workflows/sanitizers.yml").read_text(encoding="utf-8")
        self.assertIn("group: sanitizers-${{ github.head_ref || github.ref_name }}", workflow)
        self.assertIn("cancel-in-progress: true", workflow)


if __name__ == "__main__":
    unittest.main()
