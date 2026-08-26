from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from engineering.tools.validate_bsd_packaging import PackagingError, validate


ROOT = Path(__file__).resolve().parents[2]


class BsdPackagingTests(unittest.TestCase):
    def test_reference_ports_match_version_and_service_authorities(self) -> None:
        validate(ROOT)

    def test_release_mode_fails_without_native_tag_and_account_evidence(self) -> None:
        with self.assertRaisesRegex(PackagingError, "not proven"):
            validate(ROOT, release=True)

    def test_release_mode_does_not_require_a_circular_source_archive_distinfo(self) -> None:
        with tempfile.TemporaryDirectory(prefix="glyphastore-bsd-release-test-") as temporary:
            copied = Path(temporary)
            shutil.copy2(ROOT / "VERSION", copied / "VERSION")
            shutil.copy2(ROOT / "ABI_VERSION", copied / "ABI_VERSION")
            shutil.copy2(ROOT / "LICENSE", copied / "LICENSE")
            shutil.copytree(ROOT / "packaging", copied / "packaging")
            for platform in ("freebsd", "openbsd"):
                (copied / "packaging" / platform / "PORTS_ACCOUNT_REGISTERED").touch()

            validate(copied, release=True)
            self.assertFalse((copied / "packaging/freebsd/distinfo").exists())
            self.assertFalse((copied / "packaging/openbsd/distinfo").exists())

    def test_native_ci_consumes_installed_abi_and_retains_logs(self) -> None:
        freebsd_script = (ROOT / "scripts/ci-freebsd.sh").read_text(encoding="utf-8")
        openbsd_script = (ROOT / "scripts/ci-openbsd-libressl.sh").read_text(encoding="utf-8")
        for script in (freebsd_script, openbsd_script):
            self.assertIn("cmake --install", script)
            self.assertIn("tests/consumer/abi.c", script)
            self.assertIn("pkg-config --cflags --libs glyphastore-abi", script)
            self.assertIn('LD_LIBRARY_PATH="$install_root/lib"', script)
        freebsd_workflow = (ROOT / ".github/workflows/freebsd.yml").read_text(encoding="utf-8")
        openbsd_workflow = (ROOT / ".github/workflows/openbsd-libressl.yml").read_text(
            encoding="utf-8"
        )
        for workflow in (freebsd_workflow, openbsd_workflow):
            self.assertIn("copyback: true", workflow)
            self.assertIn("actions/upload-artifact@", workflow)
            self.assertIn("engineering/evidence/native-ci", workflow)

    def test_abi_drift_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="glyphastore-bsd-port-test-") as temporary:
            copied = Path(temporary)
            shutil.copy2(ROOT / "VERSION", copied / "VERSION")
            shutil.copy2(ROOT / "ABI_VERSION", copied / "ABI_VERSION")
            shutil.copy2(ROOT / "LICENSE", copied / "LICENSE")
            shutil.copytree(ROOT / "packaging", copied / "packaging")
            makefile = copied / "packaging/openbsd/Makefile"
            makefile.write_text(
                makefile.read_text(encoding="utf-8").replace(
                    "SHARED_LIBS +=  glyphastore 1.0", "SHARED_LIBS +=  glyphastore 2.0"
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(PackagingError, "OpenBSD ABI"):
                validate(copied)

    def test_license_metadata_drift_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="glyphastore-bsd-license-test-") as temporary:
            copied = Path(temporary)
            shutil.copy2(ROOT / "VERSION", copied / "VERSION")
            shutil.copy2(ROOT / "ABI_VERSION", copied / "ABI_VERSION")
            shutil.copy2(ROOT / "LICENSE", copied / "LICENSE")
            shutil.copytree(ROOT / "packaging", copied / "packaging")
            makefile = copied / "packaging/freebsd/Makefile"
            makefile.write_text(
                makefile.read_text(encoding="utf-8").replace("BSD3CLAUSE", "APACHE20"),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(PackagingError, "FreeBSD BSD-3-Clause"):
                validate(copied)


if __name__ == "__main__":
    unittest.main()
