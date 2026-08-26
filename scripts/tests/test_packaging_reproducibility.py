from __future__ import annotations

import gzip
import hashlib
import io
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


def write_test_gem(path: Path, *, compresslevel: int, mtime: int) -> None:
    members = {
        "metadata.gz": gzip.compress(b"---\nname: glyphastore\n", compresslevel=compresslevel, mtime=mtime),
        "data.tar.gz": gzip.compress(b"canonical data tar payload", compresslevel=compresslevel, mtime=mtime),
        "checksums.yaml.gz": gzip.compress(b"stale\n", compresslevel=compresslevel, mtime=mtime),
    }
    with tarfile.open(path, "w") as archive:
        for name, payload in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            info.mtime = mtime
            info.mode = 0o444
            archive.addfile(info, io.BytesIO(payload))


def write_test_perl_dist(path: Path, *, generated_by: str, backend: str) -> None:
    payloads = {
        "GlyphaStore-0.1.0/META.json": (
            '{\n   "generated_by" : "'
            + generated_by
            + '",\n   "x_serialization_backend" : "'
            + backend
            + '"\n}\n'
        ).encode(),
        "GlyphaStore-0.1.0/META.yml": (
            f"generated_by: '{generated_by}'\n"
            f"x_serialization_backend: '{backend}'\n"
        ).encode(),
        "GlyphaStore-0.1.0/lib/GlyphaStore.pm": b"package GlyphaStore; 1;\n",
    }
    with tarfile.open(path, "w:gz") as archive:
        for name, payload in payloads.items():
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))


class PackagingReproducibilityTests(unittest.TestCase):
    def test_perl_metadata_normalization_removes_generator_version_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.tar.gz"
            second = root / "second.tar.gz"
            write_test_perl_dist(first, generated_by="ExtUtils 7.62", backend="JSON::PP 4.07")
            write_test_perl_dist(second, generated_by="ExtUtils 7.78", backend="JSON::PP 4.16")
            normalizer = ROOT / "scripts/normalize-perl-dist-metadata.sh"
            tar_normalizer = ROOT / "scripts/normalize-tar-gz.sh"
            env = dict(os.environ, SOURCE_DATE_EPOCH="1780000000")
            for archive in (first, second):
                subprocess.run([normalizer, archive], check=True, env=env, capture_output=True, text=True)
                subprocess.run([tar_normalizer, archive], check=True, env=env, capture_output=True, text=True)
            self.assertEqual(first.read_bytes(), second.read_bytes())

    def test_ruby_gem_normalization_removes_compressor_and_metadata_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.gem"
            second = root / "second.gem"
            write_test_gem(first, compresslevel=1, mtime=100)
            write_test_gem(second, compresslevel=9, mtime=200)
            env = dict(os.environ, SOURCE_DATE_EPOCH="1780000000")
            normalizer = ROOT / "scripts/normalize-ruby-gem.sh"
            subprocess.run([normalizer, first], check=True, env=env, capture_output=True, text=True)
            subprocess.run([normalizer, second], check=True, env=env, capture_output=True, text=True)
            self.assertEqual(first.read_bytes(), second.read_bytes())

            with tarfile.open(first) as archive:
                metadata = archive.extractfile("metadata.gz")
                data = archive.extractfile("data.tar.gz")
                checksums = archive.extractfile("checksums.yaml.gz")
                assert metadata is not None and data is not None and checksums is not None
                metadata_bytes = metadata.read()
                data_bytes = data.read()
                checksum_text = gzip.decompress(checksums.read()).decode("utf-8")
            self.assertIn(hashlib.sha256(metadata_bytes).hexdigest(), checksum_text)
            self.assertIn(hashlib.sha512(metadata_bytes).hexdigest(), checksum_text)
            self.assertIn(hashlib.sha256(data_bytes).hexdigest(), checksum_text)
            self.assertIn(hashlib.sha512(data_bytes).hexdigest(), checksum_text)


if __name__ == "__main__":
    unittest.main()
