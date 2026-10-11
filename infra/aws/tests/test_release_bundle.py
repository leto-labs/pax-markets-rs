"""Independent archive fixtures verify bounded, extraction-free release reuse."""
import gzip
import hashlib
import io
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import release_bundle


class ReleaseBundleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.directory = self.root / 'artifact'
        self.directory.mkdir()
        for name, data in [('pax-demo', b'fixture executable'), ('build.json', b'{"revision":"test"}')]:
            (self.directory / name).write_bytes(data)
            (self.directory / name).chmod(0o755 if name == 'pax-demo' else 0o644)
        checksums = ''.join(f'{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n'
                            for p in sorted(self.directory.iterdir()))
        (self.directory / 'SHA256SUMS').write_text(checksums)
        (self.directory / 'SHA256SUMS').chmod(0o644)
        self.bundle = self.root / 'bundle.tar.gz'
        self.archive()

    def archive(self, extra=None, omit=None):
        with tarfile.open(self.bundle, 'w:gz', format=tarfile.GNU_FORMAT) as archive:
            root = tarfile.TarInfo('./')
            root.type = tarfile.DIRTYPE
            archive.addfile(root)
            for path in sorted(self.directory.iterdir()):
                if path.name != omit:
                    archive.add(path, arcname='./' + path.name, recursive=False)
            if extra:
                archive.addfile(*extra)

    def test_repeated_verification_preserves_archive_and_staged_files(self):
        before = self.bundle.read_bytes()
        staged = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in self.directory.iterdir()}
        for _ in range(2):
            self.assertEqual(release_bundle.verify(self.directory, self.bundle)['files'], 3)
        self.assertEqual(self.bundle.read_bytes(), before)
        self.assertEqual(staged, {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in self.directory.iterdir()})

    def test_hashing_uses_bounded_reads_without_python311_api(self):
        for data in [b'', b'fixture', b'x' * (3 * release_bundle.CHUNK + 7)]:
            class BoundedStream(io.BytesIO):
                def read(self, size=-1):
                    if not 0 < size <= release_bundle.CHUNK:
                        raise AssertionError('unbounded staged release read')
                    return super().read(size)

            with patch.object(hashlib, 'file_digest', create=True, side_effect=AssertionError('Python 3.11 API')):
                with patch.object(Path, 'open', return_value=BoundedStream(data)):
                    self.assertEqual(release_bundle.file_digest(self.root / 'unused'), hashlib.sha256(data).hexdigest())

    def test_changed_bytes_manifest_permissions_and_files_are_refused(self):
        original = self.bundle.read_bytes()
        for name, data in [('pax-demo', b'changed executable'), ('SHA256SUMS', b'stale manifest')]:
            path = self.directory / name
            old = path.read_bytes()
            path.write_bytes(data)
            with self.assertRaises(ValueError):
                release_bundle.verify(self.directory, self.bundle)
            path.write_bytes(old)
        (self.directory / 'pax-demo').chmod(0o644)
        with self.assertRaises(ValueError):
            release_bundle.verify(self.directory, self.bundle)
        self.assertEqual(self.bundle.read_bytes(), original)

    def test_missing_extra_duplicate_linked_nested_and_absolute_members_are_refused(self):
        self.archive(omit='pax-demo')
        with self.assertRaises(ValueError):
            release_bundle.verify(self.directory, self.bundle)
        for name, kind in [('extra', tarfile.REGTYPE), ('./pax-demo', tarfile.REGTYPE),
                           ('./pax-demo', tarfile.SYMTYPE), ('./pax-demo', tarfile.LNKTYPE),
                           ('../pax-demo', tarfile.REGTYPE), ('/pax-demo', tarfile.REGTYPE),
                           ('./', tarfile.DIRTYPE)]:
            info = tarfile.TarInfo(name)
            info.type = kind
            if kind in (tarfile.SYMTYPE, tarfile.LNKTYPE):
                info.linkname = 'pax-demo'
            self.archive(extra=(info, io.BytesIO()))
            with self.subTest(name=name, kind=kind), self.assertRaises(ValueError):
                release_bundle.verify(self.directory, self.bundle)

    def test_truncated_corrupt_and_trailing_archives_are_refused(self):
        valid = self.bundle.read_bytes()
        raw = gzip.decompress(valid)
        corrupted = bytearray(valid)
        corrupted[-8] ^= 1  # CRC, independent of member content verification.
        for packed in [valid[:-6], bytes(corrupted), gzip.compress(raw[:512]),
                       gzip.compress(raw + b'nonzero trailing data'),
                       gzip.compress(b'bad header' + raw[10:])]:
            self.bundle.write_bytes(packed)
            with self.assertRaises((ValueError, OSError, EOFError, tarfile.TarError)):
                release_bundle.verify(self.directory, self.bundle)

    def test_aliases_and_resource_limits_are_refused_without_extraction(self):
        alias = self.root / 'alias.tar.gz'
        alias.symlink_to(self.bundle)
        with self.assertRaises(ValueError):
            release_bundle.verify(self.directory, alias)
        for setting, value in [('FILE_LIMIT', 2), ('LIMIT', self.bundle.stat().st_size + 1)]:
            with patch.object(release_bundle, setting, value), self.assertRaises(ValueError):
                release_bundle.verify(self.directory, self.bundle)
        path = self.directory / 'build.json'
        path.unlink()
        path.symlink_to(self.directory / 'pax-demo')
        with self.assertRaises(ValueError):
            release_bundle.verify(self.directory, self.bundle)
        self.assertEqual(sorted(p.name for p in self.root.iterdir()), ['alias.tar.gz', 'artifact', 'bundle.tar.gz'])


if __name__ == '__main__':
    unittest.main()
