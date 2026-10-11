#!/usr/bin/env python3
"""Verify the flat regular-file subset produced by the release bundler, without extraction."""
import argparse
import gzip
import hashlib
from pathlib import Path
import stat
import tarfile

LIMIT = 1024 * 1024 * 1024
FILE_LIMIT = 128
CHUNK = 128 * 1024


def regular(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_size > LIMIT:
        raise ValueError('release input is not a bounded regular file')
    return info


def file_digest(path):
    # AL2023's Python 3.9 predates hashlib.file_digest; keep reads bounded.
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        while data := stream.read(CHUNK):
            digest.update(data)
    return digest.hexdigest()


def verify(directory, bundle):
    regular(bundle)
    paths = list(directory.iterdir())
    if not 1 <= len(paths) <= FILE_LIMIT:
        raise ValueError('release file count exceeds bounds')
    expected = {}
    for path in sorted(paths):
        name = path.name
        if not name or len(name) > 96 or not name.isascii() or any(
                not (c.isalnum() or c in '-_.') for c in name):
            raise ValueError('release filenames must be short flat ASCII names')
        info = regular(path)
        expected[name] = (info.st_size, info.st_mode & 0o7777, file_digest(path))
    checksums = ''.join(f'{value[2]}  {name}\n' for name, value in expected.items()
                        if name != 'SHA256SUMS')
    if expected.get('SHA256SUMS', (None, None, None))[2] != hashlib.sha256(checksums.encode()).hexdigest():
        raise ValueError('staged release checksum manifest differs from current files')
    seen = set()
    consumed = 0
    root_seen = False
    with bundle.open('rb') as packed, gzip.GzipFile(fileobj=packed) as stream:
        def read(size):
            nonlocal consumed
            if size > CHUNK or size < 0:
                raise ValueError('invalid release validation read bound')
            data = stream.read(size)
            consumed += len(data)
            if consumed > LIMIT:
                raise ValueError('decompressed release exceeds bound')
            return data

        while True:
            header = read(512)
            if len(header) != 512:
                raise ValueError('truncated release archive')
            if not any(header):
                padding = 512
                while trailing := read(CHUNK):
                    if any(trailing):
                        raise ValueError('unexpected data after release archive end')
                    padding += len(trailing)
                if padding < 1024 or padding % 512:
                    raise ValueError('invalid release archive padding')
                break  # Full gzip read also validates its trailer/CRC.
            member = tarfile.TarInfo.frombuf(header, 'utf-8', 'strict')
            if member.name in ('.', './') and member.isdir() and member.size == 0:
                if root_seen:
                    raise ValueError('duplicate release root entry')
                root_seen = True
                continue
            name = member.name.removeprefix('./')
            if member.type not in (tarfile.REGTYPE, tarfile.AREGTYPE) or member.linkname or name in seen or name not in expected:
                raise ValueError('unexpected, linked or duplicate release member')
            size, mode, digest = expected[name]
            if member.size != size or member.mode != mode:
                raise ValueError('release member size or permissions differ')
            observed = hashlib.sha256()
            remaining = size
            while remaining:
                data = read(min(remaining, CHUNK))
                if not data:
                    raise ValueError('truncated release member')
                observed.update(data)
                remaining -= len(data)
            padding = (-size) % 512
            if len(read(padding)) != padding or observed.hexdigest() != digest:
                raise ValueError('release member content differs')
            seen.add(name)
    if seen != expected.keys():
        raise ValueError('release membership differs from staged files')
    return {'verified': True, 'files': len(seen), 'decompressed_bytes': consumed}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('bundle', type=Path)
    args = parser.parse_args()
    verify(args.directory, args.bundle)
