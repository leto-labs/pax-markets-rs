#!/usr/bin/env python3
"""Build the public example in an isolated Cargo workspace; never publish."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--live', action='store_true', help='also observe public data for 15 seconds')
    args = parser.parse_args()
    workspace = tomllib.loads((ROOT / 'Cargo.toml').read_text())['workspace']
    with tempfile.TemporaryDirectory(prefix='pax-sdk-consumer-') as temporary:
        directory = Path(temporary)
        source = directory / 'src'
        source.mkdir()
        shutil.copyfile(ROOT / 'examples/public-book.rs', source / 'main.rs')
        for fixture in ('snapshot.json', 'xa.json'):
            shutil.copyfile(ROOT / 'crates/pax-types/tests/fixtures' / fixture, source / fixture)
        # Retain dependency versions from the repository lock; Cargo adds only
        # the independent root package and prunes unrelated workspace packages.
        shutil.copyfile(ROOT / 'Cargo.lock', directory / 'Cargo.lock')
        manifest = ['[workspace]', '[package]', 'name = "pax-sdk-consumer-check"',
                    'version = "0.0.0"', 'edition = "2024"', 'publish = false', '[dependencies]']
        for package in ('pax-types', 'pax-client', 'pax-book'):
            path = json.dumps(str(ROOT / 'crates' / package))
            defaults = ', default-features = false' if package == 'pax-client' else ''
            manifest.append(f'{package} = {{ path = {path}{defaults} }}')
        version = workspace['dependencies']['tokio']['version']
        manifest += [f'tokio = {{ version = "={version}", features = ["macros", "rt", "time"] }}',
                     '[features]', 'client-defaults = ["pax-client/default"]']
        (directory / 'Cargo.toml').write_text('\n'.join(manifest) + '\n')
        env = dict(os.environ, CARGO_TARGET_DIR=str(ROOT / 'target/sdk-consumer'))

        def cargo(*arguments, capture=False):
            return subprocess.run([str(ROOT / 'scripts/cargo.sh'), arguments[0],
                                   '--manifest-path', str(directory / 'Cargo.toml'), *arguments[1:]],
                                  cwd=ROOT, env=env, check=True, text=True,
                                  stdout=subprocess.PIPE if capture else None).stdout

        cargo('metadata', '--offline', '--format-version', '1', capture=True)
        for features in ([], ['--features', 'client-defaults']):
            metadata = json.loads(cargo('metadata', '--offline', '--locked', '--format-version', '1',
                                        *features, capture=True))
            packages = {p['name'] for p in metadata['packages']}
            forbidden = {'pax-demo', 'pax-lab', 'pax-ops', 'axum', 'rcgen'}
            if not features:
                forbidden |= {'nix', 'rtrb'}
            if packages & forbidden:
                raise SystemExit(f'unexpected consumer dependencies: {sorted(packages & forbidden)}')
            cargo('clippy', '--offline', '--locked', *features, '--', '-D', 'warnings')
            cargo('run', '--offline', '--locked', *features)
        if args.live:
            subprocess.run([str(ROOT / 'target/sdk-consumer/debug/pax-sdk-consumer-check'), '--live'],
                           check=True, timeout=35)
    print('isolated minimal/default SDK consumer verified; no registry publication')


if __name__ == '__main__':
    main()
