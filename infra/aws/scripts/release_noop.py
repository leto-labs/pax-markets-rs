"""Read-only managed-release preflight. No restart, upload or file mutation."""
import hashlib
import json
from pathlib import Path
import pwd
import re
import shlex
import subprocess
import time
import urllib.request


def bounded(path, limit=1024 * 1024):
    with path.open('rb') as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError('preflight input exceeds bound')
    return data


def digest(path):
    observed = hashlib.sha256()
    size = 0
    with path.open('rb') as stream:
        while data := stream.read(128 * 1024):
            size += len(data)
            if size > 1024 * 1024 * 1024:
                raise ValueError('preflight executable exceeds bound')
            observed.update(data)
    return observed.hexdigest()


def service_status():
    result = subprocess.run(['systemctl', 'show', 'pax-demo.service', '-p',
                             'ActiveState,SubState,MainPID,FragmentPath,NeedDaemonReload,DropInPaths'],
                            capture_output=True, text=True, check=True, timeout=10)
    if len(result.stdout) > 16384:
        raise ValueError('service metadata exceeds bound')
    return dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)


def health_status():
    with urllib.request.urlopen('http://127.0.0.1/api/state', timeout=3) as response:
        data = response.read(4 * 1024 * 1024 + 1)
    if len(data) > 4 * 1024 * 1024:
        raise ValueError('preflight health exceeds bound')
    return json.loads(data)['state']


def process_start(proc, pid):
    text = bounded(proc / str(pid) / 'stat', 16384).decode()
    if not text.startswith(f'{pid} ('):
        raise ValueError('process identity mismatch')
    tail = text.rpartition(')')[2].split()
    start = tail[19]  # /proc stat field 22; comm can contain spaces or ')'.
    if not start.isdecimal():
        raise ValueError('process start time unavailable')
    return start


def check(request, root=Path('/opt/pax'), proc=Path('/proc'),
          service=service_status, health=health_status, owner_uid=None):
    started = time.monotonic_ns()
    health_ns = None
    reason = 'request'
    try:
        identity = request['release']
        if not re.fullmatch(r'[0-9a-f]{64}-[0-9a-f]{16}', identity):
            raise ValueError('invalid release identity')
        expected = root / 'releases' / identity
        current = root / 'current'
        reason = 'release'
        if expected.is_symlink() or expected.resolve(strict=True).parent != (root / 'releases').resolve(strict=True) or not current.is_symlink() or current.resolve(strict=True) != expected.resolve(strict=True):
            raise ValueError('current release differs')
        reason = 'unit'
        desired_unit = request['unit'].encode()
        if bounded(expected / 'pax-demo.service', 65536) != desired_unit:
            raise ValueError('unit differs')
        lines = [line.removeprefix('ExecStart=') for line in request['unit'].splitlines()
                 if line.startswith('ExecStart=')]
        if len(lines) != 1:
            raise ValueError('ambiguous unit command')
        argv = shlex.split(lines[0])
        reason = 'build'
        if json.loads(bounded(expected / 'build.json')) != request['build']:
            raise ValueError('build differs')
        binary = expected / 'pax-demo'
        binary_digest = request['build']['binary_sha256']
        if not re.fullmatch(r'[0-9a-f]{64}', binary_digest) or digest(binary) != binary_digest:
            raise ValueError('binary differs')
        reason = 'service'
        before = service()
        if any(before.get(key) != value for key, value in {
                'ActiveState': 'active', 'SubState': 'running', 'NeedDaemonReload': 'no',
                'DropInPaths': ''}.items()):
            raise ValueError('service is not the loaded managed unit')
        if Path(before['FragmentPath']).resolve(strict=True) != (expected / 'pax-demo.service').resolve(strict=True):
            raise ValueError('loaded fragment differs')
        pid = int(before['MainPID'])
        if pid <= 0:
            raise ValueError('service has no process')
        reason = 'process'
        start = process_start(proc, pid)
        running = proc / str(pid)
        if (running / 'exe').resolve(strict=True) != binary.resolve(strict=True) or digest(running / 'exe') != binary_digest:
            raise ValueError('running executable differs')
        command = bounded(running / 'cmdline', 65536)
        if command != b'\0'.join(arg.encode() for arg in argv) + b'\0':
            raise ValueError('running arguments differ')
        uid = pwd.getpwnam('pax').pw_uid if owner_uid is None else owner_uid
        status = bounded(running / 'status', 65536).decode().splitlines()
        uids = [line.split()[1:] for line in status if line.startswith('Uid:')]
        if len(uids) != 1 or uids[0] != [str(uid)] * 4:
            raise ValueError('process user differs')
        reason = 'health'
        health_started = time.monotonic_ns()
        try:
            state = health()
            books = state['view']['markets']
            markets = request['markets']
            if not state['transport_connected'] or not state['view']['connected'] or not markets:
                raise ValueError('feed is disconnected')
            if len(books) != len(markets) or {b['id'] for b in books} != set(markets) or any(
                    b['status'] != 'live' or b['sequence'] is None for b in books):
                raise ValueError('requested books are not synchronized')
        finally:
            health_ns = time.monotonic_ns() - health_started
        reason = 'changed_process'
        after = service()
        if after != before or process_start(proc, pid) != start or current.resolve(strict=True) != expected.resolve(strict=True) or (running / 'exe').resolve(strict=True) != binary.resolve(strict=True) or digest(running / 'exe') != binary_digest or bounded(running / 'cmdline', 65536) != command or bounded(expected / 'pax-demo.service', 65536) != desired_unit:
            raise ValueError('process or service changed during health check')
        return {'kind': 'pax_release_preflight', 'eligible': True, 'reason': None,
                'pid': pid, 'process_start_ticks': start, 'binary_sha256': binary_digest,
                'unit_sha256': hashlib.sha256(desired_unit).hexdigest(),
                'health_ns': health_ns,
                'validation_ns': time.monotonic_ns() - started}
    except (OSError, ValueError, KeyError, TypeError, IndexError, subprocess.SubprocessError):
        return {'kind': 'pax_release_preflight', 'eligible': False, 'reason': reason,
                'health_ns': health_ns,
                'validation_ns': time.monotonic_ns() - started}
