"""Independent process/service/book fixtures for deployment preflight."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import release_noop


class ReleaseNoopTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.identity = 'a' * 64 + '-' + 'b' * 16
        self.release = self.root / 'releases' / self.identity
        self.release.mkdir(parents=True)
        (self.root / 'current').symlink_to(self.release)
        binary = b'independent executable fixture'
        (self.release / 'pax-demo').write_bytes(binary)
        self.unit = f'[Service]\nUser=pax\nExecStart={self.root}/current/pax-demo serve --markets 1\n'
        (self.release / 'pax-demo.service').write_text(self.unit)
        self.build = {'binary_sha256': hashlib.sha256(binary).hexdigest(), 'source_revision': 'fixture'}
        (self.release / 'build.json').write_text(json.dumps(self.build))
        self.proc = self.root / 'proc'
        running = self.proc / '42'
        running.mkdir(parents=True)
        (running / 'exe').symlink_to(self.release / 'pax-demo')
        (running / 'cmdline').write_bytes(b'\0'.join(arg.encode() for arg in [str(self.root / 'current' / 'pax-demo'), 'serve', '--markets', '1']) + b'\0')
        # Field 22 follows 19 tail fields after comm, which deliberately has spaces.
        (running / 'stat').write_text('42 (demo name) R ' + '0 ' * 18 + '1234 0\n')
        (running / 'status').write_text('Name: demo\nUid:\t99\t99\t99\t99\n')
        self.service = {'ActiveState': 'active', 'SubState': 'running', 'MainPID': '42',
                        'FragmentPath': str(self.release / 'pax-demo.service'),
                        'NeedDaemonReload': 'no', 'DropInPaths': ''}
        self.health = {'transport_connected': True, 'view': {'connected': True,
                       'markets': [{'id': '1', 'status': 'live', 'sequence': '10'}]}}
        self.request = {'release': self.identity, 'unit': self.unit,
                        'build': self.build, 'markets': ['1']}

    def check(self, service=None, health=None, request=None):
        return release_noop.check(request or self.request, root=self.root, proc=self.proc,
                                  service=service or (lambda: self.service),
                                  health=health or (lambda: self.health), owner_uid=99)

    def test_healthy_match_is_read_only_and_retains_process_identity(self):
        before = {p: p.read_bytes() for p in self.root.rglob('*') if p.is_file() and not p.is_symlink()}
        result = self.check()
        self.assertTrue(result['eligible'])
        self.assertEqual(result['pid'], 42)
        self.assertEqual(result['process_start_ticks'], '1234')
        self.assertGreaterEqual(result['health_ns'], 0)
        self.assertLessEqual(result['health_ns'], result['validation_ns'])
        self.assertEqual(before, {p: p.read_bytes() for p in before})

    def test_changed_unit_build_binary_arguments_and_user_refuse_noop(self):
        for name, data, reason in [('pax-demo.service', b'different unit', 'unit'),
                                   ('build.json', b'{}', 'build'), ('pax-demo', b'different binary', 'build')]:
            path = self.release / name
            old = path.read_bytes()
            path.write_bytes(data)
            self.assertEqual(self.check()['reason'], reason)
            self.assertFalse(self.check()['eligible'])
            path.write_bytes(old)
        for name, data in [('cmdline', b'wrong\0arguments\0'), ('status', b'Uid: 0 0 0 0\n')]:
            path = self.proc / '42' / name
            old = path.read_bytes()
            path.write_bytes(data)
            self.assertEqual(self.check()['reason'], 'process')
            path.write_bytes(old)

    def test_stopped_reload_and_overridden_services_refuse_noop(self):
        for key, value in [('ActiveState', 'inactive'), ('SubState', 'failed'),
                           ('MainPID', '0'), ('NeedDaemonReload', 'yes'), ('DropInPaths', '/override.conf')]:
            changed = dict(self.service, **{key: value})
            self.assertFalse(self.check(service=lambda: changed)['eligible'])

    def test_disconnected_missing_stale_and_wrong_books_refuse_noop(self):
        for mutate in [lambda h: h.update(transport_connected=False),
                       lambda h: h['view'].update(connected=False),
                       lambda h: h['view'].update(markets=[]),
                       lambda h: h['view']['markets'][0].update(status='stale'),
                       lambda h: h['view']['markets'][0].update(id='2'),
                       lambda h: h['view']['markets'][0].update(sequence=None)]:
            changed = copy.deepcopy(self.health)
            mutate(changed)
            result = self.check(health=lambda: changed)
            self.assertEqual(result['reason'], 'health')
            self.assertGreaterEqual(result['health_ns'], 0)
            self.assertLessEqual(result['health_ns'], result['validation_ns'])

    def test_pid_or_start_time_change_during_health_refuses_noop(self):
        calls = iter([self.service, dict(self.service, MainPID='43')])
        self.assertEqual(self.check(service=lambda: next(calls))['reason'], 'changed_process')
        def changed_start():
            path = self.proc / '42' / 'stat'
            path.write_text(path.read_text().replace('1234', '5678'))
            return self.health
        self.assertEqual(self.check(health=changed_start)['reason'], 'changed_process')

    def test_same_pid_exec_or_argument_change_during_health_refuses_noop(self):
        original = self.proc / '42' / 'exe'
        alternative = self.release / 'other-demo'
        alternative.write_bytes(b'another executable')
        def changed_exe():
            original.unlink()
            original.symlink_to(alternative)
            return self.health
        self.assertEqual(self.check(health=changed_exe)['reason'], 'changed_process')
        original.unlink()
        original.symlink_to(self.release / 'pax-demo')
        def changed_args():
            (self.proc / '42' / 'cmdline').write_bytes(b'changed\0')
            return self.health
        self.assertEqual(self.check(health=changed_args)['reason'], 'changed_process')


if __name__ == '__main__':
    unittest.main()
