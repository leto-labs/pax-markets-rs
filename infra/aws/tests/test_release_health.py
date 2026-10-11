"""Independent clock, partial health and timeout fixtures; no live HTTP."""
import copy
from pathlib import Path
import unittest

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import release_health


class ReleaseHealthTests(unittest.TestCase):
    def setUp(self):
        self.now = 0
        self.state = {'transport_connected': True, 'view': {'connected': True,
                      'markets': [{'id': '1', 'status': 'live', 'sequence': '10'}]}}

    def sleep(self, seconds):
        self.now += int(seconds * 1e9)

    def wait(self, read):
        return release_health.wait(['1'], 10, read=read, clock=lambda: self.now, sleep=self.sleep)

    def test_partial_unavailable_then_synchronized_counts_waiting(self):
        attempts = iter([{}, dict(self.state, transport_connected=False), self.state])
        result = self.wait(lambda: next(attempts))
        self.assertTrue(result['healthy'])
        self.assertEqual(result['attempts'], 3)
        self.assertEqual(result['duration_ns'], 1_000_000_000)

    def test_wrong_duplicate_missing_and_stale_books_timeout(self):
        for change in [lambda h: h['view'].update(connected=False),
                       lambda h: h['view'].update(markets=[]),
                       lambda h: h['view']['markets'].append(h['view']['markets'][0]),
                       lambda h: h['view']['markets'][0].update(id='2'),
                       lambda h: h['view']['markets'][0].update(sequence=None),
                       lambda h: h['view']['markets'][0].update(status='stale')]:
            state = copy.deepcopy(self.state)
            change(state)
            result = self.wait(lambda: state)
            self.assertFalse(result['healthy'])
            self.assertEqual(result['duration_ns'], 10_000_000_000)
            self.assertEqual(result['attempts'], 20)

    def test_failed_reads_remain_bounded_and_do_not_expose_the_exception(self):
        def read():
            raise OSError('sensitive transport detail')
        result = self.wait(read)
        self.assertFalse(result['healthy'])
        self.assertNotIn('sensitive', str(result))
        for seconds in [0, 9, 181]:
            with self.assertRaises(ValueError):
                release_health.wait(['1'], seconds)

    def test_nested_phases_add_exactly_and_reject_wrong_clocks(self):
        phases = release_health.phases(10, 20, 50, 90)
        self.assertEqual([phases[k] for k in ['stage_ns', 'startup_ns', 'health_ns', 'total_ns']],
                         [10, 30, 40, 80])
        for clocks in [(-1, 1, 2, 3), (10, 9, 20, 30), (0, 1, 3, 2), (0, 1, 2, 3601_000_000_000)]:
            with self.assertRaises(ValueError):
                release_health.phases(*clocks)

    def test_restoration_uses_its_own_unambiguous_markets(self):
        self.assertEqual(release_health.unit_markets(
            '[Service]\nExecStart=/opt/pax/current/pax-demo serve --markets 2,3 --bind 0.0.0.0:80\n'),
            ['2', '3'])
        for command in ['', 'ExecStart=demo', 'ExecStart=demo --markets',
                        'ExecStart=demo --markets 1 --markets 2',
                        'ExecStart=demo --markets 1\nExecStart=demo --markets 2',
                        'ExecStart=demo --markets 0', 'ExecStart=demo --markets 1,1',
                        'ExecStart=demo --markets 1;touch', 'ExecStart=demo --markets ١']:
            with self.subTest(command=command), self.assertRaises(ValueError):
                release_health.unit_markets(command)


if __name__ == '__main__':
    unittest.main()
