"""Bounded guest synchronization health and nested cold deployment timings."""
import json
import shlex
import time
import urllib.request


def unit_markets(unit):
    """Read exactly the prior unit's market selection, without evaluating shell."""
    if len(unit) > 65536:
        raise ValueError('release unit exceeds bound')
    commands = [line.removeprefix('ExecStart=') for line in unit.splitlines()
                if line.startswith('ExecStart=')]
    if len(commands) != 1:
        raise ValueError('ambiguous release command')
    argv = shlex.split(commands[0])
    if argv.count('--markets') != 1 or argv.index('--markets') + 1 >= len(argv):
        raise ValueError('missing release markets')
    markets = argv[argv.index('--markets') + 1].split(',')
    if (not 1 <= len(markets) <= 16 or len(set(markets)) != len(markets)
            or any(not m.isascii() or not m.isdecimal() or not 0 < int(m) < 2**64 for m in markets)):
        raise ValueError('invalid release markets')
    return markets


def read_state():
    with urllib.request.urlopen('http://127.0.0.1/api/state', timeout=2) as response:
        data = response.read(1024 * 1024 + 1)
    if len(data) > 1024 * 1024:
        raise ValueError('release health exceeds bound')
    return json.loads(data)['state']


def wait(markets, seconds, read=read_state, clock=time.monotonic_ns, sleep=time.sleep):
    """Return one fixed-shape result; caller owns rollback on unhealthy output."""
    if not markets or not 10 <= seconds <= 180:
        raise ValueError('invalid bounded release health request')
    started = clock()
    deadline = started + seconds * 1_000_000_000
    attempts = 0
    healthy = False
    while clock() < deadline:
        attempts += 1
        try:
            state = read()
            view = state['view']
            books = view['markets']
            healthy = (state['transport_connected'] is True and view['connected'] is True
                       and len(books) == len(markets)
                       and {book['id'] for book in books} == set(markets)
                       and all(book['status'] == 'live' and book['sequence'] is not None
                               for book in books))
            if healthy:
                break
        except (OSError, ValueError, KeyError, TypeError):
            pass
        sleep(.5)
    return {'kind': 'pax_release_health', 'schema': 1, 'healthy': healthy,
            'attempts': attempts, 'duration_ns': clock() - started}


def phases(started, staged, health_started, finished):
    """Partition one guest monotonic clock; never add it to operator wall time."""
    if not 0 <= started <= staged <= health_started <= finished or finished - started > 3600_000_000_000:
        raise ValueError('invalid guest deployment phase clocks')
    return {'kind': 'pax_release_guest_timing', 'schema': 1, 'clock': 'guest_monotonic',
            'stage_ns': staged - started, 'startup_ns': health_started - staged,
            'health_ns': finished - health_started, 'total_ns': finished - started}
