"""Execute actual generated activation shell with fake system services and paths.

The Rust release test supplies its generated script on stdin. No root or AWS.
"""
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile


def verify(script):
    block = script.split("old=''\n", 1)[1]
    start = block.index('verify_previous_release() {\n')
    end = block.index('\n}\n', start) + len('\n}\n')
    block = block[:start] + '''verify_previous_release() {
  echo restored-health >> "$TEST_LOG"
  [[ "$TEST_FAILURE" != restored-health ]]
}
''' + block[end:]
    start = block.index('if ! python3 - "$stage_started" "$stage_done" "$health_started"')
    end = block.index('\nPY\nthen', start)
    block = block[:start] + 'if ! candidate_health\nthen' + block[end + len('\nPY\nthen'):]
    for failure in ('none', 'daemon-reload', 'enable', 'restart', 'clock', 'health', 'restored-health',
                    'term', 'term-during-restore'):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            managed, system = root / 'managed', root / 'system'
            old, candidate = managed / 'releases/old', managed / 'releases/candidate'
            old.mkdir(parents=True)
            candidate.mkdir()
            system.mkdir()
            (old / 'pax-demo.service').write_text('prior unit')
            (candidate / 'pax-demo.service').write_text('candidate unit')
            (managed / 'current').symlink_to(old)
            stage = root / 'stage'
            stage.mkdir()
            (stage / 'temporary').touch()
            captures = managed / 'captures'
            captures.mkdir()
            (captures / 'retained').write_text('capture bytes')
            stubs = root / 'bin'
            stubs.mkdir()
            (stubs / 'systemctl').write_text('''#!/bin/bash
set -eu
echo "$1 $(readlink -f "$TEST_MANAGED/current")" >> "$TEST_LOG"
if [[ $(readlink -f "$TEST_MANAGED/current") == "$TEST_MANAGED/releases/candidate" ]]; then
  if [[ "$TEST_FAILURE" == "$1" ]]; then exit 29; fi
  if [[ "$TEST_FAILURE" == term && "$1" == daemon-reload ]]; then kill -TERM "$PPID"; fi
fi
if [[ "$TEST_FAILURE" == term-during-restore && "$1" == daemon-reload &&
      $(readlink -f "$TEST_MANAGED/current") == "$TEST_MANAGED/releases/old" ]]; then kill -TERM "$PPID"; fi
''')
            (stubs / 'systemctl').chmod(0o755)
            prefix = f'''set -euo pipefail
release={shlex.quote(str(candidate))}
stage={shlex.quote(str(stage))}
old=''
stage_started=10
stage_done=20
tick() {{ [[ "$TEST_FAILURE" != clock ]] || return 31; echo 30; }}
candidate_health() {{ [[ "$TEST_FAILURE" != health && "$TEST_FAILURE" != restored-health && "$TEST_FAILURE" != term-during-restore ]]; }}
'''
            rendered = block.replace('/opt/pax', str(managed)).replace('/etc/systemd/system', str(system))
            log = root / 'calls'
            env = dict(os.environ, PATH=str(stubs) + os.pathsep + os.environ['PATH'],
                       TEST_FAILURE=failure, TEST_LOG=str(log), TEST_MANAGED=str(managed))
            result = subprocess.run(['bash', '-c', prefix + rendered], env=env,
                                    capture_output=True, text=True, timeout=10)
            assert (result.returncode == 0) == (failure == 'none'), (failure, result.stderr)
            if failure == 'term-during-restore':
                assert result.returncode == 1, 'second signal replaced original health failure'
            assert not stage.exists(), failure
            assert (captures / 'retained').read_text() == 'capture bytes', failure
            assert (managed / 'current').resolve() == (candidate if failure == 'none' else old), failure
            if failure == 'none':
                assert (managed / 'previous').resolve() == old
                assert 'restored-health' not in log.read_text()
            else:
                assert 'restored-health' in log.read_text(), failure
                assert not (managed / 'previous').exists(), failure
                if failure == 'restored-health':
                    assert 'could not be verified' in result.stderr
                    assert 'restored and synchronized' not in result.stderr
                else:
                    assert 'restored and synchronized' in result.stderr, (failure, result.stderr)
    print('nine generated activation/rollback/signal cases passed')


if __name__ == '__main__':
    verify(sys.stdin.read())
