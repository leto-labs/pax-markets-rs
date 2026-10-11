"""Unsupported artifact combinations must fail before container/output creation."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class ArtifactWorkloadTests(unittest.TestCase):
    def test_invalid_workloads_and_operator_sdk_options_refuse_without_building(self):
        root = Path(__file__).resolve().parents[3]
        for settings, cpu in [({'PAX_ARTIFACT_WORKLOAD': 'unknown'}, 'x86-64'),
                              ({'PAX_ARTIFACT_FEATURES': 'allocation-profile'}, 'x86-64'),
                              ({'PAX_ARTIFACT_PGO_SCOPE': 'core'}, 'x86-64'),
                              ({'PAX_ARTIFACT_DEFAULT_FEATURES': '0'}, 'x86-64'),
                              ({}, 'znver4')]:
            with self.subTest(settings=settings, cpu=cpu), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / 'artifact'
                environment = dict(os.environ, PAX_ARTIFACT_WORKLOAD='ops',
                                   PAX_ARTIFACT_FEATURES='', PAX_ARTIFACT_PGO_SCOPE='none', PAX_ARTIFACT_IMAGE_ID='',
                                   PAX_ARTIFACT_DEFAULT_FEATURES='1')
                environment.update(settings)
                result = subprocess.run(['bash', 'infra/aws/scripts/build-al2023.sh', str(output), cpu],
                                        cwd=root, env=environment, capture_output=True, timeout=10)
                self.assertEqual(result.returncode, 2)
                self.assertFalse(output.exists())
                self.assertIn(b'Artifact workload' if settings.get('PAX_ARTIFACT_WORKLOAD') else b'Operator artifacts', result.stderr)

    def test_run_uses_build_returned_image_without_shared_tag_lookup(self):
        for workload, cpu, image in [('sdk', 'x86-64', 'sha256:' + 'a' * 64),
                                     ('ops', 'x86-64', 'sha256:' + 'a' * 64),

                                     ('sdk', 'znver4', 'sha256:' + 'a' * 64),
                                     ('sdk', 'x86-64', 'mutable:tag')]:
            with self.subTest(workload=workload, cpu=cpu, image=image):
                self.assert_build_image(workload, cpu, image)

    def test_existing_image_is_exact_local_and_never_rebuilt_or_substituted(self):
        root = Path(__file__).resolve().parents[3]
        image = 'sha256:' + 'a' * 64
        for selected, observed, inspect_exit, expected_exit in [
            (image, image + ' linux amd64', 0, 93),
            ('mutable:tag', '', 0, 2),
            (image, '', 42, 42),
            (image, 'sha256:' + 'b' * 64 + ' linux amd64', 0, 2),
            (image, image + ' linux arm64', 0, 2),
            (image, image + ' windows amd64', 0, 2),
        ]:
            with self.subTest(selected=selected, observed=observed), tempfile.TemporaryDirectory() as directory:
                temporary = Path(directory)
                fake = temporary / 'docker'
                fake.write_text(f'#!{sys.executable}\n' + '''import json,os,sys
args=sys.argv[1:]
with open(os.environ['PAX_TEST_DOCKER_LOG'],'a') as log:log.write(json.dumps(args)+'\\n')
if args[:2]==['image','inspect']:
    print(os.environ['PAX_TEST_OBSERVED_IMAGE'])
    raise SystemExit(int(os.environ['PAX_TEST_INSPECT_EXIT']))
if args[0]=='run':raise SystemExit(93)
raise SystemExit('unexpected build, pull or mutable image lookup')
''')
                fake.chmod(0o700)
                output = temporary / 'artifact'
                log = temporary / 'docker.jsonl'
                environment = dict(os.environ, PATH=str(temporary) + os.pathsep + os.environ['PATH'],
                                   PAX_ARTIFACT_WORKLOAD='sdk', PAX_ARTIFACT_FEATURES='',
                                   PAX_ARTIFACT_DEFAULT_FEATURES='0', PAX_ARTIFACT_PGO_SCOPE='none',
                                   PAX_ARTIFACT_IMAGE_ID=selected, PAX_DOCKER_SUDO='0',
                                   PAX_TEST_DOCKER_LOG=str(log), PAX_TEST_OBSERVED_IMAGE=observed,
                                   PAX_TEST_INSPECT_EXIT=str(inspect_exit))
                result = subprocess.run(['bash', 'infra/aws/scripts/build-al2023.sh', str(output)],
                                        cwd=root, env=environment, capture_output=True, timeout=30)
                self.assertEqual(result.returncode, expected_exit, result.stderr.decode())
                calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
                if selected == 'mutable:tag':
                    self.assertEqual(calls, [])
                else:
                    self.assertEqual(calls[0], ['image', 'inspect', '--format',
                                               '{{.Id}} {{.Os}} {{.Architecture}}', image])
                if expected_exit == 93:
                    self.assertEqual(len(calls), 2)
                    self.assertIn('--pull=never', calls[1])
                    self.assertEqual(calls[1][-3:], [image, 'bash', '/src/infra/aws/scripts/build-al2023-container.sh'])
                    self.assertEqual((output / 'artifact/build-image.txt').read_text().strip(), image)
                    self.assertEqual((output / 'artifact/build-image-source.txt').read_text().strip(), 'existing')
                else:
                    self.assertFalse(output.exists())
                    self.assertLessEqual(len(calls), 1)

    def assert_build_image(self, workload, cpu, image):
        root = Path(__file__).resolve().parents[3]
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            fake = temporary / 'docker'
            fake.write_text(f'#!{sys.executable}\n' + '''import json,os,pathlib,sys
args=sys.argv[1:]
with open(os.environ['PAX_TEST_DOCKER_LOG'],'a') as log:log.write(json.dumps(args)+'\\n')
if args[0]=='build':
    pathlib.Path(args[args.index('--iidfile')+1]).write_text(os.environ['PAX_TEST_IMAGE_ID']+'\\n')
elif args[0]=='run':
    raise SystemExit(93)
else:
    raise SystemExit('unexpected image lookup or Docker operation')
''')
            fake.chmod(0o700)
            output = temporary / 'artifact'
            log = temporary / 'docker.jsonl'
            environment = dict(os.environ, PATH=str(temporary) + os.pathsep + os.environ['PATH'],
                               PAX_ARTIFACT_WORKLOAD=workload, PAX_ARTIFACT_FEATURES='',
                               PAX_ARTIFACT_DEFAULT_FEATURES='1', PAX_ARTIFACT_PGO_SCOPE='none',
                               PAX_DOCKER_SUDO='0', PAX_ARTIFACT_IMAGE_ID='', PAX_TEST_DOCKER_LOG=str(log), PAX_TEST_IMAGE_ID=image)
            result = subprocess.run(['bash', 'infra/aws/scripts/build-al2023.sh', str(output), cpu],
                                    cwd=root, env=environment, capture_output=True, timeout=30)
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertNotIn('-t', calls[0])
            if image.startswith('sha256:'):
                self.assertEqual(result.returncode, 93, result.stderr.decode())
                self.assertEqual(len(calls), 2)
                self.assertIn('--pull=never', calls[1])
                self.assertEqual(calls[1][-3:], [image, 'bash', '/src/infra/aws/scripts/build-al2023-container.sh'])
                self.assertTrue((output / 'source/infra/aws/scripts/build-al2023-container.sh').is_file())
                self.assertEqual((output / 'artifact/build-image.txt').read_text().strip(), image)
                self.assertEqual((output / 'artifact/build-image-source.txt').read_text().strip(), 'built')
            else:
                self.assertEqual(result.returncode, 2)
                self.assertEqual(len(calls), 1)
                self.assertIn(b'immutable image ID', result.stderr)


if __name__ == '__main__':
    unittest.main()
