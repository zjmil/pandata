import importlib.util
import json
import subprocess
import sys
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('policy', Path(__file__).with_name('compare-performance.py'))
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)


class PolicyTests(unittest.TestCase):
    def test_threshold(self):
        self.assertFalse(policy.slow(1, .9))
        self.assertFalse(policy.slow(1, 1.15))
        self.assertTrue(policy.slow(1, 1.151))

    def test_confirmation(self):
        for initial, confirmation, expected in [(1.2, 1.2, True), (1.2, 1.1, False)]:
            self.assertEqual(policy.slow(1, initial) and policy.slow(1, confirmation), expected)

    def test_cli_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            def write(name, cases, median):
                data = {'rows': 100_000, 'cases': {case: {
                    'median_seconds': median, 'rows_per_second': 100_000 / median,
                    'samples_seconds': [median] * 15} for case in cases}}
                path = folder / name
                path.write_text(json.dumps(data))
                return str(path)
            base = write('base.json', policy.CASES, 1)
            candidates = str(folder / 'candidates.txt')
            script = str(Path(__file__).with_name('compare-performance.py'))
            for head_value, confirmation, expected in [(.9, None, 0), (1.15, None, 0),
                                                      (1.2, 1.2, 1), (1.2, 1.1, 0)]:
                command = [sys.executable, script, base,
                           write('head.json', policy.CASES, head_value),
                           '--candidates', candidates]
                if confirmation is not None:
                    command += ['--confirmation-base', write('cb.json', policy.CASES, 1),
                                '--confirmation-head', write('ch.json', policy.CASES, confirmation)]
                result = subprocess.run(command, capture_output=True, text=True)
                self.assertEqual(result.returncode, expected, result.stderr)
            result = subprocess.run([sys.executable, script, base,
                                     write('missing.json', [], 1), '--candidates', candidates],
                                    capture_output=True)
            self.assertNotEqual(result.returncode, 0)

    def test_measurement_fixtures(self):
        case = policy.CASES[0]
        good = {'rows': 100_000, 'cases': {case: {'median_seconds': 1,
                'rows_per_second': 100_000, 'samples_seconds': [1] * 15}}}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'results.json'
            path.write_text(json.dumps(good))
            policy.load(path, [case])
            for value in [0, -1, float('nan'), float('inf'), '1']:
                good['cases'][case]['samples_seconds'][0] = value
                path.write_text(json.dumps(good))
                with self.assertRaises(ValueError):
                    policy.load(path, [case])
            path.write_text('{"rows":100000,"cases":{}}')
            with self.assertRaises(ValueError):
                policy.load(path, [case])


if __name__ == '__main__':
    unittest.main()
