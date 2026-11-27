import runpy
import unittest
from pathlib import Path


class SummaryTest(unittest.TestCase):
    def test_summary_disagrees_with_passing_case(self):
        report = runpy.run_path(str(Path(__file__).with_name("test-report.py")))
        parser = report["OutputParser"]({"example::case": {"suite": "regular"}})
        for line in (
            "Running regular tests",
            "running 1 test",
            "test example::case ... ok",
            "test result: FAILED. 0 passed; 1 failed; 0 ignored;",
            "Running performance tests",
            "running 0 tests",
            "test result: ok. 0 passed; 0 failed; 0 ignored;",
        ):
            parser.consume(line)
        errors, _, _ = report["validate_output"](parser, [], None)
        self.assertTrue(any("result count mismatch" in error for error in errors), errors)


if __name__ == "__main__":
    unittest.main()
