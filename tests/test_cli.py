"""
CLI tests for vba_clean.
    ```
    # Run this test module only
    python -m unittest tests.test_cli

    # Discover and run all tests under the tests/ folder
    python -m unittest discover
    ```
"""

import io
import os
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

import vba_clean


class MainCLITests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_argv = list(sys.argv)

    def tearDown(self) -> None:
        sys.argv = self._orig_argv

    def test_main_uses_sys_argv_when_none(self) -> None:
        sys.argv = ["vba_clean.py", "sample.xlsb"]
        expected_output_path = os.path.abspath("sample_clean.xlsb")

        with patch("vba_clean.os.path.exists", return_value=True), patch(
            "vba_clean.os.path.isdir", return_value=False
        ), patch(
            "vba_clean.process_workbook",
            return_value=({"Module1": 128}, {"Module1"}, True),
        ):
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = vba_clean.main()

        self.assertEqual(exit_code, 0)
        output = stdout.getvalue()
        self.assertIn("P-code removed from modules", output)
        self.assertIn("Module1", output)
        self.assertIn(f"Modified workbook written to: {expected_output_path}", output)

    def test_in_place_backup_failure_aborts(self) -> None:
        with patch("vba_clean.os.path.exists", return_value=True), patch(
            "vba_clean.os.path.isdir", return_value=False
        ), patch(
            "vba_clean.create_predecompile_backup", side_effect=OSError("locked")
        ), patch("vba_clean.process_workbook") as mock_process:
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = vba_clean.main(["--in-place", "sample.xlsb"])

        self.assertEqual(exit_code, 2)
        self.assertIn("Unable to create backup", stderr.getvalue())
        mock_process.assert_not_called()

    def test_reporting_for_clean_macros(self) -> None:
        with patch("vba_clean.os.path.exists", return_value=True), patch(
            "vba_clean.os.path.isdir", return_value=False
        ), patch(
            "vba_clean.process_workbook",
            return_value=({}, {"ModuleA"}, True),
        ):
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = vba_clean.main(["sample.xlsb"])

        self.assertEqual(exit_code, 0)
        output = stdout.getvalue()
        self.assertIn("already lacked P-code", output)

    def test_reporting_for_workbook_without_macros(self) -> None:
        with patch("vba_clean.os.path.exists", return_value=True), patch(
            "vba_clean.os.path.isdir", return_value=False
        ), patch(
            "vba_clean.process_workbook",
            return_value=({}, set(), True),
        ):
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = vba_clean.main(["sample.xlsb"])

        self.assertEqual(exit_code, 0)
        output = stdout.getvalue()
        self.assertIn("workbook contains no macros", output)

    def test_reporting_when_dir_parse_fails(self) -> None:
        with patch("vba_clean.os.path.exists", return_value=True), patch(
            "vba_clean.os.path.isdir", return_value=False
        ), patch(
            "vba_clean.process_workbook",
            return_value=({}, {"ModuleZ"}, False),
        ):
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                exit_code = vba_clean.main(["sample.xlsb"])

        self.assertEqual(exit_code, 0)
        output = stdout.getvalue()
        self.assertIn("dir stream could not be parsed", output)


if __name__ == "__main__":
    unittest.main()
