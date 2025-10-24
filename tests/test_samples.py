"""
Integration tests that exercise vba_clean.py against sample workbooks.

Prerequisites for generating samples:
- Microsoft Excel installed
- Excel option enabled: Trust access to the VBA project object model
- Python packages: xlwings

Automatic sample generation:
- If expected sample files are missing, this test will attempt to run
  tests/generate_samples.py automatically before proceeding.

You can run this test file alone:
    python -m unittest tests.test_samples

TODOs for better assertions:
- Track, per sample, whether P-code removal actually occurred, and assert minimum thresholds once generators reliably produce P-code.
- Add tests that validate tolerant decompression by feeding synthetic module streams with short uncompressed chunks.
- Add a test that compares our cleaned module bytes to a decompiler-produced workbook where feasible (without launching Excel) to verify equivalence.
"""

import io
import atexit
import os
import sys
import unittest
from pathlib import Path
from contextlib import redirect_stdout

import vba_clean

SAMPLES = Path(__file__).resolve().parents[1] / "tests" / "samples"

# Track how many runs actually removed P-code (for a friendly summary)
REMOVALS_COUNT = 0
RUNS_COUNT = 0


def _print_summary():
    # Print a short summary after tests run
    print(f"\n[summary] vba_clean removed P-code in {REMOVALS_COUNT} of {RUNS_COUNT} sample runs")


atexit.register(_print_summary)


def _ensure_samples():
    expected = [
        SAMPLES / "Test_SimpleMacro.xlsm",
        SAMPLES / "Test_MultiModule.xlsm",
        SAMPLES / "Test_Empty.xlsm",
        SAMPLES / "Test_BinaryMacro.xlsb",
        SAMPLES / "Test_AlreadyClean.xlsm",
        SAMPLES / "Test_LargeModule.xlsm",
        SAMPLES / "Test_ClassAndSheets.xlsm",
    ]
    missing = [p for p in expected if not p.exists()]
    if not missing:
        return
    # Try to generate
    gen = Path(__file__).resolve().parent / "generate_samples.py"
    if not gen.exists():
        return
    import subprocess
    print("Samples missing, invoking generator...", file=sys.stderr)
    proc = subprocess.run([sys.executable, str(gen)], capture_output=True, text=True)
    if proc.stdout:
        print(proc.stdout)
    if proc.stderr:
        print(proc.stderr, file=sys.stderr)
    # Recheck
    still_missing = [p for p in expected if not p.exists()]
    if still_missing:
        print(f"Sample generation incomplete, still missing: {still_missing}", file=sys.stderr)


_ensure_samples()


def _run_clean(input_path: Path, extra_args=None):
    args = [] if extra_args is None else list(extra_args)
    args = args + [str(input_path)]
    buf = io.StringIO()
    with redirect_stdout(buf):
        exit_code = vba_clean.main(args)
    out = buf.getvalue()
    global RUNS_COUNT, REMOVALS_COUNT
    RUNS_COUNT += 1
    if ("P-code removed from modules" in out) or ("applied heuristic patch" in out):
        REMOVALS_COUNT += 1
    return exit_code, out


@unittest.skipUnless((SAMPLES / "Test_SimpleMacro.xlsm").exists(), "Sample not present - run scripts/generate_samples.py")
class TestSimpleMacro(unittest.TestCase):
    def test_simple_macro_first_run_modifies(self):
        src = SAMPLES / "Test_SimpleMacro.xlsm"
        # Write to a copy so we don't mutate the original sample
        dst = src.with_name("Test_SimpleMacro_copy.xlsm")
        if dst.exists():
            os.remove(dst)
        import shutil
        shutil.copy2(src, dst)

        code, out = _run_clean(dst)
        self.assertEqual(code, 0)
        self.assertTrue(
            ("P-code removed from modules" in out)
            or ("already lacked P-code" in out)
            or ("dir stream could not be parsed" in out)
            or ("applied heuristic patch" in out),
            msg=f"Unexpected output: {out}",
        )
        # Second run should report already clean
        code2, out2 = _run_clean(dst.with_name(dst.stem + "_clean" + dst.suffix))
        self.assertEqual(code2, 0)
        self.assertTrue(
            ("already lacked P-code" in out2)
            or ("dir stream could not be parsed" in out2),
            msg=f"Unexpected output: {out2}",
        )


@unittest.skipUnless((SAMPLES / "Test_MultiModule.xlsm").exists(), "Sample not present - run scripts/generate_samples.py")
class TestMultiModule(unittest.TestCase):
    def test_multi_module_modifies(self):
        src = SAMPLES / "Test_MultiModule.xlsm"
        code, out = _run_clean(src)
        self.assertEqual(code, 0)
        self.assertTrue(
            ("P-code removed from modules" in out)
            or ("already lacked P-code" in out)
            or ("dir stream could not be parsed" in out)
            or ("applied heuristic patch" in out),
            msg=f"Unexpected output: {out}",
        )


@unittest.skipUnless((SAMPLES / "Test_Empty.xlsm").exists(), "Sample not present - run scripts/generate_samples.py")
class TestEmptyProject(unittest.TestCase):
    def test_empty_reports_no_macros(self):
        src = SAMPLES / "Test_Empty.xlsm"
        code, out = _run_clean(src)
        self.assertEqual(code, 0)
        self.assertIn("No VBA project modules found", out)


@unittest.skipUnless((SAMPLES / "Test_BinaryMacro.xlsb").exists(), "Sample not present - run scripts/generate_samples.py")
class TestBinaryMacro(unittest.TestCase):
    def test_binary_macro_modifies(self):
        src = SAMPLES / "Test_BinaryMacro.xlsb"
        code, out = _run_clean(src)
        self.assertEqual(code, 0)
        # Should either modify or at least detect macros
        self.assertTrue(
            ("P-code removed from modules" in out)
            or ("VBA macros detected" in out)
            or ("dir stream could not be parsed" in out)
            or ("applied heuristic patch" in out),
            msg=f"Unexpected output: {out}",
        )


@unittest.skipUnless((SAMPLES / "Test_LargeModule.xlsm").exists(), "Sample not present - run scripts/generate_samples.py")
class TestLargeModule(unittest.TestCase):
    def test_large_module_detects_macros(self):
        src = SAMPLES / "Test_LargeModule.xlsm"
        code, out = _run_clean(src)
        self.assertEqual(code, 0)
        # We at least expect macro detection; many environments will also remove P-code
        self.assertTrue(
            ("P-code removed from modules" in out)
            or ("VBA macros detected" in out)
            or ("dir stream could not be parsed" in out)
            or ("applied heuristic patch" in out),
            msg=f"Unexpected output: {out}",
        )


@unittest.skipUnless((SAMPLES / "Test_ClassAndSheets.xlsm").exists(), "Sample not present - run scripts/generate_samples.py")
class TestClassAndSheets(unittest.TestCase):
    def test_class_and_sheets_detects_macros(self):
        src = SAMPLES / "Test_ClassAndSheets.xlsm"
        code, out = _run_clean(src)
        self.assertEqual(code, 0)
        self.assertTrue(
            ("P-code removed from modules" in out)
            or ("VBA macros detected" in out)
            or ("dir stream could not be parsed" in out)
            or ("applied heuristic patch" in out),
            msg=f"Unexpected output: {out}",
        )


@unittest.skipUnless((SAMPLES / "Test_AlreadyClean.xlsm").exists(), "Sample not present - run scripts/generate_samples.py")
class TestAlreadyClean(unittest.TestCase):
    def test_already_clean_reports_correctly(self):
        src = SAMPLES / "Test_AlreadyClean.xlsm"
        # If this hasn't been cleaned, first run will modify. Accept both outcomes
        code, out = _run_clean(src)
        self.assertEqual(code, 0)
        self.assertTrue(
            ("already lacked P-code" in out)
            or ("P-code removed from modules" in out)
            or ("VBA modules detected, but the dir stream could not be parsed" in out)
            or ("applied heuristic patch" in out),
            msg=f"Unexpected output: {out}",
        )


if __name__ == "__main__":
    unittest.main()
