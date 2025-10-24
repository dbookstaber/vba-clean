import os
import sys
import time
from pathlib import Path

SAMPLES_DIR = Path(__file__).resolve().parents[1] / "tests" / "samples"
SAMPLES_DIR.mkdir(parents=True, exist_ok=True)

SIMPLE_XLSM = SAMPLES_DIR / "Test_SimpleMacro.xlsm"  # Ideally should contain P-code
MULTI_XLSM = SAMPLES_DIR / "Test_MultiModule.xlsm"  # Ideally should contain P-code
EMPTY_XLSM = SAMPLES_DIR / "Test_Empty.xlsm"
BINARY_XLSB = SAMPLES_DIR / "Test_BinaryMacro.xlsb"  # Ideally should contain P-code
CLEANED_XLSM = SAMPLES_DIR / "Test_AlreadyClean.xlsm"
LARGE_XLSM = SAMPLES_DIR / "Test_LargeModule.xlsm"  # Large, low-compressibility module
CLASS_SHEETS_XLSM = SAMPLES_DIR / "Test_ClassAndSheets.xlsm"  # Class + multiple sheet code-behind

INTRO = """
This script generates macro-enabled Excel workbooks for integration testing.
It requires:
  - Microsoft Excel installed
  - 'Trust access to the VBA project object model' enabled (Excel Options > Trust Center > Trust Center Settings > Macro Settings)
  - Python packages: xlwings

TODOs for richer coverage:
- Find way to get samples that we want to contain P-code to actually contain P-code.
- Create a sample with large modules to force multiple MS-OVBA chunks (compressed and uncompressed) to exercise chunk walkers.
- Add a sample that ends with a short uncompressed chunk (<4096) to validate tolerant handling.
- Include class modules and multiple sheet code-behind modules with different codepages.
- Optionally craft a synthetic xl/vbaProject.bin (not via Excel) to simulate non-standard chunk signatures seen in the wild.
""".strip()

print(INTRO)

try:
    import xlwings as xw
except Exception as exc:  # pragma: no cover
    raise SystemExit("xlwings is required to generate sample workbooks: pip install xlwings") from exc

try:
    import win32com.client  # type: ignore
    import pywintypes  # type: ignore
except Exception:
    pass


def _new_wb(app: xw.App) -> xw.Book:
    # Start from a blank workbook
    return app.books.add()


def _add_std_module(wb: xw.Book, name: str, code: str) -> None:
    vbcomp = wb.api.VBProject.VBComponents.Add(1)  # 1: vbext_ct_StdModule
    vbcomp.Name = name
    vbcomp.CodeModule.AddFromString(code)


def _add_std_module_chunked(wb: xw.Book, name: str, lines: list[str], chunk_size: int = 400) -> None:
    """Add a large standard module in chunks to avoid COM/VBA memory errors."""
    vbcomp = wb.api.VBProject.VBComponents.Add(1)
    vbcomp.Name = name
    # Start with Option Explicit
    vbcomp.CodeModule.AddFromString("Option Explicit\n")
    for i in range(0, len(lines), chunk_size):
        batch = "\n".join(lines[i:i+chunk_size])
        vbcomp.CodeModule.AddFromString(batch + "\n")


def _add_class_module(wb: xw.Book, name: str, code: str) -> None:
    vbcomp = wb.api.VBProject.VBComponents.Add(2)  # 2: vbext_ct_ClassModule
    vbcomp.Name = name
    vbcomp.CodeModule.AddFromString(code)


def _add_sheet_code(wb: xw.Book, sheet_index: int, code: str) -> None:
    vbcomp = wb.api.VBProject.VBComponents(wb.sheets[sheet_index].api.CodeName)
    vbcomp.CodeModule.AddFromString(code)


def _save(wb: xw.Book, path: Path, fileformat: int) -> None:
    # 52: xlsm, 50: xlsb
    wb.api.SaveAs(str(path), FileFormat=fileformat)


def _encourage_compilation(app: xw.App, wb: xw.Book) -> None:
    """Try a few tricks to make Excel compile VBA modules and emit P-code."""
    try:
        # Toggle a trivial code change to trigger compile
        for vbcomp in wb.api.VBProject.VBComponents:
            cm = vbcomp.CodeModule
            if cm.CountOfLines > 0:
                first = cm.Lines(1, 1)
                cm.ReplaceLine(1, first)
    except Exception:
        pass
    try:
        # Try running a known macro name if present
        app.api.Run("ForceCompile")
    except Exception:
        pass


def _try_run_macro(app: xw.App, macro_name: str) -> None:
    try:
        app.api.Run(macro_name)
    except Exception:
        # Ignore failures (macro might not exist or be inaccessible)
        pass


def create_simple_xlsm(app: xw.App) -> None:
    if SIMPLE_XLSM.exists():
        SIMPLE_XLSM.unlink()  # overwrite
    wb = _new_wb(app)
    # Include a no-UI macro we can call to encourage compilation
    _add_std_module(
        wb,
        "Module1",
        """
Option Explicit
Sub HelloWorld()
    ' no-op to avoid UI during automation
End Sub
Sub ForceCompile()
    Dim i As Long: i = 1
End Sub
""".strip(),
    )
    _save(wb, SIMPLE_XLSM, 52)
    _try_run_macro(app, "Module1.ForceCompile")
    _encourage_compilation(app, wb)
    wb.api.Save()
    wb.close()


def create_multi_xlsm(app: xw.App) -> None:
    if MULTI_XLSM.exists():
        MULTI_XLSM.unlink()
    wb = _new_wb(app)
    _add_std_module(
        wb,
        "Utilities",
        """
Option Explicit
Public Function Add(a As Long, b As Long) As Long
    Add = a + b
End Function
""".strip(),
    )
    _add_std_module(
        wb,
        "Runner",
        """
Option Explicit
Sub Run()
    Dim r As Long
    r = Add(2, 3)
End Sub
""".strip(),
    )
    _add_class_module(
        wb,
        "Greeter",
        """
Option Explicit
Public Name As String
Public Function Greet() As String
    Greet = "Hi, " & Name
End Function
""".strip(),
    )
    _add_sheet_code(
        wb,
        0,
        """
Option Explicit
Private Sub Worksheet_Change(ByVal Target As Range)
    ' no-op
End Sub
""".strip(),
    )
    _save(wb, MULTI_XLSM, 52)
    _try_run_macro(app, "Runner.Run")
    _encourage_compilation(app, wb)
    wb.api.Save()
    wb.close()


def create_empty_xlsm(app: xw.App) -> None:
    if EMPTY_XLSM.exists():
        EMPTY_XLSM.unlink()
    wb = _new_wb(app)
    # no modules added intentionally
    _save(wb, EMPTY_XLSM, 52)
    wb.close()


def create_binary_xlsb(app: xw.App) -> None:
    if BINARY_XLSB.exists():
        BINARY_XLSB.unlink()
    wb = _new_wb(app)
    _add_std_module(
        wb,
        "BinMod",
        """
Option Explicit
Sub DoStuff()
    Dim i As Long
    For i = 1 To 10
        ' loop
    Next i
End Sub
""".strip(),
    )
    _save(wb, BINARY_XLSB, 50)
    # Add a small macro to encourage compilation, then save again
    try:
        _add_std_module(
            wb,
            "BinWarm",
            """
Option Explicit
Sub ForceCompile()
    Dim x As Long: x = 1
End Sub
""".strip(),
        )
        _try_run_macro(app, "BinWarm.ForceCompile")
    except Exception:
        pass
    _encourage_compilation(app, wb)
    wb.api.Save()
    wb.close()


def create_large_xlsm(app: xw.App) -> None:
    """Create a workbook with a very large, low-compressibility module to force many chunks.

    We generate thousands of unique lines to defeat LZ compression and aim for
    uncompressed chunks, increasing the chance of short final uncompressed chunks.
    """
    if LARGE_XLSM.exists():
        LARGE_XLSM.unlink()
    wb = _new_wb(app)
    # Build a big module with pseudo-random-like lines
    lines = []
    # Keep below COM memory limits by batching; total lines still large
    for i in range(1, 12000):  # ~12k lines -> sizable module
        # Each line ~60-80 bytes with varying numbers to reduce repetition
        lines.append(f"Sub N{i}(): Dim a{i} As String: a{i} = \"{i:06d}X{i*i%9973:04d}Y{i*7%1234:04d}Z\": End Sub")
    _add_std_module_chunked(wb, "BigMod", lines, chunk_size=300)
    _save(wb, LARGE_XLSM, 52)
    _encourage_compilation(app, wb)
    wb.api.Save()
    wb.close()


def create_class_and_sheets_xlsm(app: xw.App) -> None:
    """Create a workbook with a class module and code in multiple sheets."""
    if CLASS_SHEETS_XLSM.exists():
        CLASS_SHEETS_XLSM.unlink()
    wb = _new_wb(app)
    # Add two sheets to get multiple code-behind modules
    wb.sheets.add(after=wb.sheets[0])
    wb.sheets.add(after=wb.sheets[1])
    # Class module
    _add_class_module(
        wb,
        "Worker",
        """
Option Explicit
Private m_Id As Long
Public Property Get Id() As Long: Id = m_Id: End Property
Public Property Let Id(ByVal v As Long): m_Id = v: End Property
Public Function Work(ByVal x As Long) As Long: Work = x * 2: End Function
""".strip(),
    )
    # Sheet code-behind on two sheets
    _add_sheet_code(
        wb,
        0,
        """
Option Explicit
Private Sub Worksheet_SelectionChange(ByVal Target As Range)
    ' No-op
End Sub
""".strip(),
    )
    _add_sheet_code(
        wb,
        1,
        """
Option Explicit
Private Sub Worksheet_Activate()
    ' No-op
End Sub
""".strip(),
    )
    _save(wb, CLASS_SHEETS_XLSM, 52)
    _encourage_compilation(app, wb)
    wb.api.Save()
    wb.close()


def create_already_clean(app: xw.App) -> None:
    """Create a workbook that has macros but is already cleaned (no P-code).

    We create a simple xlsm and then expect the calling test harness to run
    the cleaner on it to produce the cleaned copy. If that hasn't happened yet,
    this function falls back to creating a duplicate of SIMPLE_XLSM.
    """
    # Ensure base exists
    if not SIMPLE_XLSM.exists():
        create_simple_xlsm(app)
    # Copy and let test harness clean it; here we just duplicate for now
    if CLEANED_XLSM.exists():
        CLEANED_XLSM.unlink()
    import shutil
    shutil.copy2(SIMPLE_XLSM, CLEANED_XLSM)


def main() -> None:
    app = xw.App(visible=False, add_book=False)
    try:
        # Suppress overwrite and other prompts during automation
        try:
            app.api.DisplayAlerts = False
        except Exception:
            pass
        # Quick trust check: attempt to add a module to a temp workbook
        try:
            wb_probe = _new_wb(app)
            _add_std_module(wb_probe, "Probe", "Sub X(): End Sub")
            wb_probe.close()
        except Exception as e:
            msg = (
                "Excel denied access to the VBA project. Please enable 'Trust access to the VBA project object model'\n"
                "Steps: File > Options > Trust Center > Trust Center Settings > Macro Settings > check the box.\n"
                f"Original error: {e}"
            )
            print(msg, file=sys.stderr)
            sys.exit(3)

        create_simple_xlsm(app)
        create_multi_xlsm(app)
        create_empty_xlsm(app)
        create_binary_xlsb(app)
        create_large_xlsm(app)
        create_class_and_sheets_xlsm(app)
        create_already_clean(app)
        print(f"Samples written to: {SAMPLES_DIR}")
    finally:
        # Give Excel a moment to flush file handles
        time.sleep(0.3)
        try:
            app.api.DisplayAlerts = True
        except Exception:
            pass
        app.quit()


if __name__ == "__main__":
    main()
