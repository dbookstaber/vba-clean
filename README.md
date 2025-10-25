# VBA Cleaner (P-code remover for Excel workbooks)

`vba_clean.py` strips compiled VBA performance cache (aka "P-code") from Excel workbooks, without launching Excel. It works on both Binary Workbooks (`.xlsb`) and macro-enabled OpenXML workbooks (`.xlsm`).

## What is P-code?

When Excel compiles a VBA module, it stores two forms of the code inside the file:

1. Source code (text — what you see in the VBA editor).
2. Performance cache (P-code) — a version of the source code compiled for the computer running the workbook.

Removing P-code keeps only the source code. Excel will transparently recompile the code the next time it is run.

## Why remove P-code?

P-code is generated dynamically and Excel does not always keep it clean and consistent.  Among the benefits of removing P-code from workbook files:

- No stale or corrupted caches that can cause unstable behavior (e.g., macros that worked before suddenly raise exceptions).
- Smaller files.
- Normalized workbooks for version control.

Excel regenerates the P-code whenever VBA is run, so user experience is unchanged.

## How it works (high level)

- XLSM and XLSB workbook files are actually ZIP archives.  Any VBA contents are stored in `xl/vbaProject.bin`.
- `vbaProject.bin` is an OLE compound file.  VBA modules are individual streams under the `VBA/` storage.
- Each module stream uses the MS-OVBA compression format. The source text starts at the `ModuleOffset` recorded in the `VBA/dir` stream.
- `vba_clean.py` walks the MS-OVBA compressed stream and zeroes literal tokens that expand into the pre-`ModuleOffset` region (the P-code), preserving sizes and structure. Streams and container sizes remain valid.

A benefit of this approach (in contrast to [other VBA cleaners or decompilers](http://www.cpap.com.br/orlando/VBADecompilerMore.asp)) is that no Excel instances are used, and the cleaning is very fast.

## Requirements

- Python 3.8+
- `olefile` package.  (Install via `pip install olefile`)


## Usage

Show help: `python vba_clean.py --help`

`python vba_clean.py path\to\Workbook.xlsb` creates `path\to\Workbook_clean.xlsb` with P-code removed.

Clean in place: `python vba_clean.py --in-place path\to\Workbook.xlsb` will first create a backup `path\to\Precompiled Workbook.xlsb`.

Output messaging:

- "P-code removed from modules" — modules were detected and updated
- "VBA macros detected but already lacked P-code" — modules present, but nothing to change
- "No VBA project modules found" — no macros were present
- "VBA modules detected; applied heuristic patch (dir stream not parsed)." — modules were present and updated even though the `dir` stream couldn’t be parsed strictly; see [TECH_NOTE](TECH_NOTE.md#heuristic-patch-whywhenhow).

### Repack mode (Windows)

For a deeper clean: The `--repack` flag rebuilds module streams as source‑only and, when supported by the environment, updates the `VBA/dir` stream so `ModuleOffset=0` for those modules. On Windows, the tool uses Structured Storage APIs to fully rebuild `vbaProject.bin` when in‑memory resized writes are not possible; otherwise it falls back to the safe size‑preserving neutralization.

Example: `python vba_clean.py --repack path\to\Workbook.xlsb -o path\to\Workbook_clean.xlsb`

When repack succeeds for a module, the CLI will report `offset 0`.


## Testing

This repo includes `unittest`-based tests.  To run:

```powershell
python -m unittest discover

# Run just the test_cli tests:
python -m unittest tests.test_cli
```

### Parity harness

For module‑text parity checks between two workbooks (e.g., against a decompiler output), see [TECH_NOTE.md](TECH_NOTE.md#parity-validation-harness-usage) for the `tests/parity_harness.py` usage.


## Notes on safety and limitations

- The tool edits only `xl/vbaProject.bin` and does not touch workbook data or formulas.
- The in-place mode makes a sibling backup first: `Predecompiled <original>`; if it can't create the backup (locked, read-only, etc.), the tool aborts without modifying the source.
- If your modules use uncommon code pages, the tool attempts to honor the `PROJECTCODEPAGE` hint in the `dir` stream when decoding names.

## Internals and references

- MS-OVBA compression and the `dir` stream format are implemented per [MS-OVBA specification](https://learn.microsoft.com/openspecs/office_file_formats/ms-ovba/). See TECH_NOTE.md for notes on real‑world tolerances.
