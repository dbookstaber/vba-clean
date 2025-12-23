# Technical Notes: VBA Cleaner internals and advanced usage

This document captures implementation details, behaviors that differ slightly from the MS‑OVBA spec in the wild, and advanced tools such as the parity harness.

## Contents

- MS‑OVBA background and tolerant handling
- Heuristic patch (why/when/how)
- Repack mode and Windows Structured Storage rebuild
- Updating ModuleOffset in the dir stream
- Safety and fallbacks (contract)
- Parity validation harness (usage)

---

## MS‑OVBA background and tolerant handling

### Module Stream Structure (per MS-OVBA 2.3.4.3)

A VBA module stream consists of two parts:
1. **PerformanceCache**: Raw binary P-code, size specified by `ModuleOffset` in the dir stream
2. **CompressedContainer**: MS-OVBA compressed source text, starting at byte `ModuleOffset`

The `CompressedContainer` begins with signature byte 0x01, followed by chunks.
Each chunk has a 2‑byte header with:
  - Bit 15: compressed (1) vs uncompressed (0)
  - Bits 12..14: signature bits (typically 0b011 as per spec)
  - Bits 0..11: chunk size minus 3

### Clarifications from Microsoft (December 2025)

Following direct correspondence with Microsoft regarding observed deviations:

1. **Raw chunk size handling**: For intermediate uncompressed (raw) chunks, the Size field
   should be assumed to be 4095 (implying 4096 bytes of data). Only the final raw chunk
   may be shorter. The old VBA code hard-coded raw chunk size to 4095.

2. **Signature bits for raw chunks**: While the spec says signature should be 0b011,
   some producers emit different values for raw chunks. These should be accepted.

3. **SRP streams** (`__SRP_*`): Per MS-OVBA 2.2.6, these MUST be ignored on read and
   MUST NOT be present on write.

### Our implementation

To be robust, our decompressor and zeroing logic:
- For raw chunks: ignores the header Size field for intermediate chunks and assumes 4096 bytes
- Verifies the 0x01 container signature at `ModuleOffset`, not at byte 0
- Accepts any chunk signature bits and bounds‑checks sizes
- Accepts short final uncompressed chunks

This makes the tool resilient to variations while preserving the structure and avoiding false negatives.

## Heuristic patch (why/when/how)

Normally, we parse the `VBA/dir` stream to recover each module's `ModuleOffset` (start of 
compressed source text within the module stream). We then zero the PerformanceCache region 
(the raw bytes before `ModuleOffset`) while preserving the CompressedContainer intact.

Some files have irregular `dir` layouts or producer deviations that prevent a full parse. In that case, we:
- Enumerate modules under `VBA/` to confirm macros exist.
- Attempt a tolerant byte-by-byte dir scan to salvage any offsets we can.
- For remaining modules, heuristically guess `ModuleOffset` by scanning for the 0x01 
  compression signature followed by valid VBA markers in the decompressed output.

We only report "applied heuristic patch" if a module stream actually changes via this patch. 
The heuristic never alters the source text itself and is safe: Excel will regenerate P‑code transparently.

## Repack mode and Windows Structured Storage rebuild

The `--repack` mode aims to rebuild each module as source‑only:
- Get `ModuleOffset` from the dir stream (or heuristic)
- Decompress source text from `ModuleOffset` using `decompress_module_text()`
- Re‑emit an MS‑OVBA container containing only the source text (no PerformanceCache)
- Update the `VBA/dir` stream to set `ModuleOffset = 0` for repacked modules

Resizing OLE streams requires a proper Compound File (CFB) writer. Our strategy:
1) Attempt in‑memory resized writes.
2) If that fails, retry all writes on a temporary on‑disk OLE file.
3) On Windows, attempt a full Structured Storage rebuild (IStorage/IStream) to create a fresh vbaProject.bin with all storages/streams and repacked modules.
4) If all resized writes are blocked, fall back to size‑preserving neutralization (zeroed P‑code) to keep the workflow safe and deterministic.

When repack succeeds for a module, the CLI reports `offset 0` for that module.

## Updating ModuleOffset in the dir stream

When `--repack` repacks a module, we update the dir stream to set `ModuleOffset = 0` for that module's `STREAMNAME`.
- The `VBA/dir` stream itself is re‑emitted using only uncompressed chunks.
- If the dir stream cannot be resized, Excel typically remains tolerant, but the code attempts the update whenever possible.

## Safety and fallbacks (contract)

- The tool edits only `xl/vbaProject.bin` and never touches sheet data or formulas.
- If in‑place is requested, a backup `Predecompiled <file>` is created first; failure to create the backup aborts without modifications.
- `--repack`:
  - Prefers full rebuild with `ModuleOffset=0` when the environment supports stream resizing (Windows COM path).
  - Otherwise falls back to size‑preserving neutralization, which still removes P‑code reliably.
- Heuristic patch is used only if dir parsing fails; it never touches source text and only zeros bytes before the inferred boundary.

## Parity validation harness (usage)

Use `tests/parity_harness.py` to compare VBA module text between two workbooks (e.g., original vs cleaned, or cleaned vs a decompiler output):

Examples:

```powershell
# Verbose comparison (prints per-module results)
python tests/parity_harness.py --left path\to\Original.xlsm --right path\to\Cleaned.xlsm --verbose

# Write JSON report for automation
python tests/parity_harness.py --left A.xlsb --right B.xlsb --json parity_report.json
```

What it does:
- Reads `xl/vbaProject.bin` from each workbook.
- Uses our tolerant decompressor and dir parser to locate module text.
- Compares text regions (post‑`ModuleOffset`) module‑by‑module.
- Reports whether text is equal and shows `ModuleOffset` values per side.

Tips:
- To validate decompiler parity, run your legacy VBADecompiler to produce a "decompiled" workbook and compare that against our `--repack` output.
- Mismatches typically indicate either meaningful differences in module text or benign differences in dir metadata (e.g., when offsets are not updated because resizing was not possible and the fallback path was used).

### Interpreting mismatches and normalization options

It is common for two “cleaned” workbooks to have byte-level differences in the module text region while remaining functionally identical. Causes include:

- Line ending normalization (CRLF vs LF) and trailing whitespace.
- Presence/order of `Attribute VB_*` lines that Excel/decompilers may add or reorder.
- Code page/encoding differences and optional BOMs.
- Designer metadata and sheet/userform code-behind serialization nuances.

To reduce false mismatches, the parity harness supports:

- `--normalize`: unify line endings, remove trailing spaces, drop trailing blank lines, strip BOM if present.
- `--ignore-attributes`: when combined with `--normalize`, drop lines that start with `Attribute VB_`.

Example:

```powershell
python tests/parity_harness.py --left A.xlsb --right B.xlsb --verbose --normalize --ignore-attributes
```

This highlights substantive text differences while ignoring common cosmetic variations introduced by different cleaning/decompilation pipelines.

### Smart comparison mode

Use `--smart` to apply a stronger equivalence heuristic intended to reflect functional parity:

- Normalizes EOLs, trims trailing spaces, drops BOM.
- Drops `Attribute VB_*` lines and `Option ...` lines.
- Strips comments (apostrophe `'` and leading `Rem`).
- Joins VBA line continuations (` _`).
- Extracts only procedure bodies (`Sub`/`Function`/`Property ... End ...`).
- Compares case-insensitively with collapsed whitespace.

Example:

```powershell
python tests/parity_harness.py --left Test_repacked.xlsb --right Test_decompiled.xlsb --verbose --smart
```

This mode is aggressive and is meant for “are these modules functionally the same?” checks, at the cost of hiding benign formatting and declaration differences.
