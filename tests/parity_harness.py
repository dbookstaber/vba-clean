#!/usr/bin/env python3
"""
Parity validation harness for comparing VBA modules between two workbooks.

Compares the VBA source text (post-ModuleOffset) of all modules in xl/vbaProject.bin
between two Excel workbooks (.xlsm/.xlsb). Also reports ModuleOffset values as parsed
from the dir stream.

Usage:
    python tools/parity_harness.py --left A.xlsm --right B.xlsm [--json report.json] [--verbose]

Example:
    # Compare original vs cleaned (or decompiled) workbook
    python tools/parity_harness.py --left tests/samples/Test_LargeModule.xlsm --right tests/samples/Test_LargeModule_clean.xlsm --verbose

Exit codes:
    0 if comparison ran successfully (mismatches may still be present)
    2 on argument errors or file access issues

Notes:
    - This tool is read-only; it does not modify files.
    - It relies on vba_clean's tolerant MS-OVBA decompression and dir parsing.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import zipfile
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import difflib
import re

# Local import
import os as _os
import sys as _sys
# Ensure repository root is on sys.path for local imports
_sys.path.append(_os.path.dirname(_os.path.dirname(__file__)))
import vba_clean


@dataclass
class ModuleSnapshot:
    name: str
    offset: Optional[int]
    text: bytes


def _read_vbaproject_bytes(path: str) -> Optional[bytes]:
    try:
        with zipfile.ZipFile(path, 'r') as z:
            with z.open('xl/vbaProject.bin', 'r') as fh:
                return fh.read()
    except KeyError:
        return None
    except Exception as e:
        print(f"Error opening {path}: {e}", file=sys.stderr)
        return None


def _read_snapshot(path: str) -> Tuple[List[ModuleSnapshot], Dict[str, int]]:
    vp = _read_vbaproject_bytes(path)
    if not vp:
        return [], {}
    mods: List[ModuleSnapshot] = []
    offsets: Dict[str, int] = {}

    # Parse OLE and dir
    import olefile as _ole
    ole = _ole.OleFileIO(vp)
    dir_comp = ole.openstream(['VBA', 'dir']).read()
    try:
        dir_data = vba_clean.decompress_stream(dir_comp)
        parser = vba_clean.DirStreamParser(dir_data)
        offsets = {m.stream_name: m.text_offset for m in parser.modules}
    except Exception:
        offsets = {}
    
    # If strict parser fails, try tolerant extraction
    if not offsets:
        offsets = vba_clean._extract_offsets_from_dir(dir_comp)

    # Enumerate module streams
    special = {"dir", "project", "_vba_project", "projectwm"}
    for entry in ole.listdir():
        if len(entry) == 2 and entry[0].lower() == 'vba':
            name = entry[1]
            if name.lower() in special:
                continue
            data = ole.openstream(['VBA', name]).read()
            if not data:
                continue
            # Determine text offset (ModuleOffset)
            off = offsets.get(name)
            if off is None:
                off = vba_clean._guess_text_offset(data)
            if off is None or off < 0 or off >= len(data):
                continue
            # Validate compression signature at offset
            if data[off] != 0x01:
                continue
            try:
                text = vba_clean.decompress_module_text(data, off)
            except Exception:
                continue
            mods.append(ModuleSnapshot(name=name, offset=offsets.get(name), text=text))

    ole.close()
    return mods, offsets


def _strip_line_continuations(s: str) -> str:
    # Join lines ending with space + underscore continuation
    lines = s.split('\n')
    out: List[str] = []
    buf = ''
    for ln in lines:
        if ln.rstrip().endswith(' _'):
            buf += ln.rstrip()[:-2]
        else:
            if buf:
                out.append(buf + ln)
                buf = ''
            else:
                out.append(ln)
    if buf:
        out.append(buf)
    return '\n'.join(out)


def _strip_vba_comments(s: str) -> str:
    # Remove comments starting with ' or Rem (when at line start, ignoring spaces)
    res = []
    for ln in s.split('\n'):
        lstripped = ln.lstrip()
        if lstripped.lower().startswith('rem '):
            continue
        # Remove apostrophe comments not inside quotes: simple state machine
        out = []
        in_str = False
        i = 0
        while i < len(ln):
            ch = ln[i]
            if ch == '"':
                out.append(ch)
                # Toggle string unless it's an escaped quote ""
                if i + 1 < len(ln) and ln[i + 1] == '"':
                    out.append('"')
                    i += 2
                    continue
                in_str = not in_str
                i += 1
                continue
            if ch == "'" and not in_str:
                break  # drop rest of line
            out.append(ch)
            i += 1
        res.append(''.join(out))
    return '\n'.join(res)


def _extract_procedures(s: str) -> str:
    # Extract Sub/Function/Property bodies; return concatenation sorted by proc name for order-insensitive compare
    # Capture name as group 2
    pattern = re.compile(
        r"^\s*(sub|function|property\s+(?:get|let|set))\s+([a-zA-Z_][a-zA-Z0-9_]*)\b.*?^\s*end\s+(sub|function|property)\b",
        re.IGNORECASE | re.DOTALL | re.MULTILINE,
    )
    matches = list(pattern.finditer(s))
    if not matches:
        return s
    procs = []
    for m in matches:
        name = m.group(2).lower()
        body = m.group(0)
        procs.append((name, body))
    procs.sort(key=lambda t: t[0])
    return '\n\n'.join(p[1] for p in procs)


def _normalize_text(b: bytes, drop_attributes: bool, strip_comments: bool = False, strip_options: bool = False,
                     proc_only: bool = False, case_insensitive: bool = False, collapse_ws: bool = False) -> bytes:
    # Convert to str via latin-1 passthrough to preserve bytes; then unify EOLs
    s = b.decode('latin-1', errors='ignore')
    s = s.replace('\r\n', '\n').replace('\r', '\n')
    # Drop leading BOM if any
    if s and s[0] == '\ufeff':
        s = s[1:]
    s = _strip_line_continuations(s)
    if strip_comments:
        s = _strip_vba_comments(s)
    lines = s.split('\n')
    if drop_attributes:
        lines = [ln for ln in lines if not ln.startswith('Attribute VB_')]
    if strip_options:
        lines = [ln for ln in lines if not ln.lstrip().lower().startswith('option ')]
    # Trim trailing spaces on each line and collapse trailing blank lines
    lines = [ln.rstrip() for ln in lines]
    while lines and lines[-1] == '':
        lines.pop()
    s2 = '\n'.join(lines)
    if proc_only:
        s2 = _extract_procedures(s2)
    if collapse_ws:
        s2 = re.sub(r"\s+", " ", s2)
    if case_insensitive:
        s2 = s2.lower()
    return s2.encode('latin-1', errors='ignore')


def compare(left: str, right: str, verbose: bool = False, normalize: bool = False, ignore_attributes: bool = False,
            smart: bool = False, diff_dir: Optional[str] = None) -> Dict:
    left_mods, left_offsets = _read_snapshot(left)
    right_mods, right_offsets = _read_snapshot(right)

    left_map = {m.name: m for m in left_mods}
    right_map = {m.name: m for m in right_mods}

    names = sorted(set(left_map.keys()) | set(right_map.keys()))
    results = []
    mismatches = 0

    for name in names:
        L = left_map.get(name)
        R = right_map.get(name)
        status = {
            'module': name,
            'present_left': L is not None,
            'present_right': R is not None,
            'offset_left': left_offsets.get(name),
            'offset_right': right_offsets.get(name),
            'text_equal': None,
        }
        if L is None or R is None:
            mismatches += 1
            status['text_equal'] = False
        else:
            if normalize or smart:
                lt_bytes = _normalize_text(
                    L.text,
                    drop_attributes=(ignore_attributes or smart),
                    strip_comments=smart,
                    strip_options=smart,
                    proc_only=smart,
                    case_insensitive=smart,
                    collapse_ws=normalize or smart,
                )
                rt_bytes = _normalize_text(
                    R.text,
                    drop_attributes=(ignore_attributes or smart),
                    strip_comments=smart,
                    strip_options=smart,
                    proc_only=smart,
                    case_insensitive=smart,
                    collapse_ws=normalize or smart,
                )
                equal = (lt_bytes == rt_bytes)
            else:
                lt_bytes = L.text
                rt_bytes = R.text
                equal = (lt_bytes == rt_bytes)
            status['text_equal'] = equal
            if not equal:
                mismatches += 1
                if diff_dir:
                    try:
                        os.makedirs(diff_dir, exist_ok=True)
                        # Write normalized texts for manual inspection
                        with open(os.path.join(diff_dir, f"{name}_left.txt"), 'wb') as fh:
                            fh.write(lt_bytes)
                        with open(os.path.join(diff_dir, f"{name}_right.txt"), 'wb') as fh:
                            fh.write(rt_bytes)
                        # Also write a unified diff (best-effort, decoded as latin-1)
                        lts = lt_bytes.decode('latin-1', errors='ignore').splitlines(keepends=False)
                        rts = rt_bytes.decode('latin-1', errors='ignore').splitlines(keepends=False)
                        diff = difflib.unified_diff(lts, rts, fromfile=f"{name}_left", tofile=f"{name}_right", lineterm='')
                        with open(os.path.join(diff_dir, f"{name}.diff.txt"), 'w', encoding='utf-8', newline='\n') as fh:
                            fh.write('\n'.join(diff))
                    except Exception:
                        pass
        results.append(status)
        if verbose:
            print(f"- {name}: text_equal={status['text_equal']} offset_left={status['offset_left']} offset_right={status['offset_right']}")

    summary = {
        'left': left,
        'right': right,
        'modules_compared': len(names),
        'mismatches': mismatches,
        'results': results,
    }
    if verbose:
        print(f"Compared {len(names)} modules; mismatches={mismatches}")
    return summary


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description='Compare VBA modules between two workbooks (text region only).')
    p.add_argument('--left', required=True, help='Left workbook (.xlsm/.xlsb)')
    p.add_argument('--right', required=True, help='Right workbook (.xlsm/.xlsb)')
    p.add_argument('--json', dest='json_out', help='Optional path to write JSON report')
    p.add_argument('--verbose', action='store_true', help='Print per-module results')
    p.add_argument('--normalize', action='store_true', help='Normalize line endings and trim trailing spaces before comparing')
    p.add_argument('--ignore-attributes', action='store_true', help='When normalizing, drop lines starting with "Attribute VB_"')
    p.add_argument('--smart', action='store_true', help='Heuristic equivalence: normalize + ignore attributes + strip comments/options + compare procedures only, case-insensitive, collapsed whitespace')
    p.add_argument('--diff-dir', help='Optional directory to write per-module normalized texts and unified diffs for mismatches')
    args = p.parse_args(argv)

    left = os.path.abspath(args.left)
    right = os.path.abspath(args.right)
    if not os.path.exists(left) or not os.path.exists(right):
        print('One or both files were not found.', file=sys.stderr)
        return 2

    summary = compare(left, right, verbose=args.verbose, normalize=args.normalize, ignore_attributes=args.ignore_attributes, smart=args.smart, diff_dir=args.diff_dir)
    if args.json_out:
        try:
            with open(args.json_out, 'w', encoding='utf-8') as fh:
                json.dump(summary, fh, indent=2)
            if args.verbose:
                print(f"JSON report written to: {args.json_out}")
        except Exception as e:
            print(f"Failed to write JSON: {e}", file=sys.stderr)

    return 0


if __name__ == '__main__':
    sys.exit(main())
