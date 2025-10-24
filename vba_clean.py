#!/usr/bin/env python3
"""VBA P-code cleaner for Excel workbooks.

This script removes the compiled VBA performance cache (P-code) from every
module inside an Excel XLSB or XLSM file without launching Excel.

    # Show usage:
    python vba_clean.py --help

    # Decompile Test.xlsb to Test_clean.xlsb:
    python vba_clean.py Test.xlsb

    # Decompile in-place, after creating a "Predecompiled Test.xlsb" backup:
    python vba_clean.py --in-place Test.xlsb

Copyright 2025 by David Bookstaber.  Licensed under GNU GPL v3.
Some code adapted from the BSD-licensed `oletools` project by Philippe Lagadec.
"""
from __future__ import annotations

import argparse
import io
import math
import os
import shutil
import struct
import sys
import zipfile
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple

try:
    import olefile
except ImportError as exc:  # pragma: no cover - dependency guard
    raise SystemExit(
        "The 'olefile' package is required. Install it via 'pip install olefile'."
    ) from exc


# ---------------------------------------------------------------------------
# Helpers adapted from oletools (BSD-licensed) for MS-OVBA decompression.
# Source: https://github.com/decalage2/oletools/blob/master/oletools/olevba.py
# ---------------------------------------------------------------------------

def _copytoken_help(decompressed_current: int, decompressed_chunk_start: int) -> Tuple[int, int, int, int]:
    """Compute CopyToken helper masks as defined in MS-OVBA 2.4.1.3.19.1."""
    difference = decompressed_current - decompressed_chunk_start
    bit_count = int(math.ceil(math.log(max(difference, 1), 2)))
    bit_count = max(bit_count, 4)
    length_mask = 0xFFFF >> bit_count
    offset_mask = ~length_mask
    maximum_length = (0xFFFF >> bit_count) + 3
    return length_mask, offset_mask, bit_count, maximum_length


def decompress_stream(compressed_container: bytes) -> bytes:
    """Decompress a VBA stream according to MS-OVBA section 2.4.1."""
    if not isinstance(compressed_container, bytearray):
        compressed = bytearray(compressed_container)
    else:
        compressed = compressed_container

    if not compressed:
        return b""

    decompressed = bytearray()
    compressed_current = 0

    sig_byte = compressed[compressed_current]
    if sig_byte != 0x01:
        raise ValueError(f"Invalid compressed container signature 0x{sig_byte:02X}")
    compressed_current += 1

    while compressed_current < len(compressed):
        compressed_chunk_start = compressed_current
        compressed_chunk_header = struct.unpack_from(
            "<H", compressed, compressed_chunk_start
        )[0]
        chunk_size = (compressed_chunk_header & 0x0FFF) + 3
        chunk_signature = (compressed_chunk_header >> 12) & 0x07
        chunk_flag = (compressed_chunk_header >> 15) & 0x01

        if chunk_signature != 0b011:
            raise ValueError("Invalid CompressedChunkSignature in VBA compressed stream")
        if chunk_flag == 1 and chunk_size > 4098:
            raise ValueError("Compressed chunk size exceeds 4098 bytes")
        if chunk_flag == 0 and chunk_size != 4098:
            raise ValueError("Uncompressed chunk size must be 4098 bytes")

        compressed_end = min(len(compressed), compressed_chunk_start + chunk_size)
        compressed_current = compressed_chunk_start + 2

        if chunk_flag == 0:
            decompressed.extend(
                compressed[compressed_current : compressed_current + 4096]
            )
            compressed_current += 4096
            continue

        decompressed_chunk_start = len(decompressed)
        while compressed_current < compressed_end:
            flag_byte = compressed[compressed_current]
            compressed_current += 1
            for bit_index in range(8):
                if compressed_current >= compressed_end:
                    break
                flag_bit = (flag_byte >> bit_index) & 1
                if flag_bit == 0:  # literal token
                    decompressed.append(compressed[compressed_current])
                    compressed_current += 1
                else:  # copy token
                    copy_token = struct.unpack_from("<H", compressed, compressed_current)[0]
                    length_mask, offset_mask, bit_count, _ = _copytoken_help(
                        len(decompressed), decompressed_chunk_start
                    )
                    length = (copy_token & length_mask) + 3
                    temp1 = copy_token & offset_mask
                    temp2 = 16 - bit_count
                    offset = (temp1 >> temp2) + 1
                    copy_source = len(decompressed) - offset
                    for _ in range(length):
                        decompressed.append(decompressed[copy_source])
                        copy_source += 1
                    compressed_current += 2

    return bytes(decompressed)


# ---------------------------------------------------------------------------
# dir stream parsing (trimmed + adapted from oletools VBA_Project/VBA_Module)
# ---------------------------------------------------------------------------

@dataclass
class ModuleInfo:
    """Metadata required to patch a VBA module stream."""

    stream_name: str
    code_page: str
    text_offset: int


class DirStreamParser:
    """Parse the VBA dir stream to recover module stream names and offsets."""

    def __init__(self, dir_bytes: bytes):
        self.stream = io.BytesIO(dir_bytes)
        self.modules: List[ModuleInfo] = []
        self.codepage = "cp1252"
        self._parse_project_stream()
        # Be robust to dir layouts: scan modules from the start so we don't
        # depend on the exact cursor position after project-level records.
        self.stream.seek(0)
        self._parse_modules()

    # -- binary helpers --------------------------------------------------
    @staticmethod
    def _read_uint16(buff: io.BytesIO) -> int:
        data = buff.read(2)
        if len(data) != 2:
            raise EOFError("Unexpected end of dir stream while reading uint16")
        return struct.unpack("<H", data)[0]

    @staticmethod
    def _read_uint32(buff: io.BytesIO) -> int:
        data = buff.read(4)
        if len(data) != 4:
            raise EOFError("Unexpected end of dir stream while reading uint32")
        return struct.unpack("<L", data)[0]

    def _decode_bytes(self, data: bytes) -> str:
        return data.decode(self.codepage, errors="replace")

    # -- parsing ---------------------------------------------------------
    def _parse_project_stream(self) -> None:
        """Read top-level project records until PROJECTMODULES is reached."""
        # We only parse the subset that influences decoding (code page) and
        # positions the stream cursor at the modules table.
        while True:
            try:
                record_id = self._read_uint16(self.stream)
            except EOFError:
                break
            if record_id == 0x000F:  # PROJECTMODULES
                size = self._read_uint32(self.stream)
                # Size should be 0x0002; read module count and project cookie.
                modules_count = self._read_uint16(self.stream)
                # Keep module count to sanity-check later; the next record id
                # will be read by the outer loop on the subsequent iteration.
                self.expected_modules = modules_count
                break
            size = self._read_uint32(self.stream)
            payload = self.stream.read(size)
            if record_id == 0x0003 and size == 2:  # PROJECTCODEPAGE
                cp_value = struct.unpack("<H", payload)[0]
                self.codepage = self._resolve_codepage(cp_value)

    @staticmethod
    def _resolve_codepage(cp_value: int) -> str:
        # Windows code page to Python codec translation; fall back to cp1252.
        if cp_value == 0:
            return "cp1252"
        for candidate in (f"cp{cp_value}", f"windows-{cp_value}"):
            try:
                ''.encode(candidate)  # no-op to validate codec
            except LookupError:
                continue
            else:
                return candidate
        return "cp1252"

    def _parse_modules(self) -> None:
        """Iterate MODULE records to extract stream names and offsets."""
        modules_found = 0
        while True:
            try:
                record_id = self._read_uint16(self.stream)
            except EOFError:
                break
            if record_id != 0x0019:  # MODULENAME indicates new module
                # Skip record payload generically
                try:
                    size = self._read_uint32(self.stream)
                except EOFError:
                    break
                if size:
                    remaining = self.stream.read(size)
                    if len(remaining) != size:
                        break
                continue

            try:
                size = self._read_uint32(self.stream)
            except EOFError:
                break
            modulename_bytes = self.stream.read(size)
            if len(modulename_bytes) != size:
                break
            module_name = self._decode_bytes(modulename_bytes)

            # Optional MODULENAMEUNICODE (0x0047)
            next_id = self._read_uint16(self.stream)
            if next_id == 0x0047:
                try:
                    size = self._read_uint32(self.stream)
                except EOFError:
                    break
                skipped = self.stream.read(size)
                if len(skipped) != size:
                    break
                next_id = self._read_uint16(self.stream)

            if next_id != 0x001A:
                raise ValueError("Unexpected record sequence in dir stream")

            try:
                size = self._read_uint32(self.stream)
            except EOFError:
                break
            streamname_bytes = self.stream.read(size)
            if len(streamname_bytes) != size:
                break
            stream_name = self._decode_bytes(streamname_bytes)

            try:
                reserved = self._read_uint16(self.stream)  # 0x0032 expected
            except EOFError:
                break
            if reserved != 0x0032:
                raise ValueError("Missing MODULESTREAMNAMEUNICODE record")
            size = self._read_uint32(self.stream)
            skipped = self.stream.read(size)
            if len(skipped) != size:
                break

            try:
                section_id = self._read_uint16(self.stream)
            except EOFError:
                break
            if section_id == 0x001C:  # MODULEDOCSTRING
                size = self._read_uint32(self.stream)
                skipped = self.stream.read(size)
                if len(skipped) != size:
                    break
                reserved = self._read_uint16(self.stream)
                if reserved == 0x0048:
                    size = self._read_uint32(self.stream)
                    skipped = self.stream.read(size)
                    if len(skipped) != size:
                        break
                    section_id = self._read_uint16(self.stream)

            if section_id == 0x0031:  # MODULEOFFSET
                size = self._read_uint32(self.stream)
                if size != 4:
                    raise ValueError("MODULEOFFSET must be 4 bytes")
                text_offset = self._read_uint32(self.stream)
                try:
                    section_id = self._read_uint16(self.stream)
                except EOFError:
                    break
            else:
                raise ValueError("MODULEOFFSET record missing")

            # Skip optional sections until MODULE terminator (0x002B)
            while section_id not in (0x002B, None):
                size = self._read_uint32(self.stream)
                skipped = self.stream.read(size)
                if len(skipped) != size:
                    section_id = None
                    break
                try:
                    section_id = self._read_uint16(self.stream)
                except EOFError:
                    section_id = None
                    break

            if section_id != 0x002B:
                raise ValueError("MODULE terminator not found")
            size = self._read_uint32(self.stream)
            skipped = self.stream.read(size)
            if len(skipped) != size:
                break

            modules_found += 1
            self.modules.append(
                ModuleInfo(stream_name=stream_name, code_page=self.codepage, text_offset=text_offset)
            )

        if getattr(self, "expected_modules", None) and modules_found != self.expected_modules:
            # Do not fail hard; Excel tolerates mismatch.
            pass


# ---------------------------------------------------------------------------
# P-code neutralisation
# ---------------------------------------------------------------------------

def zero_pcode_region(compressed: bytes, text_offset: int) -> bytes:
    """Return a new compressed stream with the first ``text_offset`` bytes zeroed.

    We walk the compressed container, mirroring the decompression algorithm, and
    replace literal token bytes that contribute to the initial ``text_offset``
    decompressed region with zero. Copy tokens refer only to already processed
    bytes, so they automatically resolve to zero after literals are cleared.
    """
    if text_offset <= 0:
        return compressed

    buf = bytearray(compressed)
    compressed_current = 0
    decompressed_current = 0

    if not buf:
        return bytes(buf)
    if buf[compressed_current] != 0x01:
        raise ValueError("Invalid compressed stream signature")
    compressed_current += 1

    while compressed_current < len(buf) and decompressed_current < text_offset:
        chunk_start = compressed_current
        header = struct.unpack_from("<H", buf, chunk_start)[0]
        chunk_size = (header & 0x0FFF) + 3
        chunk_flag = (header >> 15) & 0x01

        compressed_current = chunk_start + 2
        compressed_end = min(len(buf), chunk_start + chunk_size)

        if chunk_flag == 0:
            remaining = min(4096, text_offset - decompressed_current)
            if remaining > 0:
                start = compressed_current
                end = start + remaining
                buf[start:end] = b"\x00" * remaining
            decompressed_current += 4096
            compressed_current += 4096
            continue

        chunk_decompressed_start = decompressed_current
        while compressed_current < compressed_end and decompressed_current < text_offset:
            flag_byte = buf[compressed_current]
            compressed_current += 1
            for bit_index in range(8):
                if compressed_current >= compressed_end:
                    break
                flag_bit = (flag_byte >> bit_index) & 1
                if flag_bit == 0:
                    if decompressed_current < text_offset:
                        buf[compressed_current] = 0
                    compressed_current += 1
                    decompressed_current += 1
                else:
                    copy_token = struct.unpack_from("<H", buf, compressed_current)[0]
                    length_mask, offset_mask, bit_count, _ = _copytoken_help(
                        decompressed_current, chunk_decompressed_start
                    )
                    length = (copy_token & length_mask) + 3
                    compressed_current += 2
                    decompressed_current += length
                    if decompressed_current >= text_offset:
                        break

    return bytes(buf)


# ---------------------------------------------------------------------------
# Workbook processing
# ---------------------------------------------------------------------------

def patch_vba_project(project_bytes: bytes) -> Tuple[bytes, Dict[str, int], List[str], bool]:
    """Return patched vbaProject.bin content, stats, module names, and parse status."""
    bio = io.BytesIO(project_bytes)
    ole = olefile.OleFileIO(bio, write_mode=True)

    # Parse dir stream to get module metadata
    dir_stream = ole.openstream(["VBA", "dir"]).read()
    dir_data = decompress_stream(dir_stream)
    parser = DirStreamParser(dir_data)

    modifications: Dict[str, int] = {}
    modules_seen: List[str] = []
    parse_ok = True
    if not parser.modules:
        # Fallback: enumerate streams under VBA/ to at least detect macros
        parse_ok = False
        special = {"dir", "project", "_vba_project", "projectwm"}
        for entry in ole.listdir():
            if len(entry) == 2 and entry[0].lower() == "vba":
                name = entry[1]
                if name.lower() not in special:
                    modules_seen.append(name)

    for module in parser.modules:
        modules_seen.append(module.stream_name)
        stream_path = ["VBA", module.stream_name]
        if not ole.exists(stream_path):
            continue
        module_bytes = ole.openstream(stream_path).read()
        patched = zero_pcode_region(module_bytes, module.text_offset)
        if patched != module_bytes:
            ole.write_stream(stream_path, patched)
            modifications[module.stream_name] = module.text_offset

    ole.close()
    bio.seek(0)
    return bio.read(), modifications, modules_seen, parse_ok


def process_workbook(
    input_path: str, output_path: str, in_place: bool = False
) -> Tuple[Dict[str, int], Set[str], bool]:
    """Process an XLSB workbook and return modification summary and modules."""
    target_normalized = "xl/vbaproject.bin"
    modifications: Dict[str, int] = {}
    modules_detected: Set[str] = set()
    parse_ok = True
    tmp_output = output_path
    if in_place:
        tmp_output = input_path + ".tmp"

    with zipfile.ZipFile(input_path, "r") as zin, zipfile.ZipFile(tmp_output, "w") as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)
            if item.filename.lower() == target_normalized:
                patched, mods, modules_seen, this_parse_ok = patch_vba_project(data)
                data = patched
                modifications.update(mods)
                modules_detected.update(modules_seen)
                parse_ok = parse_ok and this_parse_ok
            zout.writestr(item, data)

    if in_place:
        os.replace(tmp_output, input_path)

    return modifications, modules_detected, parse_ok


def create_predecompile_backup(source_path: str) -> str:
    """Create the required in-place backup copy and return its path."""
    parent_dir = os.path.dirname(source_path)
    backup_name = f"Predecompiled {os.path.basename(source_path)}"
    backup_path = os.path.join(parent_dir, backup_name)
    shutil.copy2(source_path, backup_path)
    return backup_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Strip VBA P-code from XLSB files")
    parser.add_argument("xlsb", help="Path to the source XLSB workbook")
    parser.add_argument(
        "--output",
        "-o",
        dest="output",
        help="Destination path (defaults to <input>_clean.xlsb)",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        dest="in_place",
        help="Overwrite the input file instead of creating a copy",
    )
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    """
    vba_clean.py - Remove VBA P-code opcodes from modules in an Excel workbook (.xlsb/.xlsm).
    usage: vba_clean.py [-h] [--in-place] [--output OUTPUT] <workbook>
    positional arguments:
        <workbook>            The .xlsb or .xlsm workbook to process
    optional arguments:
        -h, --help            show this help message and exit
        --in-place            Modify the source workbook in-place (cannot be combined with --output)
        --output OUTPUT       Destination for cleaned workbook (defaults to <workbook>_clean.xls*)

    Behavior:
        - The tool scans the workbook for VBA modules and removes P-code where present.
        - If --in-place is specified the source file is overwritten (mutually exclusive with --output).
        - On success, the program prints a summary of modifications.

    Exit status:
        0 on success, non-zero on error (e.g., file not found, invalid arguments).
    """
    parser = build_arg_parser()
    if argv is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(list(argv))

    source = os.path.abspath(args.xlsb)
    if not os.path.exists(source):
        parser.error(f"File not found: {source}")

    if args.in_place and args.output:
        parser.error("--output cannot be combined with --in-place")

    if args.in_place:
        output = source
    else:
        output = args.output
        if not output:
            root, ext = os.path.splitext(source)
            output = f"{root}_clean{ext or '.xlsb'}"
        output = os.path.abspath(output)
        if os.path.isdir(output):
            parser.error("Output path points to a directory")

    if args.in_place:
        try:
            backup_path = create_predecompile_backup(source)
        except OSError as exc:
            print(
                f"Unable to create backup 'Predecompiled {os.path.basename(source)}': {exc}",
                file=sys.stderr,
            )
            return 2
        else:
            print(f"Backup created at: {backup_path}")

    mutation_summary, modules_detected, parse_ok = process_workbook(
        source, output, in_place=args.in_place
    )

    if mutation_summary:
        print("P-code removed from modules:")
        for module, offset in sorted(mutation_summary.items()):
            print(f"  - {module} (offset {offset})")
    elif modules_detected:
        if parse_ok:
            print("VBA macros detected but already lacked P-code; no changes made.")
        else:
            print("VBA modules detected, but the dir stream could not be parsed; no changes made.")
    else:
        print("No VBA project modules found; workbook contains no macros.")

    if not args.in_place:
        print(f"Modified workbook written to: {output}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
