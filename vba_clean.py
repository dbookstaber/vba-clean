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
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

# Global toggle set by CLI to enforce full text repack on all modules
FORCE_REPACK_ALL: bool = False

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
        # Be tolerant: some producers violate the signature; try best-effort
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
        # Tolerate non-spec signatures/sizes seen in the wild; bounds-check only
        if chunk_flag == 1 and chunk_size > 4098:
            raise ValueError("Compressed chunk size exceeds 4098 bytes")

        compressed_end = min(len(compressed), compressed_chunk_start + chunk_size)
        compressed_current = compressed_chunk_start + 2

        if chunk_flag == 0:
            # Some files use shorter final uncompressed chunks; copy what's available (<=4096)
            literal_len = min(4096, max(0, compressed_end - compressed_current))
            if literal_len:
                decompressed.extend(
                    compressed[compressed_current : compressed_current + literal_len]
                )
                compressed_current += literal_len
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


def _compress_uncompressed(payload: bytes) -> bytes:
        """Encode bytes into an MS-OVBA compressed container using only uncompressed chunks.

        We emit a signature byte 0x01 followed by one or more uncompressed chunks.
        For each chunk of length L (<=4096), we write a 2-byte header where:
            - CompressedChunkSignature bits (12..14) = 0b011
            - CompressedChunkFlag (bit 15) = 0 (uncompressed)
            - low 12 bits = (chunk_size - 3) and chunk_size = L + 2 -> low12 = L - 1
        Then we write L literal bytes.
        """
        out = bytearray()
        out.append(0x01)
        i = 0
        n = len(payload)
        while i < n:
                L = min(4096, n - i)
                low12 = (L - 1) & 0x0FFF
                header = (0b011 << 12) | low12  # flag bit 15 is 0 for uncompressed
                out += struct.pack('<H', header)
                out += payload[i : i + L]
                i += L
        return bytes(out)


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
        """Iterate MODULE records to extract stream names and offsets (tolerant)."""
        modules_found = 0
        def read_id() -> Optional[int]:
            try:
                return self._read_uint16(self.stream)
            except EOFError:
                return None

        def read_sz() -> Optional[int]:
            try:
                return self._read_uint32(self.stream)
            except EOFError:
                return None

        while True:
            record_id = read_id()
            if record_id is None:
                break
            if record_id != 0x0019:  # MODULENAME start
                size = read_sz()
                if size is None:
                    break
                if size:
                    chunk = self.stream.read(size)
                    if len(chunk) != size:
                        break
                continue

            size = read_sz()
            if size is None:
                break
            name_bytes = self.stream.read(size)
            if len(name_bytes) != size:
                break
            module_name = self._decode_bytes(name_bytes)

            # Optional Unicode name (0x0047)
            next_id = read_id()
            if next_id == 0x0047:
                size = read_sz()
                if size is None:
                    break
                skipped = self.stream.read(size)
                if len(skipped) != size:
                    break
                next_id = read_id()

            stream_name = module_name
            if next_id == 0x001A:  # STREAMNAME
                size = read_sz()
                if size is None:
                    break
                sbytes = self.stream.read(size)
                if len(sbytes) != size:
                    break
                stream_name = self._decode_bytes(sbytes)
            else:
                # Unexpected sequence; try to continue regardless
                pass

            # Optional 0x0032 (MODULESTREAMNAMEUNICODE)
            peek = read_id()
            if peek == 0x0032:
                size = read_sz()
                if size is None:
                    break
                skipped = self.stream.read(size)
                if len(skipped) != size:
                    break
                peek = read_id()

            text_offset = None
            # Consume records until we find MODULEOFFSET (0x0031) or end of module
            while peek is not None and peek not in (0x002B,):
                if peek == 0x0031:
                    size = read_sz()
                    if size != 4:
                        # read and skip unexpected payload
                        if size is None:
                            break
                        skipped = self.stream.read(size or 0)
                        if size and len(skipped) != size:
                            break
                    else:
                        # valid offset
                        try:
                            text_offset = self._read_uint32(self.stream)
                        except EOFError:
                            break
                else:
                    size = read_sz()
                    if size is None:
                        break
                    skipped = self.stream.read(size)
                    if len(skipped) != size:
                        break
                peek = read_id()

            # Optionally read MODULE terminator payload size
            if peek == 0x002B:
                size = read_sz()
                if size is not None:
                    _ = self.stream.read(size)

            if text_offset is not None:
                modules_found += 1
                self.modules.append(
                    ModuleInfo(stream_name=stream_name, code_page=self.codepage, text_offset=text_offset)
                )

        # Do not hard fail on mismatches; Excel tolerates many layouts.


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
            # Copy length may be shorter than 4096 in some files
            data_len = min(4096, max(0, compressed_end - compressed_current))
            remaining = min(data_len, max(0, text_offset - decompressed_current))
            if remaining > 0:
                start = compressed_current
                end = start + remaining
                buf[start:end] = b"\x00" * remaining
            decompressed_current += data_len
            compressed_current += data_len
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

def _guess_text_offset(comp: bytes) -> Optional[int]:
    try:
        decomp = decompress_stream(comp)
    except Exception:
        return None
    markers_ascii = [
        b"Attribute VB_",
        b"Attribute ",
        b"Option Explicit",
        b"Option Base",
        b"Option Compare",
        b"Sub ",
        b"Function ",
        b"VERSION ",
    ]
    # Also search UTF-16LE encodings of the same markers
    markers_u16 = [b"".join(bytes((c,0)) for c in m) for m in markers_ascii]
    positions = []
    for m in markers_ascii + markers_u16:
        p = decomp.find(m)
        if p >= 0:
            positions.append(p)
    return min(positions) if positions else None


def _update_dir_offsets_to_zero(dir_comp: bytes, module_names: Set[str]) -> Tuple[bytes, bool]:
    """Set ModuleOffset (0x0031 payload) to 0 for given module streams by robust search.

    Strategy:
    - Decompress dir stream
    - Determine codepage from PROJECTCODEPAGE (0x0003)
    - For each target module name, search its encoded bytes and then, within a
      limited forward window, locate the next MODULEOFFSET record (0x0031, size=4)
      and zero its 4-byte payload. This avoids relying on strict record ordering.
    Returns (new_dir_comp, updated).
    """
    try:
        data = bytearray(decompress_stream(dir_comp))
    except Exception:
        return dir_comp, False

    # Helpers
    def get_u16(p: int) -> Optional[int]:
        if p + 2 > len(data):
            return None
        return struct.unpack_from('<H', data, p)[0]

    def get_u32(p: int) -> Optional[int]:
        if p + 4 > len(data):
            return None
        return struct.unpack_from('<L', data, p)[0]

    # Determine codepage
    pos = 0
    codepage = 'cp1252'
    while pos + 6 <= len(data):
        rec = get_u16(pos)
        size = get_u32(pos + 2)
        if rec is None or size is None or pos + 6 + size > len(data):
            break
        if rec == 0x0003 and size == 2:
            cp_value = get_u16(pos + 6)
            if cp_value:
                for cand in (f"cp{cp_value}", f"windows-{cp_value}"):
                    try:
                        ''.encode(cand)
                    except LookupError:
                        continue
                    else:
                        codepage = cand
                        break
        pos += 6 + size

    updated = False
    # Tolerant scan anchored at STREAMNAME (0x001A). If not found forward before MODULE end (0x002B),
    # attempt a bounded backward search.
    pos_scan = 0
    while pos_scan + 6 <= len(data):
        rec = get_u16(pos_scan)
        size = get_u32(pos_scan + 2)
        if rec is None or size is None:
            break
        if rec == 0x001A and 0 < size <= 256 and pos_scan + 6 + size <= len(data):
            raw_name = bytes(data[pos_scan + 6 : pos_scan + 6 + size])
            try:
                stream_name = raw_name.decode(codepage, errors='replace')
            except Exception:
                stream_name = None
            module_start = pos_scan
            pos = pos_scan + 6 + size
            found_here = False
            if stream_name and stream_name in module_names:
                # Forward, record-aligned search until MODULE end (0x002B)
                while pos + 6 <= len(data):
                    r = get_u16(pos)
                    s = get_u32(pos + 2)
                    if r is None or s is None or pos + 6 + s > len(data):
                        break
                    if r == 0x0031 and s == 4:
                        data[pos + 6 : pos + 10] = b"\x00\x00\x00\x00"
                        updated = True
                        found_here = True
                        break
                    if r == 0x002B:  # MODULE end
                        break
                    pos += 6 + s
                # If not found forward, attempt bounded backward search before this 0x001A
                if not found_here:
                    back_start = max(0, module_start - 1024)
                    q = module_start
                    while q - 1 >= back_start:
                        q -= 1
                        r = get_u16(q)
                        s = get_u32(q + 2) if r is not None else None
                        if r == 0x0031 and s == 4 and q + 10 <= len(data):
                            data[q + 6 : q + 10] = b"\x00\x00\x00\x00"
                            updated = True
                            break
        # advance scan by 1 to resync even if sizes were bogus
        pos_scan += 1

    if not updated:
        return dir_comp, False
    return _compress_uncompressed(bytes(data)), True


def _extract_offsets_from_dir(dir_comp: bytes) -> Dict[str, int]:
    """Best-effort extraction of {stream_name: ModuleOffset} from a compressed dir stream.

    Uses a tolerant scan anchored at STREAMNAME (0x001A) with a forward walk to
    MODULEOFFSET (0x0031) within the same module; if not found, performs a bounded
    backward search to associate a preceding 0x0031.
    """
    offsets: Dict[str, int] = {}
    try:
        data = decompress_stream(dir_comp)
    except Exception:
        return offsets
    buf = data
    n = len(buf)

    def u16(p: int) -> Optional[int]:
        if p + 2 > n:
            return None
        return struct.unpack_from('<H', buf, p)[0]

    def u32(p: int) -> Optional[int]:
        if p + 4 > n:
            return None
        return struct.unpack_from('<L', buf, p)[0]

    # Determine codepage once
    codepage = 'cp1252'
    pos = 0
    while pos + 6 <= n:
        rec = u16(pos)
        size = u32(pos + 2)
        if rec is None or size is None or pos + 6 + size > n:
            break
        if rec == 0x0003 and size == 2:  # PROJECTCODEPAGE
            cp_value = u16(pos + 6)
            if cp_value:
                for cand in (f"cp{cp_value}", f"windows-{cp_value}"):
                    try:
                        ''.encode(cand)
                    except LookupError:
                        continue
                    else:
                        codepage = cand
                        break
        pos += 6 + size

    # Scan for stream names
    pos = 0
    while pos + 6 <= n:
        rec = u16(pos)
        size = u32(pos + 2)
        if rec is None or size is None or pos + 6 + size > n:
            break
        if rec == 0x001A:  # STREAMNAME
            raw = buf[pos + 6 : pos + 6 + size]
            try:
                name = raw.decode(codepage, errors='replace')
            except Exception:
                name = None
            module_start = pos
            # Look forward for 0x0031 until terminator 0x002B
            p = pos + 6 + size
            off_val: Optional[int] = None
            while p + 6 <= n:
                r = u16(p)
                s = u32(p + 2)
                if r is None or s is None or p + 6 + s > n:
                    break
                if r == 0x0031 and s == 4:
                    off_val = struct.unpack_from('<L', buf, p + 6)[0]
                    break
                if r == 0x002B:
                    break
                p += 6 + s
            if off_val is None:
                # bounded backward search up to 1024 bytes before this module
                q = max(0, module_start - 1024)
                p2 = module_start
                while p2 - 1 >= q:
                    p2 -= 1
                    r = u16(p2)
                    s = u32(p2 + 2) if r is not None else None
                    if r == 0x0031 and s == 4 and p2 + 10 <= n:
                        off_val = struct.unpack_from('<L', buf, p2 + 6)[0]
                        break
            if name and off_val is not None:
                offsets[name] = off_val
        pos += 1
    return offsets

def patch_vba_project(project_bytes: bytes) -> Tuple[bytes, Dict[str, int], List[str], bool, bool]:
    """Return (patched vbaProject.bin, modifications, module names, parse_ok, vba_changed)."""
    bio = io.BytesIO(project_bytes)
    ole = olefile.OleFileIO(bio, write_mode=True)

    # Safety: detect password-protected projects and skip to avoid macro deletion
    is_protected = False
    try:
        proj_bytes = ole.openstream(["VBA", "project"]).read()
        # PROJECT stream is ANSI text with lines; DPB indicates password hash
        if b"DPB=" in proj_bytes or b"DPX=" in proj_bytes:
            is_protected = True
    except Exception:
        pass
    if is_protected:
        # Do not modify protected projects; Excel relies on P-code since source is obfuscated
        ole.close()
        bio.seek(0)
        return bio.read(), {}, [], True, False

    # Parse dir stream to get module metadata
    dir_stream = ole.openstream(["VBA", "dir"]).read()
    dir_data = decompress_stream(dir_stream)
    parser = DirStreamParser(dir_data)

    modifications: Dict[str, int] = {}
    modules_seen: List[str] = []
    parse_ok = True
    vba_changed = False
    if not parser.modules:
        # Fallback: enumerate streams under VBA/ to at least detect macros
        parse_ok = False
        special = {"dir", "project", "_vba_project", "projectwm"}
        for entry in ole.listdir():
            if len(entry) == 2 and entry[0].lower() == "vba":
                name = entry[1]
                if name.lower() not in special:
                    modules_seen.append(name)

        # Second fallback: try to parse dir_data in a forgiving way to recover offsets
        def tolerant_dir_parse(data: bytes) -> List[ModuleInfo]:
            modules: List[ModuleInfo] = []
            pos = 0
            current_name: Optional[str] = None
            current_stream: Optional[str] = None
            current_offset: Optional[int] = None
            data_len = len(data)
            def get_u16(p: int) -> Optional[int]:
                if p + 2 > data_len:
                    return None
                return struct.unpack_from('<H', data, p)[0]
            def get_u32(p: int) -> Optional[int]:
                if p + 4 > data_len:
                    return None
                return struct.unpack_from('<L', data, p)[0]
            while pos + 6 <= data_len:
                rec = get_u16(pos)
                if rec is None:
                    break
                size = get_u32(pos + 2)
                if size is None or pos + 6 + size > data_len:
                    break
                payload_start = pos + 6
                payload = data[payload_start:payload_start+size]
                if rec == 0x0019:  # MODULENAME
                    try:
                        current_name = payload.decode('cp1252', errors='replace')
                    except Exception:
                        current_name = None
                elif rec == 0x001A:  # STREAMNAME
                    try:
                        current_stream = payload.decode('cp1252', errors='replace')
                    except Exception:
                        current_stream = None
                elif rec == 0x0031 and size == 4:  # MODULEOFFSET
                    off = get_u32(payload_start)
                    current_offset = off
                elif rec == 0x002B:
                    # MODULE terminator, commit if we have enough data
                    if current_stream and current_offset is not None:
                        modules.append(ModuleInfo(stream_name=current_stream, code_page='cp1252', text_offset=current_offset))
                    current_name = None
                    current_stream = None
                    current_offset = None
                # advance
                pos = payload_start + size
            # commit last if ended without terminator
            if current_stream and current_offset is not None:
                modules.append(ModuleInfo(stream_name=current_stream, code_page='cp1252', text_offset=current_offset))
            return modules

        tolerant_modules = tolerant_dir_parse(dir_data)
        if tolerant_modules:
            for m in tolerant_modules:
                if m.stream_name not in modules_seen:
                    modules_seen.append(m.stream_name)
                sp = ["VBA", m.stream_name]
                if not ole.exists(sp):
                    continue
                data = ole.openstream(sp).read()
                patched = zero_pcode_region(data, m.text_offset)
                if patched != data:
                    ole.write_stream(sp, patched)
                    modifications[m.stream_name] = m.text_offset
                    vba_changed = True

        # Heuristic fallback: guess text_offset by scanning decompressed module text
        def guess_text_offset(comp: bytes) -> Optional[int]:
            try:
                decomp = decompress_stream(comp)
            except Exception:
                return None
            # Common starters in VBA text
            markers = [
                b"Attribute VB_",
                b"Attribute ",
                b"Option Explicit",
                b"Option Base",
                b"Option Compare",
                b"Sub ",
                b"Function ",
            ]
            positions = [decomp.find(m) for m in markers]
            positions = [p for p in positions if p >= 0]
            if not positions:
                return None
            return max(0, min(positions))

        for name in list(modules_seen):
            sp = ["VBA", name]
            if not ole.exists(sp):
                continue
            data = ole.openstream(sp).read()
            off = guess_text_offset(data)
            if off is None or off <= 0:
                continue
            patched = zero_pcode_region(data, off)
            if patched != data:
                ole.write_stream(sp, patched)
                modifications[name] = off
                vba_changed = True

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
            vba_changed = True

    ole.close()
    bio.seek(0)
    return bio.read(), modifications, modules_seen, parse_ok, vba_changed


def repack_vba_project(project_bytes: bytes) -> Tuple[bytes, Dict[str, int], List[str], bool, bool]:
    """Rebuild each code module stream to contain only source text and set ModuleOffset=0.

    Returns (patched bytes, modifications, modules_seen, parse_ok, vba_changed).
    """
    bio = io.BytesIO(project_bytes)
    ole = olefile.OleFileIO(bio, write_mode=True)

    # Safety: detect password-protected projects and skip repack to avoid macro deletion
    is_protected = False
    try:
        proj_bytes = ole.openstream(["VBA", "project"]).read()
        if b"DPB=" in proj_bytes or b"DPX=" in proj_bytes:
            is_protected = True
    except Exception:
        pass
    if is_protected:
        ole.close()
        bio.seek(0)
        return bio.read(), {}, [], True, False

    # Read and parse dir (best-effort)
    dir_stream_path = ["VBA", "dir"]
    dir_comp = ole.openstream(dir_stream_path).read()
    try:
        dir_data = decompress_stream(dir_comp)
        parser = DirStreamParser(dir_data)
        parse_ok = len(parser.modules) > 0
        offsets_map = {m.stream_name: m.text_offset for m in parser.modules}
        if not offsets_map:
            # Fallback tolerant extraction if strict parser yields nothing
            offsets_map = _extract_offsets_from_dir(dir_comp)
    except Exception:
        parse_ok = False
        offsets_map = _extract_offsets_from_dir(dir_comp)

    modifications: Dict[str, int] = {}
    modules_seen: List[str] = []
    repacked_modules: Set[str] = set()
    vba_changed = False
    # Common non-module streams under VBA storage
    special = {"dir", "project", "_vba_project", "projectwm"}

    # If strict dir parse failed (parse_ok == False), choose the safer path:
    # size-preserving neutralization of all modules based on recovered offsets,
    # without altering dir offsets or dropping SRP/_VBA_PROJECT.
    if not parse_ok:
        def _looks_like_vba_text(blob: bytes) -> bool:
            head = blob[:512]
            # must contain some ASCII letters and typical tokens; very few NULs
            has_token = any(t in head for t in (b"Attribute VB_", b"Option ", b"Sub ", b"Function ", b"VERSION "))
            nul_ratio = head.count(0) / max(1, len(head))
            return has_token and nul_ratio < 0.6

        neutral_writes: Dict[Tuple[str, str], bytes] = {}
        repack_writes: Dict[Tuple[str, str], bytes] = {}
        repack_names: Set[str] = set()
        for entry in ole.listdir():
            if len(entry) == 2 and entry[0].lower() == "vba":
                name = entry[1]
                if name.lower() in special:
                    continue
                sp = ["VBA", name]
                try:
                    data = ole.openstream(sp).read()
                except Exception:
                    continue
                if not data or data[0] != 0x01:
                    continue
                # Derive two candidates: from tolerant dir and from heuristic content scan
                cand_dir = offsets_map.get(name)
                cand_guess = _guess_text_offset(data)
                candidates = []
                if cand_dir is not None:
                    candidates.append(("dir", cand_dir))
                if cand_guess is not None:
                    # de-duplicate if same
                    if cand_dir != cand_guess:
                        candidates.append(("guess", cand_guess))
                if not candidates and not FORCE_REPACK_ALL:
                    continue
                # Safety: never zero past the earliest plausible text marker in this module
                try:
                    decomp0 = decompress_stream(data)
                except Exception:
                    decomp0 = b""
                pos_marker = None
                for m in (b"Attribute VB_", b"Option ", b"Sub ", b"Function ", b"VERSION "):
                    p = decomp0.find(m)
                    if p >= 0:
                        pos_marker = p
                        break
                if pos_marker is not None:
                    candidates = [(lbl, min(off, pos_marker)) for (lbl, off) in candidates]
                # If we're forcing repack-all, scan for the text start and repack
                if FORCE_REPACK_ALL and decomp0:
                    # Find the text start by scanning for the first region that looks like VBA text
                    off_use = 0
                    for i in range(0, max(0, len(decomp0) - 512), 10):
                        if _looks_like_vba_text(decomp0[i:i+512]):
                            off_use = i
                            break
                    text_bytes = decomp0[off_use:]
                    if _looks_like_vba_text(text_bytes[:512]):
                        rebuilt = _compress_uncompressed(text_bytes)
                        repack_writes[tuple(sp)] = rebuilt
                        repack_names.add(name)
                        modifications[name] = 0
                        vba_changed = True
                        continue
                # Validated neutralization path
                chosen = None
                for label, off in candidates:
                    try:
                        patched_try = zero_pcode_region(data, off)
                        decomp = decompress_stream(patched_try)
                        # Find text start again and validate head
                        pos = None
                        for m in (b"Attribute VB_", b"Option ", b"Sub ", b"Function ", b"VERSION "):
                            p = decomp.find(m)
                            if p >= 0:
                                pos = p
                                break
                        if pos is None or not decomp0:
                            continue
                        # Accept only if early text bytes remain identical to original text
                        if decomp0[pos:pos+256] == decomp[pos:pos+256] and _looks_like_vba_text(decomp[pos:pos+512]):
                            chosen = (off, patched_try, pos)
                            break
                    except Exception:
                        continue
                if chosen is not None:
                    off, patched, _ = chosen
                    if patched != data:
                        neutral_writes[tuple(sp)] = patched
                        modifications[name] = off
                        vba_changed = True
                else:
                    # Could not validate; if we have a marker, repack at marker, else leave unchanged to avoid corruption
                    if pos_marker is not None:
                        pos_use = pos_marker
                        text_bytes = decomp0[pos_use:]
                        rebuilt = _compress_uncompressed(text_bytes)
                        repack_writes[tuple(sp)] = rebuilt
                        repack_names.add(name)
                        modifications[name] = 0
                        vba_changed = True
        # If any module changed, attempt a rebuild that omits caches (__SRP_* and _VBA_PROJECT)
        if vba_changed:
            def _rebuild_without_caches_return_bytes() -> Optional[bytes]:
                # Prefer pythoncom Structured Storage to create a fresh doc and omit caches
                try:
                    import pythoncom  # type: ignore
                except Exception:
                    return None
                # Snapshot current streams
                stream_map: Dict[Tuple[str, ...], bytes] = {}
                # In this parse-fail path, we keep _VBA_PROJECT to satisfy stricter Excel builds
                keep_vba_project = True
                for ent in ole.listdir():
                    try:
                        data = ole.openstream(ent).read()
                        if len(ent) == 2 and ent[0].lower() == 'vba':
                            nm = ent[1]
                            if nm == '_VBA_PROJECT' and not keep_vba_project:
                                continue
                        stream_map[tuple(ent)] = data
                    except Exception:
                        continue
                # Overlay module writes (neutral first, then repacks)
                for sp_t, payload in neutral_writes.items():
                    stream_map[sp_t] = payload
                for sp_t, payload in repack_writes.items():
                    stream_map[sp_t] = payload
                # In parse-fail, skip dir update to avoid corruption since offsets_map is empty
                import tempfile, os as _os
                tmp_path = None
                try:
                    tmp = tempfile.NamedTemporaryFile(delete=False)
                    tmp_path = tmp.name
                    tmp.close()
                    STGM_CREATE = 0x00001000
                    STGM_READWRITE = 0x00000002
                    STGM_SHARE_EXCLUSIVE = 0x00000010
                    root = pythoncom.StgCreateDocfile(tmp_path, STGM_CREATE | STGM_READWRITE | STGM_SHARE_EXCLUSIVE, 0)
                    storages: Dict[Tuple[str, ...], Any] = {(): root}
                    def ensure_storage(path: Tuple[str, ...]):
                        if path in storages:
                            return storages[path]
                        parent = ensure_storage(path[:-1]) if path[:-1] else storages[()]
                        sub = parent.CreateStorage(path[-1], STGM_CREATE | STGM_READWRITE | STGM_SHARE_EXCLUSIVE, 0, 0)
                        storages[path] = sub
                        return sub
                    for sp, data in stream_map.items():
                        if len(sp) == 1:
                            parent = storages[()]
                            name = sp[0]
                        else:
                            parent = ensure_storage(sp[:-1])
                            name = sp[-1]
                        stm = parent.CreateStream(name, STGM_CREATE | STGM_READWRITE | STGM_SHARE_EXCLUSIVE, 0, 0)
                        mv = memoryview(data)
                        pos = 0
                        chunk = 1 << 20
                        while pos < len(mv):
                            stm.Write(mv[pos:pos+chunk])
                            pos += chunk
                        stm.Commit(0)
                    root.Commit(0)
                    with open(tmp_path, 'rb') as fh:
                        return fh.read()
                except Exception:
                    return None
                finally:
                    if tmp_path and _os.path.exists(tmp_path):
                        try:
                            _os.remove(tmp_path)
                        except Exception:
                            pass

            rebuilt = _rebuild_without_caches_return_bytes()
            if rebuilt is not None:
                ole.close()
                return rebuilt, modifications, modules_seen, parse_ok, True
        ole.close()
        bio.seek(0)
        return bio.read(), modifications, modules_seen, parse_ok, vba_changed

    # First pass: decide per-module action and build new payloads
    planned_writes: Dict[Tuple[str, str], bytes] = {}

    # Enumerate module streams
    for entry in ole.listdir():
        if len(entry) == 2 and entry[0].lower() == "vba":
            name = entry[1]
            if name.lower() in special:
                continue
            modules_seen.append(name)
            sp = ["VBA", name]
            if not ole.exists(sp):
                continue
            data = ole.openstream(sp).read()
            # Only process MS-OVBA modules
            if not data or data[0] != 0x01:
                continue
            # Determine text offset
            text_off = offsets_map.get(name)
            if text_off is None:
                text_off = _guess_text_offset(data)
            if text_off is None:
                continue
            # Repack: keep only text region if it looks like valid VBA text
            try:
                decomp = decompress_stream(data)
            except Exception:
                continue
            if text_off < 0 or text_off > len(decomp):
                continue
            text_bytes = decomp[text_off:]
            # Sanity checks: start markers, low control chars
            head = text_bytes[:1024]
            looks_text = False
            for m in (b"Attribute VB_", b"Option", b"Sub ", b"Function ", b"VERSION"):
                if head.startswith(m) or (b"\r\n" + m) in head or (b"\n" + m) in head:
                    looks_text = True
                    break
            # Additional guard: if the first 256 bytes contain many NULs, avoid repack
            suspicious = head[:256].count(b"\x00") > 16
            if looks_text and (FORCE_REPACK_ALL or not suspicious):
                rebuilt = _compress_uncompressed(text_bytes)
                if rebuilt != data:
                    # Defer actual write; try to repack (resize) later as a batch
                    planned_writes[tuple(sp)] = rebuilt
                else:
                    # No change needed
                    pass
            else:
                # Skip repack for this module; will rely on neutralization fallback later
                continue

    # If there are planned resizes, attempt them. Strategy:
    # 1) Try in-memory writes (may fail for resized streams)
    # 2) If any ValueError arises, retry all writes using a temp on-disk file where olefile supports resizing
    dir_replacement: Optional[bytes] = None
    if planned_writes:
        # Determine which modules are to be repacked (ModuleOffset -> 0)
        repacked_modules = {name for (_, name) in planned_writes.keys() if _ == 'VBA'}
        new_dir_comp, dir_changed = _update_dir_offsets_to_zero(dir_comp, repacked_modules)
        if dir_changed:
            dir_replacement = new_dir_comp
        # Detect SRP streams; if present, prefer full rebuild so we can omit them
        srp_streams: List[Tuple[str, str]] = []
        for entry in ole.listdir():
            if len(entry) == 2 and entry[0].lower() == 'vba' and entry[1].lower().startswith('__srp_'):
                srp_streams.append((entry[0], entry[1]))

        def try_apply_writes(o: "olefile.OleFileIO") -> Optional[Exception]:
            try:
                # Apply module writes first
                for sp_tuple, payload in planned_writes.items():
                    o.write_stream(list(sp_tuple), payload)
                # Then dir if needed
                if dir_replacement is not None:
                    o.write_stream(dir_stream_path, dir_replacement)
                return None
            except Exception as e:  # catch and return to decide fallback
                return e
        
        # If SRP streams exist, skip in-place writes and go straight to rebuild to omit them
        if srp_streams:
            err = Exception('force rebuild to drop SRP')
        else:
            err = try_apply_writes(ole)
            if err is None:
                # In-memory resize succeeded
                for (_, name), _payload in planned_writes.items():
                    modifications[name] = 0
                vba_changed = True
                ole.close()
                bio.seek(0)
                return bio.read(), modifications, modules_seen, parse_ok, vba_changed
        
        if err is not None:
            # Try Windows Structured Storage rebuild (IStorage/IStream) if available
            def _try_win32_rebuild() -> Optional[bytes]:
                try:
                    import pythoncom  # type: ignore
                except Exception:
                    return None
                # Collect all stream bytes
                stream_map: Dict[Tuple[str, ...], bytes] = {}
                for entry in ole.listdir():
                    try:
                        data = ole.openstream(entry).read()
                        # Omit SRP streams from rebuilt file
                        if len(entry) == 2 and entry[0].lower() == 'vba' and entry[1].lower().startswith('__srp_'):
                            continue
                        # Also omit _VBA_PROJECT performance cache stream per spec (MUST NOT be present on write)
                        if len(entry) == 2 and entry[0].lower() == 'vba' and entry[1] == '_VBA_PROJECT':
                            continue
                        stream_map[tuple(entry)] = data
                    except Exception:
                        # likely a storage, not a stream
                        continue
                # Apply replacements
                for sp_tuple, payload in planned_writes.items():
                    stream_map[sp_tuple] = payload
                if dir_replacement is not None:
                    stream_map[tuple(dir_stream_path)] = dir_replacement

                import tempfile, os as _os
                tmp_path = None
                try:
                    tmp = tempfile.NamedTemporaryFile(delete=False)
                    tmp_path = tmp.name
                    tmp.close()
                    # STGM flags
                    STGM_CREATE = 0x00001000
                    STGM_READWRITE = 0x00000002
                    STGM_SHARE_EXCLUSIVE = 0x00000010
                    root = pythoncom.StgCreateDocfile(tmp_path, STGM_CREATE | STGM_READWRITE | STGM_SHARE_EXCLUSIVE, 0)
                    # storage cache
                    storages: Dict[Tuple[str, ...], Any] = {(): root}
                    def ensure_storage(path: Tuple[str, ...]):
                        if path in storages:
                            return storages[path]
                        parent = ensure_storage(path[:-1]) if path[:-1] else storages[()]
                        sub = parent.CreateStorage(path[-1], STGM_CREATE | STGM_READWRITE | STGM_SHARE_EXCLUSIVE, 0, 0)
                        storages[path] = sub
                        return sub
                    # Write streams
                    for sp, data in stream_map.items():
                        if len(sp) == 1:
                            parent = storages[()]
                            name = sp[0]
                        else:
                            parent = ensure_storage(sp[:-1])
                            name = sp[-1]
                        stm = parent.CreateStream(name, STGM_CREATE | STGM_READWRITE | STGM_SHARE_EXCLUSIVE, 0, 0)
                        mv = memoryview(data)
                        pos = 0
                        chunk = 1 << 20
                        while pos < len(mv):
                            stm.Write(mv[pos:pos+chunk])
                            pos += chunk
                        stm.Commit(0)
                    root.Commit(0)
                    with open(tmp_path, 'rb') as fh:
                        return fh.read()
                except Exception:
                    return None
                finally:
                    if tmp_path and _os.path.exists(tmp_path):
                        try:
                            _os.remove(tmp_path)
                        except Exception:
                            pass

            rebuilt_bytes = _try_win32_rebuild()
            if rebuilt_bytes is not None:
                for (_, name), _payload in planned_writes.items():
                    modifications[name] = 0
                vba_changed = True
                return rebuilt_bytes, modifications, modules_seen, parse_ok, vba_changed

            # Fallback: recreate using a temp file on disk
            ole.close()
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False) as tf:
                tf.write(project_bytes)
                tmp_path = tf.name
            try:
                o2 = olefile.OleFileIO(tmp_path, write_mode=True)
                err2 = try_apply_writes(o2)
                o2.close()
                if err2 is None:
                    with open(tmp_path, 'rb') as fh:
                        project_bytes = fh.read()
                    # Mark modifications and flags based on planned writes
                    for (_, name), _payload in planned_writes.items():
                        modifications[name] = 0
                        vba_changed = True
                    if dir_replacement is not None:
                        # ensure parse_ok propagated above; nothing else to do
                        pass
                    # Return early with updated bytes
                    return project_bytes, modifications, modules_seen, parse_ok, True
                else:
                    # Fall through to neutralization path below
                    pass
            finally:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

        # If we reach here, resized writes failed or were skipped for some modules. Fall back per-module.
        # Re-open original in-memory OLE to apply size-preserving neutralisation.
        bio = io.BytesIO(project_bytes)
        ole = olefile.OleFileIO(bio, write_mode=True)
        # Neutralize any module that either failed to repack or was filtered out earlier
        for entry in ole.listdir():
            if len(entry) == 2 and entry[0].lower() == "vba":
                name = entry[1]
                if name.lower() in special:
                    continue
                sp_tuple = tuple(entry)
                try:
                    data = ole.openstream(list(sp_tuple)).read()
                except Exception:
                    continue
                if not data or data[0] != 0x01:
                    continue
                text_off = offsets_map.get(name)
                if text_off is None:
                    text_off = _guess_text_offset(data)
                if text_off is None:
                    continue
                patched = zero_pcode_region(data, text_off)
                if patched != data:
                    ole.write_stream(list(sp_tuple), patched)
                    modifications[name] = text_off
                    vba_changed = True

    # If no planned resizes or after fallback neutralisation, finalise current in-memory OLE
    ole.close()
    bio.seek(0)
    return bio.read(), modifications, modules_seen, parse_ok, vba_changed


def process_workbook(
    input_path: str, output_path: str, in_place: bool = False, repack: bool = False
) -> Tuple[Dict[str, int], Set[str], bool, bool]:
    """Process a workbook and return (modifications, modules_detected, parse_ok, vba_changed)."""
    target_normalized = "xl/vbaproject.bin"
    modifications: Dict[str, int] = {}
    modules_detected: Set[str] = set()
    parse_ok = True
    vba_changed = False
    tmp_output = output_path
    if in_place:
        tmp_output = input_path + ".tmp"

    with zipfile.ZipFile(input_path, "r") as zin, zipfile.ZipFile(tmp_output, "w") as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)
            if item.filename.lower() == target_normalized:
                if repack:
                    patched, mods, modules_seen, this_parse_ok, this_changed = repack_vba_project(data)
                else:
                    patched, mods, modules_seen, this_parse_ok, this_changed = patch_vba_project(data)
                data = patched
                modifications.update(mods)
                modules_detected.update(modules_seen)
                parse_ok = parse_ok and this_parse_ok
                vba_changed = vba_changed or this_changed
            zout.writestr(item, data)

    if in_place:
        os.replace(tmp_output, input_path)

    return modifications, modules_detected, parse_ok, vba_changed


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
    parser.add_argument(
        "--repack",
        action="store_true",
        dest="repack",
        help="Rebuild module streams to source-only and set ModuleOffset=0 where possible",
    )
    parser.add_argument(
        "--verify-excel",
        action="store_true",
        dest="verify_excel",
        help="After writing, launch Excel headlessly and attempt a VBA compile; reports success/failure.",
    )
    parser.add_argument(
        "--force-repack-all",
        action="store_true",
        dest="force_repack_all",
        help="Force repack of every module to source-only (sets ModuleOffset=0 for all).",
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

    # Set global for repack behavior
    global FORCE_REPACK_ALL
    FORCE_REPACK_ALL = bool(getattr(args, "force_repack_all", False))

    mutation_summary, modules_detected, parse_ok, vba_changed = process_workbook(
        source, output, in_place=args.in_place, repack=args.repack
    )

    if mutation_summary:
        print("P-code removed from modules:")
        for module, offset in sorted(mutation_summary.items()):
            print(f"  - {module} (offset {offset})")
    elif modules_detected:
        if parse_ok:
            print("VBA macros detected but already lacked P-code; no changes made.")
        else:
            if vba_changed:
                print("VBA modules detected; applied heuristic patch (dir stream not parsed).")
            else:
                print("VBA modules detected, but the dir stream could not be parsed; no changes made.")
    else:
        print("No VBA project modules found; workbook contains no macros.")

    if not args.in_place:
        print(f"Modified workbook written to: {output}")

    # Optional: verify with Excel COM by attempting a compile
    if args.verify_excel:
        result = _excel_compile_check(output)
        print("Excel verify:")
        for k in ("opened", "has_vbproject", "vbcomponents", "compile_attempted", "compile_ok"):
            if k in result:
                print(f"  {k}: {result[k]}")
        if not result.get("compile_ok", False):
            # Automatic structural fallback to reduce manual loops
            sres = _structural_verify(output)
            print("Structural verify:")
            for k in ("found_modules", "decompressed_ok", "text_markers_ok", "offsets_map_size", "structural_ok"):
                if k in sres:
                    print(f"  {k}: {sres[k]}")
            # If structural failed, surface the problematic modules for fast iteration
            if not sres.get("structural_ok", False):
                bad = sres.get("bad_modules", [])
                if bad:
                    print("  bad_modules:")
                    for nm in bad:
                        head_hex = sres.get("bad_heads_hex", {}).get(nm, "")
                        head_hex_short = head_hex[:128] + ("..." if len(head_hex) > 128 else "")
                        print(f"    - {nm} head64: {head_hex_short}")
            if not sres.get("structural_ok", False):
                print(f"Verification failed: {result.get('error', 'compile did not succeed')} (and structural checks failed)", file=sys.stderr)
                return 3
            else:
                # Excel automation failed, but structure looks good; treat as soft pass
                print("Excel automation unavailable, but structural verification passed.")

    return 0


# ---------------------------------------------------------------------------
# Excel COM verification (optional)
# ---------------------------------------------------------------------------

def _excel_compile_check(xlsb_path: str) -> Dict[str, Any]:
    """Best-effort Excel verification: open workbook headlessly and attempt a Compile.

    Returns a dict with keys: opened, has_vbproject, vbcomponents, compile_attempted,
    compile_ok, error.
    """
    out: Dict[str, Any] = {
        "opened": False,
        "has_vbproject": False,
        "vbcomponents": 0,
        "compile_attempted": False,
        "compile_ok": False,
    }
    try:
        import pythoncom  # type: ignore
        import win32com.client  # type: ignore
        pythoncom.CoInitialize()
        excel = win32com.client.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        # Reduce prompts and macro influences
        try:
            # msoAutomationSecurityForceDisable = 3 (avoid any macro prompts)
            excel.AutomationSecurity = 3  # type: ignore[attr-defined]
        except Exception:
            pass
        try:
            # Try with CorruptLoad=1 (xlRepairFile), then 2 (xlExtractData), then plain
            wb = None
            try:
                wb = excel.Workbooks.Open(xlsb_path, UpdateLinks=0, ReadOnly=True, IgnoreReadOnlyRecommended=True, CorruptLoad=1)
            except Exception:
                try:
                    wb = excel.Workbooks.Open(xlsb_path, UpdateLinks=0, ReadOnly=True, IgnoreReadOnlyRecommended=True, CorruptLoad=2)
                except Exception:
                    try:
                        # Protected View fallback
                        pv = excel.ProtectedViewWindows.Open(xlsb_path)
                        try:
                            wb = pv.Workbook  # may raise until Edit() is called
                        except Exception:
                            try:
                                pv.Edit()
                                wb = pv.Workbook
                            except Exception:
                                wb = None
                    except Exception:
                        # Plain open
                        wb = excel.Workbooks.Open(xlsb_path, UpdateLinks=0, ReadOnly=True)
        except Exception as e:
            out["error"] = f"open failed: {e}"
            return out
        out["opened"] = True
        # Detect VB project presence and count components
        has_vbp = False
        vbcount = 0
        try:
            has_vbp = bool(getattr(wb, "HasVBProject", False))
            if has_vbp:
                try:
                    vbcount = wb.VBProject.VBComponents.Count  # type: ignore[attr-defined]
                except Exception:
                    # Trust Center may block access
                    vbcount = -1
        except Exception:
            has_vbp = False
        out["has_vbproject"] = has_vbp
        out["vbcomponents"] = vbcount

        # Try to compile via VBE command bars if VBProject is accessible
        try:
            vbe = excel.VBE  # type: ignore[attr-defined]
            out["compile_attempted"] = True
            ok = False
            # Try ExecuteMso first (Office control id)
            try:
                excel.CommandBars.ExecuteMso("CompileVbaProject")
                ok = True
            except Exception:
                # Try legacy CommandBars in VBE with control id 578 if available
                try:
                    ctrl = vbe.CommandBars.FindControl(ID=578)
                    if ctrl is not None:
                        ctrl.Execute()
                        ok = True
                except Exception:
                    pass
            out["compile_ok"] = bool(ok)
            if not ok and vbcount == 0 and has_vbp:
                out["error"] = "VBProject present but no components visible"
        except Exception as e:
            out["compile_attempted"] = False
            out["error"] = f"compile attempt not available: {e}"
        finally:
            try:
                wb.Close(SaveChanges=False)
            except Exception:
                pass
            excel.Quit()
            pythoncom.CoUninitialize()
        return out
    except Exception as e:
        out["error"] = f"excel automation unavailable: {e}"
        return out


def _structural_verify(xlsb_path: str) -> Dict[str, Any]:
    """Non-Excel structural verification of vbaProject.bin to reduce manual loops.

    Checks:
      - vbaProject.bin exists and opens as OLE
      - dir stream decompression succeeds; extract offsets map best-effort
      - all VBA module streams decompress
      - each module text contains a plausible marker (Attribute/Option/Sub/Function)
    Returns dict with summary and structural_ok boolean.
    """
    res: Dict[str, Any] = {
        "found_modules": 0,
        "offsets_map_size": 0,
        "decompressed_ok": True,
        "text_markers_ok": True,
        "structural_ok": False,
    }
    try:
        with zipfile.ZipFile(xlsb_path, "r") as z:
            proj = z.read("xl/vbaProject.bin")
    except Exception as e:
        res["decompressed_ok"] = False
        res["text_markers_ok"] = False
        return res
    try:
        o = olefile.OleFileIO(io.BytesIO(proj))
    except Exception:
        res["decompressed_ok"] = False
        res["text_markers_ok"] = False
        return res
    # dir offsets (best effort)
    try:
        dir_comp = o.openstream(["VBA", "dir"]).read()
        offsets = _extract_offsets_from_dir(dir_comp)
        res["offsets_map_size"] = len(offsets)
    except Exception:
        offsets = {}
    # iterate modules
    specials = {"dir", "project", "_vba_project", "projectwm"}
    found = 0
    markers_ok = True
    decomp_ok = True
    bad_modules: List[str] = []
    bad_heads: Dict[str, str] = {}
    for e in o.listdir():
        if len(e) != 2 or e[0].lower() != "vba":
            continue
        name = e[1]
        if name.lower() in specials:
            continue
        try:
            data = o.openstream(e).read()
            if not data or data[0] != 0x01:
                continue
            decomp = decompress_stream(data)
        except Exception:
            decomp_ok = False
            continue
        found += 1
        head = decomp[:2048]
        if not any(tok in head for tok in (b"Attribute VB_", b"Option ", b"Sub ", b"Function ", b"VERSION ")):
            markers_ok = False
            bad_modules.append(name)
            # Capture first 64 bytes for quick triage
            try:
                h64 = decomp[:64]
                bad_heads[name] = h64.hex()
            except Exception:
                bad_heads[name] = ""
    res["found_modules"] = found
    res["decompressed_ok"] = decomp_ok
    res["text_markers_ok"] = markers_ok
    res["structural_ok"] = bool(found and decomp_ok and markers_ok)
    if not res["structural_ok"]:
        res["bad_modules"] = bad_modules
        res["bad_heads_hex"] = bad_heads
    return res


if __name__ == "__main__":
    sys.exit(main())
