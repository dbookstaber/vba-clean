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
    markers = [
        b"Attribute VB_",
        b"Attribute ",
        b"Option Explicit",
        b"Option Base",
        b"Option Compare",
        b"Sub ",
        b"Function ",
    ]
    pos = [decomp.find(m) for m in markers]
    pos = [p for p in pos if p >= 0]
    return min(pos) if pos else None


def _update_dir_offsets_to_zero(dir_comp: bytes, module_names: Set[str]) -> Tuple[bytes, bool]:
    """Set ModuleOffset to 0 for the specified module stream names in the dir stream.

    Returns (new_dir_comp, updated) where updated indicates if any change was applied.
    Tolerant parser: walks records, tracks current stream name, overwrites 0x0031 payloads.
    """
    try:
        data = bytearray(decompress_stream(dir_comp))
    except Exception:
        return dir_comp, False

    # Discover codepage first (PROJECTCODEPAGE id=0x0003, size=2)
    def get_u16(p: int) -> Optional[int]:
        if p + 2 > len(data):
            return None
        return struct.unpack_from('<H', data, p)[0]

    def get_u32(p: int) -> Optional[int]:
        if p + 4 > len(data):
            return None
        return struct.unpack_from('<L', data, p)[0]

    # Pass 1: read codepage and build minimal state
    pos = 0
    codepage = 'cp1252'
    while pos + 6 <= len(data):
        rec = get_u16(pos)
        if rec is None:
            break
        size = get_u32(pos + 2)
        if size is None or pos + 6 + size > len(data):
            break
        payload_start = pos + 6
        if rec == 0x0003 and size == 2:
            cp_value = get_u16(payload_start)
            if cp_value:
                for cand in (f"cp{cp_value}", f"windows-{cp_value}"):
                    try:
                        ''.encode(cand)
                    except LookupError:
                        continue
                    else:
                        codepage = cand
                        break
        pos = payload_start + size

    # Pass 2: overwrite MODULEOFFSET for matching stream names
    pos = 0
    current_stream: Optional[str] = None
    updated = False
    while pos + 6 <= len(data):
        rec = get_u16(pos)
        if rec is None:
            break
        size = get_u32(pos + 2)
        if size is None or pos + 6 + size > len(data):
            break
        payload_start = pos + 6
        payload_end = payload_start + size
        if rec == 0x0019:  # MODULENAME (bytes, but stream name follows)
            pass  # skip; we only care about STREAMNAME
        elif rec == 0x001A:  # STREAMNAME
            raw = bytes(data[payload_start:payload_end])
            try:
                current_stream = raw.decode(codepage, errors='replace')
            except Exception:
                current_stream = None
        elif rec == 0x0031 and size == 4:  # MODULEOFFSET
            if current_stream and current_stream in module_names:
                # Write 4 zero bytes
                data[payload_start:payload_end] = b"\x00\x00\x00\x00"
                updated = True
        # advance
        pos = payload_end

    if not updated:
        return dir_comp, False
    return _compress_uncompressed(bytes(data)), True

def patch_vba_project(project_bytes: bytes) -> Tuple[bytes, Dict[str, int], List[str], bool, bool]:
    """Return (patched vbaProject.bin, modifications, module names, parse_ok, vba_changed)."""
    bio = io.BytesIO(project_bytes)
    ole = olefile.OleFileIO(bio, write_mode=True)

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

    # Read and parse dir (best-effort)
    dir_stream_path = ["VBA", "dir"]
    dir_comp = ole.openstream(dir_stream_path).read()
    try:
        dir_data = decompress_stream(dir_comp)
        parser = DirStreamParser(dir_data)
        parse_ok = len(parser.modules) > 0
        offsets_map = {m.stream_name: m.text_offset for m in parser.modules}
    except Exception:
        parse_ok = False
        offsets_map = {}

    modifications: Dict[str, int] = {}
    modules_seen: List[str] = []
    repacked_modules: Set[str] = set()
    vba_changed = False

    # First pass: decide per-module action and build new payloads
    planned_writes: Dict[Tuple[str, str], bytes] = {}

    # Enumerate module streams
    special = {"dir", "project", "_vba_project", "projectwm"}
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
            # Repack: keep only text region
            try:
                decomp = decompress_stream(data)
            except Exception:
                continue
            if text_off < 0 or text_off > len(decomp):
                continue
            text_bytes = decomp[text_off:]
            rebuilt = _compress_uncompressed(text_bytes)
            if rebuilt != data:
                # Defer actual write; try to repack (resize) later as a batch
                planned_writes[tuple(sp)] = rebuilt
            else:
                # No change needed
                pass

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

        err = try_apply_writes(ole)
        if err is None:
            # In-memory resize succeeded
            for (_, name), _payload in planned_writes.items():
                modifications[name] = 0
            vba_changed = True
            ole.close()
            bio.seek(0)
            return bio.read(), modifications, modules_seen, parse_ok, vba_changed
        else:
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

        # If we reach here, resized writes failed both in-memory and on-disk. Fall back per-module.
        # Re-open original in-memory OLE to apply size-preserving neutralisation.
        bio = io.BytesIO(project_bytes)
        ole = olefile.OleFileIO(bio, write_mode=True)
        for sp_tuple, _payload in planned_writes.items():
            name = sp_tuple[1]
            sp = list(sp_tuple)
            try:
                data = ole.openstream(sp).read()
            except Exception:
                continue
            text_off = offsets_map.get(name)
            if text_off is None:
                text_off = _guess_text_offset(data)
            if text_off is None:
                continue
            patched = zero_pcode_region(data, text_off)
            if patched != data:
                ole.write_stream(sp, patched)
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

    return 0


if __name__ == "__main__":
    sys.exit(main())
