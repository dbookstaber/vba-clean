"""
Core unit tests for tolerant decompression/patching and repack wiring.
"""

import io
import os
import struct
import tempfile
import zipfile
import unittest
from unittest.mock import patch

import vba_clean


def _mk_uncompressed_container(payload: bytes, signature_bits: int = 0) -> bytes:
    # Build 0x01 + one uncompressed chunk header + payload
    # chunk header: low12 = len-1; bits 12..14 = signature_bits; bit15=0
    L = len(payload)
    low12 = (L - 1) & 0x0FFF
    header = ((signature_bits & 0x7) << 12) | low12
    return b"\x01" + struct.pack("<H", header) + payload


class TestTolerantDecompression(unittest.TestCase):
    def test_decompress_short_final_uncompressed_chunk(self):
        data = b"HELLO-WORLD"
        cont = _mk_uncompressed_container(data, signature_bits=0)  # non 0b011
        out = vba_clean.decompress_stream(cont)
        self.assertEqual(out, data)

    def test_zero_pcode_region_on_short_uncompressed(self):
        data = (b"A" * 8) + (b"B" * 8)
        cont = _mk_uncompressed_container(data, signature_bits=0)
        # Zero first 10 bytes of decompressed content
        patched = vba_clean.zero_pcode_region(cont, 10)
        self.assertNotEqual(patched, cont)
        decomp = vba_clean.decompress_stream(patched)
        self.assertEqual(decomp[:10], b"\x00" * 10)
        self.assertEqual(decomp[10:], data[10:])


class TestDirUpdate(unittest.TestCase):
    def test_update_dir_offsets_to_zero(self):
        # Build a tiny dir decompressed stream with STREAMNAME (0x001A) and MODULEOFFSET (0x0031)
        def rec(rec_id: int, payload: bytes) -> bytes:
            return struct.pack("<H", rec_id) + struct.pack("<L", len(payload)) + payload
        stream_name = b"Module1"
        module_offset_payload = struct.pack("<L", 1234)
        decomp_dir = rec(0x001A, stream_name) + rec(0x0031, module_offset_payload)
        comp_dir = vba_clean._compress_uncompressed(decomp_dir)
        new_comp, changed = vba_clean._update_dir_offsets_to_zero(comp_dir, {"Module1"})
        self.assertTrue(changed)
        new_decomp = vba_clean.decompress_stream(new_comp)
        # Parse back module offset payload and verify zeroed
        # Skip 0x001A record
        p = 0
        rid = struct.unpack_from("<H", new_decomp, p)[0]; p += 2
        size = struct.unpack_from("<L", new_decomp, p)[0]; p += 4 + size
        # Next record should be 0x0031
        rid2 = struct.unpack_from("<H", new_decomp, p)[0]; p += 2
        self.assertEqual(rid2, 0x0031)
        size2 = struct.unpack_from("<L", new_decomp, p)[0]; p += 4
        self.assertEqual(size2, 4)
        off = struct.unpack_from("<L", new_decomp, p)[0]
        self.assertEqual(off, 0)


class TestProcessWorkbook(unittest.TestCase):
    def test_cli_repack_flag_parsing(self):
        with patch("vba_clean.os.path.exists", return_value=True), patch(
            "vba_clean.os.path.isdir", return_value=False
        ), patch(
            "vba_clean.create_predecompile_backup"
        ) as mock_backup, patch(
            "vba_clean.process_workbook", return_value=({"M": 0}, {"M"}, True, True)
        ) as mock_proc:
            vba_clean.main(["--repack", "sample.xlsb"])
            args, kwargs = mock_proc.call_args
            self.assertTrue(kwargs.get("repack"), "--repack was not propagated")

    def test_process_workbook_calls_repack(self):
        # Create a temp zip with xl/vbaProject.bin so process_workbook can run
        with tempfile.TemporaryDirectory() as td:
            src = os.path.join(td, "in.xlsb")
            dst = os.path.join(td, "out.xlsb")
            # minimal vbaProject.bin content
            with zipfile.ZipFile(src, "w") as z:
                z.writestr("xl/vbaProject.bin", b"dummy")
                z.writestr("[Content_Types].xml", "<Types/>")
            with patch("vba_clean.repack_vba_project", return_value=(b"dummy", {"M": 0}, ["M"], False, True)) as mock_rep:
                mods, modules_detected, parse_ok, changed = vba_clean.process_workbook(src, dst, repack=True)
                self.assertIn("M", mods)
                self.assertTrue(changed)
                mock_rep.assert_called()


class TestRepackFallbacks(unittest.TestCase):
    def _build_streams(self):
        # Build a dir stream with MODULENAME/STREAMNAME 'M' and MODULEOFFSET=10, then MODULE terminator
        def rec(rid: int, payload: bytes) -> bytes:
            return struct.pack("<H", rid) + struct.pack("<L", len(payload)) + payload
        decomp_dir = (
            rec(0x0019, b"M") +
            rec(0x001A, b"M") +
            rec(0x0031, struct.pack("<L", 10)) +
            rec(0x002B, b"")
        )
        dir_comp = vba_clean._compress_uncompressed(decomp_dir)
        # Build module M: 10 bytes of pretext + source text
        mod_decomp = b"P" * 10 + b"Option Explicit\nSub X(): End Sub\n"
        mod_comp = vba_clean._compress_uncompressed(mod_decomp)
        return {(
            ("VBA", "dir")): dir_comp,
            ("VBA", "M"): mod_comp,
        }

    def test_repack_in_memory_resize_sets_offset_zero(self):
        streams = self._build_streams()

        class FakeStream:
            def __init__(self, data: bytes):
                self._data = data
            def read(self):
                return self._data

        class FakeOle:
            def __init__(self, _src, write_mode=False):
                # shallow copy per instance
                self.streams = dict(streams)
            def openstream(self, pathlist):
                return FakeStream(self.streams[tuple(pathlist)])
            def write_stream(self, pathlist, payload: bytes):
                # allow resized writes to simulate success path
                self.streams[tuple(pathlist)] = payload
            def listdir(self):
                return [list(k) for k in self.streams.keys()]
            def exists(self, pathlist):
                return tuple(pathlist) in self.streams
            def close(self):
                pass

        with patch.object(vba_clean, "olefile") as mock_olemod:
            mock_olemod.OleFileIO = FakeOle
            new_bytes, mods, modules, parse_ok, changed = vba_clean.repack_vba_project(b"ignored")
            self.assertTrue(changed)
            self.assertEqual(mods.get("M"), 0, "Expected offset 0 when in-memory resize succeeds")
            self.assertTrue(parse_ok)

    def test_repack_falls_back_to_neutralization_when_resize_fails(self):
        streams = self._build_streams()

        class FakeStream:
            def __init__(self, data: bytes):
                self._data = data
            def read(self):
                return self._data

        class FakeOle:
            def __init__(self, _src, write_mode=False):
                # shallow copy per instance
                self.streams = dict(streams)
            def openstream(self, pathlist):
                return FakeStream(self.streams[tuple(pathlist)])
            def write_stream(self, pathlist, payload: bytes):
                # refuse resized writes to force fallback; allow equal size
                key = tuple(pathlist)
                if len(payload) != len(self.streams[key]):
                    raise ValueError("resized write not allowed")
                self.streams[key] = payload
            def listdir(self):
                return [list(k) for k in self.streams.keys()]
            def exists(self, pathlist):
                return tuple(pathlist) in self.streams
            def close(self):
                pass

        # Ensure pythoncom path is not available
        with patch.dict("sys.modules", {"pythoncom": None}):
            with patch.object(vba_clean, "olefile") as mock_olemod:
                mock_olemod.OleFileIO = FakeOle
                new_bytes, mods, modules, parse_ok, changed = vba_clean.repack_vba_project(b"ignored")
                self.assertTrue(changed)
                self.assertEqual(mods.get("M"), 10, "Expected neutralization offset when resize is refused")
                self.assertTrue(parse_ok)


if __name__ == "__main__":
    unittest.main()
