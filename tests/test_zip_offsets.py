import zipfile
from pathlib import Path

import pytest

from shardio._zip import compute_data_offset


def test_compute_data_offset_round_trip(tmp_path: Path) -> None:
    zip_path = tmp_path / "test.zip"
    payload = b"hello world" * 100  # 1100 bytes
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED, allowZip64=True) as zf:
        zf.writestr("entry.txt", payload)
        info = zf.getinfo("entry.txt")
        offset, length = compute_data_offset(zf, info)

    assert length == len(payload)
    with open(zip_path, "rb") as f:
        f.seek(offset)
        assert f.read(length) == payload


def test_compute_data_offset_with_large_filename(tmp_path: Path) -> None:
    zip_path = tmp_path / "test.zip"
    long_name = "x" * 200 + ".bin"
    payload = b"Z" * 4096
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED, allowZip64=True) as zf:
        zf.writestr(long_name, payload)
        info = zf.getinfo(long_name)
        offset, length = compute_data_offset(zf, info)

    with open(zip_path, "rb") as f:
        f.seek(offset)
        assert f.read(length) == payload


def test_compute_data_offset_rejects_deflated_entry(tmp_path: Path) -> None:
    zip_path = tmp_path / "test.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        zf.writestr("entry.txt", b"compress me" * 100)
        info = zf.getinfo("entry.txt")
        with pytest.raises(ValueError, match="not ZIP_STORED"):
            compute_data_offset(zf, info)


def test_compute_data_offset_rejects_truncated_local_header(tmp_path: Path) -> None:
    zip_path = tmp_path / "test.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED, allowZip64=True) as zf:
        zf.writestr("entry.txt", b"hello" * 100)

    with zipfile.ZipFile(zip_path, "r") as zf:
        info = zf.getinfo("entry.txt")
        header_offset = info.header_offset

    with open(zip_path, "r+b") as f:
        f.seek(header_offset)
        f.write(b"XXXX")

    with zipfile.ZipFile(zip_path, "r") as zf:
        info = zf.getinfo("entry.txt")
        with pytest.raises(ValueError, match="bad local header"):
            compute_data_offset(zf, info)
