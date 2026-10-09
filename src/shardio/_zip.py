"""Low-level helpers for extracting payload offsets from stored zip entries."""

from __future__ import annotations

import struct
import zipfile


def compute_data_offset(zf: zipfile.ZipFile, info: zipfile.ZipInfo) -> tuple[int, int]:
    """Return (absolute_byte_offset_of_payload, payload_length) for a ZIP_STORED entry.

    The local file header starts at ``info.header_offset``. The payload begins after
    the 30-byte fixed header plus the filename and extra fields whose lengths are
    stored IN THE LOCAL HEADER (they can differ from the central-directory copy).
    We read the local header directly rather than trusting the central directory
    to account for this asymmetry.
    """
    if info.compress_type != zipfile.ZIP_STORED:
        raise ValueError(f"{info.filename!r} is not ZIP_STORED")

    fp = zf.fp
    if fp is None:
        raise RuntimeError("ZipFile has no open file pointer (is it closed?)")
    fp.seek(info.header_offset)
    fixed = fp.read(30)
    if len(fixed) != 30 or fixed[:4] != b"PK\x03\x04":
        raise ValueError(f"bad local header for {info.filename!r}")
    name_len, extra_len = struct.unpack("<HH", fixed[26:30])
    data_offset = info.header_offset + 30 + name_len + extra_len
    return data_offset, info.file_size
