#!/usr/bin/env python3
"""Generate incompressible PNG images of an exact byte size.

Pixels are random RGB noise stored without compression, so every image is
unique and its size on disk matches what GHES stores in object storage. A
padding text chunk brings the file to the exact requested size.
"""

import argparse
import math
import random
import struct
import sys
import zlib
from pathlib import Path

PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
MIN_IMAGE_BYTES = 1024
# GHES rejects image attachments larger than 10 MB.
MAX_IMAGE_BYTES = 10 * 1024 * 1024
CHUNK_OVERHEAD = 12
STORED_BLOCK_MAX = 65535
PAD_KEYWORD = b"Comment\x00"


def _chunk(chunk_type, data):
    return (
        struct.pack(">I", len(data))
        + chunk_type
        + data
        + struct.pack(">I", zlib.crc32(chunk_type + data) & 0xFFFFFFFF)
    )


def _stored_zlib_size(raw_len):
    blocks = max(1, math.ceil(raw_len / STORED_BLOCK_MAX))
    return 2 + raw_len + 5 * blocks + 4


def _stored_zlib(raw):
    # Uncompressed deflate blocks keep the size predictable and the noise intact.
    out = bytearray(b"\x78\x01")
    offsets = range(0, max(1, len(raw)), STORED_BLOCK_MAX)
    for offset in offsets:
        block = raw[offset:offset + STORED_BLOCK_MAX]
        final = 1 if offset + STORED_BLOCK_MAX >= len(raw) else 0
        out += struct.pack("<BHH", final, len(block), len(block) ^ 0xFFFF)
        out += block
    out += struct.pack(">I", zlib.adler32(raw) & 0xFFFFFFFF)
    return bytes(out)


def _png_size(width, height):
    raw_len = height * (1 + width * 3)
    return len(PNG_SIGNATURE) + (CHUNK_OVERHEAD + 13) + (CHUNK_OVERHEAD + _stored_zlib_size(raw_len)) + CHUNK_OVERHEAD


def _dimensions_for(target_bytes):
    # Leave room for the padding chunk header and keyword.
    budget = target_bytes - (CHUNK_OVERHEAD + len(PAD_KEYWORD))
    width = max(1, int(math.sqrt(max(1, budget) / 3)))
    height = 1
    while _png_size(width, height + 1) <= budget:
        height += 1
    while width > 1 and _png_size(width, height) > budget:
        width -= 1
    return width, height


def generate_png(target_bytes=None, width=None, height=None, seed=None):
    """Return PNG bytes.

    With target_bytes, the image is sized and padded to exactly that many
    bytes. With width and height, the image has those dimensions and no
    padding.
    """
    rng = random.Random(seed)
    if target_bytes is not None:
        if not MIN_IMAGE_BYTES <= target_bytes <= MAX_IMAGE_BYTES:
            raise ValueError(f"target size must be between {MIN_IMAGE_BYTES} and {MAX_IMAGE_BYTES} bytes")
        width, height = _dimensions_for(target_bytes)
    elif not (width and height and width > 0 and height > 0):
        raise ValueError("provide target_bytes or positive width and height")

    row_len = width * 3
    rows = bytearray()
    for _ in range(height):
        rows.append(0)
        rows += rng.randbytes(row_len)

    out = bytearray(PNG_SIGNATURE)
    out += _chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
    out += _chunk(b"IDAT", _stored_zlib(bytes(rows)))

    if target_bytes is not None:
        pad_len = target_bytes - len(out) - CHUNK_OVERHEAD - CHUNK_OVERHEAD - len(PAD_KEYWORD)
        if pad_len < 0:
            raise ValueError("internal sizing error: image exceeds target")
        out += _chunk(b"tEXt", PAD_KEYWORD + _random_text(rng, pad_len))

    out += _chunk(b"IEND", b"")
    if target_bytes is not None and len(out) != target_bytes:
        raise ValueError(f"internal sizing error: produced {len(out)} bytes, wanted {target_bytes}")
    return bytes(out)


_TEXT_TABLE = bytes(0x41 + (b % 26) for b in range(256))


def _random_text(rng, length):
    return rng.randbytes(length).translate(_TEXT_TABLE)


def parse_size_kb(value):
    """Parse "N" or "MIN-MAX" (KB) into a (min_bytes, max_bytes) tuple."""
    lo, sep, hi = value.partition("-")
    try:
        lo_kb = int(lo)
        hi_kb = int(hi) if sep else lo_kb
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid size '{value}', expected N or MIN-MAX in KB")
    if lo_kb > hi_kb:
        raise argparse.ArgumentTypeError(f"invalid size '{value}', MIN is greater than MAX")
    lo_b, hi_b = lo_kb * 1024, hi_kb * 1024
    if lo_b < MIN_IMAGE_BYTES or hi_b > MAX_IMAGE_BYTES:
        raise argparse.ArgumentTypeError(
            f"size must be between {MIN_IMAGE_BYTES // 1024} and {MAX_IMAGE_BYTES // 1024} KB"
        )
    return lo_b, hi_b


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("output", type=Path, help="output PNG path")
    size = parser.add_mutually_exclusive_group(required=True)
    size.add_argument("-s", "--size-kb", type=parse_size_kb, help="exact size in KB, or MIN-MAX for a random size (1-10240)")
    size.add_argument("-d", "--dimensions", help="WIDTHxHEIGHT in pixels, unpadded")
    parser.add_argument("--seed", help="seed for repeatable output (default: random)")
    args = parser.parse_args()

    try:
        if args.size_kb:
            rng = random.Random(args.seed)
            target = rng.randint(*args.size_kb)
            data = generate_png(target_bytes=target, seed=args.seed)
        else:
            w, _, h = args.dimensions.lower().partition("x")
            data = generate_png(width=int(w), height=int(h), seed=args.seed)
    except ValueError as exc:
        parser.error(str(exc))

    if len(data) > MAX_IMAGE_BYTES:
        print(f"WARN: {len(data)} bytes exceeds the 10 MB GHES image attachment limit", file=sys.stderr)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(data)
    print(f"{args.output}: {len(data)} bytes")


if __name__ == "__main__":
    main()
