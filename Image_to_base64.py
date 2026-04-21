#!/usr/bin/env python3
"""
Image to Base64 Converter
-------------------------
Compresses images to under 1 MB (preserving original resolution) and
encodes them to base64. Accepts either a single image file or a folder
containing multiple images. Each result is written to its own .txt file.

Usage:
    python image_to_base64.py <input_path> -o <output_dir>

Examples:
    python image_to_base64.py ./photo.png -o ./out
    python image_to_base64.py ./images/ -o ./out
"""

import argparse
import base64
import io
import sys
from pathlib import Path

from PIL import Image
from tqdm import tqdm

SUPPORTED_EXTS = {".jpg", ".jpeg", ".png"}
MAX_SIZE_BYTES = 1 * 1024 * 1024  # 1 MB


def compress_image(image_path: Path, max_bytes: int = MAX_SIZE_BYTES) -> bytes:
    """
    Compress an image so its byte size is <= max_bytes WITHOUT changing
    its resolution. Returns the compressed image bytes.

    Strategy:
      1. If the source file is already small enough, return it unchanged.
      2. For PNG: try lossless PNG optimization first.
      3. If still too large, fall back to JPEG with descending quality.
    """
    original_bytes = image_path.read_bytes()
    if len(original_bytes) <= max_bytes:
        return original_bytes

    img = Image.open(image_path)
    original_format = (img.format or "").upper()

    # Step 1: try lossless PNG optimization (keeps format & resolution)
    if original_format == "PNG":
        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        if buf.tell() <= max_bytes:
            return buf.getvalue()

        # PNG still too large — flatten transparency for JPEG fallback
        if img.mode in ("RGBA", "LA", "P"):
            background = Image.new("RGB", img.size, (255, 255, 255))
            if img.mode == "P":
                img = img.convert("RGBA")
            mask = img.split()[-1] if img.mode in ("RGBA", "LA") else None
            background.paste(img, mask=mask)
            img = background
        else:
            img = img.convert("RGB")
    else:
        # JPEG images with non-RGB modes need conversion
        if img.mode != "RGB":
            img = img.convert("RGB")

    # Step 2: iteratively reduce JPEG quality until size fits
    last_buf = None
    for quality in range(95, 4, -5):
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality, optimize=True)
        last_buf = buf
        if buf.tell() <= max_bytes:
            return buf.getvalue()

    # Could not hit target; return the smallest we produced
    return last_buf.getvalue() if last_buf else original_bytes


def encode_to_base64(image_bytes: bytes) -> str:
    """Return a UTF-8 base64 string for the given bytes."""
    return base64.b64encode(image_bytes).decode("utf-8")


def process_file(image_path: Path, output_dir: Path) -> int:
    """Compress + base64-encode one image. Returns compressed byte size."""
    compressed = compress_image(image_path)
    b64_str = encode_to_base64(compressed)
    output_file = output_dir / f"{image_path.stem}.txt"
    output_file.write_text(b64_str)
    return len(compressed)


def collect_images(input_path: Path) -> list:
    """Return a sorted list of image paths to process."""
    if input_path.is_file():
        if input_path.suffix.lower() not in SUPPORTED_EXTS:
            raise ValueError(
                f"Unsupported file type '{input_path.suffix}'. "
                f"Supported: {', '.join(sorted(SUPPORTED_EXTS))}"
            )
        return [input_path]

    if input_path.is_dir():
        images = sorted(
            p for p in input_path.iterdir()
            if p.is_file() and p.suffix.lower() in SUPPORTED_EXTS
        )
        if not images:
            raise ValueError(f"No supported images found in '{input_path}'.")
        return images

    raise FileNotFoundError(f"Path does not exist: {input_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Convert .jpg/.jpeg/.png images to base64. Each image is "
            "compressed to under 1 MB while preserving its original "
            "resolution, and the base64 result is saved to its own .txt file."
        )
    )
    parser.add_argument(
        "input", type=Path,
        help="Path to a single image file OR a folder containing images."
    )
    parser.add_argument(
        "-o", "--output", type=Path, required=True,
        help="Output directory where the base64 .txt files will be saved."
    )
    args = parser.parse_args()

    try:
        images = collect_images(args.input)
    except (ValueError, FileNotFoundError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    args.output.mkdir(parents=True, exist_ok=True)

    ok, failed = 0, 0
    for image_path in tqdm(images, desc="Encoding images", unit="img"):
        try:
            size = process_file(image_path, args.output)
            tqdm.write(
                f"  {image_path.name}  ->  "
                f"{image_path.stem}.txt  ({size / 1024:.1f} KB compressed)"
            )
            ok += 1
        except Exception as exc:
            tqdm.write(f"  FAILED: {image_path.name} ({exc})")
            failed += 1

    print(f"\nDone. {ok} succeeded, {failed} failed. Output dir: {args.output}")


if __name__ == "__main__":
    main()