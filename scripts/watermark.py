"""
The Chronicles - Editorial Image Processing & Watermark Engine
Applies The Chronicles luxury broadsheet brand mark to authentic news photos.
Strictly ensures 16:9 aspect ratio, high resolution, and clean professional branding.
"""

import io
import urllib.request
from PIL import Image, ImageDraw, ImageFont

# Target broadsheet display dimensions
CANONICAL_WIDTH = 1200
CANONICAL_HEIGHT = 675
ASPECT_RATIO = CANONICAL_WIDTH / CANONICAL_HEIGHT


def fetch_image_bytes(image_url: str, timeout: int = 15) -> bytes:
    """Download image binary data from a verified news wire URL."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }
    req = urllib.request.Request(image_url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def crop_to_aspect_ratio(img: Image.Image, target_ratio: float = ASPECT_RATIO) -> Image.Image:
    """Center-crop image to 16:9 broadsheet aspect ratio."""
    width, height = img.size
    current_ratio = width / height

    if abs(current_ratio - target_ratio) < 0.01:
        return img

    if current_ratio > target_ratio:
        # Image is too wide: crop left and right
        new_width = int(height * target_ratio)
        offset = (width - new_width) // 2
        return img.crop((offset, 0, offset + new_width, height))
    else:
        # Image is too tall: crop top and bottom
        new_height = int(width / target_ratio)
        offset = (height - new_height) // 2
        return img.crop((0, offset, width, offset + new_height))


def apply_chronicles_watermark(image_input) -> bytes:
    """
    Takes raw image bytes OR image URL string, crops to 16:9, resizes to 1200x675,
    and composites The Chronicles luxury editorial brand mark badge.
    Returns optimized JPEG bytes ready for Supabase storage.
    """
    if isinstance(image_input, str):
        image_data = fetch_image_bytes(image_input)
    else:
        image_data = image_input

    # 1. Open original image
    raw_img = Image.open(io.BytesIO(image_data))
    if raw_img.mode != "RGB":
        raw_img = raw_img.convert("RGB")

    # 2. Crop to 16:9 and resize to canonical dimensions
    cropped = crop_to_aspect_ratio(raw_img)
    base = cropped.resize((CANONICAL_WIDTH, CANONICAL_HEIGHT), Image.Resampling.LANCZOS)

    # 3. Create an RGBA overlay for smooth transparency
    overlay = Image.new("RGBA", (CANONICAL_WIDTH, CANONICAL_HEIGHT), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)

    # 4. Badge geometry in bottom-right corner
    badge_w = 230
    badge_h = 36
    margin_x = 24
    margin_y = 24
    x1 = CANONICAL_WIDTH - margin_x - badge_w
    y1 = CANONICAL_HEIGHT - margin_y - badge_h
    x2 = x1 + badge_w
    y2 = y1 + badge_h

    # A. Draw dark translucent pill container
    draw.rounded_rectangle(
        [(x1, y1), (x2, y2)],
        radius=4,
        fill=(14, 14, 14, 215),
        outline=(255, 255, 255, 45),
        width=1,
    )

    # B. Draw signature crimson red [CHR] emblem box
    logo_w = 34
    logo_h = 24
    logo_x1 = x1 + 6
    logo_y1 = y1 + 6
    logo_x2 = logo_x1 + logo_w
    logo_y2 = logo_y1 + logo_h
    draw.rounded_rectangle(
        [(logo_x1, logo_y1), (logo_x2, logo_y2)],
        radius=2,
        fill=(229, 9, 20, 255),  # Signature #E50914 Crimson
    )

    # C. Draw typography (Fallback to default if custom fonts not available)
    try:
        font_logo = ImageFont.truetype("arialbd.ttf", 11)
        font_main = ImageFont.truetype("arialbd.ttf", 11)
        font_sub = ImageFont.truetype("arial.ttf", 9)
    except Exception:
        font_logo = ImageFont.load_default()
        font_main = ImageFont.load_default()
        font_sub = ImageFont.load_default()

    # Draw "CHR" text inside the red badge
    draw.text((logo_x1 + 6, logo_y1 + 5), "CHR", font=font_logo, fill=(255, 255, 255, 255))

    # Draw "THE CHRONICLES" title text
    text_x = logo_x2 + 8
    draw.text((text_x, y1 + 6), "THE CHRONICLES", font=font_main, fill=(250, 250, 250, 255))

    # Draw "GLOBAL & NIGERIAN DISPATCH" subtitle
    draw.text((text_x, y1 + 19), "VERIFIED DISPATCH", font=font_sub, fill=(200, 200, 200, 220))

    # 5. Composite overlay over base image
    base_rgba = base.convert("RGBA")
    final_rgba = Image.alpha_composite(base_rgba, overlay)
    final_rgb = final_rgba.convert("RGB")

    # 6. Save as compressed, high-fidelity JPEG
    out_io = io.BytesIO()
    final_rgb.save(out_io, format="JPEG", quality=90, optimize=True)
    return out_io.getvalue()


if __name__ == "__main__":
    # Self-test: download a real news wire photo and watermark it
    sample_url = "https://ichef.bbci.co.uk/ace/standard/1024/cpsprodpb/61df/live/2d4de870-d487-11f1-b441-2b33b063138f.jpg"
    print(f"Testing watermark engine with sample: {sample_url}")
    try:
        raw = fetch_image_bytes(sample_url)
        watermarked = apply_chronicles_watermark(raw)
        with open("scripts/sample_watermarked.jpg", "wb") as f:
            f.write(watermarked)
        print(f"Success! Watermarked image saved to scripts/sample_watermarked.jpg ({len(watermarked)} bytes)")
    except Exception as e:
        print(f"Test error: {e}")
