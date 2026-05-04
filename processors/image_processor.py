import requests
import numpy as np

from PIL import Image
from io import BytesIO

# ── Tuning knobs (single-face cards) ─────────────────────────────────────────
CROP_TOP_PERCENT = 0.10
CROP_LEFT_PX = 10
CROP_RIGHT_PERCENT = 0.75
GRAYSCALE = False
# ─────────────────────────────────────────────────────────────────────────────

# ── Tuning knobs (double-faced cards) ────────────────────────────────────────
DFC_CROP_TOP_PERCENT = 0.10
DFC_CROP_LEFT_PX = 80
DFC_CROP_RIGHT_PERCENT = 0.70
# ─────────────────────────────────────────────────────────────────────────────

class ImageProcessor:
    def fetch_and_crop(self, url, is_dfc=False):
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        img = Image.open(BytesIO(response.content)).convert("RGB")
        width, height = img.size

        if is_dfc:
            left = DFC_CROP_LEFT_PX
            right = int(width * DFC_CROP_RIGHT_PERCENT)
            bottom = int(height * DFC_CROP_TOP_PERCENT)
        else:
            left = CROP_LEFT_PX
            right = int(width * CROP_RIGHT_PERCENT)
            bottom = int(height * CROP_TOP_PERCENT)

        cropped_name = img.crop((left, 0, right, bottom))

        if GRAYSCALE:
            cropped_name = cropped_name.convert("L")

        return np.array(cropped_name)