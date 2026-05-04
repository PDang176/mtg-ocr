import requests
import numpy as np
import cv2

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
    def edge_detection(self, image_array):    
        img = image_array.copy()

        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        blurred = cv2.GaussianBlur(gray, (9, 9), 0)
        edges = cv2.Canny(blurred, 25, 120)

        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (7, 7))
        edges = cv2.dilate(edges, kernel, iterations=2)
        edges = cv2.erode(edges, kernel, iterations=1)

        contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        for c in contours:
            area = cv2.contourArea(c)

            # filter noise
            if area < 3000:
                continue

            rect = cv2.minAreaRect(c)
            (cx, cy), (w, h), angle = rect

            # optional: filter non-card shapes
            if w == 0 or h == 0:
                continue

            aspect = min(w, h) / max(w, h)

            # MTG cards ≈ tall rectangles (~0.65 aspect)
            if aspect < 0.3:
                continue

            box = cv2.boxPoints(rect)
            box = np.int32(box)

            cv2.drawContours(img, [box], 0, (0, 0, 255), 2)

        return img, edges
    
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