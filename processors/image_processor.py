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
        img_contour = image_array.copy()
        img_bounds = image_array.copy()
        
        blurred = cv2.GaussianBlur(img, (5, 5), 0)
        gray = cv2.cvtColor(blurred, cv2.COLOR_BGR2GRAY)

        edges = cv2.Canny(gray, 100, 140)
        kernel = np.ones((5, 5), np.uint8)
        dilated = cv2.dilate(edges, kernel, iterations=1)

        contours, _ = cv2.findContours(dilated, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_NONE)

        width, height = 630, 880
        img_straight = None
        for c in contours:
            area = cv2.contourArea(c)
            areaMin = 5000
            epsilon = 0.02 * cv2.arcLength(c, True)
            approx = cv2.approxPolyDP(c, epsilon, True)

            if area <= areaMin or len(approx) != 4:
                continue

            cv2.drawContours(img_contour, c, -1, (0, 0, 255), 7)
            x, y, w, h = cv2.boundingRect(approx)
            cv2.rectangle(img_bounds, (x, y), (x + w, y + h), (255, 0, 0), 5)
            
            points = []
            for point in approx:
                x, y = point[0]
                points.append((x, y))

            def order_points(pts):
                pts = np.array(pts, dtype="float32")

                s = pts.sum(axis=1)
                diff = np.diff(pts, axis=1)

                top_left = pts[np.argmin(s)]
                bottom_right = pts[np.argmax(s)]
                top_right = pts[np.argmin(diff)]
                bottom_left = pts[np.argmax(diff)]

                return np.array([top_left, top_right, bottom_right, bottom_left], dtype="float32")

            card = order_points(points)
            warp = np.array([[0, 0], [width - 1, 0], [width - 1, height - 1], [0, height - 1]], dtype="float32")

            matrix = cv2.getPerspectiveTransform(card, warp)
            img_straight = cv2.warpPerspective(image_array, matrix, (width, height))

            cv2.imwrite('output_warped.png', img_straight)

        return [image_array, gray, edges, dilated, img_contour, img_bounds], img_straight
    
    def crop_image(self, img, is_dfc=False):
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