import numpy as np
import cv2

from PIL import Image

# ── Tuning knobs (single-face cards) ─────────────────────────────────────────
CROP_TOP_PERCENT = 0.125
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
    def __order_points(self, pts):
        rect = np.zeros((4, 2), dtype="float32")

        s = pts.sum(axis=1)
        diff = np.diff(pts, axis=1)

        rect[0] = pts[np.argmin(s)]      # TL
        rect[2] = pts[np.argmax(s)]      # BR
        rect[1] = pts[np.argmin(diff)]   # TR
        rect[3] = pts[np.argmax(diff)]   # BL

        return rect

    def __fix_warp(self, image, cards, width=630, height=880):
        warped_cards = []

        for card, x, y in cards:
            rect = self.__order_points(card)
            (tl, tr, br, bl) = rect

            bottom_width = np.sqrt(((br[0] - bl[0]) ** 2) + ((br[1] - bl[1]) ** 2))
            top_width = np.sqrt( ((tr[0] - tl[0]) ** 2) + ((tr[1] - tl[1]) ** 2))
            max_width = max(int(bottom_width), int(top_width))

            right_height = np.sqrt(((tr[0] - br[0]) ** 2) + ((tr[1] - br[1]) **2)) 
            left_height = np.sqrt(((tl[0] - bl[0]) ** 2) + ((tl[1] - bl[1]) **2))
            max_height = max(int(right_height), int(left_height)) 

            dest = np.array([
                [0, 0],
                [max_width - 1, 0],
                [max_width - 1, max_height - 1],
                [0, max_height - 1]
            ], dtype="float32")

            matrix = cv2.getPerspectiveTransform(rect, dest)
            warped_card = cv2.warpPerspective(image, matrix, (max_width, max_height))

            if max_width > max_height:
                center = (max_height / 2, max_height / 2)
                rotate = cv2.getRotationMatrix2D(center, 270, 1.0)
                warped_card = cv2. warpAffine(warped_card, rotate, (max_height, max_width))

            warped_cards.append((warped_card, x, y))

        return warped_cards

    def edge_detection(self, image_array):
        img = image_array.copy()
        img_contours = image_array.copy()
        img_bounds = image_array.copy()
        
        # Normalize Lighting (CLAHE)
        lab = cv2.cvtColor(img, cv2.COLOR_BGR2LAB)
        l, a, b = cv2.split(lab)

        clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8))
        l = clahe.apply(l)

        lab = cv2.merge((l, a, b))
        img_normalized = cv2.cvtColor(lab, cv2.COLOR_LAB2BGR)

        # Gaussian Blur
        img_blurred = cv2.GaussianBlur(img_normalized, (5, 5), 0)

        # Grayscale
        img_gray = cv2.cvtColor(img_blurred, cv2.COLOR_BGR2GRAY)
        
        # Canny Edge Detection
        img_edges = cv2.Canny(img_gray, 40, 120)
        kernel = np.ones((5, 5), np.uint8)
        dilated = cv2.dilate(img_edges, kernel, iterations=1)
        img_eroded = cv2.erode(dilated, kernel, iterations=1)

        # Find all contours
        contours, _ = cv2.findContours(img_eroded, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        if len(contours) == 0:
            return ([img, img_normalized, img_blurred, img_gray, img_edges, img_eroded, img_contours, img_bounds], [])

        cards = []
        minArea = 5000

        # Filter for card shape
        for c in contours:
            area = cv2.contourArea(c)
            if area < minArea:
                continue
            
            rect = cv2.minAreaRect(c)
            box = cv2.boxPoints(rect)
            box = np.int32(box)

            if len(box) != 4:
                continue

            (w, h) = rect[1]

            if w == 0 or h == 0:
                continue

            aspect_ratio = min(w, h) / max(w, h)
            if not (0.5 < aspect_ratio < 0.85):
                continue

            hull = cv2.convexHull(c)
            hull_area = cv2.contourArea(hull)
            if hull_area == 0:
                continue

            solidity = area / hull_area
            if solidity < 0.9:
                continue

            cv2.drawContours(img_contours, [box], -1, (0, 0, 255), 6)
            x, y, w, h = cv2.boundingRect(box)
            cv2.rectangle(img_bounds, (x, y), (x + w, y + h), (255, 0, 0), 3)
            
            cards.append((box, x, y))

        # Straighten Cards
        cards = self.__fix_warp(img, cards)

        return ([img, img_normalized, img_blurred, img_gray, img_edges, img_eroded, img_contours, img_bounds], cards)
    
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