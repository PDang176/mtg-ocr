import cv2
import os
import sys
import numpy as np
import threading
import time
import math
import queue

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from processors.image_processor import ImageProcessor
from processors.rapidocr_processor import RapidOCRProcessor

from config import Config
from database.card_loader import CardLoader
from card_classifier import CardClassifier, LEV_THRESHOLD
from PIL import Image

import cv2

OCR_INTERVAL = 1.0  # seconds
card_slots = [
    {"box": None, "x": 0, "y": 0, "label": None, "last_ocr": 0},
    {"box": None, "x": 0, "y": 0, "label": None, "last_ocr": 0}
]

def make_grid(images, scale=0.8, cols=None):
    if len(images) == 0:
        return None

    resized = []
    for img in images:
        if len(img.shape) == 2:
            img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)

        h, w = img.shape[:2]
        new_size = (int(w * scale), int(h * scale))
        resized.append(cv2.resize(img, new_size))

    n = len(resized)
    if cols is None:
        cols = int(math.ceil(math.sqrt(n)))
    rows = int(math.ceil(n / cols))

    # 3. find max width/height (for padding)
    max_h = max(img.shape[0] for img in resized)
    max_w = max(img.shape[1] for img in resized)

    # 4. pad images to same size
    padded = []
    for img in resized:
        h, w = img.shape[:2]

        pad_bottom = max_h - h
        pad_right = max_w - w

        padded_img = cv2.copyMakeBorder(
            img,
            0, pad_bottom,
            0, pad_right,
            cv2.BORDER_CONSTANT,
            value=(0, 0, 0)  # black padding
        )
        padded.append(padded_img)

    # 5. fill grid
    blank = np.zeros((max_h, max_w, 3), dtype=np.uint8)
    grid_rows = []

    for r in range(rows):
        row_imgs = []
        for c in range(cols):
            idx = r * cols + c
            if idx < n:
                row_imgs.append(padded[idx])
            else:
                row_imgs.append(blank)
        grid_rows.append(np.hstack(row_imgs))

    # 6. stack rows
    grid = np.vstack(grid_rows)

    return grid

def assign_to_slots(cards):
    if len(cards) == 0:
        return

    for i in range(2):
        if i < len(cards):
            box, x, y = cards[i]
            card_slots[i]["box"] = box
            card_slots[i]["x"] = x
            card_slots[i]["y"] = y
        else:
            card_slots[i]["box"] = None

def run_ocr(slot, box):
    global processor, ocr, classifier

    pillow_img = Image.fromarray(cv2.cvtColor(box, cv2.COLOR_BGR2RGB))
    cropped_name = processor.crop_image(pillow_img)

    raw, clean = ocr.extract_text(cropped_name)

    if raw:
        best_match, dist, confident = classifier.classify(clean)
        slot["label"] = best_match if confident else "Uncertain"

if __name__ == "__main__":
    processor = ImageProcessor()
    ocr = RapidOCRProcessor()

    config = Config()
    card_loader = CardLoader(config)
    classifier = CardClassifier(card_loader.load_all_names())

    cap = cv2.VideoCapture(1)
    window_name = "Webcam"

    if not cap.isOpened():
        print("Error: Could not open webcam.")
        exit()

    while True:
        ret, frame = cap.read()

        if not ret:
            print("Error: Could not read frame from webcam.")
            break
        
        images, cards = processor.edge_detection(frame)
        now = time.time()
        
        assign_to_slots(cards)

        for slot in card_slots:

            box = slot["box"]
            if box is None:
                continue

            if now - slot["last_ocr"] > OCR_INTERVAL:
                slot["last_ocr"] = now
                run_ocr(slot, box)

        for slot in card_slots:

            box = slot["box"]
            x = slot["x"]
            y = slot["y"]
            label = slot["label"]

            if box is None or label is None:
                continue

            cv2.putText(
                images[7],
                label,
                (x, y),
                cv2.FONT_HERSHEY_COMPLEX,
                1.2,
                (255, 0, 0),
                2
            )

            cv2.imwrite("labeled.png", images[7])

        cv2.imshow(window_name, make_grid(images))

        if cv2.waitKey(1) == ord('q'):
            break
    
    cap.release()
    cv2.destroyAllWindows()