import cv2
import os
import sys
import numpy as np
import threading
import time
import math

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from processors.image_processor import ImageProcessor
from processors.rapidocr_processor import RapidOCRProcessor

from config import Config
from database.card_loader import CardLoader
from card_classifier import CardClassifier, LEV_THRESHOLD
from PIL import Image

import cv2

card_names = {}
last_ocr_time = {}
lock = threading.Lock()
OCR_INTERVAL = 1.0  # seconds

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


def run_ocr(image, card_id):
    pillow_img = Image.fromarray(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
    cropped_name = processor.crop_image(pillow_img)

    pillow_img.save('output_warped.png')
    cv2.imwrite('output_cropped.png', cropped_name)

    raw, clean = ocr.extract_text(cropped_name)

    if raw:
        best_match, dist, confident = classifier.classify(clean)
        
        label = f"{best_match} (Distance: {dist})" if confident else "Uncertain"
        

        with lock:
            card_names[card_id] = label

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

        for card in cards:
            box, x, y = card
            card_id = f"{x//25}_{y//25}"
            last_time = last_ocr_time.get(card_id, 0)

            if now - last_time > OCR_INTERVAL:
                last_ocr_time[card_id] = now

                threading.Thread(target=run_ocr, args=(box, card_id), daemon=True).start()
        
        with lock:
            labels = card_names.copy() 

        for card in cards:
            box, x, y = card
            card_id = f"{x//25}_{y//25}"
            label = labels.get(card_id)
            # print(label)

            if label is None:
                continue
            
            cv2.putText(
                images[7], 
                label, 
                (x, y), 
                cv2.FONT_HERSHEY_COMPLEX, 
                1.2, 
                (255, 0, 0),
                5
            )

        cv2.imshow(window_name, make_grid(images))

        if cv2.waitKey(1) == ord('q'):
            break
    
    cap.release()
    cv2.destroyAllWindows()