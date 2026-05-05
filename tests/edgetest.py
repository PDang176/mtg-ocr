import cv2
import os
import sys
import numpy as np
import threading
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from processors.image_processor import ImageProcessor
from processors.rapidocr_processor import RapidOCRProcessor

from config import Config
from database.card_loader import CardLoader
from card_classifier import CardClassifier, LEV_THRESHOLD
from PIL import Image

import cv2

card_names = []
lock = threading.Lock()
last_ocr_time = 0
ocr_done = threading.Event()
OCR_INTERVAL = 1.0  # seconds

def make_grid(images, rows, cols, scale=0.8):
    processed = []

    for img in images:
        # Handle grayscale → BGR
        if len(img.shape) == 2:
            img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)

        # Scale while preserving aspect ratio
        img_resized = cv2.resize(img, None, fx=scale, fy=scale)
        processed.append(img_resized)

    # IMPORTANT: all images still need same size for concat
    # so we normalize them AFTER scaling
    h = min(img.shape[0] for img in processed)
    w = min(img.shape[1] for img in processed)

    processed = [cv2.resize(img, (w, h)) for img in processed]

    # Build grid
    grid_rows = []
    for r in range(rows):
        row = cv2.hconcat(processed[r*cols:(r+1)*cols])
        grid_rows.append(row)

    return cv2.vconcat(grid_rows)

def run_ocr(image, x, y):
    global card_names

    pillow_img = Image.fromarray(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
    cropped_name = processor.crop_image(pillow_img)

    # h, w = cropped_name.shape[:2]
    # images[5][50:50+h, 50:50+w] = cropped_name
    raw, clean = ocr.extract_text(cropped_name)

    if raw:
        best_match, dist, confident = classifier.classify(clean)
        
        with lock:
            card_names.append((f"{best_match} (Distance: {dist})" if confident else "Uncertain", x, y))

if __name__ == "__main__":
    processor = ImageProcessor()
    ocr = RapidOCRProcessor()

    config = Config()
    card_loader = CardLoader(config)
    classifier = CardClassifier(card_loader.load_all_names())

    # current_dir = os.path.dirname(os.path.abspath(__file__))
    # images_folder = os.path.join(current_dir, '..', 'images',)

    # image_path = os.path.join(images_folder, 'three_cards.jpg')
    # image = cv2.imread(image_path)

    # if image is not None:
    #     pictures = processor.edge_detection(image)

    #     for idx, picture in enumerate(pictures):
    #         output_path = os.path.join(images_folder, f'output_{idx}.jpg')
    #         cv2.imwrite(output_path, picture)
    #         print(f"Saved edge-detected image to: {output_path}")

    # else:
    #     print("Error: Could not read the image.")

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
        
        images, straight = processor.edge_detection(frame)
        
        now = time.time()

        if straight is not None and now - last_ocr_time > OCR_INTERVAL:
            last_ocr_time = now
            for crop, x, y in straight:
                threading.Thread(target=run_ocr, args=(crop, x, y), daemon=True).start()

        with lock:
            labels = card_names.copy()
            card_names.clear()

        for label, x, y in labels:
            cv2.putText(
                images[5], 
                label, 
                (x, y), 
                cv2.FONT_HERSHEY_COMPLEX, 
                1.2, 
                (120, 0, 0),
                2
            )

        cv2.imshow(window_name, make_grid(images, 2, 3, 0.8))

        if cv2.waitKey(1) == ord('q'):
            break
    
    cap.release()
    cv2.destroyAllWindows()