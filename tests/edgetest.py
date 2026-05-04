import cv2
import os
import sys
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from processors.image_processor import ImageProcessor
from processors.rapidocr_processor import RapidOCRProcessor

from config import Config
from database.card_loader import CardLoader
from card_classifier import CardClassifier, LEV_THRESHOLD
from PIL import Image

import cv2

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

    cap = cv2.VideoCapture(0)
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
        
        if straight is not None:
            pillow_straight = Image.fromarray(cv2.cvtColor(straight, cv2.COLOR_BGR2RGB))
            cropped_name = processor.crop_image(pillow_straight)

            h, w = cropped_name.shape[:2]
            images[5][50:50+h, 50:50+w] = cropped_name
            # raw, clean = ocr.extract_text(cropped_name)

            # if raw:
            #     best_match, dist, confident = classifier.classify(clean)
            #     cv2.putText(
            #         images[5], 
            #         (f"{best_match} ({dist})" if confident else "Uncertain"), 
            #         (10, 30), 
            #         cv2.FONT_HERSHEY_SIMPLEX, 
            #         0.7, 
            #         (0, 255, 0) if confident else (0, 0, 255),
            #         2
            #     )

        cv2.imshow(window_name, make_grid(images, 2, 3, 0.8))

        if cv2.waitKey(1) == ord('q'):
            break
    
    cap.release()
    cv2.destroyAllWindows()