import cv2
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from processors.image_processor import ImageProcessor

if __name__ == "__main__":
    processor = ImageProcessor()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    images_folder = os.path.join(current_dir, '..', 'images',)

    image_path = os.path.join(images_folder, 'three_cards.jpg')
    image = cv2.imread(image_path)

    if image is not None:
        pictures = processor.edge_detection(image)

        for idx, picture in enumerate(pictures):
            output_path = os.path.join(images_folder, f'output_{idx}.jpg')
            cv2.imwrite(output_path, picture)
            print(f"Saved edge-detected image to: {output_path}")

    else:
        print("Error: Could not read the image.")