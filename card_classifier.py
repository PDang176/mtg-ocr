import re

from rapidfuzz.distance import Levenshtein
from config import Config

LEV_THRESHOLD = 5

class CardClassifier:
    def __init__(self, config: Config):
        self.config = config

    def classify(self, cleaned_text: str, names: list[str]):
        if not cleaned_text:
            return None, None, False

        best_name = None
        best_dist = float("inf")

        for name in names:
            candidate = name.lower().split(" // ")[0]
            candidate = re.sub(r"[^a-zA-Z0-9 \-']", "", candidate)

            dist = Levenshtein.distance(cleaned_text, candidate)

            if dist < best_dist:
                best_dist = dist
                best_name = name

        confidence = best_dist <= self.config.LEV_THRESHOLD
        return best_name, best_dist, confidence