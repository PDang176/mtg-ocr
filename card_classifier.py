import re

from rapidfuzz.distance import Levenshtein

LEV_THRESHOLD = 5

class CardClassifier:
    def __init__(self, names: list[str]):
        self.names = names

    def classify(self, cleaned_text: str):
        if not cleaned_text:
            return None, None, False

        best_name = None
        best_dist = float("inf")

        for name in self.names:
            candidate = name.lower().split(" // ")[0]
            candidate = re.sub(r"[^a-zA-Z0-9 \-']", "", candidate)

            dist = Levenshtein.distance(cleaned_text, candidate)

            if dist < best_dist:
                best_dist = dist
                best_name = name

        confident = best_dist <= LEV_THRESHOLD
        return best_name, best_dist, confident