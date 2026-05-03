import json

from config import Config
from card_loader import CardLoader
from image_processor import ImageProcessor
from rapidocr_processor import RapidOCRProcessor
from card_classifier import CardClassifier, LEV_THRESHOLD
from PIL import Image

class OCRPipeline:
    def __init__(self, config: Config):
        self.config = config
        self.card_loader = CardLoader(self.config)
        self.image_processor = ImageProcessor()
        self.ocr = RapidOCRProcessor()
        self.classifier = CardClassifier(self.card_loader.load_all_names())

    def process_card(self, card, save_image=False):
        try:
            label = card["name"] + (f" [{card['face']}]" if card["face"] else "")

            print(f"Processing: {label} {'[DFC]' if card["is_dfc"] else ''}")
            print(f"  URL:        {card["url"]}")

            img = self.image_processor.fetch_and_crop(card["url"], card["is_dfc"])

            if save_image:
                safe_name = label.replace(" ", "_").replace("/", "-")[:60]
                Image.fromarray(img).save(f"namebar_{safe_name}.jpg")

            raw, clean = self.ocr.extract_text(img)

            if not raw:
                print(f"  ⚠️  OCR returned empty — skipping Levenshtein")
                best_match, dist, confident = None, None, False
                correct = False
            else:
                best_match, dist, confident = self.classifier.classify(clean)

                correct = best_match == card["name"] or (
                    card["face"] and best_match == card["name"].split(" // ")[0]
                )

                print(f"  DB name:    {card['name']}")
                print(f"  OCR raw:    {raw!r}")
                print(f"  OCR clean:  {clean!r}")
                print(f"  Best match: {best_match}  (distance={dist}, confident={confident})")
                print(f"  Correct:    {'✅' if correct else '❌'}")

            return {
                "id": card["id"],
                "name": card["name"],
                "face": card["face"],
                "ocr_raw": raw,
                "ocr_clean": clean,
                "best_match": best_match,
                "edit_dist": dist,
                "confident": confident,
                "correct": correct,
                "is_dfc": card["is_dfc"],
                "url": card["url"],
            }

        except Exception as e:
            print(f"  ❌ Error: {e}")
            return {
                "id": card["id"],
                "name": card["name"],
                "face": card["face"],
                "ocr_raw": None,
                "ocr_clean": None,
                "best_match": None,
                "edit_dist": None,
                "confident": False,
                "correct": False,
                "is_dfc": card["is_dfc"],
                "url": card["url"],
            }

    def run(self, limit=100, save_images=False):
        cards = self.card_loader.fetch_cards(limit)
        results = []

        for card in cards:
            results.append(self.process_card(card, save_images))

        self._summarize(results)
        self._save(results)

        return results

    def _summarize(self, results):
        total = len(results)
        correct = sum(1 for r in results if r.get("correct"))
        confident = sum(1 for r in results if r.get("confident"))
        empty = sum(1 for r in results if r.get("ocr_raw") == "")
        dfc_empty = sum(1 for r in results if r.get("ocr_raw") == "" and r.get("is_dfc"))

        print(f"✅ Correct:      {correct}/{total}")
        print(f"🎯 Confident:    {confident}/{total}  (edit distance ≤ {LEV_THRESHOLD})")
        print(f"⚠️  Empty OCR:   {empty}/{total}  ({dfc_empty} were DFC)")
        print(f"❌ Wrong:        {total - correct}/{total}")

    def _save(self, results):
        with open("ocr_results.json", "w") as f:
            json.dump(results, f, indent=2)
            print("\nResults saved to ocr_results.json")


# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    config = Config()
    pipeline = OCRPipeline(config)
    pipeline.run(limit=100)